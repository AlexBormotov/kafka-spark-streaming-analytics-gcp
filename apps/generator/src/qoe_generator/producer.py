"""Rate-limited Kafka producer for player events in Confluent Avro wire format."""

import json
import logging
import random
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from qoe_generator.model import Anomalies, EventFactory, Injection, RecordKind, record_kind

log = logging.getLogger("qoe_generator")

TICK_S = 0.1
# Keeps the Confluent header (magic byte + schema id) and cuts the Avro body short, so a
# consumer resolves the schema and then fails to decode: the realistic corruption case.
MALFORMED_TAIL = b"\xff\xff\xff"


@dataclass
class Config:
    bootstrap: str
    registry_url: str
    topic: str = "player-events"
    rate: float = 1_000.0
    duration_s: float | None = None
    sessions: int = 1_000
    schema_version: int = 1
    schema_dir: Path = Path("schemas")
    injection: Injection | None = None
    anomalies: Anomalies = field(default_factory=Anomalies)
    seed: int | None = None
    stats_interval_s: float = 10.0
    dry_run: bool = False


@dataclass
class Stats:
    produced: int = 0
    delivered: int = 0
    failed: int = 0

    def on_delivery(self, err: KafkaError | None, _msg: Message) -> None:
        if err is None:
            self.delivered += 1
        else:
            self.failed += 1
            log.warning("delivery failed: %s", err)


class _NullProducer:
    """Stands in for Kafka in --dry-run, to measure generation and serialization alone."""

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if on_delivery:
            on_delivery(None, None)

    def poll(self, _timeout=0):
        return 0

    def flush(self, _timeout=None):
        return 0


def registry_client(url: str) -> SchemaRegistryClient:
    if url.startswith("mock://"):
        return SchemaRegistryClient.new_client({"url": url})
    return SchemaRegistryClient({"url": f"{url.rstrip('/')}/apis/ccompat/v7"})


def kafka_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 20,
            "batch.size": 262_144,
        }
    )


class EventSender:
    """Turns one generated event into one, two (duplicate) or a corrupted Kafka record."""

    def __init__(self, cfg: Config, producer, serializer: Callable, rng: random.Random):
        self._cfg = cfg
        self._producer = producer
        self._serialize = serializer
        self._rng = rng
        self._ctx = SerializationContext(cfg.topic, MessageField.VALUE)
        self.stats = Stats()

    def _produce(self, key: bytes, value: bytes) -> None:
        while True:
            try:
                self._producer.produce(
                    self._cfg.topic, key=key, value=value, on_delivery=self.stats.on_delivery
                )
                self.stats.produced += 1
                return
            except BufferError:
                self._producer.poll(0.05)

    def send(self, event: dict) -> None:
        key = event["session_id"].encode()
        value = self._serialize(event, self._ctx)
        kind = record_kind(self._rng, self._cfg.anomalies)
        if kind is RecordKind.MALFORMED:
            value = value[:5] + MALFORMED_TAIL
        self._produce(key, value)
        if kind is RecordKind.DUPLICATE:
            self._produce(key, value)


def run(cfg: Config) -> Stats:
    rng = random.Random(cfg.seed)
    schema_str = (cfg.schema_dir / f"player_event.v{cfg.schema_version}.avsc").read_text()
    registry = registry_client("mock://dry-run" if cfg.dry_run else cfg.registry_url)
    serializer = AvroSerializer(registry, schema_str)
    producer = _NullProducer() if cfg.dry_run else kafka_producer(cfg.bootstrap)
    factory = EventFactory(
        rng, cfg.sessions, cfg.injection, cfg.anomalies, schema_version=cfg.schema_version
    )
    sender = EventSender(cfg, producer, serializer, rng)

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    started = last_report = time.monotonic()
    reported = 0
    carry = 0.0
    while not stopping:
        tick_start = time.monotonic()
        if cfg.duration_s is not None and tick_start - started >= cfg.duration_s:
            break
        carry += cfg.rate * TICK_S
        n, carry = int(carry), carry - int(carry)
        now_ms = int(time.time() * 1000)
        for _ in range(n):
            sender.send(factory.next_event(now_ms))
        producer.poll(0)

        now = time.monotonic()
        if now - last_report >= cfg.stats_interval_s:
            s = sender.stats
            rate = (s.produced - reported) / (now - last_report)
            log.info(json.dumps({"rate": round(rate), "target": cfg.rate, **vars(s)}))
            last_report, reported = now, s.produced
        time.sleep(max(0.0, TICK_S - (time.monotonic() - tick_start)))

    producer.flush(30)
    elapsed = time.monotonic() - started
    s = sender.stats
    log.info(
        json.dumps(
            {
                "final_rate": round(s.produced / max(elapsed, 1e-9)),
                "elapsed_s": round(elapsed, 1),
                **vars(s),
            }
        )
    )
    return s
