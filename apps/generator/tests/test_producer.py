import random
from pathlib import Path

import pytest
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from qoe_generator.model import NO_ANOMALIES, Anomalies, EventFactory
from qoe_generator.producer import Config, EventSender, registry_client

SCHEMAS = Path(__file__).resolve().parents[3] / "schemas"


class CapturingProducer:
    def __init__(self):
        self.records: list[tuple[str, bytes, bytes]] = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.records.append((topic, key, value))
        on_delivery(None, None)

    def poll(self, _timeout=0):
        return 0


def send(n: int, anomalies: Anomalies = NO_ANOMALIES) -> tuple[list, CapturingProducer, object]:
    cfg = Config(bootstrap="unused", registry_url="mock://test", anomalies=anomalies)
    registry = registry_client(f"mock://{random.random()}")
    serializer = AvroSerializer(registry, (SCHEMAS / "player_event.v1.avsc").read_text())
    producer = CapturingProducer()
    sender = EventSender(cfg, producer, serializer, random.Random(5))
    factory = EventFactory(random.Random(5), sessions=100)
    sent = [factory.next_event(1_762_400_000_000) for _ in range(n)]
    for e in sent:
        sender.send(e)
    return sent, producer, registry


def decode(registry, value: bytes) -> dict:
    return AvroDeserializer(registry)(
        value, SerializationContext("player-events", MessageField.VALUE)
    )


def test_key_is_session_id_and_value_is_confluent_avro():
    sent, producer, registry = send(200)
    assert len(producer.records) == 200
    for event, (topic, key, value) in zip(sent, producer.records, strict=True):
        assert topic == "player-events"
        assert key == event["session_id"].encode()
        assert value[0] == 0
        assert str(decode(registry, value)["event_id"]) == event["event_id"]


def test_duplicates_are_byte_identical_copies():
    _, producer, _ = send(2_000, Anomalies(duplicate_share=0.1))
    values = [v for _, _, v in producer.records]
    extra = len(values) - len(set(values))
    assert extra / 2_000 == pytest.approx(0.1, abs=0.02)


def test_malformed_records_keep_the_header_and_fail_to_decode():
    _, producer, registry = send(2_000, Anomalies(malformed_share=0.05))
    failures = 0
    for _, _, value in producer.records:
        assert value[0] == 0
        try:
            decode(registry, value)
        except Exception:
            failures += 1
    assert failures / 2_000 == pytest.approx(0.05, abs=0.015)
