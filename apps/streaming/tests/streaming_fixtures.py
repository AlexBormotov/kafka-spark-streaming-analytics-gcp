"""Fixture records in Confluent Avro framing, and a registry stand-in."""

import io
import json
import time
import uuid
from pathlib import Path

import fastavro

SCHEMAS = Path(__file__).resolve().parents[3] / "schemas"
# Whole second, recent: the raw table has a one-day TTL.
NOW_MS = int(time.time()) * 1000


def schema_text(version: int) -> str:
    return (SCHEMAS / f"player_event.v{version}.avsc").read_text()


# Registry stand-in: schema id -> schema text. Ids are what the Confluent header carries.
REGISTRY = {1: schema_text(1), 2: schema_text(2)}


def event(ts_ms: int = NOW_MS, version: int = 1, **overrides) -> dict:
    e = {
        "event_id": str(uuid.uuid4()),
        "event_ts": ts_ms,
        "session_id": "s-1",
        "user_id": "u-1",
        "title_id": "t-0001",
        "cdn": "cdn-a",
        "isp": "isp-1",
        "device": "tv",
        "country": "DE",
        "event_type": "HEARTBEAT",
        "bitrate_kbps": 2500,
        "rebuffer_ms": None,
        "startup_ms": None,
        "error_code": None,
        "error_message": None,
    }
    if version == 2:
        e["player_version"] = "9.1.0"
    e.update(overrides)
    return e


def encode(e: dict, schema_id: int) -> bytes:
    schema = fastavro.parse_schema(json.loads(REGISTRY[schema_id]))
    body = io.BytesIO()
    fastavro.schemaless_writer(body, schema, e)
    return b"\x00" + schema_id.to_bytes(4, "big") + body.getvalue()


def kafka_rows(values: list[bytes]) -> list[tuple]:
    """Rows shaped like the Kafka source: key, value, topic, partition, offset."""
    return [(b"s-1", v, "player-events", i % 3, 1_000 + i) for i, v in enumerate(values)]


KAFKA_SCHEMA = "key binary, value binary, topic string, partition int, offset long"
