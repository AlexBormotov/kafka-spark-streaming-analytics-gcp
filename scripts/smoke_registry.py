"""Prove the registry speaks the Confluent wire protocol for the Python client.

1. AvroSerializer registers player_event v1 through Apicurio's Confluent-compatible API.
2. The bytes carry the Confluent framing: magic byte 0 and a 4-byte schema id.
3. AvroDeserializer resolves that id through the same API and returns the original record.
4. Negative control: a schema change that breaks BACKWARD compatibility is rejected.

Usage: python scripts/smoke_registry.py http://localhost:8081
"""

import json
import struct
import sys
import uuid
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "player_event.v1.avsc"


def sample_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_ts": 1_762_400_000_000,
        "session_id": "s-1",
        "user_id": "u-1",
        "title_id": "t-1",
        "cdn": "cdn-a",
        "isp": "isp-a",
        "device": "tv",
        "country": "DE",
        "event_type": "REBUFFER",
        "bitrate_kbps": 4500,
        "rebuffer_ms": 1200,
        "startup_ms": None,
        "error_code": None,
        "error_message": None,
    }


def main(url: str) -> int:
    client = SchemaRegistryClient({"url": f"{url.rstrip('/')}/apis/ccompat/v7"})
    schema_str = SCHEMA_PATH.read_text()
    topic = f"smoke-{uuid.uuid4().hex[:8]}"
    subject = f"{topic}-value"
    ctx = SerializationContext(topic, MessageField.VALUE)

    try:
        event = sample_event()
        payload = AvroSerializer(client, schema_str)(event, ctx)

        magic, schema_id = struct.unpack(">bI", payload[:5])
        if magic != 0:
            print(f"FAIL wire format: magic byte {magic}, expected 0")
            return 1
        print(f"ok   serialized {len(payload)} bytes, schema id {schema_id}")

        decoded = AvroDeserializer(client)(payload, ctx)
        # fastavro decodes logical types to Python objects; compare in the producer's terms.
        decoded["event_id"] = str(decoded["event_id"])
        decoded["event_ts"] = int(decoded["event_ts"].timestamp() * 1000)
        if decoded != event:
            print(f"FAIL round trip:\n  sent {event}\n  got  {decoded}")
            return 1
        print("ok   deserialized record equals the original")

        breaking = json.loads(schema_str)
        breaking["fields"].append({"name": "required_without_default", "type": "string"})
        try:
            client.register_schema(subject, Schema(json.dumps(breaking), "AVRO"))
        except SchemaRegistryError as e:
            print(f"ok   incompatible schema rejected: HTTP {e.http_status_code}")
        else:
            print("FAIL incompatible schema was accepted: BACKWARD rule is not in force")
            return 1
    finally:
        try:
            client.delete_subject(subject, permanent=False)
        except SchemaRegistryError:
            pass
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8081"))
