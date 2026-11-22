"""One micro-batch: decode, route, deduplicate and split late events.

Everything here depends only on the batch itself, so a replayed batch (same Kafka offsets)
routes every row to the same output and the same Spark partition. The ClickHouse writer relies
on that: its deduplication token is per partition (plan amendments A2, A3).
"""

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

# Confluent wire format: magic byte 0, then a 4-byte big-endian schema id, then the Avro body.
HEADER_LEN = 5

# Superset of every schema version, in player_events_raw column order. A field missing from the
# writer schema of a record is null.
EVENT_FIELDS = {
    "event_id": "string",
    "event_ts": "timestamp",
    "session_id": "string",
    "user_id": "string",
    "title_id": "string",
    "cdn": "string",
    "isp": "string",
    "device": "string",
    "country": "string",
    "event_type": "string",
    "bitrate_kbps": "int",
    "rebuffer_ms": "int",
    "startup_ms": "int",
    "error_code": "string",
    "error_message": "string",
    "player_version": "string",
}
LATE_COLUMNS = ["event_id", "event_ts", "cdn", "event_type", "watermark_ts"]


@dataclass(frozen=True)
class Routed:
    events: DataFrame
    late: DataFrame
    dead: DataFrame
    # Identifies the batch by its offsets; stable on replay, new after a checkpoint reset.
    fingerprint: str


def fingerprint(raw: DataFrame) -> str:
    ranges = raw.groupBy("topic", "partition").agg(F.min("offset"), F.max("offset")).collect()
    text = ";".join(sorted(",".join(str(v) for v in r) for r in ranges))
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _dead(df: DataFrame, reason: Column) -> DataFrame:
    source = F.concat_ws("/", "topic", "partition", "offset")
    headers = F.array(
        F.struct(F.lit("error").alias("key"), reason.cast("binary").alias("value")),
        F.struct(F.lit("source").alias("key"), source.cast("binary").alias("value")),
    )
    return df.select("key", "value", headers.alias("headers"))


def route(raw: DataFrame, writer_schema: Callable[[int], str | None], late_delay_ms: int) -> Routed:
    """raw: rows of the Kafka source (key, value, topic, partition, offset, ...)."""
    framed = raw.select(
        "key",
        "value",
        "topic",
        "partition",
        "offset",
        (F.length("value") > HEADER_LEN).alias("framed"),
        (F.hex(F.substring("value", 1, 1)) == "00").alias("magic_ok"),
        F.conv(F.hex(F.substring("value", 2, 4)), 16, 10).cast("int").alias("schema_id"),
        F.expr(f"substring(value, {HEADER_LEN + 1})").alias("body"),
    )
    valid_header = F.coalesce(F.col("framed") & F.col("magic_ok"), F.lit(False))
    dead = [_dead(framed.where(~valid_header), F.lit("bad-header"))]

    ids = sorted(
        r.schema_id for r in framed.where(valid_header).select("schema_id").distinct().collect()
    )
    decoded = []
    for schema_id in ids:
        rows = framed.where(valid_header & (F.col("schema_id") == schema_id))
        schema = writer_schema(schema_id)
        if schema is None:
            dead.append(_dead(rows, F.lit(f"unknown-schema-id:{schema_id}")))
            continue
        present = {f["name"] for f in json.loads(schema)["fields"]}
        parsed = rows.withColumn("e", from_avro("body", schema, {"mode": "PERMISSIVE"}))
        # PERMISSIVE turns an undecodable body into nulls; event_id is never null in a valid one.
        ok = F.col("e.event_id").isNotNull()
        dead.append(_dead(parsed.where(~ok), F.lit("avro-decode")))
        decoded.append(
            parsed.where(ok).select(
                *[
                    (F.col(f"e.{name}") if name in present else F.lit(None)).cast(dtype).alias(name)
                    for name, dtype in EVENT_FIELDS.items()
                ]
            )
        )

    events = _union(decoded, raw).dropDuplicates(["event_id"])
    events = events.withColumn("event_ts", F.unix_millis("event_ts"))
    newest = events.agg(F.max("event_ts")).first()[0]
    watermark = newest - late_delay_ms if newest is not None else None
    is_late = F.col("event_ts") < F.lit(watermark) if watermark is not None else F.lit(False)
    late = events.where(is_late).select(
        *LATE_COLUMNS[:-1], F.lit(watermark).cast("long").alias("watermark_ts")
    )
    return Routed(
        events=events.where(~is_late),
        late=late,
        dead=reduce(DataFrame.unionByName, dead),
        fingerprint=fingerprint(raw),
    )


def _union(decoded: list[DataFrame], raw: DataFrame) -> DataFrame:
    if not decoded:
        schema = ", ".join(f"{n} {t}" for n, t in EVENT_FIELDS.items())
        return raw.sparkSession.createDataFrame([], schema)
    return reduce(DataFrame.unionByName, decoded)
