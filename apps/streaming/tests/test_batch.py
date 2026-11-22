"""Each fixture record lands where the spec's contracts say (plan U5 red check)."""

from pyspark.sql import functions as F
from qoe_streaming.batch import route
from streaming_fixtures import KAFKA_SCHEMA, NOW_MS, REGISTRY, encode, event, kafka_rows

LATE_DELAY_MS = 30_000


def run(spark, values: list[bytes], slices: int = 2):
    raw = spark.createDataFrame(kafka_rows(values), KAFKA_SCHEMA).repartition(slices)
    return route(raw, REGISTRY.get, LATE_DELAY_MS)


def ids(df) -> set[str]:
    return {r.event_id for r in df.select("event_id").collect()}


def test_each_record_lands_where_the_contract_says(spark):
    v1, v2 = event(), event(version=2)
    late = event(ts_ms=NOW_MS - 60_000)
    dup = event()
    good = encode(dup, 1)
    values = [
        encode(v1, 1),
        encode(v2, 2),
        encode(late, 1),
        good,
        good,  # duplicate: byte-identical copy
        encode(event(), 1)[:5] + b"\xff\xff\xff",  # malformed body, valid header
        b"\x01" + encode(event(), 1)[1:],  # wrong magic byte
        encode(event(), 1)[:1] + (99).to_bytes(4, "big") + encode(event(), 1)[5:],  # unknown id
    ]
    routed = run(spark, values)

    assert ids(routed.events) == {v1["event_id"], v2["event_id"], dup["event_id"]}
    assert routed.events.count() == 3
    assert ids(routed.late) == {late["event_id"]}
    errors = sorted(
        dict((h.key, bytes(h.value).decode()) for h in r.headers)["error"]
        for r in routed.dead.collect()
    )
    assert errors == ["avro-decode", "bad-header", "unknown-schema-id:99"]


def test_schema_v2_field_is_kept_and_v1_gets_null(spark):
    v1, v2 = event(), event(version=2)
    routed = run(spark, [encode(v1, 1), encode(v2, 2)])
    versions = {r.event_id: r.player_version for r in routed.events.collect()}
    assert versions == {v1["event_id"]: None, v2["event_id"]: "9.1.0"}


def test_on_time_jitter_is_not_late(spark):
    values = [encode(event(ts_ms=NOW_MS - lag), 1) for lag in (0, 2_000, 29_000)]
    routed = run(spark, values)
    assert routed.late.count() == 0
    assert routed.events.count() == 3


def test_event_time_is_epoch_millis(spark):
    routed = run(spark, [encode(event(ts_ms=NOW_MS), 1)])
    assert routed.events.first().event_ts == NOW_MS


def test_replay_routes_every_row_to_the_same_partition(spark):
    """The ClickHouse token is per partition; a replay must split rows the same way."""
    values = [encode(event(ts_ms=NOW_MS - (i % 50) * 1_000), 1) for i in range(400)]
    values += [encode(event(ts_ms=NOW_MS - 90_000), 1) for _ in range(20)]
    first, replay = run(spark, values, slices=2), run(spark, values[::-1], slices=5)

    def layout(df):
        return set(df.select(F.spark_partition_id(), "event_id").collect())

    assert first.fingerprint == replay.fingerprint
    assert layout(first.events) == layout(replay.events)
    assert layout(first.late) == layout(replay.late)
