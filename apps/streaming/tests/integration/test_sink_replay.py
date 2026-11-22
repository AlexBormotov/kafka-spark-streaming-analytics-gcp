"""The job's own write path, replayed: ClickHouse results must not change (spec AC-13)."""

import os
from dataclasses import replace

import pytest
from pyspark.sql import functions as F
from qoe_streaming.batch import route
from qoe_streaming.sink import ClickHouse, write
from streaming_fixtures import KAFKA_SCHEMA, NOW_MS, REGISTRY, encode, event, kafka_rows

pytestmark = pytest.mark.integration

RESULTS = {
    "raw": "SELECT count(), uniqExact(event_id), toUnixTimestamp64Milli(max(event_ts)) "
    "FROM qoe.player_events_raw",
    "late": "SELECT count() FROM qoe.late_events",
    "qoe_10s": "SELECT cdn, sum(events), sum(heartbeats), sum(rebuffers) "
    "FROM qoe.qoe_10s GROUP BY cdn ORDER BY cdn",
}


@pytest.fixture
def sink(ch):
    url = os.environ["CLICKHOUSE_URL"]
    return ClickHouse(url.replace("default@", ""), "default", os.environ["CLICKHOUSE_PASSWORD"])


def micro_batch(spark, values, slices):
    raw = spark.createDataFrame(kafka_rows(values), KAFKA_SCHEMA).repartition(slices)
    return route(raw, REGISTRY.get, 30_000)


def write_batch(routed, sink, batch_id):
    prefix = f"{routed.fingerprint}-{batch_id}"
    write(routed.events, sink, "player_events_raw", prefix)
    write(routed.late, sink, "late_events", prefix)


def results(ch):
    return {name: ch.query(sql).result_rows for name, sql in RESULTS.items()}


def test_replayed_batch_through_the_job_writer_changes_nothing(spark, ch, sink):
    types = ["HEARTBEAT", "HEARTBEAT", "REBUFFER", "START"]
    values = [
        encode(event(ts_ms=NOW_MS - i * 7, event_type=types[i % 4], cdn=f"cdn-{'abc'[i % 3]}"), 1)
        for i in range(3_000)
    ]
    values += [encode(event(ts_ms=NOW_MS - 90_000), 1) for _ in range(30)]

    write_batch(micro_batch(spark, values, slices=3), sink, batch_id=7)
    before = results(ch)
    assert before["raw"][0][0] == 3_000 and before["late"] == [(30,)]
    assert before["raw"][0][2] == NOW_MS

    # Reorder within each partition so the replayed blocks differ byte for byte and only
    # insert_deduplication_token (not ClickHouse's block-hash check) can deduplicate them.
    replay = micro_batch(spark, values[::-1], slices=5)
    replay = replace(replay, events=replay.events.sortWithinPartitions(F.desc("event_ts")))
    write_batch(replay, sink, batch_id=7)

    assert results(ch) == before
    # Sanity: the writer really used several partitions, so tokens were per partition.
    assert (
        micro_batch(spark, values, 3).events.select(F.spark_partition_id()).distinct().count() > 1
    )
