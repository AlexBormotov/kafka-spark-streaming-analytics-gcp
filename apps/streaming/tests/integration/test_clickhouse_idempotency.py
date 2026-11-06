"""Replaying a micro-batch must not change query results (spec: Delivery contract).

Runs against the DDL in sql/clickhouse/ on a live server: CLICKHOUSE_URL, set by
`make test-integration`.
"""

import os
import random
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import clickhouse_connect
import pytest

pytestmark = pytest.mark.integration

SQL_DIR = Path(__file__).resolve().parents[4] / "sql" / "clickhouse"
TYPES = ["START", "PLAY", "HEARTBEAT", "HEARTBEAT", "HEARTBEAT", "REBUFFER", "ERROR", "STOP"]
COLUMNS = ["event_id", "event_ts", "session_id", "user_id", "title_id", "cdn", "isp", "device",
           "country", "event_type", "bitrate_kbps", "rebuffer_ms", "startup_ms"]  # fmt: skip
# The streaming writer sets this and a per-partition insert_deduplication_token on every insert.
# 26.8 already defaults it to 1; it is explicit so a server default change cannot break replays.
DEDUP = {"deduplicate_blocks_in_dependent_materialized_views": 1}

# Quantiles of an empty set are nan, and nan != nan would fail every comparison: map nan to -1.
SNAPSHOT_QUERIES = {
    "raw_count": "SELECT count() FROM qoe.player_events_raw",
    "raw_final": "SELECT count(), uniqExact(event_id) FROM qoe.player_events_raw FINAL",
    "qoe_10s": """
        SELECT window_start, cdn, isp, device, sum(events), sum(rebuffers), sum(heartbeats),
               sum(bitrate_sum), sum(bitrate_n),
               arrayMap(x -> ifNotFinite(x, -1), quantilesMerge(0.5, 0.95)(startup_ms_q))
        FROM qoe.qoe_10s GROUP BY ALL ORDER BY ALL""",
    "qoe_1m": """
        SELECT window_start, cdn, isp, device, title_id, sum(events), sum(rebuffer_ms),
               arrayMap(x -> ifNotFinite(x, -1), quantilesMerge(0.5, 0.95)(startup_ms_q))
        FROM qoe.qoe_1m GROUP BY ALL ORDER BY ALL""",
}


def connect(url: str, password: str, timeout_s: float = 60):
    """Waits for a server that is still starting (CI service containers start in parallel)."""
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            return clickhouse_connect.get_client(dsn=url, password=password)
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


@pytest.fixture
def ch():
    url = os.environ.get("CLICKHOUSE_URL")
    if not url:
        pytest.fail("CLICKHOUSE_URL is not set: run `make test-integration`")
    client = connect(url, os.environ.get("CLICKHOUSE_PASSWORD", ""))
    client.command("DROP DATABASE IF EXISTS qoe SYNC")
    for path in sorted(SQL_DIR.glob("*.sql")):
        for statement in path.read_text().split(";\n"):
            if statement.strip():
                client.command(statement)
    yield client
    client.close()


def batch(n: int, seed: int) -> list[list]:
    rng = random.Random(seed)
    start = datetime(2026, 11, 6, 12, 0, tzinfo=UTC)
    rows = []
    for _ in range(n):
        event_type = rng.choice(TYPES)
        rows.append(
            [
                uuid.UUID(int=rng.getrandbits(128), version=4),
                start + timedelta(milliseconds=rng.randrange(180_000)),
                f"s-{rng.randrange(200)}",
                f"u-{rng.randrange(100)}",
                f"t-{rng.randrange(20):04d}",
                rng.choice(["cdn-a", "cdn-b", "cdn-c"]),
                f"isp-{rng.randrange(1, 4)}",
                rng.choice(["tv", "mobile"]),
                "DE",
                event_type,
                rng.choice([800, 2500, 6000]) if event_type == "HEARTBEAT" else None,
                rng.randrange(100, 5000) if event_type == "REBUFFER" else None,
                rng.randrange(300, 4000) if event_type == "START" else None,
            ]
        )
    return rows


def insert(ch, rows: list[list], dedup_key: str | None) -> None:
    settings = dict(DEDUP, insert_deduplication_token=dedup_key) if dedup_key else DEDUP
    ch.insert("qoe.player_events_raw", rows, column_names=COLUMNS, settings=settings)


def snapshot(ch) -> dict:
    return {name: ch.query(sql).result_rows for name, sql in SNAPSHOT_QUERIES.items()}


def write_micro_batch(ch, rows: list[list], batch_id: int, replay: bool = False) -> None:
    """Two Spark partitions, one token each, as the streaming job writes them.

    A replay reads the same offsets, but row order inside a partition is not guaranteed, so
    the replayed blocks differ byte for byte and only the token identifies them.
    """
    half = len(rows) // 2
    for p, part in enumerate([rows[:half], rows[half:]]):
        insert(ch, part[::-1] if replay else part, f"qoe-{batch_id}-{p}")


def test_replayed_micro_batch_leaves_every_result_unchanged(ch):
    write_micro_batch(ch, batch(5_000, seed=1), batch_id=0)
    write_micro_batch(ch, batch(5_000, seed=2), batch_id=1)
    before = snapshot(ch)
    assert before["raw_count"] == [(10_000,)]

    write_micro_batch(ch, batch(5_000, seed=2), batch_id=1, replay=True)

    assert snapshot(ch) == before


def test_without_a_token_the_aggregates_double_count(ch):
    """Negative control: proves the token, not the table engine, keeps aggregates exact."""
    rows = batch(5_000, seed=3)
    insert(ch, rows, dedup_key=None)
    before = snapshot(ch)

    insert(ch, rows[:1_000] + batch(1, seed=4), dedup_key=None)
    after = snapshot(ch)

    assert after["raw_final"] == [(5_001, 5_001)]
    assert after["qoe_10s"] != before["qoe_10s"]
    assert sum(r[4] for r in after["qoe_10s"]) == 6_001


def test_ingest_ts_is_set_by_the_server(ch):
    insert(ch, batch(10, seed=5), dedup_key="qoe-ingest")
    (lag_min,) = ch.query(
        "SELECT min(dateDiff('millisecond', event_ts, ingest_ts)) FROM qoe.player_events_raw"
    ).result_rows[0]
    assert lag_min > 0
