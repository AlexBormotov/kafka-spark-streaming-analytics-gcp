"""Kafka lag per partition from Spark's progress document (plan A4)."""

from datetime import UTC, datetime

from qoe_streaming.progress import progress_row

PROGRESS = {
    "batchId": 42,
    "timestamp": "2026-11-23T10:00:02.500Z",
    "numInputRows": 9_000,
    "inputRowsPerSecond": 4_500.0,
    "processedRowsPerSecond": 6_000.0,
    "durationMs": {"triggerExecution": 1_500},
    "sources": [
        {
            "endOffset": {"player-events": {"0": 1_000, "1": 2_000, "2": 500}},
            "latestOffset": {"player-events": {"0": 1_000, "1": 2_750, "2": 900}},
        }
    ],
}


def test_lag_is_latest_minus_end_per_partition():
    row = progress_row(PROGRESS, dead_letters=7)
    assert row == [
        datetime(2026, 11, 23, 10, 0, 2, 500_000, tzinfo=UTC),
        42,
        9_000,
        4_500.0,
        6_000.0,
        1_500,
        7,
        {0: 0, 1: 750, 2: 400},
    ]


def test_first_batch_without_rates_or_latest_offset():
    first = dict(PROGRESS, sources=[{"endOffset": {"player-events": {"0": 10}}}])
    del first["inputRowsPerSecond"], first["processedRowsPerSecond"]
    row = progress_row(first, dead_letters=0)
    assert row[3:5] == [0.0, 0.0]
    assert row[7] == {}
