"""Per-batch operational metrics to `qoe.stream_progress`: Kafka lag and dead letters (plan A4)."""

import json
import logging
from datetime import datetime

from pyspark.sql.streaming import StreamingQueryListener

from qoe_streaming.sink import ClickHouse

log = logging.getLogger(__name__)

COLUMNS = [
    "ts",
    "batch_id",
    "input_rows",
    "input_rows_per_s",
    "processed_rows_per_s",
    "duration_ms",
    "dead_letters",
    "lag",
]


def progress_row(progress: dict, dead_letters: int) -> list:
    """A row for `stream_progress` from a StreamingQueryProgress JSON document."""
    lag: dict[int, int] = {}
    for source in progress["sources"]:
        end, latest = source.get("endOffset") or {}, source.get("latestOffset") or {}
        for topic, partitions in latest.items():
            for partition, offset in partitions.items():
                lag[int(partition)] = offset - end.get(topic, {}).get(partition, offset)
    return [
        datetime.fromisoformat(progress["timestamp"]),
        progress["batchId"],
        progress["numInputRows"],
        # Spark omits a rate it cannot compute (NaN), e.g. on the first batch.
        progress.get("inputRowsPerSecond", 0.0),
        progress.get("processedRowsPerSecond", 0.0),
        progress["durationMs"].get("triggerExecution", 0),
        dead_letters,
        lag,
    ]


class ProgressWriter(StreamingQueryListener):
    """Runs on the driver. `foreachBatch` records dead-letter counts in `dead_letters`."""

    def __init__(self, ch: ClickHouse):
        self._ch = ch
        self.dead_letters: dict[int, int] = {}

    def onQueryStarted(self, event) -> None:
        pass

    def onQueryProgress(self, event) -> None:
        progress = json.loads(event.progress.json)
        row = progress_row(progress, self.dead_letters.pop(progress["batchId"], 0))
        try:
            client = self._ch.connect()
            try:
                client.insert("stream_progress", [row], column_names=COLUMNS)
            finally:
                client.close()
        except Exception:
            # Metrics must never stop the stream; the gap shows on the dashboard.
            log.exception("stream_progress insert failed for batch %s", progress["batchId"])

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        pass
