"""Writes a DataFrame to ClickHouse: one insert per Spark partition, each with its own token.

A replayed micro-batch produces the same partitions, so every repeated insert carries a token
ClickHouse has already seen and is dropped in the table and in its materialized views.
"""

from dataclasses import dataclass
from datetime import UTC, datetime

from pyspark import TaskContext
from pyspark.sql import DataFrame

# Columns that travel as epoch milliseconds and land in DateTime64 columns.
MILLIS_COLUMNS = frozenset({"event_ts", "watermark_ts"})


@dataclass(frozen=True)
class ClickHouse:
    url: str
    user: str
    password: str
    database: str = "qoe"

    def connect(self):
        import clickhouse_connect  # executors import it; keeps the driver import cheap

        return clickhouse_connect.get_client(
            dsn=self.url, username=self.user, password=self.password, database=self.database
        )


def _to_datetime(ms: int | None) -> datetime | None:
    return None if ms is None else datetime.fromtimestamp(ms / 1000, UTC)


def write(df: DataFrame, ch: ClickHouse, table: str, token_prefix: str) -> None:
    columns = df.columns
    millis = [i for i, c in enumerate(columns) if c in MILLIS_COLUMNS]

    def write_partition(rows) -> None:
        data = []
        for row in rows:
            values = list(row)
            for i in millis:
                values[i] = _to_datetime(values[i])
            data.append(values)
        if not data:
            return
        token = f"{token_prefix}-{TaskContext.get().partitionId()}"
        client = ch.connect()
        try:
            client.insert(
                table,
                data,
                column_names=columns,
                settings={
                    "insert_deduplication_token": token,
                    "deduplicate_blocks_in_dependent_materialized_views": 1,
                },
            )
        finally:
            client.close()

    df.foreachPartition(write_partition)
