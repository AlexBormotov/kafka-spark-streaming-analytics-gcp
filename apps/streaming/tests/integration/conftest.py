"""A live ClickHouse with the schema from sql/clickhouse/, recreated per test."""

import os
import time
from pathlib import Path

import clickhouse_connect
import pytest

SQL_DIR = Path(__file__).resolve().parents[4] / "sql" / "clickhouse"


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
