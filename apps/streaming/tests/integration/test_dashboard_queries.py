"""Every provisioned Grafana query runs against the real schema and returns rows; the alert query
separates a degraded CDN from healthy ones (plan A4, U6 red check)."""

import json
import re
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.integration

CHART = Path(__file__).resolve().parents[4] / "deploy" / "charts" / "dashboards"
ALERT_SQL = (CHART / "files" / "alerts" / "rebuffer_ratio_by_cdn.sql").read_text()
THRESHOLD = yaml.safe_load((CHART / "values.yaml").read_text())["alert"]["rebufferRatioThreshold"]
COLUMNS = ["event_id", "event_ts", "session_id", "user_id", "title_id", "cdn", "isp", "device",
           "country", "event_type", "bitrate_kbps", "rebuffer_ms", "startup_ms"]  # fmt: skip


def dashboard_queries() -> list[tuple[str, str]]:
    queries = []
    for path in sorted((CHART / "files" / "dashboards").glob("*.json")):
        for panel in json.loads(path.read_text())["panels"]:
            for target in panel["targets"]:
                queries.append((panel["title"], target["rawSql"]))
    return queries


def expand(sql: str, dim: str) -> str:
    """Grafana macros and variables as the plugin would expand them for the last hour."""
    sql = re.sub(r"\$__timeFilter\((\w+)\)", r"\1 >= now() - INTERVAL 1 HOUR", sql)
    return sql.replace("${dim}", dim)


def ticks(cdn: str, rebuffers: int, heartbeats: int, now: datetime) -> list[list]:
    rows = []
    for i in range(rebuffers + heartbeats):
        rebuffer = i < rebuffers
        rows.append([uuid.uuid4(), now - timedelta(milliseconds=i * 5), "s-1", "u-1", "t-0001",
                     cdn, "isp-1", "tv", "DE", "REBUFFER" if rebuffer else "HEARTBEAT",
                     None if rebuffer else 3000, 800 if rebuffer else None, None])  # fmt: skip
    rows.append([uuid.uuid4(), now, "s-1", "u-1", "t-0001", cdn, "isp-1", "tv", "DE", "START",
                 3000, None, 900])  # fmt: skip
    return rows


def insert_events(ch, rows: list[list]) -> None:
    ch.insert("qoe.player_events_raw", rows, column_names=COLUMNS)


def alert(ch) -> dict[str, float]:
    return dict(ch.query(ALERT_SQL).result_rows)


def test_every_dashboard_query_returns_rows(ch):
    now = datetime.now(UTC)
    insert_events(ch, ticks("cdn-a", 20, 980, now))
    ch.insert(
        "qoe.late_events",
        [[uuid.uuid4(), now - timedelta(minutes=2), "cdn-a", "HEARTBEAT", now]],
        column_names=["event_id", "event_ts", "cdn", "event_type", "watermark_ts"],
    )
    ch.insert(
        "qoe.stream_progress",
        [[now, 1, 1_000, 500.0, 800.0, 1_200, 3, {0: 10, 1: 0}]],
        column_names=["ts", "batch_id", "input_rows", "input_rows_per_s",
                      "processed_rows_per_s", "duration_ms", "dead_letters", "lag"],
    )  # fmt: skip

    queries = dashboard_queries()
    assert queries
    for title, sql in queries:
        for dim in ("cdn", "isp", "device") if "${dim}" in sql else ("cdn",):
            rows = ch.query(expand(sql, dim)).result_rows
            assert rows, f"{title} ({dim}) returned no rows"


def test_alert_fires_only_for_the_degraded_cdn(ch):
    now = datetime.now(UTC)
    insert_events(ch, ticks("cdn-a", 20, 980, now) + ticks("cdn-b", 300, 700, now))
    ratios = alert(ch)
    assert ratios["cdn-b"] > THRESHOLD > ratios["cdn-a"]


def test_alert_is_quiet_on_clean_data_and_ignores_tiny_samples(ch):
    now = datetime.now(UTC)
    insert_events(ch, ticks("cdn-a", 20, 980, now) + ticks("cdn-c", 10, 20, now))
    ratios = alert(ch)
    assert "cdn-c" not in ratios  # 30 ticks: below the 50-tick minimum
    assert all(r <= THRESHOLD for r in ratios.values())


def test_alert_ignores_windows_older_than_30_seconds(ch):
    old = datetime.now(UTC) - timedelta(minutes=2)
    insert_events(ch, ticks("cdn-b", 300, 700, old))
    assert alert(ch) == {}
