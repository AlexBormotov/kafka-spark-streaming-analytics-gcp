-- One row per player event. Replays are handled twice over:
-- 1. A writer tags every insert with a deterministic insert_deduplication_token; a repeated
--    token is dropped here and in every materialized view (non_replicated_deduplication_window).
-- 2. Duplicates that slip past (same event_id in different inserts) collapse on merge, and
--    queries that must be exact read with FINAL. Aggregates do not see this second layer.
CREATE TABLE IF NOT EXISTS qoe.player_events_raw
(
    event_id       UUID,
    event_ts       DateTime64(3, 'UTC'),
    session_id     String,
    user_id        String,
    title_id       LowCardinality(String),
    cdn            LowCardinality(String),
    isp            LowCardinality(String),
    device         LowCardinality(String),
    country        LowCardinality(String),
    event_type     Enum8('UNKNOWN' = 0, 'START' = 1, 'PLAY' = 2, 'REBUFFER' = 3,
                         'BITRATE_CHANGE' = 4, 'ERROR' = 5, 'STOP' = 6, 'HEARTBEAT' = 7),
    bitrate_kbps   Nullable(UInt32),
    rebuffer_ms    Nullable(UInt32),
    startup_ms     Nullable(UInt32),
    error_code     Nullable(String),
    error_message  Nullable(String),
    player_version Nullable(String),
    -- Set by the server on insert; freshness = ingest_ts - event_ts.
    ingest_ts      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(event_ts)
-- A duplicate carries the same event_ts, so the hour prefix keeps it next to the original
-- while time-range queries skip whole granules.
ORDER BY (toStartOfHour(event_ts), event_id)
TTL toDateTime(event_ts) + INTERVAL 1 DAY
SETTINGS non_replicated_deduplication_window = 1000;
