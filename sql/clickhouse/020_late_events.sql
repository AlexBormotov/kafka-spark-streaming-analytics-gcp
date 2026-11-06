-- Events that arrived behind the streaming watermark. They are counted here and kept out of
-- player_events_raw, so on-time aggregates do not change when late data shows up.
CREATE TABLE IF NOT EXISTS qoe.late_events
(
    event_id     UUID,
    event_ts     DateTime64(3, 'UTC'),
    cdn          LowCardinality(String),
    event_type   LowCardinality(String),
    watermark_ts DateTime64(3, 'UTC'),
    ingest_ts    DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (toStartOfHour(event_ts), event_id)
TTL toDateTime(event_ts) + INTERVAL 1 DAY
SETTINGS non_replicated_deduplication_window = 1000;
