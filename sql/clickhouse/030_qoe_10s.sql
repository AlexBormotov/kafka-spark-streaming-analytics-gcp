-- 10-second QoE windows for alerting and freshness. No title_id: 500 titles would multiply the
-- rows the 10-second alert query scans; per-title views read qoe_1m.
-- Read with sum() and quantilesMerge(), grouped by the key columns.
CREATE TABLE IF NOT EXISTS qoe.qoe_10s
(
    window_start    DateTime('UTC'),
    cdn             LowCardinality(String),
    isp             LowCardinality(String),
    device          LowCardinality(String),
    events          SimpleAggregateFunction(sum, UInt64),
    starts          SimpleAggregateFunction(sum, UInt64),
    plays           SimpleAggregateFunction(sum, UInt64),
    heartbeats      SimpleAggregateFunction(sum, UInt64),
    rebuffers       SimpleAggregateFunction(sum, UInt64),
    errors          SimpleAggregateFunction(sum, UInt64),
    rebuffer_ms     SimpleAggregateFunction(sum, UInt64),
    bitrate_sum     SimpleAggregateFunction(sum, UInt64),
    bitrate_n       SimpleAggregateFunction(sum, UInt64),
    startup_ms_q    AggregateFunction(quantiles(0.5, 0.95), Nullable(UInt32)),
    freshness_ms_q  AggregateFunction(quantiles(0.5, 0.95), Int64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, cdn, isp, device)
TTL window_start + INTERVAL 7 DAY
SETTINGS non_replicated_deduplication_window = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS qoe.qoe_10s_mv TO qoe.qoe_10s AS
SELECT
    toStartOfInterval(event_ts, INTERVAL 10 SECOND) AS window_start,
    cdn,
    isp,
    device,
    count() AS events,
    countIf(event_type = 'START') AS starts,
    countIf(event_type = 'PLAY') AS plays,
    countIf(event_type = 'HEARTBEAT') AS heartbeats,
    countIf(event_type = 'REBUFFER') AS rebuffers,
    countIf(event_type = 'ERROR') AS errors,
    sum(ifNull(rebuffer_ms, 0)) AS rebuffer_ms,
    sum(ifNull(bitrate_kbps, 0)) AS bitrate_sum,
    count(bitrate_kbps) AS bitrate_n,
    quantilesState(0.5, 0.95)(startup_ms) AS startup_ms_q,
    quantilesState(0.5, 0.95)(dateDiff('millisecond', event_ts, ingest_ts)) AS freshness_ms_q
FROM qoe.player_events_raw
GROUP BY window_start, cdn, isp, device;
