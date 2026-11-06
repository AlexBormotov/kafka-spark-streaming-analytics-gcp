-- 1-minute QoE windows with the title dimension, for breakdown panels.
CREATE TABLE IF NOT EXISTS qoe.qoe_1m
(
    window_start    DateTime('UTC'),
    cdn             LowCardinality(String),
    isp             LowCardinality(String),
    device          LowCardinality(String),
    title_id        LowCardinality(String),
    events          SimpleAggregateFunction(sum, UInt64),
    starts          SimpleAggregateFunction(sum, UInt64),
    plays           SimpleAggregateFunction(sum, UInt64),
    heartbeats      SimpleAggregateFunction(sum, UInt64),
    rebuffers       SimpleAggregateFunction(sum, UInt64),
    errors          SimpleAggregateFunction(sum, UInt64),
    rebuffer_ms     SimpleAggregateFunction(sum, UInt64),
    bitrate_sum     SimpleAggregateFunction(sum, UInt64),
    bitrate_n       SimpleAggregateFunction(sum, UInt64),
    startup_ms_q    AggregateFunction(quantiles(0.5, 0.95), Nullable(UInt32))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, cdn, isp, device, title_id)
TTL window_start + INTERVAL 7 DAY
SETTINGS non_replicated_deduplication_window = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS qoe.qoe_1m_mv TO qoe.qoe_1m AS
SELECT
    toStartOfMinute(event_ts) AS window_start,
    cdn,
    isp,
    device,
    title_id,
    count() AS events,
    countIf(event_type = 'START') AS starts,
    countIf(event_type = 'PLAY') AS plays,
    countIf(event_type = 'HEARTBEAT') AS heartbeats,
    countIf(event_type = 'REBUFFER') AS rebuffers,
    countIf(event_type = 'ERROR') AS errors,
    sum(ifNull(rebuffer_ms, 0)) AS rebuffer_ms,
    sum(ifNull(bitrate_kbps, 0)) AS bitrate_sum,
    count(bitrate_kbps) AS bitrate_n,
    quantilesState(0.5, 0.95)(startup_ms) AS startup_ms_q
FROM qoe.player_events_raw
GROUP BY window_start, cdn, isp, device, title_id;
