-- One row per Spark micro-batch, written by the job's StreamingQueryListener (plan A4).
-- Spark does not commit offsets to a consumer group, so this is where its Kafka lag is visible.
-- A replayed batch adds a second row with the same batch_id: operational metrics, not results.
CREATE TABLE IF NOT EXISTS qoe.stream_progress
(
    ts                      DateTime64(3, 'UTC'),
    batch_id                UInt64,
    input_rows              UInt64,
    input_rows_per_s        Float64,
    processed_rows_per_s    Float64,
    duration_ms             UInt64,
    dead_letters            UInt64,
    -- Kafka partition -> offsets between the end of this batch and the latest offset.
    lag                     Map(UInt32, Int64)
)
ENGINE = MergeTree
ORDER BY ts
TTL toDateTime(ts) + INTERVAL 7 DAY;
