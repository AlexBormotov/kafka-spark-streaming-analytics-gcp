"""Structured Streaming job: player-events (Kafka, Confluent Avro) to ClickHouse.

Configuration comes from QOE_* environment variables, set by the SparkApplication.
"""

import logging
import os

from pyspark.sql import DataFrame, SparkSession

from qoe_streaming.batch import route
from qoe_streaming.registry import SchemaCache
from qoe_streaming.sink import ClickHouse, write

log = logging.getLogger("qoe_streaming")


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise SystemExit(f"{name} is not set")
    return value


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    bootstrap = env("QOE_KAFKA_BOOTSTRAP")
    dlq_topic = env("QOE_DLQ_TOPIC", "player-events-dlq")
    late_delay_ms = int(float(env("QOE_LATE_DELAY_S", "30")) * 1000)
    schemas = SchemaCache(env("QOE_REGISTRY_URL"))
    ch = ClickHouse(
        env("QOE_CLICKHOUSE_URL"), env("QOE_CLICKHOUSE_USER"), env("QOE_CLICKHOUSE_PASSWORD")
    )

    spark = (
        SparkSession.builder.appName("qoe-streaming")
        # Fixed partitioning: the ClickHouse token is per partition, so a replayed batch must
        # split the same way. AQE could coalesce partitions differently.
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", env("QOE_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    def process(raw: DataFrame, batch_id: int) -> None:
        raw = raw.persist()
        try:
            routed = route(raw, schemas, late_delay_ms)
            events = routed.events.persist()
            prefix = f"{routed.fingerprint}-{batch_id}"
            write(events, ch, "player_events_raw", prefix)
            write(routed.late, ch, "late_events", prefix)
            # At-least-once: a replayed batch can repeat dead letters.
            routed.dead.write.format("kafka").option("kafka.bootstrap.servers", bootstrap).option(
                "topic", dlq_topic
            ).save()
            events.unpersist()
        finally:
            raw.unpersist()

    query = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", env("QOE_TOPIC", "player-events"))
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", env("QOE_MAX_OFFSETS_PER_TRIGGER", "200000"))
        # Local Kafka storage is ephemeral; a lost offset must not stop the stream.
        .option("failOnDataLoss", "false")
        .load()
        .writeStream.foreachBatch(process)
        .option("checkpointLocation", env("QOE_CHECKPOINT"))
        .trigger(processingTime=env("QOE_TRIGGER", "2 seconds"))
        .start()
    )
    log.info("started query %s", query.id)
    query.awaitTermination()


if __name__ == "__main__":
    main()
