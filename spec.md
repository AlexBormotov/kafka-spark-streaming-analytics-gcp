---
id: QOE-SPEC-L1
title: Layer 1 - streaming core and platform foundation
requirement: QOE-INT-001
owner: alexvicbor@gmail.com
status: approved by owner on 2026-11-05 (D1-D4 as recommended)
---

# QOE-SPEC-L1: Layer 1 - streaming core and platform foundation

Layer 1 delivers the path Kafka -> Spark -> ClickHouse -> Grafana, plus the platform every later
layer stands on: local cluster, cloud cluster, cost guards, GitOps and CI.

## Acceptance criteria covered (copied verbatim from `intent.md`)

- **AC-01** The whole platform comes up with `make up`, produces load with `make demo`, and is fully
  removed with `make down`. Data is synthetic only, and its volume is a parameter.
- **AC-02** A cloud session of 15-30 minutes costs at most $12. Only resources that are free or
  near-free while idle persist between sessions: the Terraform state bucket and container images.
- **AC-03** A forgotten cloud session is stopped without the owner: a project budget with email
  alerts, and a time-to-live after which the cluster is deleted even if the owner's machine is off.
- **AC-04** Every layer is first proven on a local Kubernetes cluster with the same charts and
  operators. A cloud run happens only after the local run passes its checks.
- **AC-10** A player-event generator produces 20-50 thousand events per second at nominal load,
  scaled by a parameter, serialized with a schema held in a schema registry.
- **AC-11** Freshness: the 95th percentile of the delay from event time to availability in the
  analytical store is at most 10 seconds at nominal load, and is visible on a dashboard.
- **AC-12** Detection: when the generator injects a degradation for one CDN, a dashboard alert
  fires within 60 seconds of the injection start. Without injection, the alert does not fire.
- **AC-13** Late and out-of-order events, duplicates, schema evolution with a dead-letter path,
  and hot-title skew are each demonstrated and explained.
- **AC-40** Infrastructure is code: one small module for the cluster and the cloud resources
  around it; everything inside the cluster is deployed by GitOps.
- **AC-41** CI runs on every change with the same commands a contributor runs locally. Primary
  repository on GitLab, mirrored to GitHub, both public.
- **AC-42** No long-lived cloud keys anywhere: CI and workloads authenticate with workload identity.
- **AC-44** Everything committed to the repository is written in English.

AC-43 (README packaging) is completed after layer 3; layer 1 contributes its screenshots, decision
records and cost report.

## Fixed decisions (settled with the owner)

| Topic | Decision |
| --- | --- |
| Cloud | GCP project `qoe-spark-streaming`, region `us-central1`, GKE on spot nodes |
| Local | kind cluster with the same operators and Helm charts; only `values-local.yaml` differs |
| Spark | PySpark Structured Streaming, run by Spark Operator |
| Kafka | Strimzi, KRaft mode |
| ClickHouse | Altinity ClickHouse operator |
| Generator | Python (`confluent-kafka` + `fastavro`), throughput = replicas x rate per pod |
| Terraform state | GCS bucket with versioning, created once by `make bootstrap` |
| Images | Artifact Registry; GitLab CI pushes through Workload Identity Federation |
| Cost guard | Billing budget $12 with 50/90/100% email alerts; Cloud Scheduler deletes the cluster after `TTL_MINUTES` (default 30); `make extend MIN=n` postpones it |
| GitOps | ArgoCD app-of-apps; `make dev` deploys with Helm directly for fast local iteration |
| Hosting | GitLab `kafka-spark-streaming-analytics-gcp` (primary, CI), push-mirror to GitHub `AlexBormotov/kafka-spark-streaming-analytics-gcp`, both public |

## Open decisions (owner approves with this spec)

**D1. Schema registry.** Confluent Schema Registry is under the Confluent Community License;
Apicurio Registry is Apache 2.0 and serves the Confluent-compatible API.
Recommendation: Apicurio with its Confluent-compatible API enabled. Producers and Spark use the
standard Confluent wire format, so the registry can be swapped without code changes.

**D2. Spark to ClickHouse write path.** (a) `foreachBatch` with the ClickHouse JDBC driver;
(b) the ClickHouse Spark connector (DataSource V2); (c) Spark writes to Kafka and ClickHouse reads
with its Kafka table engine.
Recommendation: (a). It is the simplest path with explicit batch control, and idempotency is
handled by ClickHouse engines (see Contracts), not by the writer.

**D3. What KEDA scales.** Spark Structured Streaming scales through its own executors, not through
pod replicas, so KEDA does not fit it. Candidates for a lag-scaled consumer: the dead-letter
processor, or a raw-event sink consumer.
Recommendation: move KEDA to layer 2, where Kafka Connect workers (Elasticsearch sink) are a natural
lag-scaled target. Layer 1 demonstrates backpressure with Spark `maxOffsetsPerTrigger` instead.

**D4. Node pool sizing for layer 1.** The original estimate of 5 x `e2-standard-8` is sized for
Elasticsearch in layer 2. Recommendation: node count is a Terraform variable; layer 1 starts with
3 x `e2-standard-8` spot and the real need is measured on the first cloud run.

## Contracts

- **Event identity.** Every player event carries `event_id` (UUID) and `event_ts` (event time,
  milliseconds UTC). `event_id` is the deduplication key everywhere downstream.
- **Topics.** `player-events` (Avro, key = `session_id`, so one session stays ordered within one
  partition), `player-events-dlq` (raw bytes plus error headers).
- **Schema evolution.** Registry compatibility mode `BACKWARD`. A new optional field with a default
  must deploy without stopping the pipeline. A record that fails to decode goes to the dead-letter
  topic and never stops the stream.
- **Delivery.** Producers: `acks=all`, idempotent producer enabled. Spark: at-least-once with
  checkpoints. End-to-end effect is exactly-once in query results because ClickHouse deduplicates:
  raw events land in `ReplacingMergeTree` keyed by `event_id`; aggregates are built by materialized
  views into `AggregatingMergeTree`. Writing the same micro-batch twice must not change query
  results.
- **Late data.** Watermark on `event_ts`; the delay is a parameter (default 30 s). Events later than
  the watermark are counted in a `late_events` metric, not silently dropped.
- **Freshness metric.** `ingest_ts` is set when a row is written to ClickHouse. Freshness =
  `ingest_ts - event_ts`.

## Completion condition

Layer 1 is complete when all of the following pass, in this order:

1. `make lint test` passes locally and in GitLab CI with the same commands.
2. Local run: `make up ENV=local`, then `make demo`, then `make verify-l1`, then `make down ENV=local`.
3. Cloud run: `make up ENV=gke`, then `make demo`, then `make verify-l1`, then `make down ENV=gke`,
   then `make status` reports no billable resources except the state bucket and the registry.

`make verify-l1` checks, and fails with a non-zero exit code otherwise:

| Check | Pass condition | Covers |
| --- | --- | --- |
| Throughput | generator sustains the configured rate (nominal: 20 000 events/s on GKE; local rate is lower and recorded) for 2 minutes | AC-10 |
| Freshness | p95(`ingest_ts - event_ts`) <= 10 s over the last 2 minutes | AC-11 |
| Negative control | no QoE alert in Firing state during a clean 2-minute run | AC-12 |
| Detection | after `--inject cdn=<x>` starts, the alert for that CDN is Firing within 60 s | AC-12 |
| Idempotency | replaying one micro-batch leaves `count()` and aggregate results unchanged | AC-13 |
| Late events | injected late events appear in `late_events`, on-time totals unchanged | AC-13 |
| Schema evolution | producing with schema v2 (one added optional field) keeps the pipeline running | AC-13 |
| Dead letter | injected malformed records land in `player-events-dlq`, the pipeline keeps running | AC-13 |

Hot-title skew (AC-13) is demonstrated by a generator mode concentrating a share of traffic on one
title, with partition lag shown in Grafana. It is explained in a decision record rather than gated
by a numeric threshold.

Cost (AC-02) is recorded from the billing export after the cloud run, in the cost report.

## Out of scope for layer 1

- Debezium, Postgres, Elasticsearch, Kibana, KEDA (layer 2).
- Iceberg, Hive Metastore, BigQuery, Dataform, Airflow (layer 3).
- Sessionization beyond the windowed QoE aggregates needed for the alert.
- External Secrets and NetworkPolicy beyond what layer 1 components need.
