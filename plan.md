---
id: QOE-PLAN-L1
title: Layer 1 implementation plan
spec: QOE-SPEC-L1
research: docs/research/l1-versions.md
owner: alexvicbor@gmail.com
status: approved by owner on 2026-11-05
---

# QOE-PLAN-L1: Layer 1 implementation plan

The plan is a set of work units. A unit starts only when its blockers have landed. Each unit names
the files it may touch, the invariants it must keep, the check that goes red when an invariant
breaks, and how it is reversed. A file touched outside a unit's set stops work and returns this plan
for approval.

No unit runs a billable cloud command without the owner's go-ahead in the current session.

## Repository layout

```
Makefile                      one command per task; CI calls the same targets
.tool-versions                pinned CLI versions
.env.example                  every variable, no values
.gitlab-ci.yml                lint, test, build, terraform plan, mirror
.gitlab/CODEOWNERS, REVIEW.md
apps/generator/               Python event generator, Dockerfile, tests
apps/streaming/               PySpark job, Dockerfile, tests
schemas/                      Avro schemas (player_event v1, v2)
sql/clickhouse/               ClickHouse DDL, applied in order
deploy/kind/cluster.yaml      local cluster definition
deploy/argocd/                root app-of-apps and one Application per component
deploy/values/{local,gke}/    values for upstream charts (operators, Grafana, Argo CD)
deploy/charts/                own Helm charts: platform (Kafka, Apicurio, ClickHouse CRs),
                              generator, streaming, dashboards
infra/terraform/bootstrap/    persistent, free while idle: APIs, Artifact Registry, WIF pool,
                              CI and runtime service accounts, billing budget
infra/terraform/session/      per session: GKE cluster, spot node pool, TTL scheduler job
scripts/                      verify_l1.py, smoke tests, status
docs/adr/, docs/research/
```

## Cross-cutting decisions in this plan

**P1. Who installs what.** Operators and Grafana are installed by Argo CD from upstream Helm charts
with values files from this repository, in both environments. Own charts (`deploy/charts/*`) are
installed by Argo CD when `GITOPS=1`, and by `helm upgrade --install` under `make dev`.
Alternative rejected: installing operators by Helm in `make dev` as well. It duplicates chart
versions in two places that will drift. Operators change rarely, so the fast loop does not need them.

**P2. State bucket bootstrap.** `make bootstrap` creates the state bucket with `gcloud storage
buckets create` (idempotent, versioning on), then runs `terraform apply` in `bootstrap/` with the
GCS backend. Alternative rejected: bootstrap with local state migrated to GCS. It adds a migration
step and a window where state exists only on a laptop.

**P3. Versions** come from `docs/research/l1-versions.md`. Spark 4.0.5 everywhere: pyspark in tests,
image `apache/spark:4.0.5-python3`, `_2.13:4.0.5` packages.

**P4. Change size.** The repository has no history, so there is no p75 to set a ceiling from. Each
unit lands as one reviewed change; the ceiling is set after layer 1 from measured data.

## Work units

### U0. Repository skeleton and the local quality loop

- Blockers: none.
- Files: `Makefile`, `.tool-versions`, `.env.example`, `.gitlab-ci.yml` (lint and test jobs only),
  `.gitlab/CODEOWNERS`, `REVIEW.md`, `pyproject.toml` per app, `tasks/todo.md`, `tasks/lessons.md`,
  `README.md` (stub).
- Invariants: `make lint` and `make test` are the only commands CI runs for those stages.
- Red check: CI job fails if it calls anything other than the `make` target (job script is a single
  line, reviewed). `make lint` runs `ruff`, `terraform fmt -check`, `helm lint`, `yamllint`.
- Reverse: revert the commit.

### U1. Local cluster with Argo CD and operators

- Blockers: U0.
- Files: `deploy/kind/cluster.yaml`, `deploy/argocd/**`, `deploy/values/local/**`,
  `deploy/values/gke/**`, Makefile targets `up`, `down`, `status` for `ENV=local`.
- Invariants: kind node image pinned by digest to Kubernetes 1.36 (matches GKE Regular). Same
  Argo CD Applications in both environments; only the values directory differs.
- Red check: `make up ENV=local` waits for every Argo CD Application to be `Synced` and `Healthy`
  with a timeout and exits non-zero otherwise. `make status ENV=local` lists anything not Ready.
- Reverse: `make down ENV=local` deletes the kind cluster; revert the commit.

### U2. Kafka, schema registry and topics (risk spike first)

- Blockers: U1.
- Files: `deploy/charts/platform/**` (Kafka, KafkaNodePool, KafkaTopic, Apicurio Deployment),
  `schemas/player_event.v1.avsc`, `scripts/smoke_registry.py`, Makefile target `smoke-registry`.
- Approach: Apicurio 3.3.4 as a plain Deployment with `kafkasql` storage (no Postgres). Alternative
  rejected: Apicurio operator, which needs OLM in the cluster.
- First step, before anything else in the unit: prove the unverified research item. A
  `confluent-kafka` `AvroSerializer` pointed at `/apis/ccompat/v7` registers the schema, produces a
  record, and a deserializer reads it back with the same schema id. If this fails, work stops and
  this plan returns with the evidence (fallback candidate: Karapace, Apache 2.0).
- Invariants: topic `player-events` keyed by `session_id`; `player-events-dlq` exists; registry
  compatibility `BACKWARD`.
- Red check: `make smoke-registry` round-trips one record and fails on mismatch; it also attempts
  an incompatible schema change and expects rejection (negative control).
- Reverse: remove the Argo CD Application; revert the commit.

### U3. Event generator

- Blockers: U2.
- Files: `apps/generator/**`, `schemas/player_event.v2.avsc`, `deploy/charts/generator/**`.
- Behaviour: rate per pod and replicas as parameters; modes `--inject cdn=<x>` (degradation),
  `--late-share`, `--duplicate-share`, `--malformed-share`, `--hot-title-share`, `--schema v2`.
- Invariants: every event has `event_id` (UUID) and `event_ts` (ms UTC); key is `session_id`;
  producer `acks=all`, idempotence on.
- Red check: unit tests per mode (shares within tolerance, injected CDN metrics shifted, keys
  correct). A throughput test records events per second per pod locally; the number goes into the
  research record, not into a guess.
- Reverse: revert the commit.

### U4. ClickHouse schema

- Blockers: U1.
- Files: `sql/clickhouse/**`, ClickHouse installation in `deploy/charts/platform/**`,
  `apps/streaming/tests/integration/test_clickhouse_idempotency.py`.
- Design: `player_events_raw` as `ReplacingMergeTree` ordered by `event_id` within a time
  partition, carrying `ingest_ts` default `now64(3)`; materialized views into `qoe_1m` and
  `qoe_10s` (`AggregatingMergeTree`) by CDN, ISP, device, title; `late_events` table.
- Invariants: inserting the same rows twice does not change `FINAL` counts or aggregate results.
  Note: a materialized view sees both inserts, so aggregates use `uniqState(event_id)`-style or
  deduplicated sources where double counting matters. The integration test is the judge.
- Red check: integration test against a ClickHouse container inserts one batch twice and compares
  query results; it is red if any result changes.
- Reverse: drop the database; revert the commit.

### U5. Spark streaming job

- Blockers: U2, U4.
- Files: `apps/streaming/**`, `deploy/charts/streaming/**` (SparkApplication).
- Approach: read `player-events`; strip the 5-byte Confluent prefix; `from_avro` with the reader
  schema fetched from the registry at start; records that fail to decode go to
  `player-events-dlq` with error headers; watermark on `event_ts` (default 30 s); events behind the
  watermark counted into `late_events`; `foreachBatch` writes to ClickHouse with JDBC
  (`clickhouse-jdbc:0.11.0:all`); `maxOffsetsPerTrigger` bounds micro-batches; checkpoints on a
  PVC locally and in GCS on GKE.
- Invariants: the stream never stops on a bad record; a replayed micro-batch does not change query
  results (relies on U4); one Spark version (P3).
- Red check: pytest with local PySpark (Java 17) on fixture records: valid v1, valid v2, malformed
  bytes, late event, duplicate. Each lands where the contract says or the test is red.
- Reverse: delete the SparkApplication; revert the commit.

### U6. Grafana dashboards and alerts

- Blockers: U4.
- Files: `deploy/charts/dashboards/**`, `deploy/values/*/grafana.yaml`.
- Content: QoE overview (rebuffer ratio, start failures, bitrate by CDN/ISP/device/title),
  freshness p95 panel, Kafka lag, late and DLQ counters; alert rule "rebuffer ratio by CDN above
  threshold", evaluated every 10 s.
- Invariants: dashboards and alert rules are provisioned from files only; nothing is edited in the
  UI.
- Red check: `scripts/verify_l1.py` reads the alert through the Grafana API (U7).
- Reverse: revert the commit.

### U7. `make verify-l1` and the local completion run

- Blockers: U3, U5, U6.
- Files: `scripts/verify_l1.py`, Makefile targets `demo`, `verify-l1`.
- Content: the eight checks of the spec's completion table, in order, each with its own exit code
  and a one-line result.
- Red check: the script is itself checked by a negative control: with the alert threshold raised
  out of reach, the detection check must fail.
- Done when: spec completion step 2 passes locally. Results go into `tasks/todo.md` with the
  command output.

### U8. Terraform and cost guards

- Blockers: U0. Can run in parallel with U1-U7.
- Files: `infra/terraform/bootstrap/**`, `infra/terraform/session/**`, Makefile targets
  `bootstrap`, `up ENV=gke`, `down ENV=gke`, `extend`, `status ENV=gke`.
- Bootstrap (persistent): enable APIs; Artifact Registry repository; Workload Identity pool and
  GitLab provider restricted to this project path; CI service account (push images, plan); runtime
  service account (GCS checkpoints); budget $12 with 50/90/100% email alerts.
- Session: GKE Standard zonal cluster, Regular channel, spot node pool (count variable, default 3 x
  `e2-standard-8`), Workload Identity on; Cloud Scheduler job that sends `DELETE` to the GKE API
  for this cluster at now + `TTL_MINUTES`.
- Invariants: no service account keys are created anywhere; `make down` leaves only bootstrap
  resources.
- Red checks: `terraform validate` and `tflint` in `make lint`; `terraform plan` in CI. The
  unverified Scheduler path is proven cheaply: after bootstrap, a Scheduler job targeting a
  non-existent cluster must return 404 (authorized, not found), not 403. `make status ENV=gke`
  lists every billable resource type (clusters, disks, load balancers, IPs) and is non-empty
  after `make down` only if something leaked.
- Owner actions: create project `qoe-spark-streaming`, link billing, then give the go-ahead for
  `make bootstrap`.
- Reverse: `terraform destroy` per stack; the state bucket is deleted by hand only.

### U9. CI images and mirror

- Blockers: U3, U5, U8 bootstrap applied.
- Files: `.gitlab-ci.yml` (build and push jobs, terraform plan), README section on CI.
- Approach: GitLab `id_tokens` exchanged through the Workload Identity pool for a short-lived token
  of the CI service account. Alternative considered: GitLab's Google Cloud IAM integration
  (`identity: google_cloud`), which hides the mechanism; the explicit exchange shows it and works on
  any GitLab.
- Mirror: GitLab push-mirror to `github.com/AlexBormotov/kafka-spark-streaming-analytics-gcp`
  with a GitHub token the owner creates.
- Red check: a pipeline on a branch pushes images tagged with the commit SHA; a job without the
  `id_tokens` block cannot push (negative control, run once and recorded).
- Reverse: revert the commit; delete the WIF provider.

### U10. Cloud run, cost report and decision records

- Blockers: U7, U8, U9; owner go-ahead for the session.
- Steps: `make up ENV=gke`, `make demo`, `make verify-l1`, screenshots, `make down ENV=gke`,
  `make status ENV=gke`.
- Output: `docs/cost-report-l1.md` from billing data; decision records in `docs/adr/` (registry
  choice, wire-format handling, idempotency in ClickHouse rather than in Spark, TTL guard, KEDA
  moved to layer 2, operator install split P1).
- Done when: spec completion step 3 passes and the cost is within AC-02.

## Order

U0 -> U1 -> U2 -> {U3, U4} -> U5 -> U6 -> U7. U8 runs alongside from U0. Then U9, then U10.
