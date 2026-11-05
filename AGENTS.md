# AGENTS.md

Working agreement for any agent in this repository. Vendor-neutral. `CLAUDE.md` only imports this file.

## What this repository is

Portfolio pet project: real-time video QoE monitoring on GCP. Source of requirements: `intent.md`.
Working rules: `.agents/skills/` (`cherny-workflow`, `simplicity-ladder`, `grilling`, `domain-modeling` and others).

## Approval chain, before any code

1. `intent.md`, approved by the owner.
2. `spec.md`, with a checkable completion condition per layer, approved by the owner.
3. `plan.md`, per layer: approaches, files touched, invariants, the check that goes red, how to reverse. Approved by the owner.

No implementation starts before the artifact above it is approved. If implementation contradicts a fact in `plan.md`, or touches a file outside its approved set: stop, record the evidence in `plan.md`, return it for approval.

## Build order

Each layer must be a working demo before the next starts.

1. Kafka -> Spark Structured Streaming -> ClickHouse -> Grafana.
2. Debezium (Postgres CDC) and Elasticsearch/Kibana.
3. Iceberg on GCS, BigQuery, Dataform, Airflow batch.

## Hard rules

- Synthetic data only. Volume is a parameter.
- Session cost ceiling: $12 per 15-30 minute session. Persistent resources only if free while idle (state bucket, registry images).
- Every cloud session ends with `make down`. Budget alert and auto-destroy timer must exist before the first paid run.
- Never run `terraform apply`, `make up` or any billable cloud command without the owner's explicit go-ahead in the current session.
- Cloud commands target only the GCP project `qoe-spark-streaming`. Pass `--project` explicitly; never rely on the gcloud default.
- One command per task in the `Makefile`; CI runs the same command a contributor runs.
- Skills live in `.agents/skills/`; `.claude/skills` is a symlink to it, never a copy.

## Commands

Not yet defined. Planned surface: `make up`, `make demo`, `make down`, `make test`, `make lint`.

## Task tracking

- Plan and checkable items: `tasks/todo.md`.
- Corrections from the owner: `tasks/lessons.md`. Read it at session start.
