---
id: QOE-INT-001
title: Real-time video playback quality (QoE) monitoring on GCP
owner: alexvicbor@gmail.com
status: approved by owner on 2026-11-04
review_date: 2026-12-04
---

# QOE-INT-001: Real-time video playback quality (QoE) monitoring on GCP

## Outcome and who wants it

**Business view (simulated).** A video streaming service needs to see playback degradation
(buffering, start failures, bitrate drops) broken down by CDN, ISP, device and title within seconds,
before support tickets arrive and before churn starts. Operations needs to drill down from an alert
to the exact affected sessions. Finance needs a daily batch report of watch minutes for royalty
calculation and retention cohorts.

**Owner view (real).** The owner is building a Senior Data Engineer portfolio. A recruiter or
interviewer will not run the project. They read the README, the architecture diagram, the decision
records, the cost report and a 3-minute demo video. Seniority is shown by the decisions and the
solved problems, not by the list of tools.

## Acceptance criteria

Each criterion keeps its identifier. Specs and work units copy them verbatim.

**Operating envelope**

- **AC-01** The whole platform comes up with `make up`, produces load with `make demo`, and is fully
  removed with `make down`. Data is synthetic only, and its volume is a parameter.
- **AC-02** A cloud session of 15-30 minutes costs at most $12. Only resources that are free or
  near-free while idle persist between sessions: the Terraform state bucket and container images.
- **AC-03** A forgotten cloud session is stopped without the owner: a project budget with email
  alerts, and a time-to-live after which the cluster is deleted even if the owner's machine is off.
- **AC-04** Every layer is first proven on a local Kubernetes cluster with the same charts and
  operators. A cloud run happens only after the local run passes its checks.

**Layer 1: streaming core (Kafka -> Spark -> ClickHouse -> Grafana)**

- **AC-10** A player-event generator produces 20-50 thousand events per second at nominal load,
  scaled by a parameter, serialized with a schema held in a schema registry.
- **AC-11** Freshness: the 95th percentile of the delay from event time to availability in the
  analytical store is at most 10 seconds at nominal load, and is visible on a dashboard.
- **AC-12** Detection: when the generator injects a degradation for one CDN, a dashboard alert
  fires within 60 seconds of the injection start. Without injection, the alert does not fire.
- **AC-13** Late and out-of-order events, duplicates, schema evolution with a dead-letter path,
  and hot-title skew are each demonstrated and explained.

**Layer 2: CDC and search**

- **AC-20** Subscription and plan data changes in an operational database reach the event stream
  as change events and enrich player events.
- **AC-21** From an alert, the operator opens a search UI showing the concrete affected sessions,
  with full-text search over player error messages.
- **AC-22** A searchable title catalog with autocomplete is kept in sync with the operational
  database by change data capture.
- **AC-23** Index mapping control, index lifecycle with rollover, idempotent indexing, and
  zero-downtime reindexing are each demonstrated and explained.

**Layer 3: cold storage and batch reporting**

- **AC-30** Raw history is kept in an open table format on object storage and can be replayed.
- **AC-31** A daily batch loads the closed day into the data warehouse and builds a royalty report
  (watch minutes by title, country and plan) and retention cohorts, with data-quality assertions.
- **AC-32** Small-file compaction, backfill and replay are scheduled batch jobs.

**Engineering practice**

- **AC-40** Infrastructure is code: one small module for the cluster and the cloud resources
  around it; everything inside the cluster is deployed by GitOps.
- **AC-41** CI runs on every change with the same commands a contributor runs locally. Primary
  repository on GitLab, mirrored to GitHub, both public.
- **AC-42** No long-lived cloud keys anywhere: CI and workloads authenticate with workload identity.
- **AC-43** The README contains an architecture diagram, 3-5 decision records, a cost report of a
  real run, and a 3-minute demo video, plus screenshots of the infrastructure and dashboards.
- **AC-44** Everything committed to the repository is written in English.

## Out of scope

- Real user data or production traffic.
- Multi-region or multi-cloud deployment (EKS, AKS).
- High availability beyond what a single spot-node cluster provides.
- A long-running environment: the platform exists only for the duration of a session.
- A video player or end-user UI.

## Order of delivery

Layers are delivered in order 1, 2, 3. Each layer is a working demo before the next one starts.
A spec and a plan are written for one layer at a time.
