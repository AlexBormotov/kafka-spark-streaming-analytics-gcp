# Layer 1 progress

Work units are defined in `plan.md`.

- [x] U0 Repository skeleton and the local quality loop
  - [x] `make lint test` green locally (2026-11-05); negative control: bad Python or YAML -> exit 2
  - [x] same targets green on GitLab CI, first pipeline on `main` (2026-11-05)
  - [x] `.gitlab/CODEOWNERS` username confirmed (`BormotovAV`)
- [x] U1 Local cluster with Argo CD and operators
  - [x] `make up ENV=local` from scratch: exit 0 in 134 s, all 5 Applications Synced/Healthy (2026-11-06)
  - [x] stability: 4 min after `up`, Grafana still on one ReplicaSet (restart loop fixed in c3ba4a6)
  - [x] CI green on 6393009, 173cd5b, c3ba4a6
  - [x] plan amendment A1 (`deploy/values/common/`) approved by owner
  - Known gap: the `up` health wait passes a workload that flaps; stability is checked by hand.
- [x] U2 Kafka, schema registry and topics
  - [x] risk spike passed: confluent-kafka AvroSerializer/Deserializer work against Apicurio
    `/apis/ccompat/v7`; Confluent framing (magic 0 + schema id) confirmed. Karapace not needed.
  - [x] `make up ENV=local` from scratch, exit 0 in 296 s; `make smoke-registry` green (2026-11-06)
  - [x] negative control: global compatibility NONE -> smoke fails with exit 2
  - [x] CI green on 926d06c
  - Found on the way: Apicurio 3 health endpoints are on management port 9000 (404 on 8080).
- [x] U3 Event generator
  - [x] 16 unit tests: Avro v1/v2 validity, identity contract, late share, per-CDN injection,
    hot title, duplicate and malformed shares, Confluent framing, key = session_id
  - [x] mutation check: disabling injection or late events turns exactly its own test red
  - [x] throughput measured in kind: 41-66 k events/s per pod, 0 failures (docs/research)
  - Owner-visible note: one pod covers the nominal 20 k/s, the 10-20 k/s estimate was low.
- [x] U4 ClickHouse schema (plan amendment A2 approved 2026-11-06)
  - [x] `make test-integration`: replaying a micro-batch with rows in a different order leaves raw
    counts, `FINAL` counts, `qoe_10s` and `qoe_1m` unchanged (3 tests, ClickHouse 26.8.33.10)
  - [x] mutation check, each turns the replay test red: no token; MV dedup setting 0; no dedup
    window on the raw table; no dedup window on `qoe_10s`
  - [x] negative control in the suite: without a token the aggregates double count
  - [x] `make dev` in kind: CHI Completed, schema Job applied 6 objects, no Keeper; user `qoe`
    authenticates from the Secret, wrong password and passwordless `default` from another pod
    are rejected
  - [x] CI green on ef497fb and fa8f76f (`test-integration` with a ClickHouse service)
  - [x] security review: Service pinned to ClusterIP, user `qoe` limited to 10.0.0.0/8 (fa8f76f)
- [ ] U5 Spark streaming job
- [ ] U6 Grafana dashboards and alerts
- [ ] U7 `make verify-l1` and the local completion run
- [ ] U8 Terraform and cost guards
- [ ] U9 CI images and mirror
- [ ] U10 Cloud run, cost report and decision records
