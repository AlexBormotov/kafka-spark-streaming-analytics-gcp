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
- [x] U5 Spark streaming job (plan amendment A3 approved 2026-11-10)
  - [x] 5 unit tests (`test_batch.py`, local PySpark 4.0.5 + Java 17): v1, v2, late, duplicate,
    malformed Avro, wrong magic byte, unknown schema id, deterministic partition routing on replay
  - [x] `make test-integration` (`test_sink_replay.py`): 3 000 on-time + 30 late events replayed
    with reordered rows and different input slices leave `player_events_raw`, `late_events` and
    `qoe_10s` unchanged
  - [x] mutation checks: removing `dropDuplicates` turns both the duplicate count and the partition
    replay tests red; removing `insert_deduplication_token` in `sink.py` turns `test_sink_replay`
    red (`raw` 4 492 vs 3 000, `qoe_10s` doubled)
  - [x] `make dev` in kind: `SparkApplication` `RUNNING`, 263 487 events (`uniqExact == count`,
    41 487 schema v2), 879 `late_events`, 475 dead letters in `player-events-dlq` with
    `error:avro-decode` and `source` headers, p95 freshness 6.9 s; 1 executor (2 cores) sustains
    ~9.1 k events/s through the Python writer (docs/research)
  - [x] CI green on 983acaa (`lint`, `test`, `test-integration`)
- [x] U6 Grafana dashboards and alerts (plan amendment A4 approved 2026-11-23)
  - [x] `make test-integration` (`test_dashboard_queries.py`): all 12 dashboard queries return rows
    on the real schema; the alert query returns the degraded CDN above 0.1, nothing for clean
    data, under 50 ticks or older than 30 s
  - [x] mutation checks: dropping the 50-tick minimum, the 30 s window, or breaking the ratio or a
    table name each turn one test red
  - [x] kind: datasource healthy as read-only `grafana` (`readonly = 1`, only `max_execution_time`
    <= 60 changeable; INSERT, DROP and `url()` rejected with code 164); all 12 panels return data
    through `/api/ds/query`
  - [x] kind, 3 000 events/s: 2 min clean, all CDNs `Normal` (ratio ~0.02); `inject cdn=cdn-b`
    Firing for `cdn-b` only 15 s after `helm upgrade`; back to `Normal` ~45 s after removal
  - [x] `stream_progress`: lag per partition and dead letters (3 970 at malformed share 0.005 of
    790 250 events, `uniqExact == count`), 7 980 late events
  - [x] CI green on 84be768 (`lint`, `test`, `test-integration`); security review fixes:
    no `readonly = 2` (table functions), `max_execution_time` bounded to 1-60 s
- [ ] U7 `make verify-l1` and the local completion run
- [ ] U8 Terraform and cost guards
- [ ] U9 CI images and mirror
- [ ] U10 Cloud run, cost report and decision records

## Later

- [ ] README: explain the write path (token per partition, A2/A3) and the per-batch watermark (A3)
- [ ] U7: local demo rate must fit one executor (~9.1 k events/s measured in U5): pick <= 8 k/s
  or add an executor, and record the chosen rate
- [ ] U8/U10: `deploy/charts/streaming/values-gke.yaml` sized for 20 k events/s nominal
  (>= 3 executors at ~9 k/s each), re-measured on the first cloud run
