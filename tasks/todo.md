# Layer 1 progress

Work units are defined in `plan.md`.

- [x] U0 Repository skeleton and the local quality loop
  - [x] `make lint test` green locally (2026-11-05); negative control: bad Python or YAML -> exit 2
  - [x] same targets green on GitLab CI, first pipeline on `main` (2026-11-05)
  - [x] `.gitlab/CODEOWNERS` username confirmed (`BormotovAV`)
- [ ] U1 Local cluster with Argo CD and operators
- [ ] U2 Kafka, schema registry and topics
- [ ] U3 Event generator
- [ ] U4 ClickHouse schema
- [ ] U5 Spark streaming job
- [ ] U6 Grafana dashboards and alerts
- [ ] U7 `make verify-l1` and the local completion run
- [ ] U8 Terraform and cost guards
- [ ] U9 CI images and mirror
- [ ] U10 Cloud run, cost report and decision records
