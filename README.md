# Real-time video QoE monitoring on GCP

A streaming platform that shows video playback degradation (buffering, start failures, bitrate
drops) by CDN, ISP, device and title within seconds.

Kafka (Strimzi, KRaft) with a schema registry, Spark Structured Streaming, ClickHouse and Grafana
alerts, running on GKE spot nodes, deployed by Argo CD, provisioned by Terraform.

Status: layer 1 (streaming core) in progress. Requirements: [intent.md](intent.md).

## Development

Requirements are pinned in [.tool-versions](.tool-versions).

```bash
make help    # list targets
make lint    # static checks, same as CI
make test    # unit tests, same as CI
```
