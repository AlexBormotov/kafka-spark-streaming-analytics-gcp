# Research record: layer 1 component versions

Checked 2026-11-05 against primary sources (GitHub releases, Maven Central, PyPI, Docker Hub, Helm
index files, official docs). Items marked **unverified** must be proven by a smoke test before the
work unit that depends on them is considered done.

| Component | Pinned version | Source | Notes |
| --- | --- | --- | --- |
| Strimzi operator | 1.3.1 | https://github.com/strimzi/strimzi-kafka-operator/releases/tag/1.3.1 | Kafka 4.2.x / 4.3.x; KRaft only; CRD API `v1` only; resources on `KafkaNodePool`; needs Kubernetes >= 1.32. Chart `oci://quay.io/strimzi-helm/strimzi-kafka-operator` |
| Kafka | 4.2.2 | same | Spark 4.0 ships kafka-clients 3.9.x; client 3.9 against broker 4.x is **unverified** |
| Apicurio Registry | 3.3.4 | https://github.com/Apicurio/apicurio-registry/releases | No official Helm chart: deploy with a plain Deployment. Storage `kafkasql` reuses Kafka. Confluent-compatible path `/apis/ccompat/v7` and byte compatibility with `confluent-kafka` `AvroSerializer` are **unverified** |
| Spark Operator (Kubeflow) | 2.5.2 | https://github.com/kubeflow/spark-operator/releases | Built and tested on `apache/spark:4.0.4`. Helm repo `https://kubeflow.github.io/spark-operator` is **unverified** |
| Apache Spark | 4.0.5 | https://downloads.apache.org/spark/ , https://hub.docker.com/r/apache/spark/tags | Closest to the operator's tested 4.0.4. Image `apache/spark:4.0.5-python3`. Packages `spark-sql-kafka-0-10_2.13:4.0.5`, `spark-avro_2.13:4.0.5`. 4.3.0 has no official image yet |
| Altinity clickhouse-operator | 0.27.4 | https://github.com/Altinity/clickhouse-operator/releases | Helm repo `https://docs.altinity.com/clickhouse-operator/` |
| ClickHouse server | 26.8 LTS | https://github.com/ClickHouse/ClickHouse/releases | Single replica, non-replicated engines: no Keeper (**unverified**, proven by deploying without it) |
| clickhouse-jdbc | 0.11.0, classifier `all` | https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.11.0/ | No `shaded-all` classifier exists. Fallback 0.9.9 |
| Grafana | 13.3.0, chart `grafana-community/grafana` 14.4.0 | https://github.com/grafana/grafana/releases | Chart `grafana/grafana` is deprecated and frozen at Grafana 12.3.1. Dashboards, datasources and alert rules provisioned from ConfigMaps via sidecars |
| ClickHouse datasource | `grafana-clickhouse-datasource` 4.23.4 | https://github.com/grafana/clickhouse-datasource/releases | |
| Argo CD | 3.5.7, chart `argo/argo-cd` | https://github.com/argoproj/argo-cd/releases | 3.6.x is one day old at time of writing; take the previous minor |
| kind | 0.33.0, node image pinned to Kubernetes 1.36.x | https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0 | kind defaults to 1.37; GKE Regular is 1.36. Pin by digest to match GKE |
| GKE | Regular channel, 1.36 | https://docs.cloud.google.com/kubernetes-engine/docs/release-notes | |
| confluent-kafka (Python) | 2.16.1 | https://pypi.org/project/confluent-kafka/ | Python 3.12 wheel **unverified** |
| fastavro | 1.13.1 | https://pypi.org/project/fastavro/ | |
| Terraform google provider | 8.x | https://github.com/hashicorp/terraform-provider-google/releases | `google_billing_budget` and `google_cloud_scheduler_job` with `oauth_token` exist. Scheduler calling `DELETE` on the GKE API is **unverified** |
| GitLab CI -> GCP | `id_tokens` + Workload Identity Federation | https://docs.gitlab.com/ci/cloud_services/google_cloud/ | GitLab.com also offers the Google Cloud IAM integration (`identity: google_cloud`) |

## Design-relevant findings

1. Spark `from_avro` does not understand the Confluent wire format (magic byte + 4-byte schema id).
   The job strips the 5-byte prefix and passes the reader schema fetched from the registry.
2. Strimzi 1.x rejects `v1beta2` manifests and ZooKeeper configuration found in most tutorials.
3. One Spark version everywhere: pyspark in tests, the container image and `--packages` coordinates.
