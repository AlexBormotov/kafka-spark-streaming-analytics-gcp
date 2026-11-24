"""Layer 1 completion checks (spec QOE-SPEC-L1, `make verify-l1`).

Drives the generator through Helm and reads results from ClickHouse, Kafka and the Grafana API.
Prints one line per check in the spec's order and exits with the number of the first failed
check (1-8), or 0 when all pass.

    python scripts/verify_l1.py --context kind-qoe --replicas 2 --rate-per-pod 3000
    python scripts/verify_l1.py --context kind-qoe --only 4      # one check
"""

import argparse
import base64
import json
import subprocess
import sys
import time
import urllib.request
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CH_POD = ("clickhouse", "chi-qoe-qoe-0-0-0")
KAFKA_POD = ("kafka", "qoe-dual-0")
DRIVER_POD = ("streaming", "qoe-streaming-driver")
CHECKPOINT = "/checkpoint/qoe-streaming"
GRAFANA_PORT = 3301

WARMUP_S = 20
MEASURE_S = 120
DETECT_S = 60
ANOMALY_S = 60
FRESHNESS_P95_S = 10
RATE_TOLERANCE = 0.95
INJECT_CDN = "cdn-b"

NAMES = {
    1: "throughput",
    2: "freshness",
    3: "negative control",
    4: "detection",
    5: "idempotency",
    6: "late events",
    7: "schema evolution",
    8: "dead letter",
}

# Queries take values as ClickHouse query parameters ({name:Type}), never by string formatting.
# `since` is a Unix time in milliseconds.
Q_STORED_BETWEEN = (
    "SELECT count() FROM qoe.player_events_raw "
    "WHERE ingest_ts >= fromUnixTimestamp64Milli({since:Int64}) "
    "AND ingest_ts < fromUnixTimestamp64Milli({until:Int64})"
)
Q_STORED_SINCE = (
    "SELECT count() FROM qoe.player_events_raw "
    "WHERE ingest_ts >= fromUnixTimestamp64Milli({since:Int64})"
)
Q_FRESHNESS_P95_S = (
    "SELECT quantileExact(0.95)(dateDiff('millisecond', event_ts, ingest_ts)) / 1000 "
    "FROM qoe.player_events_raw WHERE ingest_ts >= fromUnixTimestamp64Milli({since:Int64})"
)
Q_SNAPSHOT = (
    "SELECT (SELECT count() FROM qoe.player_events_raw), "
    "(SELECT uniqExact(event_id) FROM qoe.player_events_raw), "
    "(SELECT sum(events) FROM qoe.qoe_10s), (SELECT sum(rebuffers) FROM qoe.qoe_10s), "
    "(SELECT sum(events) FROM qoe.qoe_1m), (SELECT count() FROM qoe.late_events)"
)
Q_BATCH_ROWS = "SELECT max(input_rows) FROM qoe.stream_progress WHERE batch_id = {batch:UInt64}"
Q_BATCH_REPLAYED = (
    "SELECT count() FROM qoe.stream_progress WHERE batch_id = {batch:UInt64} "
    "AND ts >= fromUnixTimestamp64Milli({since:Int64})"
)
Q_LATE_SINCE = (
    "SELECT count() FROM qoe.late_events WHERE ingest_ts >= fromUnixTimestamp64Milli({since:Int64})"
)
Q_LATE_LEAKED = (
    "SELECT count() FROM qoe.player_events_raw WHERE event_id IN "
    "(SELECT event_id FROM qoe.late_events "
    "WHERE ingest_ts >= fromUnixTimestamp64Milli({since:Int64}))"
)
Q_V2_SINCE = Q_STORED_SINCE + " AND player_version IS NOT NULL"


def ms(t: float) -> int:
    return int(t * 1000)


class Cluster:
    def __init__(self, context: str):
        self.context = context

    def run(self, *args: str) -> str:
        out = subprocess.run(args, capture_output=True, text=True, check=False)  # noqa: S603
        if out.returncode != 0:
            raise RuntimeError(f"{' '.join(args[:6])}...: {out.stderr.strip()[-400:]}")
        return out.stdout

    def kubectl(self, *args: str) -> str:
        return self.run("kubectl", "--context", self.context, *args)

    def exec(self, pod: tuple[str, str], *cmd: str) -> str:
        ns, name = pod
        container = ["-c", "spark-kubernetes-driver"] if pod == DRIVER_POD else []
        return self.kubectl("-n", ns, "exec", name, *container, "--", *cmd)

    def sql(self, query: str, **params: int) -> list[list[str]]:
        args = [f"--param_{k}={v}" for k, v in params.items()]
        out = self.exec(CH_POD, "clickhouse-client", "--format", "TSV", *args, "-q", query)
        return [line.split("\t") for line in out.splitlines()]

    def scalar(self, query: str, **params: int) -> float:
        return float(self.sql(query, **params)[0][0])

    def generator(self, replicas: int, rate: int, **anomalies) -> None:
        """Every value explicit: no state carries over from an earlier run."""
        values = {
            "replicas": replicas,
            "ratePerPod": rate,
            "schema": anomalies.get("schema", "v1"),
            "inject": anomalies.get("inject", "").replace(",", "\\,"),
            "anomalies.lateShare": anomalies.get("late", 0),
            "anomalies.malformedShare": anomalies.get("malformed", 0),
            "anomalies.duplicateShare": 0,
            "anomalies.hotTitleShare": 0,
        }
        chart = str(ROOT / "deploy" / "charts" / "generator")
        sets = [f"--set={k}={v}" for k, v in values.items()]
        self.run(
            "helm", "--kube-context", self.context, "upgrade", "generator", chart,
            "-n", "generator", "--reset-values", *sets, "--wait", "--timeout", "5m",
        )  # fmt: skip

    def end_offsets(self, topic: str) -> dict[str, int]:
        out = self.exec(
            KAFKA_POD, "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server", "localhost:9092", "--topic", topic,
        )  # fmt: skip
        rows = (line.rsplit(":", 2) for line in out.split())
        return {partition: int(offset) for _, partition, offset in rows}

    def dlq_size(self) -> int:
        return sum(self.end_offsets("player-events-dlq").values())

    def drained_batch(self) -> int | None:
        """The last committed batch once nothing is left to process, else None.

        Progress rows alone are not enough: `ts` is when a batch started, so one can still be
        running, and a stopping generator pod flushes a last few records.
        """
        if self.kubectl("-n", "generator", "get", "pods", "-o", "name").strip():
            return None
        planned, committed = (
            max(int(n) for n in self.exec(DRIVER_POD, "ls", f"{CHECKPOINT}/{d}").split())
            for d in ("offsets", "commits")
        )
        if planned != committed:
            return None
        offsets = self.exec(DRIVER_POD, "cat", f"{CHECKPOINT}/offsets/{planned}").splitlines()[-1]
        if json.loads(offsets)["player-events"] != self.end_offsets("player-events"):
            return None
        # The listener writes a batch's progress row after its commit.
        return planned if self.scalar(Q_BATCH_ROWS, batch=planned) else None

    def spark_state(self) -> str:
        return self.kubectl(
            "-n", "streaming", "get", "sparkapplication", "qoe-streaming",
            "-o", "jsonpath={.status.applicationState.state}",
        )  # fmt: skip


class Grafana:
    def __init__(self, cluster: Cluster):
        secret = cluster.kubectl(
            "-n", "monitoring", "get", "secret", "grafana-admin", "-o", "jsonpath={.data}"
        )  # fmt: skip
        data = {k: base64.b64decode(v).decode() for k, v in json.loads(secret).items()}
        token = base64.b64encode(f"{data['admin-user']}:{data['admin-password']}".encode())
        self._headers = {"Authorization": f"Basic {token.decode()}"}

    def firing(self) -> set[str]:
        """CDNs whose QoE alert is Firing (Grafana state `Alerting`)."""
        url = f"http://localhost:{GRAFANA_PORT}/api/prometheus/grafana/api/v1/alerts"
        req = urllib.request.Request(url, headers=self._headers)
        with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310 (localhost)
            alerts = json.load(resp)["data"]["alerts"]
        return {a["labels"].get("cdn", "?") for a in alerts if a["state"] == "Alerting"}


@contextmanager
def port_forward(cluster: Cluster):
    cmd = ["kubectl", "--context", cluster.context, "-n", "monitoring", "port-forward",
           "svc/grafana", f"{GRAFANA_PORT}:80"]  # fmt: skip
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)  # noqa: S603
    try:
        time.sleep(3)
        yield
    finally:
        proc.terminate()


def wait_for(condition, timeout_s: float, step_s: float = 5):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(step_s)
    return None


class Run:
    def __init__(self, args):
        self.args = args
        self.cluster = Cluster(args.context)
        self.grafana = Grafana(self.cluster)
        self.target = args.replicas * args.rate_per_pod
        self.results: dict[int, bool] = {}

    def record(self, number: int, ok: bool, detail: str) -> None:
        self.results[number] = ok
        print(f"{'PASS' if ok else 'FAIL'} {number} {NAMES[number]}: {detail}", flush=True)

    def wanted(self, *numbers: int) -> bool:
        return not self.args.only or any(n in self.args.only for n in numbers)

    def load(self, **anomalies) -> None:
        self.cluster.generator(self.args.replicas, self.args.rate_per_pod, **anomalies)

    def stop_load(self) -> None:
        self.cluster.generator(0, self.args.rate_per_pod)

    def clean_load(self) -> None:
        """Checks 1-3: a clean run at the configured rate."""
        c = self.cluster
        self.load()
        time.sleep(WARMUP_S)
        start, firing = time.time(), set()
        while time.time() - start < MEASURE_S:
            firing |= self.grafana.firing()
            time.sleep(5)
        end = time.time()
        rate = c.scalar(Q_STORED_BETWEEN, since=ms(start), until=ms(end)) / (end - start)
        if self.wanted(1):
            self.record(
                1,
                rate >= RATE_TOLERANCE * self.target,
                f"{rate:,.0f} events/s stored over {end - start:.0f} s, target {self.target:,} "
                f"({self.args.replicas} x {self.args.rate_per_pod:,})",
            )
        if self.wanted(2):
            p95 = c.scalar(Q_FRESHNESS_P95_S, since=ms(end - MEASURE_S))
            self.record(
                2,
                p95 <= FRESHNESS_P95_S,
                f"p95 {p95:.1f} s over the last 2 min (limit {FRESHNESS_P95_S} s)",
            )
        if self.wanted(3):
            self.record(3, not firing, f"Firing during the clean run: {sorted(firing) or 'none'}")

    def detection(self) -> None:
        """Check 4: an injected CDN degradation fires its alert within 60 s."""
        if not self.wanted(1, 2, 3):  # no clean run before: start the load here
            self.load()
            time.sleep(WARMUP_S)
        start = time.monotonic()
        self.load(inject=f"cdn={INJECT_CDN},rebuffer_ratio=0.3")
        left = DETECT_S - (time.monotonic() - start)
        fired = wait_for(lambda: INJECT_CDN in self.grafana.firing(), left, step_s=2)
        elapsed = time.monotonic() - start
        detail = (
            f"{INJECT_CDN} Firing {elapsed:.0f} s after the generator upgrade (limit {DETECT_S} s)"
            if fired
            else f"{INJECT_CDN} not Firing within {DETECT_S} s"
        )
        self.record(4, bool(fired), detail)

    def idempotency(self) -> None:
        """Check 5: Spark re-runs its last micro-batch; every result stays the same.

        Deleting the last commit marker makes Spark replay that batch from its stored offsets
        on restart: the real recovery path, not a test double.
        """
        c = self.cluster
        self.stop_load()
        batch = wait_for(c.drained_batch, 180)
        if batch is None:
            self.record(5, False, "Spark did not drain the topic within 180 s")
            return
        before = c.sql(Q_SNAPSHOT)
        rows = c.scalar(Q_BATCH_ROWS, batch=batch)
        # With its checksum: a stale .crc makes Spark fail when it writes the commit again.
        marker = [f"{CHECKPOINT}/commits/{batch}", f"{CHECKPOINT}/commits/.{batch}.crc"]
        c.exec(DRIVER_POD, "rm", *marker)
        restarted = time.time()
        c.kubectl("-n", "streaming", "delete", "pod", DRIVER_POD[1], "--wait=false")
        replayed = wait_for(
            lambda: c.scalar(Q_BATCH_REPLAYED, batch=batch, since=ms(restarted)), 300
        )
        if not replayed:
            self.record(5, False, f"batch {batch} was not replayed within 300 s")
            return
        after = c.sql(Q_SNAPSHOT)
        self.record(
            5,
            after == before,
            f"batch {batch} ({rows:,.0f} rows) replayed; raw count, uniq, qoe_10s events and "
            f"rebuffers, qoe_1m events, late: {before[0]} -> {after[0]}",
        )

    def anomalies(self) -> None:
        """Checks 6-8: late events, schema v2 and malformed records in one run."""
        c = self.cluster
        dlq_before, start = c.dlq_size(), time.time()
        self.load(schema="v2", late=0.02, malformed=0.01)
        time.sleep(ANOMALY_S)
        recent = c.scalar(Q_STORED_SINCE, since=ms(time.time() - 10))
        state = c.spark_state()
        running = state == "RUNNING" and recent > 0
        if self.wanted(6):
            late = c.scalar(Q_LATE_SINCE, since=ms(start))
            leaked = c.scalar(Q_LATE_LEAKED, since=ms(start))
            self.record(
                6,
                late > 0 and leaked == 0,
                f"{late:,.0f} late events in late_events, {leaked:,.0f} of them in the on-time "
                "table",
            )
        if self.wanted(7):
            v2 = c.scalar(Q_V2_SINCE, since=ms(start))
            self.record(
                7,
                v2 > 0 and running,
                f"{v2:,.0f} v2 events stored; Spark {state}, {recent:,.0f} events in the last 10 s",
            )
        if self.wanted(8):
            dead = c.dlq_size() - dlq_before
            self.record(
                8,
                dead > 0 and running,
                f"{dead:,} records added to player-events-dlq; pipeline running: {running}",
            )
        self.stop_load()

    def main(self) -> int:
        state = self.cluster.spark_state()
        if state != "RUNNING":
            print(f"SparkApplication qoe-streaming is {state or 'missing'}: run `make up` first")
            return 1
        with port_forward(self.cluster):
            if self.wanted(1, 2, 3):
                self.clean_load()
            if self.wanted(4):
                self.detection()
        if self.wanted(5):
            self.idempotency()
        elif self.wanted(4):
            self.stop_load()
        if self.wanted(6, 7, 8):
            self.anomalies()
        failed = [n for n, ok in sorted(self.results.items()) if not ok]
        print(f"verify-l1: {len(self.results) - len(failed)}/{len(self.results)} passed")
        return failed[0] if failed else 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--context", required=True, help="kube context")
    p.add_argument("--replicas", type=int, default=2)
    p.add_argument("--rate-per-pod", type=int, default=3000)
    p.add_argument("--only", type=int, nargs="*", choices=sorted(NAMES), help="check numbers")
    return Run(p.parse_args()).main()


if __name__ == "__main__":
    sys.exit(main())
