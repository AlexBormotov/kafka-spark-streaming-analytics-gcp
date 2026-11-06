"""CLI: python -m qoe_generator --rate 5000 --inject cdn=cdn-b,rebuffer_ratio=0.3"""

import argparse
import logging
import os
import sys
from pathlib import Path

from qoe_generator.model import Anomalies, Injection
from qoe_generator.producer import Config, run


def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(f"GEN_{name}", default)


def parse_args(argv: list[str]) -> Config:
    p = argparse.ArgumentParser(prog="qoe_generator", description=__doc__)
    p.add_argument("--bootstrap", default=_env("BOOTSTRAP", "localhost:9092"))
    p.add_argument("--registry", default=_env("REGISTRY", "http://localhost:8081"))
    p.add_argument("--topic", default=_env("TOPIC", "player-events"))
    p.add_argument(
        "--rate", type=float, default=float(_env("RATE", "1000")), help="events/s per process"
    )
    p.add_argument("--duration-s", type=float, default=_env("DURATION_S"))
    p.add_argument("--sessions", type=int, default=int(_env("SESSIONS", "1000")))
    p.add_argument("--schema", choices=("v1", "v2"), default=_env("SCHEMA", "v1"))
    p.add_argument("--schema-dir", type=Path, default=Path(_env("SCHEMA_DIR", "schemas")))
    p.add_argument("--inject", default=_env("INJECT"), help="cdn=<cdn>,rebuffer_ratio=<0..1>")
    p.add_argument("--late-share", type=float, default=float(_env("LATE_SHARE", "0")))
    p.add_argument("--duplicate-share", type=float, default=float(_env("DUPLICATE_SHARE", "0")))
    p.add_argument("--malformed-share", type=float, default=float(_env("MALFORMED_SHARE", "0")))
    p.add_argument("--hot-title-share", type=float, default=float(_env("HOT_TITLE_SHARE", "0")))
    p.add_argument("--seed", type=int, default=_env("SEED"))
    p.add_argument("--stats-interval-s", type=float, default=float(_env("STATS_INTERVAL_S", "10")))
    p.add_argument(
        "--dry-run", action="store_true", help="no Kafka: measure generation + serialization"
    )
    a = p.parse_args(argv)
    return Config(
        bootstrap=a.bootstrap,
        registry_url=a.registry,
        topic=a.topic,
        rate=a.rate,
        duration_s=float(a.duration_s) if a.duration_s is not None else None,
        sessions=a.sessions,
        schema_version=int(a.schema[1]),
        schema_dir=a.schema_dir,
        injection=Injection.parse(a.inject) if a.inject else None,
        anomalies=Anomalies(a.late_share, a.duplicate_share, a.malformed_share, a.hot_title_share),
        seed=int(a.seed) if a.seed is not None else None,
        stats_interval_s=a.stats_interval_s,
        dry_run=a.dry_run,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = run(parse_args(sys.argv[1:]))
    sys.exit(1 if stats.failed else 0)


if __name__ == "__main__":
    main()
