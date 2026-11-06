import json
import random
import uuid
from collections import Counter
from pathlib import Path

import fastavro
import pytest
from qoe_generator.model import (
    CDNS,
    HOT_TITLE,
    LATE_DELAY_MS,
    ON_TIME_JITTER_MS,
    Anomalies,
    EventFactory,
    Injection,
    RecordKind,
    record_kind,
)

SCHEMAS = Path(__file__).resolve().parents[3] / "schemas"
NOW_MS = 1_762_400_000_000
N = 20_000


def events(n=N, seed=7, **kwargs) -> list[dict]:
    factory = EventFactory(random.Random(seed), sessions=500, **kwargs)
    return [factory.next_event(NOW_MS) for _ in range(n)]


def rebuffer_ratio(evts: list[dict]) -> float:
    counts = Counter(e["event_type"] for e in evts)
    return counts["REBUFFER"] / (counts["REBUFFER"] + counts["HEARTBEAT"])


@pytest.mark.parametrize("version", [1, 2])
def test_events_match_the_avro_schema(version):
    schema = fastavro.parse_schema(
        json.loads((SCHEMAS / f"player_event.v{version}.avsc").read_text())
    )
    for e in events(2_000, schema_version=version):
        assert fastavro.validate(e, schema, raise_errors=True)


def test_event_identity_contract():
    evts = events(5_000)
    ids = [e["event_id"] for e in evts]
    assert len(set(ids)) == len(ids)
    assert all(uuid.UUID(i).version == 4 for i in ids)
    assert all(isinstance(e["event_ts"], int) for e in evts)


def test_on_time_events_lag_at_most_the_jitter():
    lags = [NOW_MS - e["event_ts"] for e in events()]
    assert 0 <= min(lags) and max(lags) <= ON_TIME_JITTER_MS


def test_late_share():
    evts = events(anomalies=Anomalies(late_share=0.1))
    late = [NOW_MS - e["event_ts"] for e in evts if NOW_MS - e["event_ts"] > ON_TIME_JITTER_MS]
    assert len(late) / len(evts) == pytest.approx(0.1, abs=0.01)
    assert all(LATE_DELAY_MS[0] <= lag <= LATE_DELAY_MS[1] for lag in late)


def test_baseline_rebuffer_ratio_is_low_on_every_cdn():
    evts = events(60_000)
    for cdn in CDNS:
        assert rebuffer_ratio([e for e in evts if e["cdn"] == cdn]) < 0.05


def test_injection_degrades_only_the_target_cdn():
    evts = events(60_000, injection=Injection.parse("cdn=cdn-b,rebuffer_ratio=0.3"))
    by_cdn = {cdn: rebuffer_ratio([e for e in evts if e["cdn"] == cdn]) for cdn in CDNS}
    assert by_cdn["cdn-b"] == pytest.approx(0.3, abs=0.03)
    assert by_cdn["cdn-a"] < 0.05 and by_cdn["cdn-c"] < 0.05


def test_injection_spec_rejects_unknown_cdn():
    with pytest.raises(ValueError):
        Injection.parse("cdn=cdn-z")


def test_hot_title_share():
    # Sessions are drawn once and replaced on STOP, so the share holds per session, not per event.
    evts = events(60_000, anomalies=Anomalies(hot_title_share=0.3))
    sessions = {e["session_id"]: e["title_id"] for e in evts}
    hot = sum(t == HOT_TITLE for t in sessions.values()) / len(sessions)
    assert hot == pytest.approx(0.3, abs=0.05)


def test_record_kind_shares():
    rng = random.Random(1)
    a = Anomalies(duplicate_share=0.05, malformed_share=0.02)
    kinds = Counter(record_kind(rng, a) for _ in range(N))
    assert kinds[RecordKind.DUPLICATE] / N == pytest.approx(0.05, abs=0.01)
    assert kinds[RecordKind.MALFORMED] / N == pytest.approx(0.02, abs=0.005)


def test_same_seed_same_stream():
    assert events(500, seed=3) == events(500, seed=3)
