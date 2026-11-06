"""Synthetic playback sessions and the player events they emit.

Rebuffer ratio, the QoE signal the alert watches, is defined here as
REBUFFER events / (HEARTBEAT + REBUFFER events): the share of playback ticks that stalled.
"""

import random
import uuid
from dataclasses import dataclass
from enum import Enum

CDNS = ("cdn-a", "cdn-b", "cdn-c")
ISPS = tuple(f"isp-{i}" for i in range(1, 9))
DEVICES = ("tv", "mobile", "web", "console")
COUNTRIES = ("US", "DE", "GB", "BR", "IN", "JP")
TITLES = tuple(f"t-{i:04d}" for i in range(1, 501))
HOT_TITLE = "t-0000"
BITRATES_KBPS = (800, 1600, 3000, 4500, 8000)

BASE_REBUFFER_P = 0.02
ERROR_P = 0.002
BITRATE_CHANGE_P = 0.03
EVENTS_PER_SESSION = (50, 400)
ON_TIME_JITTER_MS = 2_000
LATE_DELAY_MS = (40_000, 120_000)


class RecordKind(Enum):
    NORMAL = "normal"
    DUPLICATE = "duplicate"
    MALFORMED = "malformed"


@dataclass(frozen=True)
class Injection:
    """Degradation forced on one CDN: its rebuffer ratio becomes `rebuffer_ratio`."""

    cdn: str
    rebuffer_ratio: float = 0.3

    @classmethod
    def parse(cls, spec: str) -> "Injection":
        """Parse `cdn=cdn-b,rebuffer_ratio=0.3`."""
        fields = dict(part.split("=", 1) for part in spec.split(",") if part)
        if fields.get("cdn") not in CDNS:
            raise ValueError(f"inject needs cdn= one of {CDNS}, got {spec!r}")
        return cls(cdn=fields["cdn"], rebuffer_ratio=float(fields.get("rebuffer_ratio", 0.3)))


@dataclass(frozen=True)
class Anomalies:
    late_share: float = 0.0
    duplicate_share: float = 0.0
    malformed_share: float = 0.0
    hot_title_share: float = 0.0


NO_ANOMALIES = Anomalies()


@dataclass
class _Session:
    session_id: str
    user_id: str
    title_id: str
    cdn: str
    isp: str
    device: str
    country: str
    bitrate_kbps: int
    remaining: int
    started: bool = False


def record_kind(rng: random.Random, anomalies: Anomalies) -> RecordKind:
    r = rng.random()
    if r < anomalies.malformed_share:
        return RecordKind.MALFORMED
    if r < anomalies.malformed_share + anomalies.duplicate_share:
        return RecordKind.DUPLICATE
    return RecordKind.NORMAL


class EventFactory:
    """Keeps a pool of concurrent sessions and emits one event per call."""

    def __init__(
        self,
        rng: random.Random,
        sessions: int = 1_000,
        injection: Injection | None = None,
        anomalies: Anomalies = NO_ANOMALIES,
        schema_version: int = 1,
    ):
        self._rng = rng
        self._injection = injection
        self._anomalies = anomalies
        self._schema_version = schema_version
        self._pool = [self._new_session() for _ in range(sessions)]

    def _uuid(self) -> str:
        return str(uuid.UUID(int=self._rng.getrandbits(128), version=4))

    def _new_session(self) -> _Session:
        rng = self._rng
        hot = rng.random() < self._anomalies.hot_title_share
        return _Session(
            session_id=self._uuid(),
            user_id=f"u-{rng.randrange(200_000):06d}",
            title_id=HOT_TITLE if hot else rng.choice(TITLES),
            cdn=rng.choice(CDNS),
            isp=rng.choice(ISPS),
            device=rng.choice(DEVICES),
            country=rng.choice(COUNTRIES),
            bitrate_kbps=rng.choice(BITRATES_KBPS),
            remaining=rng.randint(*EVENTS_PER_SESSION),
        )

    def _degraded(self, s: _Session) -> bool:
        return self._injection is not None and s.cdn == self._injection.cdn

    def _event_ts(self, now_ms: int) -> int:
        if self._rng.random() < self._anomalies.late_share:
            return now_ms - self._rng.randint(*LATE_DELAY_MS)
        return now_ms - self._rng.randint(0, ON_TIME_JITTER_MS)

    def next_event(self, now_ms: int) -> dict:
        rng = self._rng
        i = rng.randrange(len(self._pool))
        s = self._pool[i]
        degraded = self._degraded(s)

        fields: dict = {"bitrate_kbps": s.bitrate_kbps}
        if not s.started:
            event_type = "START"
            s.started = True
            fields["startup_ms"] = (
                rng.randint(2_000, 8_000) if degraded else rng.randint(300, 2_500)
            )
        elif s.remaining <= 0:
            event_type = "STOP"
            self._pool[i] = self._new_session()
        else:
            s.remaining -= 1
            r = rng.random()
            if r < ERROR_P:
                event_type = "ERROR"
                fields["error_code"] = rng.choice(("E_NETWORK", "E_DECODE", "E_DRM", "E_MANIFEST"))
                fields["error_message"] = f"{fields['error_code']}: playback failed on {s.cdn}"
            elif r < ERROR_P + BITRATE_CHANGE_P:
                event_type = "BITRATE_CHANGE"
                s.bitrate_kbps = BITRATES_KBPS[0] if degraded else rng.choice(BITRATES_KBPS)
                fields["bitrate_kbps"] = s.bitrate_kbps
            else:
                p = self._injection.rebuffer_ratio if degraded else BASE_REBUFFER_P
                if rng.random() < p:
                    event_type = "REBUFFER"
                    fields["rebuffer_ms"] = rng.randint(500, 6_000)
                else:
                    event_type = "HEARTBEAT"

        event = {
            "event_id": self._uuid(),
            "event_ts": self._event_ts(now_ms),
            "session_id": s.session_id,
            "user_id": s.user_id,
            "title_id": s.title_id,
            "cdn": s.cdn,
            "isp": s.isp,
            "device": s.device,
            "country": s.country,
            "event_type": event_type,
            "bitrate_kbps": None,
            "rebuffer_ms": None,
            "startup_ms": None,
            "error_code": None,
            "error_message": None,
        }
        event.update(fields)
        if self._schema_version == 2:
            event["player_version"] = "2.4.1"
        return event
