"""
Deterministic offline replay of captured sessions (no network).

    events = load_sessions([dir1, dir2])        # normalized events, gaps, failures, corruption report
    for ev in Replayer(events.events):           # event-by-event, as fast as possible
    for ev in Replayer(events.events, speed=1.0) # real time (sleeps between receive times)
    for ev in Replayer(events.events, speed=20)  # 20x faster than real time

Order is the ORIGINAL ARRIVAL order: within a session by ingest_seq (the collector's arrival counter),
sessions by their first arrival. Receive times are never rewritten, so the same capture always
produces the same stream and therefore the same features (tested).
"""
import time
from dataclasses import dataclass, field

from market_data.gaps import Gap
from market_data.storage import read_session
from market_data.types import MarketEvent, ParseFailure, RawMessage


@dataclass
class LoadedSessions:
    events: list = field(default_factory=list)
    raw: list = field(default_factory=list)
    gaps: list = field(default_factory=list)
    failures: list = field(default_factory=list)
    corrupt: list = field(default_factory=list)
    invalid: list = field(default_factory=list)
    session_ids: list = field(default_factory=list)


def load_sessions(session_dirs, include_raw=False):
    out = LoadedSessions()
    blocks = []
    for d in session_dirs:
        r = read_session(d)
        evs = []
        out.corrupt += [(d,) + c for c in r.corrupt]
        for kind, data in r.records:
            try:
                if kind == "event":
                    evs.append(MarketEvent.from_dict(data))
                elif kind == "gap":
                    out.gaps.append(Gap.from_dict(data))
                elif kind == "failure":
                    out.failures.append(ParseFailure.from_dict(data))
                elif kind == "raw" and include_raw:
                    out.raw.append(RawMessage.from_dict(data))
            except (TypeError, ValueError, KeyError) as e:
                out.invalid.append((d, kind, str(e)[:120]))
        evs.sort(key=lambda e: e.ingest_seq)
        sid = evs[0].session_id if evs else d
        blocks.append(((evs[0].receive_ts_ms if evs else 0), sid, evs))
        out.session_ids.append(sid)
    for _first, _sid, evs in sorted(blocks, key=lambda b: (b[0], b[1])):
        out.events += evs
    out.gaps.sort(key=lambda g: (g.start_ts_ms, g.source, g.stream))
    return out


class Replayer:
    def __init__(self, events, speed=None, sleeper=time.sleep, max_sleep_s=5.0):
        if speed is not None and speed <= 0:
            raise ValueError("speed must be > 0 (or None for as-fast-as-possible)")
        self.events = list(events)
        self.speed, self.sleeper, self.max_sleep_s = speed, sleeper, max_sleep_s
        self.slept_s = 0.0

    def __iter__(self):
        prev = None
        for e in self.events:
            if self.speed is not None and prev is not None:
                dt_s = (e.receive_ts_ms - prev) / 1000.0 / self.speed
                if dt_s > 0:
                    dt_s = min(dt_s, self.max_sleep_s)
                    self.sleeper(dt_s)
                    self.slept_s += dt_s
            prev = e.receive_ts_ms
            yield e
