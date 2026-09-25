"""
Deterministic offline replay of captured derivatives sessions (no network), reusing Step 3's store reader
and Replayer (event-by-event, real-time or accelerated).

    loaded = load_perp_sessions([session_dir, ...])      # reads <session>/perp/ (or a perp dir directly)
    for ev in Replayer(loaded.events, speed=None | 1.0 | 20.0): ...

Order = original arrival order (ingest_seq within a session; sessions by first arrival). Receive times are
never rewritten. renormalize() re-runs the venue adapters over the stored RAW messages and must reproduce
the stored events exactly (raw -> normalization is deterministic, including snapshot+delta state).
"""
import json
import os
from dataclasses import dataclass, field

from market_data.gaps import Gap
from market_data.replay import Replayer  # noqa: F401  (re-export: the same replayer as Step 3)
from market_data.storage import read_session
from market_data.types import ParseFailure, RawMessage
from perp_data.types import PerpEvent


@dataclass
class LoadedPerp:
    events: list = field(default_factory=list)
    raw: list = field(default_factory=list)
    gaps: list = field(default_factory=list)
    failures: list = field(default_factory=list)
    corrupt: list = field(default_factory=list)
    invalid: list = field(default_factory=list)
    session_ids: list = field(default_factory=list)
    manifests: list = field(default_factory=list)


def perp_dir(d):
    return d if os.path.basename(os.path.normpath(d)) == "perp" else os.path.join(d, "perp")


def load_perp_sessions(session_dirs, include_raw=False):
    out = LoadedPerp()
    blocks = []
    for d in session_dirs:
        pd = perp_dir(d)
        r = read_session(pd)
        evs = []
        out.corrupt += [(pd,) + c for c in r.corrupt]
        for kind, data in r.records:
            try:
                if kind == "event":
                    evs.append(PerpEvent.from_dict(data))
                elif kind == "gap":
                    out.gaps.append(Gap.from_dict(data))
                elif kind == "failure":
                    out.failures.append(ParseFailure.from_dict(data))
                elif kind == "raw" and include_raw:
                    out.raw.append(RawMessage.from_dict(data))
            except (TypeError, ValueError, KeyError) as e:
                out.invalid.append((pd, kind, str(e)[:120]))
        evs.sort(key=lambda e: e.ingest_seq)
        sid = evs[0].session_id if evs else os.path.basename(os.path.dirname(pd))
        blocks.append(((evs[0].receive_ts_ms if evs else 0), sid, evs))
        out.session_ids.append(sid)
        try:
            with open(os.path.join(pd, "manifest.json"), encoding="utf-8") as f:
                out.manifests.append(json.load(f))
        except (OSError, ValueError):
            out.manifests.append(None)
    for _first, _sid, evs in sorted(blocks, key=lambda b: (b[0], b[1])):
        out.events += evs
    out.gaps.sort(key=lambda g: (g.known_at_ms or g.end_ts_ms, g.ingest_seq or 0))
    return out


def renormalize(raw_messages, adapters, session_id):
    """Re-run normalization over stored RAW messages (arrival order). Returns the PerpEvents it produces,
    re-using each raw message's receive time and arrival number so the output equals the stored events."""
    from perp_data.sources.base import PerpCtx
    raws = sorted(raw_messages, key=lambda r: r.ingest_seq)
    out = []
    counter = {"n": None}

    def seq():
        counter["n"] += 1
        return counter["n"]
    for r in raws:
        ad = adapters.get(r.source)
        if ad is None:
            continue
        counter["n"] = r.ingest_seq
        ctx = PerpCtx(session_id, seq, r.receive_ts_ms, r.receive_mono_ns, r.ingest_seq,
                      "rest" if r.stream.startswith("rest:") else "ws")
        if r.stream.startswith("rest:"):
            res = ad.parse_rest(r.stream[5:], json.loads(r.text), ctx)
        else:
            res = ad.parse(r.text, ctx)
        out += res.events
    return out
