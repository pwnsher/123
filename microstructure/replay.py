"""
Deterministic offline replay of captured book sessions (no network), reusing Step 3's store reader and Replayer.

    loaded = load_micro_sessions([session_dir, ...])     # reads <session>/micro/ (or a micro dir directly)
    books = rebuild_books(loaded.events)                 # the same BookReconstructor the live collector used
    for ev in Replayer(loaded.events, speed=None | 1.0 | 20.0): ...

Order = original arrival order (ingest_seq within a session; sessions by first arrival). Receive times are never
rewritten. renormalize() re-runs the adapters over the stored RAW messages (connection numbers come from the raw
stream "ws#<conn>") and must reproduce the stored adapter events exactly; collector-generated BOOK_RESET events
(channel "collector") are not derived from raw text and are compared separately.
"""
import json
import os
from dataclasses import dataclass, field

from market_data.gaps import Gap
from market_data.replay import Replayer  # noqa: F401  (re-export)
from market_data.storage import read_session
from market_data.types import ParseFailure, RawMessage
from microstructure.reconstruction import BookReconstructor
from microstructure.sources.base import MicroCtx, conn_of
from microstructure.types import MicroEvent


@dataclass
class LoadedMicro:
    events: list = field(default_factory=list)
    raw: list = field(default_factory=list)
    gaps: list = field(default_factory=list)
    failures: list = field(default_factory=list)
    feed: list = field(default_factory=list)
    corrupt: list = field(default_factory=list)
    invalid: list = field(default_factory=list)
    session_ids: list = field(default_factory=list)
    manifests: list = field(default_factory=list)


def micro_dir(d):
    return d if os.path.basename(os.path.normpath(d)) == "micro" else os.path.join(d, "micro")


def load_micro_sessions(session_dirs, include_raw=False):
    out = LoadedMicro()
    blocks = []
    for d in session_dirs:
        md = micro_dir(d)
        if not os.path.isdir(md):
            continue
        r = read_session(md)
        evs = []
        out.corrupt += [(md,) + c for c in r.corrupt]
        for kind, data in r.records:
            try:
                if kind == "event":
                    evs.append(MicroEvent.from_dict(data))
                elif kind == "gap":
                    out.gaps.append(Gap.from_dict(data))
                elif kind == "failure":
                    out.failures.append(ParseFailure.from_dict(data))
                elif kind == "feed":
                    out.feed.append(data)
                elif kind == "raw" and include_raw:
                    out.raw.append(RawMessage.from_dict(data))
            except (TypeError, ValueError, KeyError) as e:
                out.invalid.append((md, kind, str(e)[:120]))
        evs.sort(key=lambda e: e.ingest_seq)
        sid = evs[0].session_id if evs else os.path.basename(os.path.dirname(md))
        blocks.append(((evs[0].receive_ts_ms if evs else 0), sid, evs))
        out.session_ids.append(sid)
        try:
            with open(os.path.join(md, "manifest.json"), encoding="utf-8") as f:
                out.manifests.append(json.load(f))
        except (OSError, ValueError):
            out.manifests.append(None)
    for _first, _sid, evs in sorted(blocks, key=lambda b: (b[0], b[1])):
        out.events += evs
    out.gaps.sort(key=lambda g: (g.known_at_ms or g.end_ts_ms, g.ingest_seq or 0))
    return out


def renormalize(raw_messages, adapters, session_id):
    raws = sorted(raw_messages, key=lambda r: r.ingest_seq)
    out = []
    counter = {"n": None}

    def seq():
        counter["n"] += 1
        return counter["n"]
    conns = {}
    for r in raws:
        ad = adapters.get(r.source)
        if ad is None:
            continue
        counter["n"] = r.ingest_seq
        rest = r.stream.startswith("rest:")
        if not rest:
            c = conn_of(r.stream)
            if conns.get(r.source) != c:
                conns[r.source] = c
                ad.on_new_connection(c)
        ctx = MicroCtx(session_id, seq, r.receive_ts_ms, r.receive_mono_ns, r.ingest_seq, "rest" if rest else "ws",
                       conns.get(r.source, 0))
        res = ad.parse_rest(r.stream[5:], json.loads(r.text), ctx) if rest else ad.parse(r.text, ctx)
        out += res.events
    return out


def rebuild_books(events, warmup_ms=1000):
    """Replay book events through a fresh reconstructor; returns it (tracks, statuses, valid intervals)."""
    rc = BookReconstructor(warmup_ms=warmup_ms)
    for ev in sorted(events, key=lambda e: (e.receive_ts_ms, e.ingest_seq)):
        rc.apply(ev)
    return rc
