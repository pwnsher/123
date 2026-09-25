#!/usr/bin/env python3
"""
Deterministic OFFLINE replay of captured order-book sessions (no network). RESEARCH ONLY.

    py scripts/replay_microstructure.py market_data_sessions/<session>              # per-book reconstruction summary
    py scripts/replay_microstructure.py <session> --speed 1 | --speed 20 | --print 20
    py scripts/replay_microstructure.py <session> --digest                          # stream + book-state digest
    py scripts/replay_microstructure.py <session> --renormalize                     # re-run the adapters over the RAW text

The books are rebuilt with the SAME reconstructor the live collector used, so the replayed statuses, gaps and
resnapshots equal the live ones. --renormalize proves RAW -> NORMALIZATION is reproducible (every stored adapter
event is re-created from its raw message; compared on everything except the arrival counter).
"""
import argparse
import hashlib
import json
import os
import sys
import time
from collections import Counter

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from microstructure.replay import Replayer, load_micro_sessions, rebuild_books, renormalize  # noqa: E402


def comparable(e):
    d = e.to_dict()
    d.pop("ingest_seq")
    return json.dumps(d, sort_keys=True, default=str)


def adapters_for(manifest, assets):
    from microstructure.synthetic import ADAPTERS
    out = {}
    for n, info in ((manifest or {}).get("book_sources") or {}).items():
        if n in ADAPTERS:
            out[n] = ADAPTERS[n](assets, depth=info.get("depth")) if info.get("depth") else ADAPTERS[n](assets)
    return out


def book_digest(rc):
    h = hashlib.sha256()
    for k, tr in sorted(rc.tracks.items()):
        h.update(json.dumps([list(k), tr.base.value, tr.book.top("bid", 50), tr.book.top("ask", 50), tr.intervals,
                             tr.counts], default=str).encode())
    return h.hexdigest()


def main(argv=None):
    ap = argparse.ArgumentParser(description="Offline replay of Step-5 book sessions (no network).")
    ap.add_argument("sessions", nargs="+")
    ap.add_argument("--speed", type=float, default=None)
    ap.add_argument("--print", type=int, default=0, dest="n_print")
    ap.add_argument("--digest", action="store_true")
    ap.add_argument("--renormalize", action="store_true")
    a = ap.parse_args(argv)
    t0 = time.perf_counter()
    loaded = load_micro_sessions(a.sessions, include_raw=a.renormalize)
    n = 0
    h = hashlib.sha256()
    for ev in Replayer(loaded.events, speed=a.speed):
        n += 1
        if a.digest:
            h.update(comparable(ev).encode())
        if n <= a.n_print:
            print(f"{ev.receive_ts_ms} {ev.source:18s} {ev.event_type.value:14s} {ev.symbol} {json.dumps(ev.payload, default=str)[:140]}")
    rc = rebuild_books(loaded.events)
    dt = time.perf_counter() - t0
    by = Counter(f"{e.source}:{e.event_type.value}" for e in loaded.events)
    print(f"sessions {loaded.session_ids}: {n} events, {len(loaded.gaps)} gap records, {len(loaded.failures)} parse failures, "
          f"{len(loaded.corrupt)} corrupt, replayed in {dt:.2f} s")
    for k, v in sorted(by.items()):
        print(f"  {k:40s} {v}")
    end = loaded.events[-1].receive_ts_ms if loaded.events else 0
    for k, tr in sorted(rc.tracks.items()):
        print(f"  book {k[0]}:{k[1]:26s} status@end={rc.status(k, end).value:17s} snapshots={tr.counts['snapshots']} "
              f"resnapshots={tr.counts['resnapshots']} deltas={tr.counts['deltas']} gaps={tr.counts['gaps']} "
              f"invalid={tr.counts['invalid']} crossed={tr.counts['crossed']} checksum ok/fail={tr.counts['checksum_ok']}/"
              f"{tr.counts['checksum_fail']} valid intervals={len(tr.intervals)}" + (f" reason={tr.reason}" if tr.reason else ""))
    if a.digest:
        print(f"stream digest {h.hexdigest()}")
        print(f"book digest   {book_digest(rc)}")
    rc_ok = True
    if a.renormalize:
        man = next((m for m in loaded.manifests if m), {})
        ads = adapters_for(man, man.get("assets") or [])
        again = renormalize(loaded.raw, ads, man.get("session_id", ""))
        stored = [comparable(e) for e in loaded.events if e.channel != "collector"]
        redo = [comparable(e) for e in again]
        rc_ok = stored == redo
        print(f"renormalize: {len(redo)} events from {len(loaded.raw)} raw messages -> "
              + ("IDENTICAL to the stored events" if rc_ok else f"DIFFERENT ({len(stored)} stored)"))
    return 0 if rc_ok else 1


if __name__ == "__main__":
    sys.exit(main())
