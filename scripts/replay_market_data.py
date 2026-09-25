#!/usr/bin/env python3
"""
Deterministic OFFLINE replay of captured market-data sessions (no network). RESEARCH ONLY.

    py scripts/replay_market_data.py market_data_sessions/<session>                 # summary, as fast as possible
    py scripts/replay_market_data.py <session> --speed 1                            # real time
    py scripts/replay_market_data.py <session> --speed 20 --print 50                # 20x, print first 50 events
    py scripts/replay_market_data.py <s1> <s2> --digest                             # stream digest (determinism)

Order = original arrival order (ingest_seq within a session; sessions by first arrival). Receive times
are never rewritten, so the same capture always yields the same stream (the digest proves it).
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

from market_data.replay import Replayer, load_sessions  # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("sessions", nargs="+")
    ap.add_argument("--speed", type=float, default=None, help="x real time (omit = as fast as possible)")
    ap.add_argument("--print", type=int, default=0, dest="n_print")
    ap.add_argument("--digest", action="store_true")
    a = ap.parse_args(argv)
    t0 = time.perf_counter()
    loaded = load_sessions(a.sessions)
    t_load = time.perf_counter() - t0
    h = hashlib.sha256()
    by = Counter()
    t0 = time.perf_counter()
    for i, ev in enumerate(Replayer(loaded.events, speed=a.speed)):
        by[(ev.source, ev.event_type.value, ev.mode.value)] += 1
        if a.digest:
            h.update(json.dumps(ev.to_dict(), sort_keys=True, separators=(",", ":")).encode())
        if i < a.n_print:
            print(json.dumps(ev.to_dict(), sort_keys=True))
    t_replay = time.perf_counter() - t0
    print(f"sessions: {', '.join(loaded.session_ids)}")
    print(f"events {len(loaded.events)}  gaps {len(loaded.gaps)}  parse failures {len(loaded.failures)}  "
          f"corrupt {len(loaded.corrupt)}  invalid {len(loaded.invalid)}")
    for (src, et, mode), n in sorted(by.items()):
        print(f"  {src:14s} {et:18s} {mode:10s} {n}")
    if loaded.events:
        span = (loaded.events[-1].receive_ts_ms - loaded.events[0].receive_ts_ms) / 1000.0
        print(f"span {span:.1f} s  load {t_load:.2f} s  replay {t_replay:.2f} s  "
              f"({len(loaded.events) / max(t_replay, 1e-9):,.0f} events/s)")
    if a.digest:
        print(f"stream digest sha256 {h.hexdigest()}")
    for c in loaded.corrupt[:10]:
        print(f"  CORRUPT {c}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
