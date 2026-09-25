#!/usr/bin/env python3
"""
Deterministic OFFLINE replay of captured derivatives sessions (no network). RESEARCH ONLY.

    py scripts/replay_perp_data.py market_data_sessions/<session>                  # summary, as fast as possible
    py scripts/replay_perp_data.py <session> --speed 1 | --speed 20 | --print 20   # real time / accelerated / show events
    py scripts/replay_perp_data.py <session> --digest                             # stream digest (determinism)
    py scripts/replay_perp_data.py <session> --renormalize                        # re-run the adapters over the RAW text

--renormalize proves RAW -> NORMALIZATION is reproducible: every stored event must be re-created from its raw
message (compared on everything except the arrival counter, which other concurrent sources also advance).
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

from perp_data.replay import Replayer, load_perp_sessions, renormalize  # noqa: E402


def comparable(e):
    d = e.to_dict()
    d.pop("ingest_seq")
    return json.dumps(d, sort_keys=True, default=str)


def adapters_for(manifest, assets):
    from perp_data.sources.binance import BinanceUsdmAdapter
    from perp_data.sources.bybit import BybitLinearAdapter
    from perp_data.sources.kalshi_perp import KalshiPerpAdapter
    from perp_data.sources.okx import OkxSwapAdapter
    from perp_data.sources.stablecoin import CoinbaseUsdtAdapter
    mk = {"binance_usdm": BinanceUsdmAdapter, "bybit_linear": BybitLinearAdapter, "okx_swap": OkxSwapAdapter,
          "kalshi_perp": lambda a: KalshiPerpAdapter(a, overrides={}), "coinbase_usdt": lambda a: CoinbaseUsdtAdapter()}
    out = {}
    for n in (manifest or {}).get("derivatives_sources", {}):
        if n in mk:
            ad = mk[n](assets)
            url = manifest["derivatives_sources"][n].get("url") or ""
            for d in (5, 10, 20):
                if f"@depth{d}@" in url:
                    ad.depth = d
            out[n] = ad
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description="Offline replay of derivatives sessions.")
    ap.add_argument("sessions", nargs="+")
    ap.add_argument("--speed", type=float, default=None)
    ap.add_argument("--print", type=int, default=0, dest="n_print")
    ap.add_argument("--digest", action="store_true")
    ap.add_argument("--renormalize", action="store_true")
    a = ap.parse_args(argv)
    t0 = time.perf_counter()
    L = load_perp_sessions(a.sessions, include_raw=a.renormalize)
    t_load = time.perf_counter() - t0
    h = hashlib.sha256()
    by = Counter()
    t0 = time.perf_counter()
    for i, ev in enumerate(Replayer(L.events, speed=a.speed)):
        by[(ev.source, ev.event_type.value, ev.mode.value, ev.quality)] += 1
        if a.digest:
            h.update(json.dumps(ev.to_dict(), sort_keys=True, separators=(",", ":"), default=str).encode())
        if i < a.n_print:
            print(json.dumps(ev.to_dict(), sort_keys=True, default=str))
    t_rep = time.perf_counter() - t0
    print(f"sessions: {', '.join(L.session_ids)}")
    print(f"events {len(L.events)}  gaps {len(L.gaps)}  parse failures {len(L.failures)}  corrupt {len(L.corrupt)}  invalid {len(L.invalid)}")
    for (src, et, mode, q), n in sorted(by.items()):
        print(f"  {src:14s} {et:18s} {mode:10s} {q:16s} {n}")
    if L.events:
        print(f"load {t_load:.2f} s  replay {t_rep:.2f} s ({len(L.events) / max(t_rep, 1e-9):,.0f} events/s)")
    if a.digest:
        print(f"stream digest sha256 {h.hexdigest()}")
    rc = 0
    if a.renormalize:
        for sid, man in zip(L.session_ids, L.manifests):
            evs = [e for e in L.events if e.session_id == sid]
            raws = [r for r in L.raw if r.session_id == sid]
            ads = adapters_for(man, (man or {}).get("assets", []))
            again = renormalize(raws, ads, sid)
            same = sorted(map(comparable, evs)) == sorted(map(comparable, again))
            print(f"renormalize {sid}: {len(again)} events from {len(raws)} raw messages -> {'IDENTICAL' if same else 'DIFFERENT'}")
            rc = rc or (0 if same else 4)
    return rc


if __name__ == "__main__":
    sys.exit(main())
