#!/usr/bin/env python3
"""
Live-collected vs historical CF observations (offline). Live = cfb_ws / cfb_ws_via_kalshi sources,
history = cfb_rest_history / cfb_rest_via_kalshi, unless --live-store / --history-store are given.

    py scripts/compare_settlement_overlap.py
    py scripts/compare_settlement_overlap.py --live-store live.jsonl --history-store hist.jsonl
    output: analysis_output/settlement_overlap.json
"""
import argparse
import os
import sys

import _settlement_cli as cli
from settlement.cache import load, write_json_atomic
from settlement.overlap import compare_market, compare_observations, compare_published
from settlement.policy import window_policy


def run(live_obs, hist_obs, markets, published, wpol_id=None, abs_tol=1e-6):
    wpol = window_policy(wpol_id)
    out = {"window_policy": wpol.policy_id, "points": compare_observations(live_obs, hist_obs, abs_tol=abs_tol),
           "markets": [], "published_vs_live": compare_published(markets, published, live_obs, wpol)}
    live_idx = {o.index_id for o in live_obs}
    hist_idx = {o.index_id for o in hist_obs}
    for m in sorted(markets, key=lambda m: (m.close_ts_ms, m.ticker)):
        if m.index_id not in live_idx or m.index_id not in hist_idx:
            continue
        lo = wpol.lookback_start(m.close_ts_ms) - 600_000
        lv = [o for o in live_obs if o.index_id == m.index_id and lo <= o.event_ts_ms <= m.close_ts_ms + 60_000]
        hv = [o for o in hist_obs if o.index_id == m.index_id and lo <= o.event_ts_ms <= m.close_ts_ms + 60_000]
        if lv and hv:
            out["markets"].append(compare_market(m, lv, hv, wpol, abs_tol=abs_tol))
    fin = [r["final_abs_diff"] for r in out["markets"] if r["final_abs_diff"] is not None]
    out["summary"] = {"markets_compared": len(out["markets"]), "finals_compared": len(fin),
                      "max_final_abs_diff": max(fin) if fin else None,
                      "markets_with_boundary_disagreements": sum(1 for r in out["markets"] if r["boundary_disagreements"]),
                      "status": "COMPARED" if out["points"]["common"] else "NO_OVERLAP"}
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", action="append")
    ap.add_argument("--live-store", action="append")
    ap.add_argument("--history-store", action="append")
    ap.add_argument("--window-policy", default=None)
    ap.add_argument("--abs-tol", type=float, default=1e-6)
    ap.add_argument("--out", default=os.path.join(cli.REPO, "analysis_output", "settlement_overlap.json"))
    a = ap.parse_args(argv)
    if a.live_store or a.history_store:
        L, H = load(a.live_store or []), load(a.history_store or [])
        live, hist = L.observations, H.observations
        markets = list({**H.markets, **L.markets}.values())
        published = L.published
    else:
        paths = cli.store_paths(a.store)
        if not paths:
            print("No settlement store found; nothing to compare.")
            return 1
        st = load(paths)
        live, hist = cli.split_live_history(st.observations)
        markets, published = list(st.markets.values()), st.published
    rep = run(live, hist, markets, published, a.window_policy, a.abs_tol)
    write_json_atomic(a.out, rep)
    print(rep["summary"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
