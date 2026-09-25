#!/usr/bin/env python3
"""
OFFLINE lead-lag analysis across CF / Coinbase / Kraken / Binance / Bybit / OKX / Kalshi (research only).

    py scripts/lead_lag_analysis.py market_data_sessions/<session> --asset BTC --out analysis_output/lead_lag_btc.json
    py scripts/lead_lag_analysis.py <session> --asset BTC --step-ms 100 --max-lag-ms 5000 --discovery 0.6

The best lag is chosen on the chronological DISCOVERY part only and then reported on the HOLDOUT part (after an
embargo of max |lag|); the holdout is never used to choose. Nothing here is a live feature.
"""
import argparse
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from market_data.replay import load_sessions                        # noqa: E402
from microstructure.leadlag import lead_lag, price_series           # noqa: E402
from microstructure.replay import load_micro_sessions               # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser(description="Offline lead-lag (discovery / holdout).")
    ap.add_argument("sessions", nargs="+")
    ap.add_argument("--asset", default="BTC")
    ap.add_argument("--kalshi-ticker", default=None)
    ap.add_argument("--step-ms", type=int, default=250)
    ap.add_argument("--max-lag-ms", type=int, default=5000)
    ap.add_argument("--discovery", type=float, default=0.6)
    ap.add_argument("--out", default=None)
    a = ap.parse_args(argv)
    s3 = load_sessions(a.sessions)
    m5 = load_micro_sessions(a.sessions)
    evs = list(s3.events) + list(m5.events)
    ser = price_series(evs, a.asset, a.kalshi_ticker)
    ts = [e.receive_ts_ms for e in evs]
    res = lead_lag(ser, min(ts) + 60_000, max(ts), a.step_ms, a.max_lag_ms, a.discovery)
    res["asset"], res["sources_found"] = a.asset, sorted(ser)
    for pair, r in sorted(res["pairs"].items()):
        if r["status"] == "OK":
            print(f"{pair:36s} best lag (discovery) {r['best_lag_ms_discovery']:+6d} ms  corr disc {r['discovery_corr_at_best']:+.3f}"
                  f"  holdout@best {r['holdout_corr_at_discovery_best'] if r['holdout_corr_at_discovery_best'] is None else round(r['holdout_corr_at_discovery_best'], 3)}"
                  f"  holdout@0 {r['holdout_corr_at_0'] if r['holdout_corr_at_0'] is None else round(r['holdout_corr_at_0'], 3)}")
        else:
            print(f"{pair:36s} {r['status']}")
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        with open(a.out, "w", encoding="utf-8") as f:
            json.dump(res, f, indent=2, sort_keys=True)
        print(f"-> {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
