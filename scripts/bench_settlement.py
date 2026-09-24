#!/usr/bin/env python3
"""
Settlement-engine latency / throughput benchmark (local, synthetic, no network).

    py scripts/bench_settlement.py [--out analysis_output/settlement_performance.json]

Measures: accumulator ingest latency, state() latency (incremental), checkpoint-generation latency
(11 checkpoints per market), batch reconstruction throughput, and resolution verification throughput.
Numbers are machine-specific; they document orders of magnitude, not guarantees.
"""
import argparse
import os
import platform
import statistics
import sys
import time

import _settlement_cli as cli
from settlement.accumulator import SettlementAccumulator
from settlement.cache import write_json_atomic
from settlement.checkpoints import build_checkpoints
from settlement.engine import arrival_order
from settlement.kalshi_markets import parse_market
from settlement.policy import available_ts, reconstruction_policy, window_policy
from settlement.reconstruction import reconstruct
from settlement.resolution import verify_all
from settlement.synthetic import as_synthetic, demo_dataset, observations, price_path


def _pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100.0 * len(xs)))]


def run(n_markets=20, feed_ms=200):
    wpol, rpol = window_policy(), reconstruction_policy("test_synthetic_v1")
    d = demo_dataset(n_markets)
    markets = [parse_market(m)[0] for m in d["markets"]]
    res = {m["ticker"]: parse_market(m)[1] for m in d["markets"]}
    c0 = markets[0].close_ts_ms
    path = price_path(c0 - 900_000, c0 + 5_000, 100_000.0, step_ms=feed_ms, vol_bp=1.0, seed=3)
    obs = observations("BTC", path)
    acc = SettlementAccumulator(markets[0], wpol, rpol)
    ing, sts = [], []
    for o in arrival_order(obs, rpol):
        t0 = time.perf_counter()
        acc.ingest(o)
        t1 = time.perf_counter()
        acc.state(available_ts(o, rpol))
        t2 = time.perf_counter()
        ing.append((t1 - t0) * 1e6)
        sts.append((t2 - t1) * 1e6)
    all_obs = as_synthetic(observations("BTC", price_path(c0 - 1_020_000, markets[-1].close_ts_ms + 30_000,
                                                          100_000.0, 1000, 2.0, 7)))
    t0 = time.perf_counter()
    for m in markets:
        lo = wpol.lookback_start(m.close_ts_ms)
        reconstruct(m, [o for o in all_obs if lo <= o.event_ts_ms <= m.close_ts_ms], wpol, rpol)
    recon_s = time.perf_counter() - t0
    ck = []
    for m in markets[:5]:
        w = [o for o in all_obs if m.close_ts_ms - 660_000 <= o.event_ts_ms <= m.close_ts_ms]
        t0 = time.perf_counter()
        build_checkpoints(m, w, res.get(m.ticker), wpol, rpol)
        ck.append((time.perf_counter() - t0) * 1000)
    t0 = time.perf_counter()
    verify_all(markets, res, all_obs, None, rpol)
    ver_s = time.perf_counter() - t0
    return {"python": platform.python_version(), "machine": platform.machine(),
            "feed_interval_ms": feed_ms, "observations_streamed": len(obs),
            "accumulator_ingest_us": {"median": statistics.median(ing), "p99": _pct(ing, 99), "max": max(ing)},
            "accumulator_state_us": {"median": statistics.median(sts), "p99": _pct(sts, 99), "max": max(sts)},
            "checkpoints_11_per_market_ms": {"median": statistics.median(ck), "max": max(ck)},
            "reconstruction_markets_per_s": n_markets / recon_s if recon_s else None,
            "resolution_4_policies_markets_per_s": n_markets / ver_s if ver_s else None}


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(cli.REPO, "analysis_output", "settlement_performance.json"))
    ap.add_argument("--markets", type=int, default=20)
    a = ap.parse_args(argv)
    r = run(a.markets)
    write_json_atomic(a.out, r)
    for k, v in r.items():
        print(f"{k}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
