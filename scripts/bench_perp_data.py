#!/usr/bin/env python3
"""
Perp-data pipeline benchmark (local, SYNTHETIC workload, no network). RESEARCH ONLY.

    py scripts/bench_perp_data.py [--assets BTC,ETH,SOL,XRP] [--minutes 10] [--out analysis_output/perp_data_performance.json]

Workload: 4 assets x Binance / Bybit / OKX websocket streams (trades, BBO, depth, mark / index / funding, OI) +
Kalshi perps REST + USDT rate, a 2-minute liquidation cascade (bursts on every venue), REST OI / funding polls.
Measures ingest throughput (raw -> normalize -> store, fsync per flush), flush latency, storage size, replay
throughput, feature-engine ingest, features_at latency (all perp features for one asset), joint-dataset build time,
and peak memory. SYNTHETIC and machine-specific: NOT a claim about production latency.
"""
import argparse
import json
import os
import platform
import shutil
import sys
import tempfile
import time
import tracemalloc

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from market_data.replay import load_sessions  # noqa: E402
from perp_data.dataset import build_research_dataset  # noqa: E402
from perp_data.features.definitions import FEATURE_NAMES  # noqa: E402
from perp_data.features.engine import PerpFeatureEngine, event_key  # noqa: E402
from perp_data.replay import Replayer, load_perp_sessions  # noqa: E402
from perp_data.synthetic import run_joint_session  # noqa: E402


def _pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100.0 * len(xs)))] if xs else None


def run(assets, minutes, tmp):
    start = 1_790_000_100_000 - 60_000
    dur = int(minutes * 60)
    out = {"python": platform.python_version(), "platform": platform.platform(), "assets": assets,
           "synthetic_minutes": minutes, "perp_features_per_asset": len(FEATURE_NAMES),
           "note": "SYNTHETIC workload; machine-specific; not a production latency claim"}
    t0 = time.perf_counter()
    c3, pc = run_joint_session(tmp, assets=tuple(assets), start_ms=start, duration_s=dur, fsync=True,
                               cascade=(start + 120_000, start + 240_000))
    dt = time.perf_counter() - t0
    out["collect_joint"] = {"seconds": round(dt, 3), "perp_raw": pc.counts["raw"], "perp_events": pc.counts["events"],
                            "step3_raw": c3.counts["raw"],
                            "perp_msgs_per_s_incl_step3": round((pc.counts["raw"] + c3.counts["raw"]) / dt),
                            "perp_flushes": pc.writer.flushes,
                            "flush_ms_p50": round(_pct(pc.writer.flush_ms, 50), 3),
                            "flush_ms_p99": round(_pct(pc.writer.flush_ms, 99), 3),
                            "perp_bytes_compressed": pc.writer.bytes_compressed,
                            "perp_bytes_per_event": round(pc.writer.bytes_compressed / max(pc.counts["events"], 1), 1),
                            "liquidation_events": sum(v for k, v in pc.by_type.items() if k.endswith("LIQUIDATION"))}
    t0 = time.perf_counter()
    L4 = load_perp_sessions([c3.dir])
    L3 = load_sessions([c3.dir])
    dl = time.perf_counter() - t0
    t0 = time.perf_counter()
    n = sum(1 for _ in Replayer(L4.events))
    dr = time.perf_counter() - t0
    out["replay"] = {"perp_events": n, "load_both_s": round(dl, 3), "iterate_events_per_s": round(n / max(dr, 1e-9))}
    evs = sorted(L3.events + L4.events, key=event_key)
    eng = PerpFeatureEngine(assets[0])
    lat = []
    last = None
    t0 = time.perf_counter()
    for e in evs:
        eng.ingest(e)
        if (last is None or e.receive_ts_ms - last >= 2000) and e.receive_ts_ms - evs[0].receive_ts_ms > 320_000:
            last = e.receive_ts_ms
            s = time.perf_counter()
            eng.features_at(e.receive_ts_ms)
            lat.append((time.perf_counter() - s) * 1000)
    de = time.perf_counter() - t0
    out["perp_feature_engine"] = {"events_ingested": len(evs), "total_s_incl_rows": round(de, 3), "rows": len(lat),
                                  "features_at_ms_p50": round(_pct(lat, 50), 2), "features_at_ms_p99": round(_pct(lat, 99), 2),
                                  "features_at_ms_max": round(max(lat), 2) if lat else None}
    t0 = time.perf_counter()
    ds = build_research_dataset(L3, L4, assets)
    out["joint_dataset"] = {"rows": len(ds.rows), "seconds": round(time.perf_counter() - t0, 3)}
    del L3, L4, evs, eng, ds
    tracemalloc.start()
    L4 = load_perp_sessions([c3.dir])
    L3 = load_sessions([c3.dir])
    eng = PerpFeatureEngine(assets[0])
    for e in sorted(L3.events + L4.events, key=event_key):
        eng.ingest(e)
    cur, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    out["memory"] = {"what": "load both stores + one perp engine over them (separate pass)",
                     "peak_mb_traced": round(peak / 2**20, 1), "current_mb_traced": round(cur / 2**20, 1)}
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--assets", default="BTC,ETH,SOL,XRP")
    ap.add_argument("--minutes", type=float, default=10)
    ap.add_argument("--out", default=None)
    a = ap.parse_args(argv)
    tmp = tempfile.mkdtemp(prefix="bench-perp-")
    try:
        res = run([x.strip().upper() for x in a.assets.split(",")], a.minutes, tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print(json.dumps(res, indent=1))
    if a.out:
        from market_data.storage import write_json_atomic
        write_json_atomic(a.out, res)
    return 0


if __name__ == "__main__":
    sys.exit(main())
