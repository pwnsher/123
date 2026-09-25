#!/usr/bin/env python3
"""
Market-data pipeline benchmark (local, SYNTHETIC data, no network). RESEARCH ONLY.

    py scripts/bench_market_data.py [--assets BTC,ETH,SOL,XRP] [--minutes 20] [--out analysis_output/market_data_performance.json]

Measures: collector ingest throughput (raw -> parse -> store), per-message ingest latency, disk write
(flush) latency, replay throughput, incremental feature-engine ingest rate, features_at() latency,
single-pass dataset build time, and peak memory (tracemalloc). Machine-specific orders of magnitude,
not guarantees.
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

from market_data.features.dataset import build_dataset  # noqa: E402
from market_data.features.engine import FeatureEngine  # noqa: E402
from market_data.replay import Replayer, load_sessions  # noqa: E402
from market_data.synthetic import run_session  # noqa: E402


def _pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100.0 * len(xs)))] if xs else None


def run(assets, minutes, tmp):
    out = {"python": platform.python_version(), "platform": platform.platform(), "assets": assets,
           "synthetic_minutes": minutes, "note": "SYNTHETIC data; machine-specific"}
    t0 = time.perf_counter()
    col = run_session(tmp, assets=tuple(assets), duration_s=minutes * 60, fsync=True)
    dt_c = time.perf_counter() - t0
    n_raw = col.counts["raw"]
    out["collector"] = {"raw_messages": n_raw, "events": col.counts["events"], "seconds": round(dt_c, 3),
                        "messages_per_s": round(n_raw / dt_c), "mean_us_per_message": round(dt_c / n_raw * 1e6, 1),
                        "flushes": col.writer.flushes,
                        "flush_ms_p50": round(_pct(col.writer.flush_ms, 50), 3),
                        "flush_ms_p99": round(_pct(col.writer.flush_ms, 99), 3),
                        "flush_ms_max": round(max(col.writer.flush_ms), 3),
                        "bytes_compressed": col.writer.bytes_compressed,
                        "bytes_per_event": round(col.writer.bytes_compressed / max(col.counts["events"], 1), 1)}
    t0 = time.perf_counter()
    loaded = load_sessions([col.dir])
    dt_l = time.perf_counter() - t0
    t0 = time.perf_counter()
    n = sum(1 for _ in Replayer(loaded.events))
    dt_r = time.perf_counter() - t0
    out["replay"] = {"events": n, "load_s": round(dt_l, 3), "load_events_per_s": round(n / dt_l),
                     "iterate_events_per_s": round(n / max(dt_r, 1e-9))}
    eng = FeatureEngine(assets[0])
    t0 = time.perf_counter()
    lat = []
    last_eval = None
    for ev in loaded.events:
        eng.ingest(ev)
        if last_eval is None or ev.receive_ts_ms - last_eval >= 1000:
            last_eval = ev.receive_ts_ms
            if ev.receive_ts_ms - loaded.events[0].receive_ts_ms > 360_000:
                s = time.perf_counter()
                eng.features_at(ev.receive_ts_ms)
                lat.append((time.perf_counter() - s) * 1000)
    dt_e = time.perf_counter() - t0
    out["feature_engine"] = {"events_ingested": len(loaded.events), "total_s_incl_features": round(dt_e, 3),
                             "features_at_calls": len(lat), "features_at_ms_p50": round(_pct(lat, 50), 3),
                             "features_at_ms_p99": round(_pct(lat, 99), 3), "features_at_ms_max": round(max(lat), 3)}
    t0 = time.perf_counter()
    ds = build_dataset(loaded, assets)
    dt_d = time.perf_counter() - t0
    out["dataset"] = {"rows": len(ds.rows), "seconds": round(dt_d, 3)}
    del loaded, eng, ds
    # memory: a SEPARATE pass (tracemalloc slows Python several-fold, so it is not active while timing)
    tracemalloc.start()
    loaded = load_sessions([col.dir])
    eng = FeatureEngine(assets[0])
    for ev in loaded.events:
        eng.ingest(ev)
    cur, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    out["memory"] = {"what": "load session + one incremental engine over it", "peak_mb_traced": round(peak / 2**20, 1),
                     "current_mb_traced": round(cur / 2**20, 1)}
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--assets", default="BTC,ETH,SOL,XRP")
    ap.add_argument("--minutes", type=float, default=20)
    ap.add_argument("--out", default=None)
    a = ap.parse_args(argv)
    tmp = tempfile.mkdtemp(prefix="bench-md-")
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
