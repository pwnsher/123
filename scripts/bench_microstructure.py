#!/usr/bin/env python3
"""
Step-5 microstructure benchmark on a SYNTHETIC session (offline; NOT market data - real venues differ in message
rates and sizes; the projection below states its assumptions).

    py scripts/bench_microstructure.py [--assets BTC,ETH,SOL,XRP] [--seconds 300] [--out analysis_output/microstructure_performance.json]

Measures: raw messages / s and events / s through the live collector path (parse + store + live reconstruction),
book-update latency (reconstructor apply per event, p50 / p99), feature-row latency (features_at, p50 / p99), replay
throughput (load + rebuild), disk use (bytes per raw message / per hour, per compression level) and peak memory.
"""
import argparse
import json
import os
import sys
import time
import tracemalloc

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from market_data.replay import load_sessions                                # noqa: E402
from market_data.storage import read_session                                # noqa: E402
from microstructure.features.engine import MicroFeatureEngine, event_key    # noqa: E402
from microstructure.reconstruction import BookReconstructor                 # noqa: E402
from microstructure.replay import load_micro_sessions                       # noqa: E402
from microstructure.storage import MicroStoreWriter                         # noqa: E402
from microstructure.synthetic import run_research_session                   # noqa: E402
from perp_data.replay import load_perp_sessions                             # noqa: E402

# ASSUMED real message rates (messages / s per symbol) for the projection only - order of magnitude, busy hours.
ASSUMED_REAL_RATES = {"coinbase_l2": 15.0, "kraken_book": 10.0, "binance_usdm_book": 10.0, "bybit_linear_book": 10.0,
                      "okx_swap_book": 10.0, "kalshi_ws": 2.0}


def pct(xs, q):
    v = sorted(xs)
    return v[min(len(v) - 1, int(q * len(v)))] if v else None


def dir_bytes(d):
    return sum(os.path.getsize(os.path.join(r, f)) for r, _ds, fs in os.walk(d) for f in fs)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--assets", default="BTC,ETH,SOL,XRP")
    ap.add_argument("--seconds", type=int, default=300)
    ap.add_argument("--out", default=os.path.join(REPO, "analysis_output", "microstructure_performance.json"))
    a = ap.parse_args(argv)
    assets = tuple(x.strip().upper() for x in a.assets.split(",") if x.strip())
    import tempfile
    root = tempfile.mkdtemp(prefix="bench-micro-")
    start = 1_790_001_000_000 - a.seconds * 1000 + 60_000
    tracemalloc.start()
    t0 = time.perf_counter()
    c3, c4, c5 = run_research_session(root, assets, start_ms=start, duration_s=a.seconds, seed=5)
    t_collect = time.perf_counter() - t0
    _cur, peak_collect = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    micro_dir = c5.dir
    raw_n = c5.counts["raw"]
    by_src = {}
    raw_bytes = {}
    for k, d in read_session(micro_dir).records:
        if k == "raw":
            by_src[d["source"]] = by_src.get(d["source"], 0) + 1
            raw_bytes[d["source"]] = raw_bytes.get(d["source"], 0) + len(d["text"])
    disk = dir_bytes(micro_dir)
    # replay throughput
    t0 = time.perf_counter()
    m5 = load_micro_sessions([c3.dir])
    t_load = time.perf_counter() - t0
    rc = BookReconstructor()
    lat = []
    t0 = time.perf_counter()
    for ev in m5.events:
        s = time.perf_counter()
        rc.apply(ev)
        lat.append((time.perf_counter() - s) * 1e6)
    t_rebuild = time.perf_counter() - t0
    # feature rows: streaming engine for one asset, a row every second over the last 120 s
    s3 = load_sessions([c3.dir]); p4 = load_perp_sessions([c3.dir])
    evs = sorted(list(s3.events) + list(p4.events) + list(m5.events), key=event_key)
    tracemalloc.start()
    eng = MicroFeatureEngine(assets[0])
    row_lat, i = [], 0
    t_ing = 0.0
    end = evs[-1].receive_ts_ms
    for T in range(end - 120_000, end + 1, 1000):
        s = time.perf_counter()
        while i < len(evs) and evs[i].receive_ts_ms <= T:
            eng.ingest(evs[i]); i += 1
        t_ing += time.perf_counter() - s
        s = time.perf_counter()
        eng.features_at(T)
        row_lat.append((time.perf_counter() - s) * 1000)
    s = time.perf_counter()
    while i < len(evs):
        eng.ingest(evs[i]); i += 1
    t_ing += time.perf_counter() - s
    _cur, peak_engine = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    # compression levels on the same records
    recs = read_session(micro_dir).records
    comp = {}
    for lvl in (1, 6, 9):
        d = tempfile.mkdtemp(prefix=f"lvl{lvl}-")
        w = MicroStoreWriter(d, compression_level=lvl, fsync=False)
        t0 = time.perf_counter()
        for k, dd in recs:
            w.write(k, dd)
        w.close()
        comp[lvl] = {"bytes": w.bytes_compressed, "ratio": w.bytes_uncompressed / max(w.bytes_compressed, 1),
                     "write_seconds": round(time.perf_counter() - t0, 3)}
    per_msg = disk / max(raw_n, 1)
    hours = a.seconds / 3600
    proj = {}
    for v, rate in ASSUMED_REAL_RATES.items():
        n_v = by_src.get(v, 0)
        bpm = (disk * (raw_bytes.get(v, 0) / max(sum(raw_bytes.values()), 1))) / max(n_v, 1) if n_v else per_msg
        proj[v] = {"assumed_msgs_per_s_per_symbol": rate, "synthetic_bytes_per_msg_on_disk": round(bpm, 1),
                   "projected_gib_per_day_all_assets": round(rate * len(assets) * 86400 * bpm / (1 << 30), 3)}
    res = {
        "synthetic": True,
        "note": "SYNTHETIC session (microstructure.synthetic) - NOT market data. Real message rates / sizes differ; the "
                "projection multiplies synthetic bytes-per-message by ASSUMED real rates and is an order-of-magnitude guide.",
        "assets": list(assets), "seconds_simulated": a.seconds,
        "collection": {"raw_messages": raw_n, "events": c5.counts["events"], "wall_seconds": round(t_collect, 2),
                       "raw_msgs_per_s_processed": round(raw_n / t_collect, 1),
                       "events_per_s_processed": round(c5.counts["events"] / t_collect, 1),
                       "note": "includes the Step-3 / Step-4 synthetic collectors running in the same loop",
                       "synthetic_msgs_by_venue": by_src},
        "book_update_latency_us": {"p50": round(pct(lat, 0.5), 2), "p99": round(pct(lat, 0.99), 2), "n": len(lat)},
        "feature_row_latency_ms": {"p50": round(pct(row_lat, 0.5), 2), "p99": round(pct(row_lat, 0.99), 2), "n": len(row_lat),
                                   "features_per_row": len(eng.features_at(end).values)},
        "feature_ingest_events_per_s": round(len(evs) / max(t_ing, 1e-9), 1),
        "replay": {"events": len(m5.events), "load_seconds": round(t_load, 2), "rebuild_seconds": round(t_rebuild, 2),
                   "rebuild_events_per_s": round(len(m5.events) / max(t_rebuild, 1e-9), 1)},
        "disk": {"micro_store_bytes": disk, "bytes_per_raw_message": round(per_msg, 1),
                 "synthetic_bytes_per_hour": round(disk / hours), "compression_levels": comp,
                 "projection_with_assumed_real_rates": proj},
        "memory": {"collection_peak_mib": round(peak_collect / (1 << 20), 1), "engine_peak_mib": round(peak_engine / (1 << 20), 1)},
        "python": sys.version.split()[0],
    }
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    with open(a.out, "w", encoding="utf-8", newline="\n") as f:
        json.dump(res, f, indent=1, sort_keys=True)
    print(json.dumps({k: res[k] for k in ("collection", "book_update_latency_us", "feature_row_latency_ms", "replay", "memory")},
                     indent=1))
    print(f"disk: {res['disk']['bytes_per_raw_message']} B/msg; -> {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
