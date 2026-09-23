#!/usr/bin/env python3
"""
bench_perf.py — deterministic local micro-benchmarks (no network). Development tool.

    python bench_perf.py --repo . --out performance_after.json

Runs the same workloads against any repository directory so before/after numbers are
comparable. Wall-clock uses time.perf_counter(); peak memory uses tracemalloc in a SEPARATE
pass (tracemalloc slows Python, so it is never mixed with the timing pass).
"""
import argparse, builtins, json, math, os, platform, random, sys, tempfile, time, tracemalloc


def _timed(fn, repeat=1):
    best = None
    for _ in range(repeat):
        t0 = time.perf_counter(); fn(); dt = time.perf_counter() - t0
        best = dt if best is None else min(best, dt)
    return best


def _peak(fn):
    tracemalloc.start(); fn(); peak = tracemalloc.get_traced_memory()[1]; tracemalloc.stop()
    return peak


def build_telemetry(pt, coins=("BTC", "ETH", "SOL", "XRP"), n=450):
    rng = random.Random(1)
    store = pt.SnapshotStore(list(coins), 600, 1800.0)
    tel = pt.PerpTelemetry(None, list(coins), sampler=None, max_lag_s=8.0, session_id="BENCH")
    tel.store = store
    now = 2_000_000_000.0
    for c in coins:
        mid = 100.0
        for i in range(n):
            ts = now - (n - i) * 4.0
            mid *= math.exp(rng.gauss(0, 4e-4))
            store.add(pt.PerpSnapshot(ts=ts, coin=c, perp_symbol="X", perp_bid=mid - .01, perp_ask=mid + .01,
                                      perp_mid=mid, perp_last=mid, perp_mark=mid, index_price=mid * (1 - rng.gauss(0, 2e-4)),
                                      index_ts_ms=int(ts * 1000), funding_rate=1e-4, source_status=pt.STATUS_FRESH))
            tel.spot_long[c].append(ts + 1.0, mid * 0.5)
    return tel, now


def run(repo):
    sys.path.insert(0, os.path.abspath(repo))
    import perp_telemetry as pt, analyze_perp_predictive as ap, analyze_perp_shadow as ash
    import analyze_perp_integration as ai, step5_fixture as fx
    res = {"python": platform.python_version(), "platform": platform.platform(), "repo": os.path.abspath(repo)}
    tel, now = build_telemetry(pt)
    r = {"status": "ok", "spot_raw": 51.0, "ticker": "T", "fav": "UP", "p_up": 93.0, "conf": 93.0,
         "side_ask": 88.0, "raw_edge": 5.0, "net_edge": 3.0, "signal": True}
    one = lambda: tel._causal("BTC", r, now, mutate=False)
    res["C_one_causal_feature_ms"] = _timed(one, repeat=50) * 1000
    thousand = lambda: [tel._causal(c, r, now, mutate=False) for _ in range(250) for c in ("BTC", "ETH", "SOL", "XRP")]
    res["D_1000_causal_features_s"] = _timed(thousand, repeat=3)
    res["D_1000_causal_features_peak_bytes"] = _peak(thousand)
    d = tempfile.mkdtemp()
    rows = [{c: 1.23456 for c in pt.CSV_COLUMNS} for _ in range(4)]
    opens = {"n": 0}
    real_open = builtins.open
    def counting_open(*a, **k):
        opens["n"] += 1; return real_open(*a, **k)
    def csv_loop():
        path = os.path.join(d, f"t{time.perf_counter_ns()}.csv")
        lg = pt.TelemetryCSVLogger(path)
        for _ in range(2500):
            lg.write_rows(rows)                      # 10,000 rows in 4-row cycles
    res["E_csv_10000_rows_s"] = _timed(csv_loop, repeat=2)
    builtins.open = counting_open
    try:
        csv_loop()
    finally:
        builtins.open = real_open
    res["E_csv_10000_rows_open_calls"] = opens["n"]
    series = [(float(i * 4 + (i % 3)), 100.0 + i) for i in range(600)]
    targets = [random.Random(2).uniform(-10, 2500) for _ in range(100000)]
    look = lambda: [pt.point_at_or_before(series, t, 10.0) for t in targets]
    res["F_100k_point_lookups_s"] = _timed(look, repeat=3)
    res["F_100k_point_lookups_peak_bytes"] = _peak(look)
    rng = random.Random(3)
    clusters = [f"t{i}" for i in range(2000)]
    deltas = {"b": [rng.gauss(0, 1) for _ in clusters], "l": [rng.gauss(0, 1) for _ in clusters]}
    boot3 = lambda: ap.cluster_bootstrap(clusters, deltas, 1000, 7)
    res["G_step3_bootstrap_1000reps_s"] = _timed(boot3)
    res["G_step3_bootstrap_1000reps_peak_bytes"] = _peak(boot3)
    rows4 = []
    for i in range(2000):
        dec = "WOULD_BLOCK" if rng.random() < 0.12 else "ALLOW"
        rows4.append({"ticker": f"T{i}", "decision": dec, "win": rng.random() > 0.3, "pc": rng.uniform(-90, 12)})
    for reps in (1000, 2000):
        b4 = lambda: (ash._bootstrap(rows4, ash._enrichment_stat, reps, 1), ash._bootstrap(rows4, ash._pc_improvement_stat, reps, 2))
        res[f"H_step4_bootstrap_{reps}reps_s"] = _timed(b4)
        res[f"H_step4_bootstrap_{reps}reps_peak_bytes"] = _peak(b4)
    pol = {"policy_id": "P", "policy_hash": "H", "conflict_threshold": -0.1}
    jrows, labels, calls = fx.gen_rows(pol, 2.0e9, seed=1)
    exp = {"source_policy_id": "P", "source_policy_hash": "H", "frozen_threshold": -0.1, "step5_start_cutoff": 2.0e9,
           "strategy_constants": {"MIN_CONF": 80.0, "MIN_PRICE": 85.0, "EDGE_THRESH": 0.0, "ENTRY_COST_CENTS": 2.0},
           "coin_or_group": "ALL", "feature_name": "causal_premium_bps", "experiment_id": "X", "synthetic": True,
           "step5_start_cutoff_utc": "x"}
    G = {"min_calendar_days": 7.0, "min_settled_total": 200, "min_tuning_settled": 100, "min_holdout_settled": 100,
         "min_holdout_filled": 80, "min_holdout_yes": 20, "min_holdout_no": 20}
    b5 = lambda: ai.analyze_experiment(exp, jrows, labels, calls, G, 2000, 3)
    res["I_step5_analysis_2000reps_s"] = _timed(b5)
    res["I_step5_analysis_2000reps_peak_bytes"] = _peak(b5)
    return res


if __name__ == "__main__":
    a = argparse.ArgumentParser()
    a.add_argument("--repo", default="."); a.add_argument("--out", default="performance_after.json")
    args = a.parse_args()
    out = run(args.repo)
    json.dump(out, open(args.out, "w"), indent=1, sort_keys=True)
    print(json.dumps(out, indent=1, sort_keys=True))
