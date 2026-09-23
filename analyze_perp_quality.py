#!/usr/bin/env python3
"""
analyze_perp_quality.py — offline DATA-INTEGRITY report for kalshi_perp_telemetry.csv.

    python analyze_perp_quality.py
    python analyze_perp_quality.py --file kalshi_perp_telemetry.csv --json quality_report.json

Reads the CSV only (no network). It reports whether the dataset is complete,
synchronized and structurally sound. It deliberately does NOT look at outcomes,
win rates, correlations, P&L or any predictive quantity — that is Step 3.
Bad rows are counted and reported, never deleted.

Percentiles use linear interpolation between closest ranks (numpy's default).

Schema v3 adds a volatility-regime section: coverage of every new continuous feature
(overall, by coin, and on analysis_ready rows), vol_regime / stability-state distributions
(including % UNKNOWN), the reason each new feature is blank (warm-up, missing input, ...),
and extreme / pathological values. Values are REPORTED, never clipped or winsorised here;
any winsorisation belongs to the analysis layer (Step 3 fits 1/99 winsorisation on
training rows only).
"""
import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict

COINS = ("BTC", "ETH", "SOL", "XRP")
STATUSES = ("fresh", "stale", "error", "unavailable", "disabled")
PRICE_COLS = ("spot_price", "perp_bid", "perp_ask", "perp_mid", "perp_last", "perp_mark", "index_price")
FEATURES = [
    "premium_bps", "causal_premium_bps",
    "premium_change_30s_bps", "premium_change_60s_bps", "premium_change_180s_bps",
    "premium_z_5m", "premium_z_15m",
    "causal_perp_ret_30s_bps", "causal_perp_ret_60s_bps", "causal_perp_ret_180s_bps",
    "causal_spot_ret_30s_bps", "causal_spot_ret_60s_bps", "causal_spot_ret_180s_bps",
    "momentum_gap_30s_bps", "momentum_gap_60s_bps", "momentum_gap_180s_bps",
    "perp_rv_60s_bps", "perp_rv_300s_bps", "perp_rv_900s_bps",
    "spot_rv_60s_bps", "spot_rv_300s_bps", "spot_rv_900s_bps",
    "perp_vol_shock_60v300", "perp_vol_shock_60v900",
    "spot_vol_shock_60v300", "spot_vol_shock_60v900",
    "perp_spread_bps", "mark_index_premium_bps", "last_index_premium_bps", "mid_mark_basis_bps",
]
# schema v3 volatility-regime research features (continuous)
VOL_FEATURES = [
    "perp_momentum_z_30s", "perp_momentum_z_60s", "perp_momentum_z_180s",
    "spot_momentum_z_30s", "spot_momentum_z_60s", "spot_momentum_z_180s",
    "momentum_gap_z_30s", "momentum_gap_z_60s", "momentum_gap_z_180s",
    "perp_spread_median_5m_bps", "perp_spread_ratio_5m", "premium_stress_5m",
]
FEATURES = FEATURES + VOL_FEATURES
VOL_REGIMES = ("LOW", "NORMAL", "HIGH", "EXTREME", "UNKNOWN")          # == perp_telemetry.VOL_REGIMES
STABILITY_STATES = ("STABLE", "CAUTION", "UNSTABLE", "UNKNOWN")        # == perp_telemetry.STABILITY_STATES
# Plausibility bounds used ONLY to report obviously pathological values (nothing is clipped).
_Z_BOUNDS = (-50.0, 50.0)
PATHOLOGICAL_BOUNDS = dict(
    {f: _Z_BOUNDS for f in VOL_FEATURES if "_z_" in f},
    perp_spread_median_5m_bps=(0.0, 1000.0), perp_spread_ratio_5m=(0.0, 50.0), premium_stress_5m=(0.0, 50.0),
    perp_vol_shock_60v300=(0.0, 50.0), perp_vol_shock_60v900=(0.0, 50.0),
    spot_vol_shock_60v300=(0.0, 50.0), spot_vol_shock_60v900=(0.0, 50.0))
NON_NUMERIC = {"ts_utc", "coin", "binary_status", "binary_ticker", "binary_close_time", "spot_source",
               "fav", "binary_signal", "binary_reason", "perp_symbol", "perp_market_status",
               "index_source", "funding_next_time", "funding_computed_time", "source_status",
               "source_error", "causal_pair_ok", "analysis_ready", "quality_flags", "feature_version",
               "telemetry_session_id", "funding_available",
               "vol_regime", "perp_stability_state", "perp_stability_reasons"}


def fnum(v):
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except ValueError:
        return None
    return x


def pct(values, q):
    v = sorted(x for x in values if x is not None and math.isfinite(x))
    if not v:
        return None
    if len(v) == 1:
        return v[0]
    pos = (len(v) - 1) * q / 100.0
    lo = int(math.floor(pos)); hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def dist(values, qs=(50, 90, 95, 99)):
    vals = [x for x in values if x is not None and math.isfinite(x)]
    out = {"n": len(vals)}
    for q in qs:
        out["median" if q == 50 else f"p{q}"] = _rd(pct(vals, q))
    out["max"] = _rd(max(vals)) if vals else None
    return out


def _rd(x, n=3):
    return None if x is None else round(x, n)


def share(n, d):
    return round(100.0 * n / d, 2) if d else None


def truthy(v):
    return str(v).strip().lower() == "true"


def intervals(ts_list):
    ts = sorted(set(t for t in ts_list if t is not None))
    return [(b - a) / 1000.0 for a, b in zip(ts, ts[1:])], ts


def cadence_rows_of(rows, col):
    """Intervals are computed WITHIN each (session, coin) stream, then pooled —
    interleaving different coins' timestamps would fabricate short intervals."""
    groups = defaultdict(list)
    for r in rows:
        groups[(r.get("telemetry_session_id") or "", r.get("coin"))].append(fnum(r.get(col)))
    iv, starts = [], []
    for vals in groups.values():
        g_iv, g_ts = intervals(vals)
        iv += g_iv; starts += g_ts[:-1]
    if not iv:
        return {"n_intervals": 0, "median_s": None, "p95_s": None, "max_s": None, "longest_gap_start_ms": None}
    i = max(range(len(iv)), key=lambda j: iv[j])
    return {"n_intervals": len(iv), "median_s": _rd(pct(iv, 50)), "p95_s": _rd(pct(iv, 95)),
            "max_s": _rd(max(iv)), "longest_gap_start_ms": int(starts[i])}


def cadence(ts_list):
    iv, ts = intervals(ts_list)
    if not iv:
        return {"n_intervals": 0, "median_s": None, "p95_s": None, "max_s": None, "longest_gap_start_ms": None}
    i = max(range(len(iv)), key=lambda j: iv[j])
    return {"n_intervals": len(iv), "median_s": _rd(pct(iv, 50)), "p95_s": _rd(pct(iv, 95)),
            "max_s": _rd(max(iv)), "longest_gap_start_ms": int(ts[i])}


def analyze_group(rows):
    n = len(rows)
    rep = {"rows": n}
    if not n:
        return rep
    tsv = [fnum(r.get("ts_epoch_ms")) for r in rows]
    tsv = [t for t in tsv if t is not None]
    rep["first_ts_utc"] = min(rows, key=lambda r: fnum(r.get("ts_epoch_ms")) or 0).get("ts_utc")
    rep["last_ts_utc"] = max(rows, key=lambda r: fnum(r.get("ts_epoch_ms")) or 0).get("ts_utc")
    rep["time_span_hours"] = _rd((max(tsv) - min(tsv)) / 3.6e6) if tsv else None
    rep["sessions"] = len({r.get("telemetry_session_id") for r in rows if r.get("telemetry_session_id")})
    rep["unique_binary_tickers"] = len({r.get("binary_ticker") for r in rows if r.get("binary_ticker")})

    sc = Counter((r.get("source_status") or "").strip() for r in rows)
    rep["source_status"] = {s: {"count": sc.get(s, 0), "pct": share(sc.get(s, 0), n)} for s in STATUSES}
    other = {k: v for k, v in sc.items() if k not in STATUSES}
    if other:
        rep["source_status"]["other"] = other

    v2 = [r for r in rows if r.get("telemetry_schema_version")]
    rep["rows_with_step2_schema"] = len(v2)
    rep["causal_pair_ok_pct"] = share(sum(truthy(r.get("causal_pair_ok")) for r in v2), len(v2))
    rep["analysis_ready_pct"] = share(sum(truthy(r.get("analysis_ready")) for r in v2), len(v2))
    rep["perp_lag_to_spot_ms"] = dist([fnum(r.get("perp_lag_to_spot_ms")) for r in rows])
    rep["index_age_ms"] = dist([fnum(r.get("index_age_ms")) for r in rows], qs=(50, 95))
    rep["spot_request_latency_ms"] = dist([fnum(r.get("spot_request_latency_ms")) for r in rows], qs=(50, 95))

    rep["cadence_rows"] = cadence_rows_of(rows, "ts_epoch_ms")
    rep["cadence_perp_snapshots"] = cadence_rows_of(rows, "perp_snapshot_ts_epoch_ms")
    rep["cadence_spot_observations"] = cadence_rows_of(rows, "spot_observed_ts_epoch_ms")

    by_sess = defaultdict(lambda: [0, 0, 0])
    for r in rows:
        sid = r.get("telemetry_session_id") or "?"
        for i, c in enumerate(("submitted_cycles_total", "processed_cycles_total", "dropped_cycles_total")):
            x = fnum(r.get(c))
            if x is not None:
                by_sess[sid][i] = max(by_sess[sid][i], int(x))
    rep["queue"] = {"submitted": sum(v[0] for v in by_sess.values()),
                    "processed": sum(v[1] for v in by_sess.values()),
                    "dropped": sum(v[2] for v in by_sess.values()),
                    "note": "max cumulative counter per session, summed; counters are shared across coins"}

    two = sum(1 for r in rows if fnum(r.get("perp_mid")) is not None)
    rep["book"] = {"two_sided_pct": share(two, n), "perp_spread_bps": dist([fnum(r.get("perp_spread_bps")) for r in rows], qs=(50, 95))}

    rep["feature_coverage_pct"] = {f: share(sum(1 for r in rows if (r.get(f) or "") != ""), n) for f in FEATURES}
    rep["feature_coverage_pct_when_analysis_ready"] = {
        f: share(sum(1 for r in v2 if truthy(r.get("analysis_ready")) and (r.get(f) or "") != ""),
                 sum(1 for r in v2 if truthy(r.get("analysis_ready")))) for f in FEATURES}

    fc = Counter()
    for r in rows:
        for flag in (r.get("quality_flags") or "").split(";"):
            if flag:
                fc[flag] += 1
    rep["quality_flags"] = dict(sorted(fc.items(), key=lambda kv: -kv[1]))
    rep["volatility"] = volatility_section(rows)
    return rep


def _blank(r, c):
    return (r.get(c) or "").strip() == ""


def missing_reason(r, f):
    """Why a schema-v3 feature is blank on this row, derived ONLY from other logged columns.
    perp_spread_baseline_n is written whenever the selected perp snapshot was usable, so it
    marks 'perp usable' without re-running the telemetry."""
    perp_ok = not _blank(r, "perp_spread_baseline_n")
    spot_ok = not _blank(r, "spot_price") and not _blank(r, "spot_observed_ts_epoch_ms")
    h = f.rsplit("_", 1)[-1]
    if f.startswith(("perp_momentum_z_", "spot_momentum_z_")):
        side = f.split("_", 1)[0]
        if not (perp_ok if side == "perp" else spot_ok):
            return "PERP_SNAPSHOT_NOT_USABLE" if side == "perp" else "SPOT_UNAVAILABLE"
        if _blank(r, f"causal_{side}_ret_{h}_bps"):
            return f"{side.upper()}_{h.upper()}_RETURN_WARMUP_OR_GAP"
        if _blank(r, f"{side}_rv_300s_bps"):
            return f"{side.upper()}_RV300_WARMUP_OR_GAP"
        return "RV300_BELOW_NUMERICAL_FLOOR"
    if f.startswith("momentum_gap_z_"):
        if not perp_ok:
            return "PERP_SNAPSHOT_NOT_USABLE"
        if not spot_ok:
            return "SPOT_UNAVAILABLE"
        if _blank(r, f"momentum_gap_{h}_bps"):
            return f"GAP_{h.upper()}_UNAVAILABLE"
        for side in ("perp", "spot"):
            if _blank(r, f"{side}_rv_300s_bps"):
                return f"{side.upper()}_RV300_WARMUP_OR_GAP"
        return "RV300_BELOW_NUMERICAL_FLOOR"
    if not perp_ok:
        return "PERP_SNAPSHOT_NOT_USABLE"
    if f == "premium_stress_5m":
        return "PREMIUM_Z_5M_WARMUP_OR_ZERO_VARIANCE"
    if f == "perp_spread_ratio_5m" and _blank(r, "perp_spread_bps"):
        return "NO_VALID_CURRENT_SPREAD"
    if _blank(r, "perp_spread_median_5m_bps"):
        return "SPREAD_BASELINE_INSUFFICIENT"
    return "SPREAD_BASELINE_ZERO"


def extremes(values):
    v = [x for x in values if x is not None and math.isfinite(x)]
    if not v:
        return {"n": 0}
    return {"n": len(v), "min": _rd(min(v), 4), "p01": _rd(pct(v, 1), 4), "median": _rd(pct(v, 50), 4),
            "p99": _rd(pct(v, 99), 4), "max": _rd(max(v), 4)}


def _dist_of(rows, col, cats):
    c = Counter((r.get(col) or "").strip() or "UNKNOWN" for r in rows)
    out = {k: {"count": c.get(k, 0), "pct": share(c.get(k, 0), len(rows))} for k in cats}
    other = {k: v for k, v in c.items() if k not in cats}
    if other:
        out["unrecognised"] = other
    return out


def volatility_section(rows):
    """Schema-v3 regime/stability diagnostics for one group of rows (outcomes never read)."""
    ready = [r for r in rows if truthy(r.get("analysis_ready"))]
    reasons = {f: dict(Counter(missing_reason(r, f) for r in rows if _blank(r, f)).most_common()) for f in VOL_FEATURES}
    sr = Counter()
    for r in rows:
        for x in (r.get("perp_stability_reasons") or "").split(";"):
            if x:
                sr[x] += 1
    return {"vol_regime": _dist_of(rows, "vol_regime", VOL_REGIMES),
            "vol_regime_when_analysis_ready": _dist_of(ready, "vol_regime", VOL_REGIMES),
            "vol_regime_unknown_pct": share(sum(1 for r in rows if (r.get("vol_regime") or "UNKNOWN") == "UNKNOWN"), len(rows)),
            "stability_state": _dist_of(rows, "perp_stability_state", STABILITY_STATES),
            "stability_state_when_analysis_ready": _dist_of(ready, "perp_stability_state", STABILITY_STATES),
            "stability_unknown_pct": share(sum(1 for r in rows if (r.get("perp_stability_state") or "UNKNOWN") == "UNKNOWN"), len(rows)),
            "stability_reason_counts": dict(sr.most_common()),
            "missing_reasons": reasons,
            "extremes": {f: extremes([fnum(r.get(f)) for r in rows]) for f in VOL_FEATURES}}


def pathological(rows):
    """Values outside PATHOLOGICAL_BOUNDS (or non-finite). Reported with examples, never clipped."""
    out = {}
    for f, (lo, hi) in PATHOLOGICAL_BOUNDS.items():
        bad = []
        for i, r in enumerate(rows):
            raw = (r.get(f) or "").strip()
            if not raw:
                continue
            x = fnum(raw)
            if x is None or not math.isfinite(x) or x < lo or x > hi:
                bad.append({"row": i, "coin": r.get("coin"), "ts_utc": r.get("ts_utc"), "value": raw})
        if bad:
            out[f] = {"count": len(bad), "bounds": [lo, hi], "examples": bad[:5]}
    return out


def structural(rows, header):
    out = {}
    seen = Counter(tuple(r.get(c, "") for c in header) for r in rows)
    out["duplicate_rows"] = sum(c - 1 for c in seen.values() if c > 1)
    per = defaultdict(list)
    for i, r in enumerate(rows):
        per[(r.get("telemetry_session_id") or "", r.get("coin"))].append((i, fnum(r.get("ts_epoch_ms"))))
    dup_ts = oo = 0
    for seq in per.values():
        tsc = Counter(t for _, t in seq if t is not None)
        dup_ts += sum(c - 1 for c in tsc.values() if c > 1)
        last = None
        for _, t in seq:
            if t is None:
                continue
            if last is not None and t < last:
                oo += 1
            last = t if last is None else max(last, t)
    out["duplicate_timestamps_per_coin"] = dup_ts
    out["out_of_order_rows"] = oo
    neg = fut = 0
    for r in rows:
        lag = fnum(r.get("perp_lag_to_spot_ms"))
        if lag is not None and lag < 0:
            neg += 1
        ps, fe = fnum(r.get("perp_snapshot_ts_epoch_ms")), fnum(r.get("feature_end_ts_epoch_ms"))
        if ps is not None and fe is not None and ps > fe:
            fut += 1
    out["negative_causal_lag"] = neg
    out["perp_snapshot_after_feature_end"] = fut
    nonfinite = Counter()
    for r in rows:
        for c in header:
            if c in NON_NUMERIC:
                continue
            v = (r.get(c) or "").strip().lower()
            if v in ("nan", "inf", "-inf", "+inf", "infinity", "-infinity"):
                nonfinite[c] += 1
    out["non_finite_values"] = dict(nonfinite)
    bad_px = Counter()
    for r in rows:
        for c in PRICE_COLS:
            x = fnum(r.get(c))
            if x is not None and x <= 0:
                bad_px[c] += 1
    out["impossible_prices"] = dict(bad_px)
    out["negative_spreads"] = sum(1 for r in rows if (fnum(r.get("perp_spread_bps")) or 0) < 0)
    crossed = 0
    for r in rows:
        b, a = fnum(r.get("perp_bid")), fnum(r.get("perp_ask"))
        if b is not None and a is not None and b > a:
            crossed += 1
    out["crossed_books"] = crossed
    out["analysis_ready_but_not_causal"] = sum(1 for r in rows if truthy(r.get("analysis_ready"))
                                               and not truthy(r.get("causal_pair_ok")))
    out["schema_versions_present"] = dict(Counter(r.get("telemetry_schema_version") or "none(step1)" for r in rows))
    out["feature_versions_present"] = dict(Counter(r.get("feature_version") or "none(step1)" for r in rows))
    out["pathological_values"] = pathological(rows)
    return out


def analyze(path):
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        rows = list(reader)
    rep = {"file": path, "columns": len(header), "overall": analyze_group(rows),
           "by_coin": {c: analyze_group([r for r in rows if r.get("coin") == c]) for c in COINS},
           "structural": structural(rows, header)}
    missing = [c for c in ("telemetry_schema_version", "feature_end_ts_epoch_ms", "analysis_ready") if c not in header]
    if missing:
        rep["warning"] = f"file lacks Step 2 columns {missing}; Step 2 metrics will be empty"
    return rep


def _fmt_dist(d):
    if not d or not d.get("n"):
        return "n=0"
    return "  ".join(f"{k}={v}" for k, v in d.items())


def render(rep):
    L = []
    o = rep["overall"]
    L.append(f"PERP TELEMETRY DATA-QUALITY REPORT  ({rep['file']}, {rep['columns']} columns)")
    L.append("Integrity only - no outcome / predictive analysis.")
    if rep.get("warning"):
        L.append("WARNING: " + rep["warning"])
    for name, g in [("OVERALL", o)] + [(c, rep["by_coin"][c]) for c in COINS]:
        L.append("")
        L.append(f"== {name} ==")
        if not g.get("rows"):
            L.append("  rows: 0"); continue
        L.append(f"  rows {g['rows']}  span {g['time_span_hours']}h  {g['first_ts_utc']} -> {g['last_ts_utc']}")
        L.append(f"  sessions {g['sessions']}  unique binary tickers {g['unique_binary_tickers']}")
        L.append("  source_status " + "  ".join(f"{s}={v['count']}({v['pct']}%)" for s, v in g["source_status"].items() if isinstance(v, dict) and "count" in v))
        L.append(f"  causal_pair_ok {g['causal_pair_ok_pct']}%   analysis_ready {g['analysis_ready_pct']}%   (of {g['rows_with_step2_schema']} step2 rows)")
        L.append(f"  perp_lag_to_spot_ms      {_fmt_dist(g['perp_lag_to_spot_ms'])}")
        L.append(f"  index_age_ms             {_fmt_dist(g['index_age_ms'])}")
        L.append(f"  spot_request_latency_ms  {_fmt_dist(g['spot_request_latency_ms'])}")
        for k in ("cadence_rows", "cadence_perp_snapshots", "cadence_spot_observations"):
            c = g[k]
            if not c["n_intervals"]:
                L.append(f"  {k:26s} n/a (fewer than two distinct timestamps)"); continue
            L.append(f"  {k:26s} median {c['median_s']}s  p95 {c['p95_s']}s  max {c['max_s']}s  (longest gap starts {c['longest_gap_start_ms']})")
        q = g["queue"]
        L.append(f"  queue submitted {q['submitted']}  processed {q['processed']}  dropped {q['dropped']}")
        L.append(f"  book two-sided {g['book']['two_sided_pct']}%   spread_bps {_fmt_dist(g['book']['perp_spread_bps'])}")
        L.append("  feature coverage % (all rows | analysis_ready rows):")
        for f in FEATURES:
            L.append(f"    {f:28s} {str(g['feature_coverage_pct'][f]):>7} | {g['feature_coverage_pct_when_analysis_ready'][f]}")
        L.append("  quality_flags: " + (", ".join(f"{k}={v}" for k, v in g["quality_flags"].items()) or "none"))
        v = g["volatility"]
        dfmt = lambda d: "  ".join(f"{k}={x['count']}({x['pct']}%)" for k, x in d.items() if isinstance(x, dict) and "count" in x)
        L.append("  volatility regime (telemetry, observational):")
        L.append(f"    all rows            {dfmt(v['vol_regime'])}")
        L.append(f"    analysis_ready rows {dfmt(v['vol_regime_when_analysis_ready'])}")
        L.append(f"    UNKNOWN {v['vol_regime_unknown_pct']}%")
        L.append("  stability state (rule-based, observational):")
        L.append(f"    all rows            {dfmt(v['stability_state'])}")
        L.append(f"    analysis_ready rows {dfmt(v['stability_state_when_analysis_ready'])}")
        L.append(f"    UNKNOWN {v['stability_unknown_pct']}%   reasons: "
                 + (", ".join(f"{k}={n}" for k, n in v["stability_reason_counts"].items()) or "none"))
        L.append("  blank-value reasons (schema v3 features):")
        for f in VOL_FEATURES:
            if v["missing_reasons"][f]:
                L.append(f"    {f:28s} " + ", ".join(f"{k}={n}" for k, n in v["missing_reasons"][f].items()))
        L.append("  extremes (min / p01 / median / p99 / max; reported, never clipped):")
        for f in VOL_FEATURES:
            e = v["extremes"][f]
            if e["n"]:
                L.append(f"    {f:28s} {e['min']} / {e['p01']} / {e['median']} / {e['p99']} / {e['max']}  (n={e['n']})")
    L.append("")
    L.append("== STRUCTURAL CHECKS (rows are reported, never deleted) ==")
    for k, v in rep["structural"].items():
        L.append(f"  {k:34s} {v}")
    return "\n".join(L)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--file", default="kalshi_perp_telemetry.csv")
    ap.add_argument("--json", default=None, help="also write the report as JSON")
    a = ap.parse_args(argv)
    try:
        rep = analyze(a.file)
    except FileNotFoundError:
        print(f"no such file: {a.file}", file=sys.stderr)
        return 2
    print(render(rep))
    if a.json:
        with open(a.json, "w") as f:
            json.dump(rep, f, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
