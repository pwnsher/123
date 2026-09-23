#!/usr/bin/env python3
"""
analyze_perp_shadow.py — STEP 4 Phase B: PROSPECTIVE validation of frozen shadow policies.
Offline (four local files, no network). Evaluates each frozen rule EXACTLY as created:
it never changes a threshold, never searches thresholds/quantiles/features/coins, never
refits anything, and never writes the policy file.

    python analyze_perp_shadow.py
    python analyze_perp_shadow.py --journal kalshi_perp_shadow.csv --labels kalshi_binary_outcomes.csv \
        --calls kalshi_calls.json --policies perp_shadow_policies.json --outdir analysis_output

Only shadow rows with signal time AFTER the policy's discovery cutoff are used.
Settlement outcomes join by exact ticker. Paper P&L joins kalshi_calls.json by exact ticker
(legacy call rows without a ticker are never fuzzy-matched). Unfilled maker orders are not
trades. Primary P&L metric: per-contract P&L, with WOULD_BLOCK calls counted as 0 (no trade):
    baseline_mean_pc = sum(pc_all) / N_filled
    shadow_mean_pc   = sum(pc_allowed) / N_filled
    improvement      = shadow_mean_pc - baseline_mean_pc = -sum(pc_blocked) / N_filled
"""
import argparse
import csv
import datetime as dt
import json
import math
import os
import random
import sys
import time

import analyze_perp_predictive as ap
import perp_shadow as ps

DEFAULT_SHADOW_GATES = {
    "min_calendar_days": 7.0,
    "min_settled": 200,
    "min_blocked": 30,
    "min_allowed": 150,
    "min_coverage_pct": 90.0,
    "min_retention_pct": 75.0,
}
BOOTSTRAP_REPS = 2000
SEED = ap.SEED
POLICY_MAX_AGE_DAYS = 30
BLOCK_RATE_SHIFT_LOW, BLOCK_RATE_SHIFT_HIGH = 0.5, 2.0
MIN_ROWS_FOR_SHIFT = 50
FEATURE_SHIFT_SMD = 1.0
MIN_SUBGROUP_SETTLED = 30
MIN_SUBGROUP_SIDE = 10
INCONSISTENCY_PP = 5.0

S_NO_REAL = "NO_REAL_CANDIDATE"
S_COLLECTING = "COLLECTING_SHADOW_DATA"
S_FAILED = "SHADOW_FAILED"
S_UNSTABLE = "SHADOW_UNSTABLE"
S_VALIDATED = "SHADOW_VALIDATED_CANDIDATE"


def _f(v):
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def favored_win(y, fav):
    return (y == 1) if fav == "UP" else ((y == 0) if fav == "DOWN" else None)


def load_journal(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def load_calls(path):
    """Index filled/unfilled paper-call rows by EXACT ticker. Legacy rows (no ticker) are
    counted and ignored. If a ticker has several rows (e.g. a restart re-logged it), the
    earliest-signalled one is the call the first signal produced."""
    rows = []
    if os.path.exists(path):
        with open(path) as f:
            try:
                rows = json.load(f)
            except ValueError:
                rows = []
    by, legacy = {}, 0
    for r in rows:
        tk = r.get("ticker")
        if not tk:
            legacy += 1
            continue
        key = (r.get("signal_ts_epoch_ms") or float("inf"), r.get("ts") or "")
        if tk not in by or key < by[tk][0]:
            by[tk] = (key, r)
    return {tk: v[1] for tk, v in by.items()}, legacy, len(rows)


def _rate(sub, pred):
    return (sum(1 for x in sub if pred(x)) / len(sub)) if sub else None


def _mean(v):
    return sum(v) / len(v) if v else None


def _median(v):
    return ap.percentile(v, 50) if v else None


def settlement_metrics(rows):
    blk = [r for r in rows if r["decision"] == ps.WOULD_BLOCK]
    alw = [r for r in rows if r["decision"] == ps.ALLOW]
    m = {"n": len(rows), "n_blocked": len(blk), "n_allowed": len(alw),
         "baseline_win_rate": _rate(rows, lambda r: r["win"]), "allowed_win_rate": _rate(alw, lambda r: r["win"]),
         "blocked_win_rate": _rate(blk, lambda r: r["win"]),
         "baseline_loss_rate": _rate(rows, lambda r: not r["win"]), "allowed_loss_rate": _rate(alw, lambda r: not r["win"]),
         "blocked_loss_rate": _rate(blk, lambda r: not r["win"])}
    bl, al = m["blocked_loss_rate"], m["allowed_loss_rate"]
    m["blocked_minus_allowed_loss_pp"] = (bl - al) * 100.0 if bl is not None and al is not None else None
    return m


def pnl_metrics(filled):
    alw = [r for r in filled if r["decision"] == ps.ALLOW]
    blk = [r for r in filled if r["decision"] == ps.WOULD_BLOCK]
    n = len(filled)
    base_total = sum(r["pnl"] for r in filled)
    shadow_total = sum(r["pnl"] for r in alw)
    m = {"n_filled": n, "n_filled_allowed": len(alw), "n_filled_blocked": len(blk),
         "baseline_total_pnl": base_total, "baseline_mean_pnl": _mean([r["pnl"] for r in filled]),
         "baseline_mean_per_contract": _mean([r["pc"] for r in filled]),
         "baseline_median_per_contract": _median([r["pc"] for r in filled]),
         "allowed_total_pnl": shadow_total, "allowed_mean_pnl": _mean([r["pnl"] for r in alw]),
         "allowed_mean_per_contract": _mean([r["pc"] for r in alw]),
         "allowed_median_per_contract": _median([r["pc"] for r in alw]),
         "blocked_total_pnl": sum(r["pnl"] for r in blk), "blocked_mean_pnl": _mean([r["pnl"] for r in blk]),
         "blocked_mean_per_contract": _mean([r["pc"] for r in blk]),
         "blocked_median_per_contract": _median([r["pc"] for r in blk]),
         "shadow_filtered_total_pnl": shadow_total, "shadow_pnl_difference": shadow_total - base_total,
         "shadow_filtered_mean_per_contract": (sum(r["pc"] for r in alw) / n) if n else None}
    m["per_contract_improvement"] = (m["shadow_filtered_mean_per_contract"] - m["baseline_mean_per_contract"]
                                     if n else None)
    return m


def _bootstrap(rows, stat, reps, seed):
    """Ticker-clustered bootstrap. Tickers (the cluster unit) are aggregated ONCE into the
    sufficient statistics of the two supported statistics; each replicate then sums those
    aggregates for the resampled tickers instead of rebuilding a row list. The RNG draws are
    identical to the row-list version (same seed, same rng.choices call per replicate), and the
    arithmetic is identical: integer counts for the loss rates, and blocked per-contract P&L
    summed in the same resampled order (each ticker holds one row after dedupe)."""
    by = {}
    for r in rows:
        by.setdefault(r["ticker"], []).append(r)
    keys = sorted(by)
    if not keys:
        return []
    kind = "enrichment" if stat is _enrichment_stat else ("pnl" if stat is _pc_improvement_stat else None)
    rng = random.Random(seed)
    if kind is None:                                      # generic fallback (not used by this analyzer)
        return [stat([r for j in rng.choices(range(len(keys)), k=len(keys)) for r in by[keys[j]]])
                for _ in range(reps)]
    n_keys = len(keys)
    if kind == "enrichment":                              # integer sufficient statistics per ticker
        nb, lb, na, la = [0] * n_keys, [0] * n_keys, [0] * n_keys, [0] * n_keys
        for j, key in enumerate(keys):
            for r in by[key]:
                if r["decision"] == ps.WOULD_BLOCK:
                    nb[j] += 1; lb[j] += (not r["win"])
                elif r["decision"] == ps.ALLOW:
                    na[j] += 1; la[j] += (not r["win"])
    else:                                                 # row count + blocked P&L (sparse, row order kept)
        nrow = [len(by[key]) for key in keys]
        bpc = {}
        for j, key in enumerate(keys):
            vals = tuple(r["pc"] for r in by[key] if r["decision"] == ps.WOULD_BLOCK)
            if vals:
                bpc[j] = vals
    del by                                                # the row grouping is no longer needed
    idx = range(n_keys)
    out = []
    for _ in range(reps):
        pick = rng.choices(idx, k=n_keys)
        if kind == "enrichment":
            b = sum(nb[j] for j in pick)
            a = sum(na[j] for j in pick)
            out.append((sum(lb[j] for j in pick) / b - sum(la[j] for j in pick) / a) * 100.0 if b and a else 0.0)
        else:
            n = sum(nrow[j] for j in pick)
            out.append((-sum(v for j in pick if j in bpc for v in bpc[j]) / n) if n else 0.0)
    return out


def _enrichment_stat(sample):
    m = settlement_metrics(sample)
    return m["blocked_minus_allowed_loss_pp"] if m["blocked_minus_allowed_loss_pp"] is not None else 0.0


def _pc_improvement_stat(sample):
    n = len(sample)
    return (-sum(r["pc"] for r in sample if r["decision"] == ps.WOULD_BLOCK) / n) if n else 0.0


def _subgroup(rows, filled, key):
    out = {}
    for val in sorted({key(r) for r in rows} | {key(r) for r in filled}):
        s = [r for r in rows if key(r) == val]
        f = [r for r in filled if key(r) == val]
        m = settlement_metrics(s)
        out[str(val)] = {"n_settled": m["n"], "block_rate": (m["n_blocked"] / m["n"]) if m["n"] else None,
                         "baseline_win_rate": m["baseline_win_rate"], "allowed_win_rate": m["allowed_win_rate"],
                         "blocked_win_rate": m["blocked_win_rate"],
                         "blocked_minus_allowed_loss_pp": m["blocked_minus_allowed_loss_pp"],
                         "n_blocked": m["n_blocked"], "n_allowed": m["n_allowed"],
                         "n_filled": len(f), "mean_per_contract": _mean([r["pc"] for r in f])}
    return out


def _inconsistent(groups):
    """Two adequately sized groups whose loss enrichment points in opposite directions."""
    eff = [g["blocked_minus_allowed_loss_pp"] for g in groups.values()
           if g["n_settled"] >= MIN_SUBGROUP_SETTLED and g["n_blocked"] >= MIN_SUBGROUP_SIDE
           and g["n_allowed"] >= MIN_SUBGROUP_SIDE and g["blocked_minus_allowed_loss_pp"] is not None]
    return any(e >= INCONSISTENCY_PP for e in eff) and any(e <= -INCONSISTENCY_PP for e in eff)


def _summary(values):
    v = [x for x in values if x is not None]
    if not v:
        return None
    mu = sum(v) / len(v)
    return {"n": len(v), "mean": mu, "std": math.sqrt(sum((x - mu) ** 2 for x in v) / len(v)),
            **{f"p{int(q):02d}": ap.percentile(v, q) for q in (5, 25, 50, 75, 95)}}


def analyze_policy(pol, journal, labels, calls, gates, reps, seed, now):
    g = dict(DEFAULT_SHADOW_GATES, **(gates or {}))
    pid, thr, cutoff = pol["policy_id"], float(pol["conflict_threshold"]), float(pol["discovery_cutoff_epoch"])
    flags, reasons = [], []
    raw = [r for r in journal if r.get("policy_id") == pid]
    integrity = {"policy_hash_mismatch": 0, "threshold_mismatch": 0, "decision_mismatch": 0,
                 "duplicate_rows": 0, "pre_discovery_rows": 0}
    seen, rows = set(), []
    for r in sorted(raw, key=lambda r: (_f(r.get("signal_ts_epoch_ms")) or 0, r.get("binary_ticker") or "")):
        if r.get("policy_hash") != pol["policy_hash"]:
            integrity["policy_hash_mismatch"] += 1
        rt = _f(r.get("conflict_threshold"))
        if rt is None or abs(rt - thr) > 1e-12:
            integrity["threshold_mismatch"] += 1
        dec, cs = r.get("shadow_decision"), _f(r.get("conflict_score"))
        if dec in (ps.ALLOW, ps.WOULD_BLOCK):
            expect = ps.WOULD_BLOCK if (cs is not None and cs <= thr) else ps.ALLOW
            if cs is None or expect != dec:
                integrity["decision_mismatch"] += 1
        tk = r.get("binary_ticker")
        if tk in seen:
            integrity["duplicate_rows"] += 1
            continue
        seen.add(tk)
        ts_ms = _f(r.get("signal_ts_epoch_ms"))
        if ts_ms is None or ts_ms / 1000.0 <= cutoff:
            integrity["pre_discovery_rows"] += 1          # PRE_DISCOVERY_SHADOW_ROW: never evaluated
            continue
        rows.append({"ticker": tk, "coin": r.get("coin"), "fav": r.get("fav"), "decision": dec,
                     "ts": ts_ms / 1000.0, "fv": _f(r.get("candidate_feature_value")), "cs": cs})
    if integrity["pre_discovery_rows"]:
        flags.append("PRE_DISCOVERY_SHADOW_ROW")
    if integrity["duplicate_rows"]:
        flags.append("DUPLICATE_SHADOW_ROW")
    if integrity["policy_hash_mismatch"] or integrity["threshold_mismatch"] or integrity["decision_mismatch"]:
        flags.append("POLICY_OR_DECISION_MUTATION")

    total = len(rows)
    avail = [r for r in rows if r["decision"] in (ps.ALLOW, ps.WOULD_BLOCK)]
    for r in avail:
        lab = labels.get(r["ticker"])
        r["y"] = lab["y"] if lab else None
        r["win"] = favored_win(r["y"], r["fav"]) if lab else None
    settled = [r for r in avail if r["win"] is not None]
    filled, unfilled, unmatched = [], 0, 0
    for r in avail:
        c = calls.get(r["ticker"])
        if c is None:
            unmatched += 1
            continue
        if c.get("exit_reason") == "unfilled" or c.get("win") is None:
            unfilled += 1                                   # maker never filled: not a trade
            continue
        filled.append(dict(r, pnl=float(c.get("pnl", 0.0)), pc=float(c.get("per_contract", 0.0))))
    n_blk = sum(1 for r in avail if r["decision"] == ps.WOULD_BLOCK)
    res = {"policy_id": pid, "feature": pol["feature_name"], "coin_or_group": pol["coin_or_group"],
           "synthetic": pol["synthetic"], "discovery_cutoff_utc": pol["discovery_cutoff_utc"],
           "frozen_threshold": thr, "threshold_used": thr, "integrity": integrity,
           "prospective_first_ts": min((r["ts"] for r in rows), default=None),
           "prospective_last_ts": max((r["ts"] for r in rows), default=None),
           "shadow_total": total, "shadow_available": len(avail), "shadow_unavailable": total - len(avail),
           "coverage_pct": 100.0 * len(avail) / total if total else None,
           "would_block": n_blk, "would_allow": len(avail) - n_blk,
           "block_fraction": n_blk / len(avail) if avail else None,
           "retention_fraction": (len(avail) - n_blk) / len(avail) if avail else None,
           "settled_total": len(settled), "calls_matched_filled": len(filled),
           "calls_matched_unfilled_maker": unfilled, "calls_unmatched": unmatched}
    res["prospective_days"] = ((res["prospective_last_ts"] - res["prospective_first_ts"]) / 86400.0
                               if rows else 0.0)
    res["settlement"] = settlement_metrics(settled)
    res["pnl"] = pnl_metrics(filled)
    enr = _bootstrap(settled, _enrichment_stat, reps, ap._derived_seed(seed, pid, "enrichment"))
    pci = _bootstrap(filled, _pc_improvement_stat, reps, ap._derived_seed(seed, pid, "pnl"))
    res["loss_enrichment_ci95_pp"] = list(ap.ci95(enr)) if enr else [None, None]
    res["per_contract_improvement_ci95"] = list(ap.ci95(pci)) if pci else [None, None]
    res["bootstrap_reps"] = reps

    exp_bf = pol.get("expected_training_block_fraction")
    res["expected_training_block_fraction"] = exp_bf
    if exp_bf and len(avail) >= MIN_ROWS_FOR_SHIFT and res["block_fraction"] is not None:
        if res["block_fraction"] < BLOCK_RATE_SHIFT_LOW * exp_bf or res["block_fraction"] > BLOCK_RATE_SHIFT_HIGH * exp_bf:
            flags.append("BLOCK_RATE_SHIFT")
    tr = pol.get("training_feature_summary") or {}
    pr = _summary([r["fv"] for r in avail])
    drift = {"training": tr, "prospective": pr}
    if tr and pr and tr.get("std"):
        drift["standardized_mean_shift"] = (pr["mean"] - tr["mean"]) / tr["std"]
        drift["median_shift_in_training_std"] = (pr["p50"] - tr["p50"]) / tr["std"]
        if pr["n"] >= MIN_SUBGROUP_SETTLED and abs(drift["standardized_mean_shift"]) > FEATURE_SHIFT_SMD:
            flags.append("FEATURE_DISTRIBUTION_SHIFT")
    res["drift"] = drift
    res["direction"] = _subgroup(settled, filled, lambda r: r["fav"])
    if _inconsistent(res["direction"]):
        flags.append("DIRECTION_INCONSISTENT")
    if pol["coin_or_group"] == "ALL":
        res["coins"] = _subgroup(settled, filled, lambda r: r["coin"])
        if _inconsistent(res["coins"]):
            flags.append("COIN_INCONSISTENT")
    if settled:
        mid = ap.percentile([r["ts"] for r in settled], 50)
        res["time_halves"] = _subgroup(settled, filled, lambda r: "first_half" if r["ts"] <= mid else "second_half")
        if _inconsistent(res["time_halves"]):
            flags.append("TIME_INCONSISTENT")
    created = ap._parse_iso(pol.get("created_at_utc"))
    if created is not None and now - created > POLICY_MAX_AGE_DAYS * 86400.0:
        flags.append("POLICY_STALE")
    if pol["synthetic"]:
        flags.append("SYNTHETIC_TEST_POLICY")

    st, sm = res["settlement"], res["pnl"]
    sample_ok = (res["prospective_days"] >= g["min_calendar_days"] and len(settled) >= g["min_settled"]
                 and st["n_blocked"] >= g["min_blocked"] and st["n_allowed"] >= g["min_allowed"]
                 and (res["coverage_pct"] or 0) >= g["min_coverage_pct"]
                 and 100.0 * (res["retention_fraction"] or 0) >= g["min_retention_pct"])
    enr_pt, pc_pt = st["blocked_minus_allowed_loss_pp"], sm["per_contract_improvement"]
    enr_lo, pc_lo = res["loss_enrichment_ci95_pp"][0], res["per_contract_improvement_ci95"][0]
    if "POLICY_OR_DECISION_MUTATION" in flags:
        status, reasons = S_FAILED, ["frozen policy/threshold/decision integrity violated"]
    elif not sample_ok:
        status, reasons = S_COLLECTING, ["prospective sample below validation gates"]
    elif enr_pt is None or enr_pt <= 0 or pc_pt is None or pc_pt <= 0:
        status, reasons = S_FAILED, ["blocked calls not worse, or filtered per-contract P&L not better"]
    elif enr_lo is None or enr_lo <= 0 or pc_lo is None or pc_lo <= 0:
        # subgroup heterogeneity around an unestablished effect is noise, not "instability"
        status, reasons = S_FAILED, ["prospective effect not established (a 95% CI includes 0)"]
    elif any(f in flags for f in ("DIRECTION_INCONSISTENT", "COIN_INCONSISTENT", "TIME_INCONSISTENT",
                                  "BLOCK_RATE_SHIFT", "FEATURE_DISTRIBUTION_SHIFT")):
        status, reasons = S_UNSTABLE, ["effect varies by direction/coin/time or distribution shifted"]
    else:
        status, reasons = S_VALIDATED, ["all prospective gates met — eligible for Step 5 review only; nothing enabled"]
    res["flags"], res["status"], res["status_reasons"] = flags, status, reasons
    return res


def run(journal_path, labels_path, calls_path, policies_path, outdir="analysis_output", allow_synthetic=False,
        gates=None, reps=BOOTSTRAP_REPS, seed=SEED, now=None, log=print):
    g = dict(DEFAULT_SHADOW_GATES, **(gates or {}))
    if g != DEFAULT_SHADOW_GATES and not allow_synthetic:
        raise ValueError("validation gates may only be overridden with --allow-synthetic-test-data")
    now = time.time() if now is None else now
    pols, errs, status = ps.load_policy_registry(policies_path, allow_synthetic=allow_synthetic)
    journal = load_journal(journal_path)
    labels = {}
    if os.path.exists(labels_path):
        labels, _ = ap.load_labels(labels_path)
    calls, legacy, n_calls = load_calls(calls_path)
    results = [analyze_policy(p.p, journal, labels, calls, g, reps, seed, now) for p in pols]
    report = {"title": "STEP 4 OF 6 — PROSPECTIVE PERP FILTER SHADOW VALIDATION",
              "policy_load_status": status, "policy_errors": errs,
              "status": S_NO_REAL if not pols else "EVALUATED", "gates": g,
              "call_journal": {"rows": n_calls, "legacy_rows_without_ticker": legacy, "tickers": len(calls)},
              "journal_rows": len(journal), "policies": results}
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "perp_shadow_report.json"), "w") as f:
        json.dump(dict(report, generated_at_utc=dt.datetime.now(dt.timezone.utc).isoformat()),
                  f, indent=1, sort_keys=True, default=str)
    if log:
        log(render(report))
    return report


def _p(x, n=2, pct=False):
    if x is None:
        return "-"
    return f"{x * 100:.{n}f}%" if pct else f"{x:.{n}f}"


def render(rep):
    L = [rep["title"], "", "Shadow evaluation only. No policy is enabled; the bot is unchanged.", ""]
    if rep["policy_errors"]:
        L.append(f"Policy load errors: {rep['policy_errors']}")
    if not rep["policies"]:
        L.append("No real shadow policy exists yet because Step 3 has no real first-signal candidate.")
        return "\n".join(L)
    for r in rep["policies"]:
        st, pm = r["settlement"], r["pnl"]
        ts = lambda t: dt.datetime.fromtimestamp(t, dt.timezone.utc).isoformat() if t else "-"
        L += [f"Policy ID          {r['policy_id']}{'  (SYNTHETIC TEST POLICY)' if r['synthetic'] else ''}",
              f"Feature            {r['feature']}", f"Coin/group         {r['coin_or_group']}",
              f"Discovery cutoff   {r['discovery_cutoff_utc']}", f"Frozen threshold   {r['frozen_threshold']:.6f}",
              f"Prospective days   {r['prospective_days']:.1f}  ({ts(r['prospective_first_ts'])} -> {ts(r['prospective_last_ts'])})",
              f"Calls observed     {r['shadow_total']}   coverage {_p(r['coverage_pct'], 1)}%   settled {r['settled_total']}",
              f"Would block        {r['would_block']}   would allow {r['would_allow']}",
              f"Win%  baseline {_p(st['baseline_win_rate'], 1, True)}  allowed {_p(st['allowed_win_rate'], 1, True)}"
              f"  blocked {_p(st['blocked_win_rate'], 1, True)}",
              f"Filled calls       {pm['n_filled']}  (unfilled maker {r['calls_matched_unfilled_maker']}, "
              f"unmatched {r['calls_unmatched']})",
              f"Mean P&L/contract  baseline {_p(pm['baseline_mean_per_contract'])}c   shadow-filtered "
              f"{_p(pm['shadow_filtered_mean_per_contract'])}c   difference {_p(pm['per_contract_improvement'])}c "
              f"95% CI [{_p(r['per_contract_improvement_ci95'][0])}, {_p(r['per_contract_improvement_ci95'][1])}]",
              f"Block loss enrichment {_p(st['blocked_minus_allowed_loss_pp'], 1)}pp  95% CI "
              f"[{_p(r['loss_enrichment_ci95_pp'][0], 1)}, {_p(r['loss_enrichment_ci95_pp'][1], 1)}]",
              f"Block rate         training {_p(r['expected_training_block_fraction'], 1, True)}   prospective "
              f"{_p(r['block_fraction'], 1, True)}",
              f"Flags              {', '.join(r['flags']) or 'none'}",
              f"Status             {r['status']}  ({'; '.join(r['status_reasons'])})", ""]
    return "\n".join(L)


def main(argv=None):
    a_ = argparse.ArgumentParser(description="Prospective shadow validation of frozen perp policies (offline).")
    a_.add_argument("--journal", default="kalshi_perp_shadow.csv")
    a_.add_argument("--labels", default="kalshi_binary_outcomes.csv")
    a_.add_argument("--calls", default="kalshi_calls.json")
    a_.add_argument("--policies", default="perp_shadow_policies.json")
    a_.add_argument("--outdir", default="analysis_output")
    a_.add_argument("--bootstrap-reps", type=int, default=BOOTSTRAP_REPS)
    a_.add_argument("--allow-synthetic-test-data", action="store_true", help="TESTS ONLY")
    for k, v in DEFAULT_SHADOW_GATES.items():
        a_.add_argument("--" + k.replace("_", "-"), type=type(v), default=v)
    a = a_.parse_args(argv)
    gates = {k: getattr(a, k) for k in DEFAULT_SHADOW_GATES}
    try:
        run(a.journal, a.labels, a.calls, a.policies, a.outdir, a.allow_synthetic_test_data, gates, a.bootstrap_reps)
    except ValueError as e:
        print(f"SHADOW ANALYSIS REFUSED: {e}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
