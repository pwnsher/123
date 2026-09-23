#!/usr/bin/env python3
"""
analyze_perp_integration.py — STEP 5: offline validation of the conservative perp probability
overlay for frozen Step 5 experiments. No network; changes nothing in the bot.

    python analyze_perp_integration.py
    python analyze_perp_integration.py --experiments perp_integration_experiments.json \
        --journal kalshi_perp_shadow.csv --labels kalshi_binary_outcomes.csv --calls kalshi_calls.json \
        --policies perp_shadow_policies.json --shadow-report analysis_output/perp_shadow_report.json

Universe: existing FIRST signals scored by the validated Step 4 policy, signal time strictly
after step5_start_cutoff (never Step 4 validation data), Step 4 decision ALLOW (Step 4's block
stays the base filter; WOULD_BLOCK rows are descriptive only). One row per ticker.
Whole close-time groups are split chronologically: first 50% ALPHA_TUNING, last 50% ALPHA_HOLDOUT.
Alpha is chosen from the fixed grid on TUNING only (smallest qualifying alpha), frozen, and only
then evaluated once on HOLDOUT. P&L comes from actual paper calls joined by exact ticker; a Step 5
block counts as 0 P&L; unfilled maker orders are not trades.
"""
import argparse
import csv
import datetime as dt
import json
import math
import os
import sys
import time

import analyze_perp_predictive as ap
import build_perp_integration_experiment as bx
import perp_probability as pp
import perp_shadow as ps

DEFAULT_GATES = {
    "min_calendar_days": 14.0,
    "min_settled_total": 400,
    "min_tuning_settled": 200,
    "min_holdout_settled": 200,
    "min_holdout_filled": 150,
    "min_holdout_yes": 50,
    "min_holdout_no": 50,
    "min_feature_coverage_pct": 95.0,
}
MIN_RETENTION_PCT = 90.0             # tuning + holdout
MAX_ADDITIONAL_BLOCK_PCT = 10.0      # tuning
BOOTSTRAP_REPS = 2000
SEED = ap.SEED
MIN_SUBGROUP = 50
INCONSISTENT_BRIER = 0.001
DELTA_CAP_FREQUENT_RATE = 0.25
PROB_TOL = 1e-9

S_NONE = "NO_SHADOW_VALIDATED_POLICY"
S_COLLECTING = "COLLECTING_INTEGRATION_DATA"
S_NO_ALPHA = "NO_ALPHA_QUALIFIED"
S_FAILED = "PROBABILITY_OVERLAY_FAILED"
S_UNSTABLE = "PROBABILITY_OVERLAY_UNSTABLE"
S_VALIDATED = "PROBABILITY_OVERLAY_VALIDATED"

SCREEN_COLUMNS = [
    "experiment_id", "policy_id", "alpha", "tuning_n", "tuning_brier_legacy", "tuning_brier_integrated",
    "tuning_brier_improvement", "tuning_logloss_legacy", "tuning_logloss_integrated", "tuning_logloss_improvement",
    "tuning_retention_pct", "tuning_additional_block_pct", "tuning_legacy_pc_pnl", "tuning_integrated_pc_pnl",
    "tuning_pc_improvement", "qualifies_tuning", "selected_alpha", "holdout_n", "holdout_brier_legacy",
    "holdout_brier_integrated", "holdout_brier_improvement", "holdout_brier_ci_low", "holdout_brier_ci_high",
    "holdout_logloss_improvement", "holdout_logloss_ci_low", "holdout_logloss_ci_high", "holdout_pc_improvement",
    "holdout_pc_ci_low", "holdout_pc_ci_high", "holdout_retention_pct", "holdout_additional_block_pct", "status"]


class IntegrityError(Exception):
    pass


def _f(v):
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def validate_experiment(e, allow_synthetic=False):
    errs = []
    if not isinstance(e, dict):
        return ["experiment is not an object"]
    for k in ("experiment_id", "experiment_hash", "source_policy_id", "source_policy_hash", "step5_start_cutoff",
              "strategy_constants", "alpha_grid", "max_abs_perp_delta", "overlay_version", "synthetic", "mode",
              "source_hashes", "frozen_threshold", "coin_or_group", "feature_name"):
        if k not in e:
            return [f"missing {k}"]
    if bx.compute_experiment_hash(e) != e["experiment_hash"]:
        errs.append("experiment_hash does not match content (edited after freezing?)")
    if e["mode"] != "SHADOW_ANALYSIS_ONLY":
        errs.append("unexpected mode")
    if any(w in k.lower() for k in e for w in ("activation", "enabled", "live", "trading")):
        errs.append("activation-like keys are not allowed in an experiment")
    if e["synthetic"] is not False and not allow_synthetic:
        errs.append("synthetic/test experiment refused")
    if tuple(e["alpha_grid"]) != pp.ALPHA_GRID or e["max_abs_perp_delta"] != pp.MAX_ABS_PERP_DELTA \
            or e["overlay_version"] != pp.OVERLAY_VERSION:
        errs.append("experiment was frozen with a different overlay definition")
    try:
        pp.check_constants(e["strategy_constants"])
    except ValueError as x:
        errs.append(str(x))
    return errs


def load_json(path):
    try:
        return json.load(open(path))
    except (OSError, ValueError):
        return None


def load_calls(path):
    rows = load_json(path) or []
    by, legacy = {}, 0
    for r in rows if isinstance(rows, list) else []:
        tk = r.get("ticker")
        if not tk:
            legacy += 1
            continue
        key = (r.get("signal_ts_epoch_ms") or float("inf"), r.get("ts") or "")
        if tk not in by or key < by[tk][0]:
            by[tk] = (key, r)
    return {tk: v[1] for tk, v in by.items()}, legacy


# ─────────────────────── row preparation (post-cutoff, integrity-checked) ───────────────────────
def prepare_rows(exp, journal, labels, calls):
    pid, cutoff = exp["source_policy_id"], float(exp["step5_start_cutoff"])
    thr = float(exp["frozen_threshold"])
    k = exp["strategy_constants"]
    c = {"rows_policy": 0, "pre_step5_cutoff": 0, "duplicate": 0, "probability_integrity_error": 0,
         "legacy_edge_mismatch": 0, "step4_would_block": 0, "step4_unavailable": 0, "unsettled": 0,
         "unfilled_maker": 0, "call_unmatched": 0}
    raw = [r for r in journal if r.get("policy_id") == pid]
    c["rows_policy"] = len(raw)
    for r in raw:                                        # frozen Step 4 policy must be unchanged: FAIL CLOSED
        rt = _f(r.get("conflict_threshold"))
        if r.get("policy_hash") != exp["source_policy_hash"] or rt is None or abs(rt - thr) > 1e-12:
            raise IntegrityError(f"shadow row {r.get('binary_ticker')} was scored by a different policy/threshold")
    rows, seen, allrows = [], set(), []
    for r in sorted(raw, key=lambda r: (_f(r.get("signal_ts_epoch_ms")) or 0.0, r.get("binary_ticker") or "")):
        tk = r.get("binary_ticker")
        if tk in seen:
            c["duplicate"] += 1
            continue
        seen.add(tk)
        ts_ms = _f(r.get("signal_ts_epoch_ms"))
        if ts_ms is None or ts_ms / 1000.0 <= cutoff:
            c["pre_step5_cutoff"] += 1                   # PRE_STEP5_CUTOFF_ROW: never Step 5 evidence
            continue
        close = ap._parse_iso(r.get("binary_close_time"))
        lab = labels.get(tk)
        if close is None and lab:
            close = lab.get("close")
        base = {"ticker": tk, "coin": r.get("coin"), "fav": r.get("fav"), "ts": ts_ms / 1000.0,
                "close": close if close is not None else ts_ms / 1000.0, "step4": r.get("shadow_decision")}
        allrows.append(base)
        if base["step4"] == ps.UNAVAILABLE:
            c["step4_unavailable"] += 1
            continue
        pb, pc = _f(r.get("model_b_p_up")), _f(r.get("model_c_p_up"))
        pbf, pcf, cs = _f(r.get("model_b_p_favored")), _f(r.get("model_c_p_favored")), _f(r.get("conflict_score"))
        ok = (pb is not None and pc is not None and 0 <= pb <= 1 and 0 <= pc <= 1 and base["fav"] in ("UP", "DOWN"))
        if ok:
            ebf, ecf = ps.p_favored(pb, base["fav"]), ps.p_favored(pc, base["fav"])
            ok = (pbf is not None and abs(pbf - ebf) <= PROB_TOL and pcf is not None and abs(pcf - ecf) <= PROB_TOL
                  and cs is not None and abs(cs - (ecf - ebf)) <= PROB_TOL
                  and ((cs <= thr) == (base["step4"] == ps.WOULD_BLOCK)))
        if not ok:
            c["probability_integrity_error"] += 1        # SHADOW_PROBABILITY_INTEGRITY_ERROR: excluded
            base["corrupt"] = True
            continue
        p_pct, ask = _f(r.get("base_p_up")), _f(r.get("side_ask"))
        bconf, braw, bnet = _f(r.get("base_conf")), _f(r.get("raw_edge")), _f(r.get("net_edge"))
        legacy_ok = None not in (p_pct, ask, bconf, braw, bnet) and ((p_pct >= 50.0) == (base["fav"] == "UP"))
        if legacy_ok:
            conf = max(p_pct, 100.0 - p_pct)             # exact legacy formula (evaluate())
            legacy_ok = (abs(conf - bconf) <= 0.06 and abs((conf - ask) - braw) <= 0.06
                         and abs((braw - k["ENTRY_COST_CENTS"]) - bnet) <= 0.11)
        if not legacy_ok:
            c["legacy_edge_mismatch"] += 1
            base["corrupt"] = True
            continue
        base.update(pb=pb, pc=pc, p_base=p_pct / 100.0, side_ask=ask, legacy_conf=bconf)
        base["y"] = lab["y"] if lab else None
        base["win"] = ((base["y"] == 1) if base["fav"] == "UP" else (base["y"] == 0)) if lab else None
        call = calls.get(tk)
        base["filled"] = False
        if call is None:
            c["call_unmatched"] += 1
        elif call.get("exit_reason") == "unfilled" or call.get("win") is None:
            c["unfilled_maker"] += 1                     # not a trade
        else:
            base.update(filled=True, pc_pnl=float(call.get("per_contract", 0.0)), pnl=float(call.get("pnl", 0.0)))
        if base["step4"] == ps.WOULD_BLOCK:
            c["step4_would_block"] += 1
            base["step4_blocked"] = True
            continue                                     # outside the Step 5 alpha universe
        if base["y"] is None:
            c["unsettled"] += 1
            continue
        rows.append(base)
    usable = sum(1 for r in allrows if r["step4"] in (ps.ALLOW, ps.WOULD_BLOCK) and not r.get("corrupt"))
    c["coverage_pct"] = 100.0 * usable / len(allrows) if allrows else None
    c["post_cutoff_rows"] = len(allrows)
    return rows, c, allrows


def split_tuning_holdout(rows):
    """Chronological 50/50 split over WHOLE close-time groups (simultaneous closes stay together)."""
    groups = {}
    for r in rows:
        groups.setdefault(r["close"], []).append(r)
    order = sorted(groups)
    tuning, holdout, n = [], [], len(rows)
    for t in order:
        (tuning if len(tuning) < n / 2.0 else holdout).extend(sorted(groups[t], key=lambda r: r["ticker"]))
    return tuning, holdout


# ─────────────────────── metrics for one alpha on one set ───────────────────────
def score_rows(rows, alpha, constants):
    out = []
    for r in rows:
        o = pp.overlay(r["p_base"], r["pb"], r["pc"], alpha, r["fav"], r["side_ask"], constants, legacy_signal=True)
        out.append(dict(r, o=o))
    return out


def set_metrics(scored):
    n = len(scored)
    y = [r["y"] for r in scored]
    pl = [pp.clamp_p(r["p_base"]) for r in scored]
    pi = [r["o"]["integrated_p_up"] for r in scored]
    blk = [r for r in scored if r["o"]["decision"] != pp.WOULD_ALLOW]
    alw = [r for r in scored if r["o"]["decision"] == pp.WOULD_ALLOW]
    filled = [r for r in scored if r["filled"]]
    rate = lambda s, f: (sum(1 for r in s if f(r)) / len(s)) if s else None
    m = {"n": n, "yes": sum(y), "no": n - sum(y),
         "brier_legacy": ap.brier(pl, y) if n else None, "brier_integrated": ap.brier(pi, y) if n else None,
         "logloss_legacy": ap.logloss(pl, y) if n else None, "logloss_integrated": ap.logloss(pi, y) if n else None,
         "auc_legacy": ap.auc(pl, y) if n else None, "auc_integrated": ap.auc(pi, y) if n else None,
         "ece_legacy": ap.ece(pl, y) if n else None, "ece_integrated": ap.ece(pi, y) if n else None,
         "would_allow": len(alw), "would_block": len(blk),
         "block_reasons": {d: sum(1 for r in blk if r["o"]["decision"] == d) for d in pp.BLOCK_DECISIONS},
         "additional_block_fraction": len(blk) / n if n else None, "retention_fraction": len(alw) / n if n else None,
         "legacy_win_rate": rate(scored, lambda r: r["win"]), "allowed_win_rate": rate(alw, lambda r: r["win"]),
         "blocked_win_rate": rate(blk, lambda r: r["win"]),
         "n_filled": len(filled),
         "new_calls_created": 0, "direction_reversals_traded": sum(
             1 for r in alw if pp.direction_flips(r["o"]["integrated_p_up"], r["fav"])),
         "numeric_ok": all(math.isfinite(p) and 0 < p < 1 for p in pi)}
    for a, b in (("brier", "improvement"), ("logloss", "improvement")):
        if m[f"{a}_legacy"] is not None:
            m[f"{a}_improvement"] = m[f"{a}_legacy"] - m[f"{a}_integrated"]
    m["auc_improvement"] = (m["auc_integrated"] - m["auc_legacy"]) if None not in (m["auc_legacy"], m["auc_integrated"]) else None
    bl = rate(blk, lambda r: not r["win"]); al = rate(alw, lambda r: not r["win"])
    m["blocked_minus_allowed_loss_pp"] = (bl - al) * 100.0 if bl is not None and al is not None else None
    if filled:
        nf = len(filled)
        m["legacy_total_pnl"] = sum(r["pnl"] for r in filled)
        m["integrated_total_pnl"] = sum(r["pnl"] for r in filled if r["o"]["decision"] == pp.WOULD_ALLOW)
        m["pnl_difference"] = m["integrated_total_pnl"] - m["legacy_total_pnl"]
        m["legacy_mean_pnl_per_call"] = m["legacy_total_pnl"] / nf
        m["integrated_mean_pnl_per_call"] = m["integrated_total_pnl"] / nf
        m["legacy_mean_pc_pnl"] = sum(r["pc_pnl"] for r in filled) / nf
        m["integrated_mean_pc_pnl"] = sum(r["pc_pnl"] for r in filled if r["o"]["decision"] == pp.WOULD_ALLOW) / nf
        m["pc_improvement"] = m["integrated_mean_pc_pnl"] - m["legacy_mean_pc_pnl"]
    else:
        for key in ("legacy_total_pnl", "integrated_total_pnl", "pnl_difference", "legacy_mean_pnl_per_call",
                    "integrated_mean_pnl_per_call", "legacy_mean_pc_pnl", "integrated_mean_pc_pnl", "pc_improvement"):
            m[key] = None
    return m


def tuning_reasons(m, coverage_pct, gates):
    """Tuning-only gates (spec 42). Empty = qualifies."""
    why = []
    if m.get("brier_improvement") is None or m["brier_improvement"] <= 0:
        why.append("Brier not improved")
    if m.get("logloss_improvement") is None or m["logloss_improvement"] <= 0:
        why.append("log loss not improved")
    if m.get("pc_improvement") is None or m["pc_improvement"] < 0:
        why.append("filtered per-contract P&L worse than legacy")
    if m.get("retention_fraction") is None or 100.0 * m["retention_fraction"] < MIN_RETENTION_PCT:
        why.append("retention below 90%")
    if m.get("additional_block_fraction") is None or 100.0 * m["additional_block_fraction"] > MAX_ADDITIONAL_BLOCK_PCT:
        why.append("additional block fraction above 10%")
    if coverage_pct is None or coverage_pct < gates["min_feature_coverage_pct"]:
        why.append("coverage below minimum")
    if not m.get("numeric_ok", False):
        why.append("impossible numeric values")
    return why


def select_alpha(screen):
    """Smallest qualifying alpha from the fixed grid (NOT the best-looking one)."""
    for a in pp.ALPHA_GRID:
        s = screen.get(a)
        if s and s["qualifies"]:
            return a
    return None


def _dist(v):
    v = [x for x in v if x is not None]
    if not v:
        return None
    mu = sum(v) / len(v)
    return {"n": len(v), "mean": mu, "std": math.sqrt(sum((x - mu) ** 2 for x in v) / len(v)),
            "min": min(v), "max": max(v), **{f"p{int(q):02d}": ap.percentile(v, q) for q in (5, 25, 50, 75, 95)}}


def _group_diag(scored, key):
    out = {}
    for val in sorted({str(key(r)) for r in scored}):
        s = [r for r in scored if str(key(r)) == val]
        m = set_metrics(s)
        out[val] = {"n": m["n"], "brier_improvement": m.get("brier_improvement"),
                    "logloss_improvement": m.get("logloss_improvement"),
                    "additional_block_rate": m["additional_block_fraction"], "legacy_win_rate": m["legacy_win_rate"],
                    "allowed_win_rate": m["allowed_win_rate"], "pc_improvement": m.get("pc_improvement")}
    return out


def _inconsistent(groups):
    imps = [g["brier_improvement"] for g in groups.values() if g["n"] >= MIN_SUBGROUP and g["brier_improvement"] is not None]
    return any(x >= INCONSISTENT_BRIER for x in imps) and any(x <= -INCONSISTENT_BRIER for x in imps)


def evaluate_holdout(holdout, alpha, constants, coin_group, reps, seed):
    """Called ONCE, after alpha is frozen from the tuning half."""
    scored = score_rows(holdout, alpha, constants)
    m = set_metrics(scored)
    y = [r["y"] for r in scored]

    def ll1(p, yy):
        pc = pp.clamp_p(p)
        return -(yy * math.log(pc) + (1 - yy) * math.log(1 - pc))
    if scored:
        db = [(pp.clamp_p(r["p_base"]) - r["y"]) ** 2 - (r["o"]["integrated_p_up"] - r["y"]) ** 2 for r in scored]
        dl = [ll1(r["p_base"], r["y"]) - ll1(r["o"]["integrated_p_up"], r["y"]) for r in scored]
        b = ap.cluster_bootstrap([r["ticker"] for r in scored], {"b": db, "l": dl}, reps, ap._derived_seed(seed, "prob"))
        m["brier_ci95"], m["logloss_ci95"] = list(ap.ci95(b["b"])), list(ap.ci95(b["l"]))
    else:
        m["brier_ci95"] = m["logloss_ci95"] = [None, None]
    filled = [r for r in scored if r["filled"]]
    if filled:
        dp = [(0.0 if r["o"]["decision"] != pp.WOULD_ALLOW else r["pc_pnl"]) - r["pc_pnl"] for r in filled]
        m["pc_ci95"] = list(ap.ci95(ap.cluster_bootstrap([r["ticker"] for r in filled], {"p": dp}, reps,
                                                          ap._derived_seed(seed, "pnl"))["p"]))
    else:
        m["pc_ci95"] = [None, None]
    m["bootstrap_reps"] = reps
    m["delta_distribution"] = {"delta_raw": _dist([r["o"]["delta_raw"] for r in scored]),
                               "delta_capped": _dist([r["o"]["delta_capped"] for r in scored]),
                               "applied_delta": _dist([r["o"]["applied_delta"] for r in scored])}
    m["delta_cap_hit_rate"] = (sum(1 for r in scored if r["o"]["cap_hit"]) / len(scored)) if scored else None
    m["direction"] = _group_diag(scored, lambda r: r["fav"])
    if coin_group == "ALL":
        m["coins"] = _group_diag(scored, lambda r: r["coin"])
    if scored:
        mid = ap.percentile([r["close"] for r in scored], 50)
        m["time_halves"] = _group_diag(scored, lambda r: "first_half" if r["close"] <= mid else "second_half")
    m["start"] = min((r["ts"] for r in scored), default=None)
    m["end"] = max((r["ts"] for r in scored), default=None)
    return m


def analyze_experiment(exp, journal, labels, calls, gates, reps, seed):
    g = dict(DEFAULT_GATES, **(gates or {}))
    k = exp["strategy_constants"]
    res = {"experiment_id": exp["experiment_id"], "policy_id": exp["source_policy_id"],
           "feature": exp["feature_name"], "coin_or_group": exp["coin_or_group"], "synthetic": exp["synthetic"],
           "step5_start_cutoff": exp["step5_start_cutoff"], "step5_start_cutoff_utc": exp.get("step5_start_cutoff_utc"),
           "flags": [], "screen": {}, "selected_alpha": None}
    try:
        rows, counts, allrows = prepare_rows(exp, journal, labels, calls)
    except IntegrityError as e:
        res.update(status=S_FAILED, status_reasons=[f"POLICY_INTEGRITY_FAILURE: {e}"], counts={})
        res["flags"].append("POLICY_INTEGRITY_FAILURE")
        return res
    res["counts"] = counts
    for key, flag in (("pre_step5_cutoff", "PRE_STEP5_CUTOFF_ROW"), ("probability_integrity_error",
                      "SHADOW_PROBABILITY_INTEGRITY_ERROR"), ("legacy_edge_mismatch", "LEGACY_EDGE_MISMATCH"),
                      ("duplicate", "DUPLICATE_SHADOW_ROW")):
        if counts.get(key):
            res["flags"].append(flag)
    if exp["synthetic"]:
        res["flags"].append("SYNTHETIC_TEST_EXPERIMENT")
    res["step4_would_block_descriptive"] = sum(1 for r in allrows if r["step4"] == ps.WOULD_BLOCK)
    res["new_observations"] = len(allrows)
    res["calendar_days"] = ((max(r["ts"] for r in allrows) - min(r["ts"] for r in allrows)) / 86400.0) if allrows else 0.0
    tuning, holdout = split_tuning_holdout(rows)
    res["tuning_n"], res["holdout_n"] = len(tuning), len(holdout)
    res["tuning_start"] = min((r["ts"] for r in tuning), default=None)
    res["tuning_end"] = max((r["ts"] for r in tuning), default=None)
    res["holdout_start"] = min((r["ts"] for r in holdout), default=None)
    res["holdout_end"] = max((r["ts"] for r in holdout), default=None)
    res["tuning_close_max"] = max((r["close"] for r in tuning), default=None)
    res["holdout_close_min"] = min((r["close"] for r in holdout), default=None)
    # ---- TUNING ONLY: screen the fixed grid, then freeze the smallest qualifying alpha ----
    cov = counts["coverage_pct"]
    for a in pp.ALPHA_GRID:
        m = set_metrics(score_rows(tuning, a, k))
        why = tuning_reasons(m, cov, g)
        res["screen"][a] = dict(m, qualifies=not why, reasons=why)
    res["legacy_reference_alpha0"] = set_metrics(score_rows(tuning, pp.LEGACY_ALPHA, k))
    ho_filled = sum(1 for r in holdout if r["filled"])
    ho_yes = sum(r["y"] for r in holdout)
    sample_ok = (res["calendar_days"] >= g["min_calendar_days"] and len(rows) >= g["min_settled_total"]
                 and len(tuning) >= g["min_tuning_settled"] and len(holdout) >= g["min_holdout_settled"]
                 and ho_filled >= g["min_holdout_filled"] and ho_yes >= g["min_holdout_yes"]
                 and len(holdout) - ho_yes >= g["min_holdout_no"]
                 and cov is not None and cov >= g["min_feature_coverage_pct"])
    if not sample_ok:
        res.update(status=S_COLLECTING, status_reasons=["Step 5 sample below gates (days/settled/filled/classes/coverage)"])
        return res
    alpha = select_alpha(res["screen"])
    res["selected_alpha"] = alpha
    if alpha is None:
        res.update(status=S_NO_ALPHA, status_reasons=["no grid alpha passed the tuning gates"])
        return res
    # ---- HOLDOUT: first and only touch, with the frozen alpha ----
    h = evaluate_holdout(holdout, alpha, k, exp["coin_or_group"], reps, seed)
    res["holdout"] = h
    for name, key in (("DIRECTION_INCONSISTENT", "direction"), ("COIN_INCONSISTENT", "coins"),
                      ("TIME_INCONSISTENT", "time_halves")):
        if key in h and _inconsistent(h[key]):
            res["flags"].append(name)
    if h["delta_cap_hit_rate"] is not None and h["delta_cap_hit_rate"] > DELTA_CAP_FREQUENT_RATE:
        res["flags"].append("DELTA_CAP_FREQUENT")
    fails = []
    if not (h.get("brier_improvement", 0) > 0 and (h["brier_ci95"][0] or 0) > 0):
        fails.append("holdout Brier improvement not established")
    if not (h.get("logloss_improvement", 0) > 0 and (h["logloss_ci95"][0] or 0) > 0):
        fails.append("holdout log-loss improvement not established")
    if not ((h.get("pc_improvement") or 0) > 0 and (h["pc_ci95"][0] or 0) > 0):
        fails.append("holdout per-contract P&L improvement not established")
    if h["retention_fraction"] is None or 100.0 * h["retention_fraction"] < MIN_RETENTION_PCT:
        fails.append("holdout retention below 90%")
    if h["new_calls_created"] or h["direction_reversals_traded"]:
        fails.append("overlay created a trade or reversed a direction")
    if fails:
        res.update(status=S_FAILED, status_reasons=fails)
    elif any(f in res["flags"] for f in ("DIRECTION_INCONSISTENT", "COIN_INCONSISTENT", "TIME_INCONSISTENT")):
        res.update(status=S_UNSTABLE, status_reasons=["holdout effect varies by direction/coin/time"])
    else:
        res.update(status=S_VALIDATED, status_reasons=[
            "all Step 5 gates met on untouched holdout — eligible for Step 6 consideration only; nothing activated"])
    return res


def candidate_from(exp, res):
    h, t = res["holdout"], res["screen"][res["selected_alpha"]]
    return {"candidate_schema_version": 1, "status": S_VALIDATED, "mode": "VALIDATED_CANDIDATE_ONLY",
            "activation_allowed": False, "synthetic": exp["synthetic"],
            "experiment_id": exp["experiment_id"], "source_policy_id": exp["source_policy_id"],
            "source_policy_hash": exp["source_policy_hash"],
            "source_step4_report_hash": exp["source_hashes"]["shadow_report"],
            "feature_name": exp["feature_name"], "coin_or_group": exp["coin_or_group"],
            "step5_start_cutoff": exp["step5_start_cutoff"], "tuning_start": res["tuning_start"],
            "tuning_end": res["tuning_end"], "holdout_start": res["holdout_start"], "holdout_end": res["holdout_end"],
            "selected_alpha": res["selected_alpha"], "max_abs_perp_delta": pp.MAX_ABS_PERP_DELTA,
            "strategy_constants": exp["strategy_constants"], "dashboard_sha256": exp["dashboard_sha256"],
            "tuning_metrics": {kk: v for kk, v in t.items() if kk != "reasons"},
            "holdout_metrics": {kk: v for kk, v in h.items() if kk not in ("direction", "coins", "time_halves")},
            "holdout_brier_improvement": h["brier_improvement"], "brier_ci95": h["brier_ci95"],
            "holdout_logloss_improvement": h["logloss_improvement"], "logloss_ci95": h["logloss_ci95"],
            "holdout_pc_improvement": h["pc_improvement"], "pc_ci95": h["pc_ci95"],
            "retention": h["retention_fraction"], "additional_block_fraction": h["additional_block_fraction"],
            "direction_diagnostics": h.get("direction"), "coin_diagnostics": h.get("coins"),
            "time_diagnostics": h.get("time_halves"), "source_hashes": exp["source_hashes"]}


def run(experiments_path, journal_path, labels_path, calls_path, policies_path, report_path,
        outdir="analysis_output", allow_synthetic=False, gates=None, reps=BOOTSTRAP_REPS, seed=SEED, log=print):
    g = dict(DEFAULT_GATES, **(gates or {}))
    if g != DEFAULT_GATES and not allow_synthetic:
        raise ValueError("sample gates may only be overridden with --allow-synthetic-test-data")
    reg = load_json(experiments_path) if os.path.exists(experiments_path) else {"experiments": []}
    exps, errs = [], []
    for e in (reg or {}).get("experiments", []):
        why = validate_experiment(e, allow_synthetic)
        (errs.append(f"{e.get('experiment_id', '?')}: {'; '.join(why)}") if why else exps.append(e))
    policies = {p.get("policy_id"): p for p in ((load_json(policies_path) or {}).get("policies", []))} \
        if os.path.exists(policies_path) else {}
    report_sha = bx.sha256_file(report_path) if os.path.exists(report_path) else None
    rep4 = load_json(report_path) if report_path and os.path.exists(report_path) else None
    validated4 = sum(1 for r in (rep4 or {}).get("policies", [])
                     if r.get("status") == "SHADOW_VALIDATED_CANDIDATE" and (allow_synthetic or not r.get("synthetic")))
    journal = list(csv.DictReader(open(journal_path, newline=""))) if os.path.exists(journal_path) else []
    labels = ap.load_labels(labels_path)[0] if os.path.exists(labels_path) else {}
    calls, legacy_calls = load_calls(calls_path)
    results, candidates, screen_rows = [], [], []
    for e in exps:
        pol = policies.get(e["source_policy_id"])
        if pol is None or pol.get("policy_hash") != e["source_policy_hash"] or ps.compute_policy_hash(pol) != pol.get("policy_hash"):
            results.append({"experiment_id": e["experiment_id"], "policy_id": e["source_policy_id"], "status": S_FAILED,
                            "status_reasons": ["POLICY_INTEGRITY_FAILURE: source Step 4 policy missing or changed"],
                            "flags": ["POLICY_INTEGRITY_FAILURE"]})
            continue
        if report_sha is not None and report_sha != e["source_hashes"]["shadow_report"]:
            results.append({"experiment_id": e["experiment_id"], "policy_id": e["source_policy_id"], "status": S_FAILED,
                            "status_reasons": ["SOURCE_ARTIFACT_CHANGED: Step 4 report differs from the one the "
                                               "experiment was frozen from (analyse against the archived copy)"],
                            "flags": ["SOURCE_ARTIFACT_CHANGED"]})
            continue
        res = analyze_experiment(e, journal, labels, calls, g, reps, seed)
        results.append(res)
        if res["status"] == S_VALIDATED:
            candidates.append(candidate_from(e, res))
        h = res.get("holdout") or {}
        for a in pp.ALPHA_GRID:
            s = res["screen"].get(a) or {}
            sel = a == res.get("selected_alpha")
            screen_rows.append({
                "experiment_id": e["experiment_id"], "policy_id": e["source_policy_id"], "alpha": a,
                "tuning_n": s.get("n"), "tuning_brier_legacy": s.get("brier_legacy"),
                "tuning_brier_integrated": s.get("brier_integrated"), "tuning_brier_improvement": s.get("brier_improvement"),
                "tuning_logloss_legacy": s.get("logloss_legacy"), "tuning_logloss_integrated": s.get("logloss_integrated"),
                "tuning_logloss_improvement": s.get("logloss_improvement"),
                "tuning_retention_pct": 100.0 * s["retention_fraction"] if s.get("retention_fraction") is not None else None,
                "tuning_additional_block_pct": 100.0 * s["additional_block_fraction"] if s.get("additional_block_fraction") is not None else None,
                "tuning_legacy_pc_pnl": s.get("legacy_mean_pc_pnl"), "tuning_integrated_pc_pnl": s.get("integrated_mean_pc_pnl"),
                "tuning_pc_improvement": s.get("pc_improvement"), "qualifies_tuning": s.get("qualifies"),
                "selected_alpha": sel, "holdout_n": h.get("n") if sel else None,
                "holdout_brier_legacy": h.get("brier_legacy") if sel else None,
                "holdout_brier_integrated": h.get("brier_integrated") if sel else None,
                "holdout_brier_improvement": h.get("brier_improvement") if sel else None,
                "holdout_brier_ci_low": h["brier_ci95"][0] if sel and h else None,
                "holdout_brier_ci_high": h["brier_ci95"][1] if sel and h else None,
                "holdout_logloss_improvement": h.get("logloss_improvement") if sel else None,
                "holdout_logloss_ci_low": h["logloss_ci95"][0] if sel and h else None,
                "holdout_logloss_ci_high": h["logloss_ci95"][1] if sel and h else None,
                "holdout_pc_improvement": h.get("pc_improvement") if sel else None,
                "holdout_pc_ci_low": h["pc_ci95"][0] if sel and h else None,
                "holdout_pc_ci_high": h["pc_ci95"][1] if sel and h else None,
                "holdout_retention_pct": 100.0 * h["retention_fraction"] if sel and h.get("retention_fraction") is not None else None,
                "holdout_additional_block_pct": 100.0 * h["additional_block_fraction"] if sel and h.get("additional_block_fraction") is not None else None,
                "status": res["status"]})
    report = {"title": "STEP 5 OF 6 — VALIDATED PERP PROBABILITY OVERLAY",
              "validated_step4_policies": validated4, "step5_experiments": len(exps),
              "experiment_errors": errs, "probability_overlay_candidates": len(candidates),
              "status": S_NONE if not exps else "EVALUATED", "gates": g,
              "tuning_gates": {"min_retention_pct": MIN_RETENTION_PCT, "max_additional_block_pct": MAX_ADDITIONAL_BLOCK_PCT},
              "legacy_call_rows_without_ticker": legacy_calls, "experiments": results}
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "perp_integration_report.json"), "w") as f:
        json.dump(dict(report, generated_at_utc=dt.datetime.now(dt.timezone.utc).isoformat()), f,
                  indent=1, sort_keys=True, default=str)
    with open(os.path.join(outdir, "perp_integration_alpha_screen.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SCREEN_COLUMNS)
        w.writeheader()
        for r in screen_rows:
            w.writerow({c2: ("" if r.get(c2) is None else r.get(c2)) for c2 in SCREEN_COLUMNS})
    with open(os.path.join(outdir, "perp_integration_candidate.json"), "w") as f:
        json.dump({"candidate_schema_version": 1, "candidates": candidates}, f, indent=1, sort_keys=True, default=str)
    if log:
        log(render(report))
    return report


def _n(x, d=4):
    return "-" if x is None else f"{x:.{d}f}"


def render(rep):
    L = [rep["title"], "", "Offline shadow analysis only. Nothing is activated; the bot is unchanged.", "",
         f"Validated Step 4 policies: {rep['validated_step4_policies']}",
         f"Step 5 experiments: {rep['step5_experiments']}",
         f"Probability-overlay candidates: {rep['probability_overlay_candidates']}", ""]
    if rep["experiment_errors"]:
        L.append(f"Experiment load errors: {rep['experiment_errors']}")
    if not rep["experiments"]:
        L += ["No SHADOW_VALIDATED_CANDIDATE policy exists yet.", "Step 5 integration remains inactive."]
        return "\n".join(L)
    for r in rep["experiments"]:
        L.append(f"Experiment {r['experiment_id']}  (Step 4 policy {r['policy_id']})"
                 + ("  SYNTHETIC TEST" if r.get("synthetic") else ""))
        if "screen" not in r:
            L += [f"  Status {r['status']}: {'; '.join(r['status_reasons'])}", ""]
            continue
        L += [f"  Feature {r['feature']}   Coin/group {r['coin_or_group']}   Step 5 cutoff {r['step5_start_cutoff_utc']}",
              f"  New observations {r['new_observations']}   Calendar days {r['calendar_days']:.1f}",
              f"  Tuning N {r['tuning_n']}   Holdout N {r['holdout_n']}",
              "  Alpha candidates: " + ", ".join(f"{a}{'*' if s['qualifies'] else ''}" for a, s in r["screen"].items())
              + "   (* = passed tuning gates)",
              f"  Selected alpha {r['selected_alpha']}"]
        h = r.get("holdout")
        if h:
            dd = h["delta_distribution"]["delta_raw"] or {}
            L += [f"  Brier    legacy {_n(h['brier_legacy'])}  integrated {_n(h['brier_integrated'])}  "
                  f"improvement {_n(h['brier_improvement'], 5)} CI [{_n(h['brier_ci95'][0], 5)}, {_n(h['brier_ci95'][1], 5)}]",
                  f"  Log loss legacy {_n(h['logloss_legacy'])}  integrated {_n(h['logloss_integrated'])}  "
                  f"improvement {_n(h['logloss_improvement'], 5)} CI [{_n(h['logloss_ci95'][0], 5)}, {_n(h['logloss_ci95'][1], 5)}]",
                  f"  P&L/contract legacy {_n(h['legacy_mean_pc_pnl'], 2)}c  integrated {_n(h['integrated_mean_pc_pnl'], 2)}c  "
                  f"improvement {_n(h['pc_improvement'], 2)}c CI [{_n(h['pc_ci95'][0], 2)}, {_n(h['pc_ci95'][1], 2)}]",
                  f"  Additional block rate {_n(100 * h['additional_block_fraction'], 1)}%   Retention {_n(100 * h['retention_fraction'], 1)}%",
                  f"  Delta raw mean {_n(dd.get('mean'), 4)} p05 {_n(dd.get('p05'), 4)} p95 {_n(dd.get('p95'), 4)}   "
                  f"cap-hit rate {_n(100 * (h['delta_cap_hit_rate'] or 0), 1)}%"]
        L += [f"  Flags {', '.join(r['flags']) or 'none'}",
              f"  Status {r['status']}: {'; '.join(r['status_reasons'])}", ""]
    return "\n".join(L)


def main(argv=None):
    a_ = argparse.ArgumentParser(description="Offline Step 5 probability-overlay validation.")
    a_.add_argument("--experiments", default="perp_integration_experiments.json")
    a_.add_argument("--journal", default="kalshi_perp_shadow.csv")
    a_.add_argument("--labels", default="kalshi_binary_outcomes.csv")
    a_.add_argument("--calls", default="kalshi_calls.json")
    a_.add_argument("--policies", default="perp_shadow_policies.json")
    a_.add_argument("--shadow-report", default=os.path.join("analysis_output", "perp_shadow_report.json"))
    a_.add_argument("--outdir", default="analysis_output")
    a_.add_argument("--bootstrap-reps", type=int, default=BOOTSTRAP_REPS)
    a_.add_argument("--allow-synthetic-test-data", action="store_true", help="TESTS ONLY")
    for kk, v in DEFAULT_GATES.items():
        a_.add_argument("--" + kk.replace("_", "-"), type=type(v), default=v)
    a = a_.parse_args(argv)
    gates = {kk: getattr(a, kk) for kk in DEFAULT_GATES}
    try:
        run(a.experiments, a.journal, a.labels, a.calls, a.policies, a.shadow_report, a.outdir,
            a.allow_synthetic_test_data, gates, a.bootstrap_reps)
    except ValueError as e:
        print(f"STEP 5 ANALYSIS REFUSED: {e}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
