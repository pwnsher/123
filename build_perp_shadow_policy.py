#!/usr/bin/env python3
"""
build_perp_shadow_policy.py — STEP 4 Phase A: turn a REAL Step 3 first-signal candidate into
a frozen, SHADOW_ONLY veto policy. Offline (no network). Changes nothing in the bot.

    python build_perp_shadow_policy.py \
        --telemetry kalshi_perp_telemetry.csv --labels kalshi_binary_outcomes.csv \
        --report analysis_output/perp_predictive_report.json \
        --candidates analysis_output/perp_candidate_manifest.json \
        --output perp_shadow_policies.json

Rules (fail closed on any inconsistency):
  * The Step 3 candidate manifest is the ONLY candidate source. Empty -> zero policies (success).
  * Only cohort == first_signal candidates can become call filters; fixed-horizon candidates
    are NOT_CALL_FILTER_ELIGIBLE. Each candidate is handled independently (no combining).
  * Source files must hash-match what Step 3 analysed; Step 3 results are recomputed and must
    match. Settlement conflicts or causal-invariant violations abort.
  * Discovery cutoff = latest binary close time Step 3 analysed. Nothing after it is used.
  * Thresholds are tuned ONLY on out-of-fold (walk-forward) MODEL C vs MODEL B conflict scores,
    over a predeclared 5/10/15/20/25th-percentile grid, with count, block-rate, effect-size,
    bootstrap and BH gates; the MOST CONSERVATIVE qualifying threshold is chosen.
  * Data that does not carry real-collection provenance is refused unless
    --allow-synthetic-test-data (tests only); such policies are marked synthetic and the live
    shadow loader refuses them.
"""
import argparse
import datetime as dt
import hashlib
import json
import math
import os
import random
import re
import sys
import tempfile

import analyze_perp_predictive as ap
import perp_shadow as ps

POLICY_VERSION = "step4_v1"
THRESHOLD_QUANTILES = (5.0, 10.0, 15.0, 20.0, 25.0)
DEFAULT_GATES = {
    "min_threshold_total": 500,
    "min_threshold_blocked": 50,
    "min_threshold_kept": 300,
    "min_block_fraction": 0.05,
    "max_block_fraction": 0.25,
    "min_loss_enrichment_pp": 5.0,
    "q_max": 0.05,
}
BOOTSTRAP_REPS = 1000
SEED = ap.SEED
LABEL_SOURCE = "kalshi GET /trade-api/v2/markets/{ticker} market.result"   # == label_binary_outcomes.SOURCE
REAL_SESSION_RE = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{8}$")               # perp_telemetry session format

ST_READY = "SHADOW_POLICY_READY"
ST_NOT_CALL_FILTER = "NOT_CALL_FILTER_ELIGIBLE"
ST_NO_THRESHOLD = "NO_TRAINING_THRESHOLD"
ST_ALREADY = "ALREADY_FROZEN"
ST_SYNTHETIC = "REJECTED_SYNTHETIC_OR_UNVERIFIED_DATA"
ST_NO_REAL = "NO_REAL_CANDIDATE"


class BuildError(Exception):
    """Artifacts are missing or inconsistent: fail closed, write nothing."""


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_json(path, what):
    if not os.path.exists(path):
        raise BuildError(f"{what} not found: {path}")
    try:
        with open(path) as f:
            return json.load(f)
    except ValueError as e:
        raise BuildError(f"{what} is not valid JSON: {e}")


# ─────────────────────── artifact verification (fail closed) ───────────────────────
def verify_artifacts(report, manifest, hashes):
    errs = []
    m = report.get("dataset_manifest") if isinstance(report, dict) else None
    if not isinstance(m, dict):
        return ["predictive report has no dataset_manifest"]
    if str(m.get("telemetry_schema_version")) != ap.EXPECTED_SCHEMA_VERSION:
        errs.append("report telemetry schema differs from this code")
    if m.get("feature_version") != ap.EXPECTED_FEATURE_VERSION:
        errs.append("report feature version differs from this code")
    if m.get("analysis_code_version") != ap.ANALYSIS_CODE_VERSION:
        errs.append("report produced by a different Step 3 analysis version")
    if m.get("telemetry_sha256") != hashes["telemetry"]:
        errs.append("telemetry file differs from the one Step 3 analysed (sha256 mismatch)")
    if m.get("labels_sha256") != hashes["labels"]:
        errs.append("labels file differs from the one Step 3 analysed (sha256 mismatch)")
    if not isinstance(manifest, list):
        errs.append("candidate manifest is not a list")
    elif ps.canonical_json(manifest) != ps.canonical_json(report.get("candidates", [])):
        errs.append("candidate manifest does not match the report's candidate list")
    return errs


def find_screen_entry(report, cand):
    hits = [r for r in report.get("feature_screen", [])
            if r.get("cohort") == cand.get("cohort") and r.get("group") == cand.get("coin")
            and r.get("horizon_min") == cand.get("horizon") and r.get("feature_name") == cand.get("feature")]
    return hits


def verify_candidate(cand, report):
    """Return (screen_entry, errors). The candidate must be a genuine Step 3 PROMISING_CANDIDATE."""
    errs = []
    hits = find_screen_entry(report, cand)
    if len(hits) != 1:
        return None, [f"candidate not uniquely present in the feature screen ({len(hits)} matches)"]
    e = hits[0]
    if e.get("status") != ap.ST_CANDIDATE:
        errs.append(f"Step 3 status is {e.get('status')}, not {ap.ST_CANDIDATE}")
    if e.get("primary_or_secondary") != "primary" or cand.get("feature") not in ap.PRIMARY_FEATURES:
        errs.append("not a primary Step 3 feature")
    if cand.get("feature") in ap.EXPLORATORY_FUNDING or e.get("feature_family") == "EXPLORATORY_FUNDING":
        errs.append("funding is exploratory only")
    try:
        ap.assert_allowed_predictor(cand.get("feature"))
    except ValueError as x:
        errs.append(str(x))
    if e.get("feature_family") != cand.get("feature_family"):
        errs.append("feature family mismatch")
    if e.get("n_available") != cand.get("sample_size"):
        errs.append("sample size mismatch between manifest and report")
    for a, b in (("q_value", "q_value"), ("brier_improvement_vs_b", "oof_brier_improvement_vs_b")):
        if e.get(a) is None or cand.get(b) is None or abs(e[a] - cand[b]) > 1e-12:
            errs.append(f"{a} mismatch between manifest and report")
    return e, errs


def label_conflicts(labels_path):
    import csv
    with open(labels_path, newline="") as f:
        return sorted(r["binary_ticker"] for r in csv.DictReader(f) if r.get("label_status") == "conflict")


def provenance_problems(meta, labels_path, tickers):
    """Real data is produced by perp_telemetry (session id format) and the real labeler
    (source string, labeled strictly after close). Anything else is treated as synthetic."""
    import csv
    probs = []
    bad_sessions = [s for s in meta["sessions"] if not REAL_SESSION_RE.match(s or "")]
    if bad_sessions:
        probs.append(f"telemetry session ids not produced by perp_telemetry: {bad_sessions[:3]}")
    with open(labels_path, newline="") as f:
        lab = {r["binary_ticker"]: r for r in csv.DictReader(f)}
    bad = 0
    for tk in tickers:
        r = lab.get(tk)
        close, at = ap._parse_iso(r.get("binary_close_time")) if r else None, ap._parse_iso(r.get("labeled_at_utc")) if r else None
        if not r or r.get("source") != LABEL_SOURCE or close is None or at is None or at <= close:
            bad += 1
    if bad:
        probs.append(f"{bad} label rows lack real labeler provenance (source/labeled_at after close)")
    return probs


# ─────────────────────── discovery data ───────────────────────
def discovery_data(telemetry, labels_path):
    rows, meta = ap.load_telemetry(telemetry)
    nv, ex = meta["causal_violations"]
    if nv:
        raise BuildError(f"causal invariant violations in telemetry: {ex}")
    labels, _ = ap.load_labels(labels_path)
    excluded = ap.ticker_metadata(rows, labels, meta["ticker_coins"], meta["ticker_closes"])
    by = ap.index_by_ticker(rows)
    horizon = ap.build_horizon_observations(by, labels, excluded)
    signal = ap.build_first_signal_cohort(by, labels, excluded)
    closes = [o["close"] for h in horizon.values() for o in h] + [o["close"] for o in signal]
    if not closes:
        raise BuildError("no analysed observations")
    return signal, max(closes), meta


def discovery_cohort(obs, group, cutoff):
    """The explicit discovery guard: nothing closing after the cutoff is ever used."""
    return [o for o in obs if o["close"] <= cutoff and (group == "ALL" or o["coin"] == group)]


def favored_win(y, fav):
    return (y == 1) if fav == "UP" else (y == 0)


def oof_conflicts(obs, feature, group, step3_thresholds, seed):
    """Walk-forward OUT-OF-FOLD MODEL B/C probabilities from the Step 3 routine itself."""
    scr = ap.screen_feature(obs, feature, group=group, horizon="first_signal", cohort="first_signal",
                            reps=10, seed=seed, thresholds=step3_thresholds, keep_oof=True)
    out = []
    for r in scr.pop("_oof", []):
        if r["fav"] not in ("UP", "DOWN"):
            continue
        pbf, pcf = ps.p_favored(r["pb"], r["fav"]), ps.p_favored(r["pc"], r["fav"])
        out.append({"ticker": r["ticker"], "coin": r["coin"], "fold": r["fold"], "fav": r["fav"], "y": r["y"],
                    "win": favored_win(r["y"], r["fav"]), "p_b_fav": pbf, "p_c_fav": pcf,
                    "conflict": pcf - pbf, "feature_value": r["fv"]})
    return out, scr


# ─────────────────────── threshold construction ───────────────────────
def threshold_grid(conflicts, quantiles=THRESHOLD_QUANTILES):
    """Predeclared grid: percentiles of the OOF conflict-score distribution. Nothing else."""
    return [(q, ap.percentile(conflicts, q)) for q in quantiles] if conflicts else []


def threshold_metrics(rows, thr):
    blk = [r for r in rows if r["conflict"] <= thr]
    kpt = [r for r in rows if r["conflict"] > thr]
    n = len(rows)

    def rate(sub, key):
        return (sum(1 for r in sub if r["win"] == key) / len(sub)) if sub else None
    m = {"threshold": thr, "n_total": n, "n_blocked": len(blk), "n_kept": len(kpt),
         "block_fraction": len(blk) / n if n else None, "retention_fraction": len(kpt) / n if n else None,
         "baseline_win_rate": rate(rows, True), "blocked_win_rate": rate(blk, True), "kept_win_rate": rate(kpt, True),
         "baseline_loss_rate": rate(rows, False), "blocked_loss_rate": rate(blk, False),
         "kept_loss_rate": rate(kpt, False)}
    bl, kl = m["blocked_loss_rate"], m["kept_loss_rate"]
    m["loss_enrichment_pp"] = (bl - kl) * 100.0 if bl is not None and kl is not None else None
    m["odds_ratio_blocked_vs_kept_loss"] = ((bl / (1 - bl)) / (kl / (1 - kl))
                                            if bl is not None and kl is not None and 0 < bl < 1 and 0 < kl < 1 else None)
    return m


def threshold_bootstrap(rows, thr, reps, seed):
    """Ticker-clustered bootstrap. Returns (enrichment_pp reps, kept_win - baseline_win reps).
    A replicate with an empty blocked or kept set contributes 0 enrichment (conservative)."""
    by = {}
    for r in rows:
        by.setdefault(r["ticker"], []).append(r)
    keys = sorted(by)
    agg = []
    for k in keys:
        rs = by[k]
        b = [r for r in rs if r["conflict"] <= thr]
        kk = [r for r in rs if r["conflict"] > thr]
        agg.append((len(b), sum(1 for r in b if not r["win"]), len(kk), sum(1 for r in kk if not r["win"]),
                    sum(1 for r in rs if r["win"]), len(rs), sum(1 for r in kk if r["win"])))
    rng = random.Random(seed)
    enr, kw = [], []
    idx = range(len(keys))
    for _ in range(reps):
        nb = lb = nk = lk = w = n = wk = 0
        for j in rng.choices(idx, k=len(keys)):
            a = agg[j]
            nb += a[0]; lb += a[1]; nk += a[2]; lk += a[3]; w += a[4]; n += a[5]; wk += a[6]
        enr.append((lb / nb - lk / nk) * 100.0 if nb and nk else 0.0)
        kw.append((wk / nk - w / n) if nk and n else 0.0)
    return enr, kw


def evaluate_thresholds(rows, gates=None, reps=BOOTSTRAP_REPS, seed=SEED):
    g = dict(DEFAULT_GATES, **(gates or {}))
    evals = []
    for q, thr in threshold_grid([r["conflict"] for r in rows]):
        m = threshold_metrics(rows, thr)
        m["source_quantile"] = q
        enr, kw = threshold_bootstrap(rows, thr, reps, ap._derived_seed(seed, "threshold", q))
        m["loss_enrichment_ci95_pp"] = list(ap.ci95(enr))
        m["kept_minus_baseline_win_ci95"] = list(ap.ci95(kw))
        m["threshold_p"] = ap.bootstrap_p_improvement(enr)          # (#{enr <= 0} + 1) / (B + 1)
        evals.append(m)
    for m, q in zip(evals, ap.bh_qvalues([m["threshold_p"] for m in evals])):
        m["threshold_q"] = q
    for m in evals:
        m["ineligible_reasons"] = eligibility_reasons(m, g)
        m["eligible"] = not m["ineligible_reasons"]
    return evals


def eligibility_reasons(m, gates=None):
    """Threshold gates (spec 26). Empty list = eligible. Candidate-level gates (real Step 3
    PROMISING_CANDIDATE, first_signal cohort, causal invariants) are enforced upstream."""
    g = dict(DEFAULT_GATES, **(gates or {}))
    why = []
    if m.get("threshold") is None or not (m["threshold"] < 0):
        why.append("threshold not negative (would block confirming calls)")
    if (m.get("n_total") or 0) < g["min_threshold_total"]:
        why.append("total below minimum")
    if (m.get("n_blocked") or 0) < g["min_threshold_blocked"]:
        why.append("blocked count below minimum")
    if (m.get("n_kept") or 0) < g["min_threshold_kept"]:
        why.append("kept count below minimum")
    bf = m.get("block_fraction")
    if bf is None or bf < g["min_block_fraction"] or bf > g["max_block_fraction"]:
        why.append("block fraction outside allowed range")
    if m.get("loss_enrichment_pp") is None or m["loss_enrichment_pp"] < g["min_loss_enrichment_pp"]:
        why.append("loss enrichment below minimum effect size")
    lo = (m.get("loss_enrichment_ci95_pp") or [None])[0]
    if lo is None or lo <= 0:
        why.append("bootstrap CI for loss enrichment includes 0")
    if m.get("threshold_q") is None or m["threshold_q"] > g["q_max"]:
        why.append("BH q above limit")
    if m.get("kept_win_rate") is None or m.get("baseline_win_rate") is None or m["kept_win_rate"] < m["baseline_win_rate"]:
        why.append("kept win rate worse than baseline")
    return why


def select_threshold(evals):
    """MOST CONSERVATIVE eligible threshold = smallest fraction of calls blocked.
    (Deliberately NOT the best-looking historical result.)"""
    ok = [m for m in evals if m["eligible"]]
    if not ok:
        return None
    return min(ok, key=lambda m: (m["block_fraction"], m["source_quantile"]))


# ─────────────────────── final frozen fit (discovery data only) ───────────────────────
def final_fit(obs, feature, group, controls):
    need = list(controls) + [feature]
    rel = ap.feature_family(feature) == "reliability"
    by_coin = group == "ALL"
    rows = sorted((o for o in obs if all(ap._ok(o["f"].get(c)) for c in need) and ap._ok(o["base_logit"])),
                  key=lambda o: (o["close"], o["ticker"]))
    sc = ap.fit_scalers(rows, need, by_coin)
    if by_coin:
        ok = {k for k, c in sc if c == feature}
        rows = [o for o in rows if o["coin"] in ok]
    y = [o["y"] for o in rows]
    xb = [ap.model_design(o, "B", controls, feature, rel, sc, by_coin) for o in rows]
    xc = [ap.model_design(o, "C", controls, feature, rel, sc, by_coin) for o in rows]
    fb, fc = ap.fit_logistic(xb, y), ap.fit_logistic(xc, y)
    scalers = {}
    for (key, col), s in sorted(sc.items()):
        scalers.setdefault(key, {})[col] = s.stats()
    return {"scalers": scalers, "beta_b": fb["beta"], "beta_c": fc["beta"], "n": len(rows),
            "converged": fb["converged"] and fc["converged"], "reliability": rel, "by_coin": by_coin,
            "raw_feature": [o["f"][feature] for o in rows]}


def distribution_summary(values):
    v = [x for x in values if x is not None and math.isfinite(x)]
    if not v:
        return None
    mu = sum(v) / len(v)
    sd = math.sqrt(sum((x - mu) ** 2 for x in v) / len(v))
    return {"n": len(v), "mean": mu, "std": sd, **{f"p{int(q):02d}": ap.percentile(v, q) for q in (5, 25, 50, 75, 95)}}


def make_policy_id(feature, group, cohort, cutoff_utc, source_hashes, gates):
    ident = {"feature": feature, "group": group, "cohort": cohort, "discovery_cutoff_utc": cutoff_utc,
             "source_hashes": source_hashes, "policy_version": POLICY_VERSION,
             "threshold_quantiles": list(THRESHOLD_QUANTILES), "gates": gates}
    return hashlib.sha256(ps.canonical_json(ident).encode()).hexdigest()[:16]


def assemble_policy(cand, screen_entry, cutoff, sel, evals, fit, oof_rows, source_hashes, gates, synthetic, now_utc):
    cutoff_utc = dt.datetime.fromtimestamp(cutoff, dt.timezone.utc).isoformat()
    p = {
        "policy_id": make_policy_id(cand["feature"], cand["coin"], cand["cohort"], cutoff_utc, source_hashes, gates),
        "policy_schema_version": ps.POLICY_SCHEMA_VERSION, "policy_version": POLICY_VERSION,
        "mode": ps.MODE, "synthetic": bool(synthetic), "created_at_utc": now_utc,
        "feature_name": cand["feature"], "feature_family": cand["feature_family"],
        "reliability_interaction": fit["reliability"], "cohort": cand["cohort"], "coin_or_group": cand["coin"],
        "by_coin_scaling": fit["by_coin"],
        "discovery_cutoff_utc": cutoff_utc, "discovery_cutoff_epoch": cutoff,
        "controls": list(screen_entry["controls"]), "scalers": fit["scalers"],
        "winsorization_percentiles": list(ap.WINSOR_PCT), "probability_clamp": ap.PROB_CLAMP,
        "model_b_coefficients": fit["beta_b"], "model_c_coefficients": fit["beta_c"],
        "final_fit_n": fit["n"], "final_fit_converged": fit["converged"],
        "conflict_threshold": sel["threshold"], "threshold_source_quantile": sel["source_quantile"],
        "expected_training_block_fraction": sel["block_fraction"],
        "training_total": sel["n_total"], "training_blocked": sel["n_blocked"], "training_kept": sel["n_kept"],
        "training_baseline_win_rate": sel["baseline_win_rate"], "training_blocked_win_rate": sel["blocked_win_rate"],
        "training_kept_win_rate": sel["kept_win_rate"],
        "training_loss_enrichment_pp": sel["loss_enrichment_pp"],
        "training_loss_enrichment_ci95_pp": sel["loss_enrichment_ci95_pp"],
        "threshold_p": sel["threshold_p"], "threshold_q": sel["threshold_q"],
        "threshold_grid_evaluation": [{k: v for k, v in m.items()} for m in evals],
        "threshold_gates": gates,
        "training_feature_summary": distribution_summary(fit["raw_feature"]),
        "training_oof_conflict_summary": distribution_summary([r["conflict"] for r in oof_rows]),
        "step3_candidate": cand,
        "step3_metrics": {k: screen_entry.get(k) for k in (
            "n_available", "yes_count", "no_count", "valid_folds", "oof_brier_b", "oof_brier_c",
            "brier_improvement_vs_b", "logloss_improvement_vs_b", "brier_ci_low", "brier_ci_high",
            "q_value", "sign_consistency_pct", "coefficient_sign", "controls")},
        "telemetry_schema_version": ap.EXPECTED_SCHEMA_VERSION, "feature_version": ap.EXPECTED_FEATURE_VERSION,
        "analysis_code_version": ap.ANALYSIS_CODE_VERSION,
        "source_hashes": source_hashes,
    }
    p["policy_hash"] = ps.compute_policy_hash(p)
    return p


# ─────────────────────── orchestration ───────────────────────
def _load_registry(path):
    if not os.path.exists(path):
        return {"policy_schema_version": ps.POLICY_SCHEMA_VERSION, "policies": []}
    reg = _load_json(path, "existing policy registry")
    if not isinstance(reg, dict) or reg.get("policy_schema_version") != ps.POLICY_SCHEMA_VERSION \
            or not isinstance(reg.get("policies"), list):
        raise BuildError("existing policy registry has an unsupported format; refusing to overwrite it")
    return reg


def _write_registry(path, reg):
    d = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(prefix=".policies-", suffix=".json", dir=d)
    with os.fdopen(fd, "w") as f:
        json.dump(reg, f, indent=1, sort_keys=True)
    os.replace(tmp, path)


def build(telemetry, labels, report_path, manifest_path, output, allow_synthetic=False, gates=None,
          reps=BOOTSTRAP_REPS, seed=SEED, now=None, log=print):
    g = dict(DEFAULT_GATES, **(gates or {}))
    if g != DEFAULT_GATES and not allow_synthetic:
        raise BuildError("threshold gates may only be overridden together with --allow-synthetic-test-data")
    now_utc = dt.datetime.fromtimestamp(now if now is not None else __import__("time").time(),
                                        dt.timezone.utc).isoformat()
    for pth, what in ((telemetry, "telemetry"), (labels, "labels")):
        if not os.path.exists(pth):
            raise BuildError(f"{what} not found: {pth}")
    manifest = _load_json(manifest_path, "Step 3 candidate manifest")
    report = _load_json(report_path, "Step 3 predictive report")
    hashes = {"telemetry": sha256_file(telemetry), "labels": sha256_file(labels),
              "step3_report": sha256_file(report_path), "step3_candidate_manifest": sha256_file(manifest_path)}
    errs = verify_artifacts(report, manifest, hashes)
    if errs:
        raise BuildError("Step 3 artifacts are inconsistent: " + "; ".join(errs))
    reg = _load_registry(output)
    summary = {"real_step3_candidates": 0, "first_signal_candidates": 0, "policies_created": 0,
               "candidates": [], "synthetic": bool(allow_synthetic), "status": ST_NO_REAL}
    first = [c for c in manifest if c.get("cohort") == "first_signal"]
    summary["step3_candidates"] = len(manifest)
    summary["first_signal_candidates"] = len(first)
    for c in manifest:
        if c.get("cohort") != "first_signal":
            summary["candidates"].append({"candidate": c, "status": ST_NOT_CALL_FILTER,
                                          "reason": "fixed-horizon candidate; not proven at the call moment"})
    if first:
        conflicts = label_conflicts(labels)
        if conflicts:
            raise BuildError(f"settlement conflicts present ({conflicts[:5]}); resolve before building policies")
        obs, cutoff, meta = discovery_data(telemetry, labels)
        th3 = report["config"]["thresholds"]
        seed3 = report["dataset_manifest"]["bootstrap_seed"]
        tickers = sorted({o["ticker"] for o in obs if o["close"] <= cutoff})
        probs = provenance_problems(meta, labels, tickers)
        real = not probs
        summary["real_step3_candidates"] = len(manifest) if real else 0
        if not real and not allow_synthetic:
            for c in first:
                summary["candidates"].append({"candidate": c, "status": ST_SYNTHETIC, "reason": "; ".join(probs)})
        else:
            if not real and log:
                log("WARNING: --allow-synthetic-test-data: building a SYNTHETIC test policy. "
                    "It is marked synthetic=true and the live shadow loader will refuse it.")
            for c in first:                                   # each candidate independently
                entry, cerrs = verify_candidate(c, report)
                if cerrs:
                    raise BuildError(f"candidate {c.get('feature')}/{c.get('coin')}: " + "; ".join(cerrs))
                cohort = discovery_cohort(obs, c["coin"], cutoff)
                rows, scr = oof_conflicts(cohort, c["feature"], c["coin"], th3, seed3)
                for k in ("oof_brier_b", "oof_brier_c", "n_available"):
                    if scr.get(k) is None or entry.get(k) is None or abs(scr[k] - entry[k]) > 1e-9:
                        raise BuildError(f"recomputed Step 3 {k} does not match the report for {c['feature']}")
                if scr["controls"] != entry["controls"]:
                    raise BuildError("recomputed controls differ from the Step 3 report")
                evals = evaluate_thresholds(rows, g, reps, ap._derived_seed(seed, c["feature"], c["coin"]))
                sel = select_threshold(evals)
                if sel is None:
                    summary["candidates"].append({"candidate": c, "status": ST_NO_THRESHOLD,
                                                  "threshold_grid": evals})
                    continue
                fit = final_fit(cohort, c["feature"], c["coin"], entry["controls"])
                pol = assemble_policy(c, entry, cutoff, sel, evals, fit, rows, hashes, g, not real, now_utc)
                ps.FrozenPolicy(pol, allow_synthetic=not real)        # must validate before freezing
                if any(p.get("policy_id") == pol["policy_id"] for p in reg["policies"]):
                    summary["candidates"].append({"candidate": c, "status": ST_ALREADY, "policy_id": pol["policy_id"]})
                    continue
                reg["policies"].append(pol)
                summary["policies_created"] += 1
                summary["candidates"].append({"candidate": c, "status": ST_READY, "policy_id": pol["policy_id"],
                                              "threshold": sel["threshold"], "quantile": sel["source_quantile"]})
    if summary["policies_created"]:
        summary["status"] = ST_READY
    elif any(x["status"] == ST_NO_THRESHOLD for x in summary["candidates"]):
        summary["status"] = ST_NO_THRESHOLD
    _write_registry(output, reg)
    if log:
        log("STEP 4 OF 6 — SHADOW POLICY BUILDER\n")
        log(f"Real Step 3 candidates: {summary['real_step3_candidates']}")
        log(f"First-signal candidates: {summary['first_signal_candidates']}")
        log(f"Policies created: {summary['policies_created']}\n")
        for x in summary["candidates"]:
            c = x["candidate"]
            log(f"  {c.get('cohort')} {c.get('coin')} {c.get('horizon')} {c.get('feature')}: {x['status']}"
                + (f" ({x.get('reason')})" if x.get("reason") else "")
                + (f" policy {x['policy_id']} threshold {x['threshold']:.5f} (q{x['quantile']:.0f})"
                   if x.get("policy_id") and x.get("threshold") is not None else ""))
        if not summary["policies_created"]:
            log("No eligible real Step 3 first-signal candidates." if not any(
                x["status"] == ST_NO_THRESHOLD for x in summary["candidates"]) else
                "No candidate threshold met the predeclared training gates.")
            log("No shadow filter policy created.")
            log("Collect more real telemetry and settlement outcomes, then rerun Step 3.")
    return summary


def main(argv=None):
    ap_ = argparse.ArgumentParser(description="Build frozen SHADOW_ONLY perp filter policies (offline).")
    ap_.add_argument("--telemetry", default="kalshi_perp_telemetry.csv")
    ap_.add_argument("--labels", default="kalshi_binary_outcomes.csv")
    ap_.add_argument("--report", default=os.path.join("analysis_output", "perp_predictive_report.json"))
    ap_.add_argument("--candidates", default=os.path.join("analysis_output", "perp_candidate_manifest.json"))
    ap_.add_argument("--output", default="perp_shadow_policies.json")
    ap_.add_argument("--bootstrap-reps", type=int, default=BOOTSTRAP_REPS)
    ap_.add_argument("--allow-synthetic-test-data", action="store_true",
                     help="TESTS ONLY: build from synthetic data; the policy is marked synthetic and "
                          "will be refused by the live shadow loader")
    for k2, v in DEFAULT_GATES.items():
        ap_.add_argument("--" + k2.replace("_", "-"), type=type(v), default=v)
    a = ap_.parse_args(argv)
    gates = {k2: getattr(a, k2) for k2 in DEFAULT_GATES}
    try:
        build(a.telemetry, a.labels, a.report, a.candidates, a.output, a.allow_synthetic_test_data, gates,
              a.bootstrap_reps)
    except BuildError as e:
        print(f"POLICY BUILD FAILED CLOSED: {e}", file=sys.stderr)
        return 3
    except (ap.SchemaError, ap.CausalInvariantError) as e:
        print(f"POLICY BUILD FAILED CLOSED ({type(e).__name__}): {e}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
