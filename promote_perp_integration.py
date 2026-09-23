#!/usr/bin/env python3
"""
promote_perp_integration.py — STEP 6 manual promotion. Offline. Creates an artifact ONLY.

    python promote_perp_integration.py \
        --candidate analysis_output/perp_integration_candidate.json \
        --experiments perp_integration_experiments.json --policies perp_shadow_policies.json \
        --shadow-report analysis_output/perp_shadow_report.json \
        --baseline step5_baseline_manifest.json --dashboard kalshi_dashboard.py \
        --experiment-id <id> --confirm LIVE_VETO_ONLY --output perp_live_promotion.json

It writes perp_live_promotion.json and NOTHING else: it does not enable anything, does not
touch the environment and does not start the bot. Activation additionally requires
PERP_LIVE_VETO_ENABLED=1 and a restart. Any chain mismatch => PROMOTION_REFUSED.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
import sys
import tempfile
import time

import perp_live as pl
import perp_probability as pp
import perp_shadow as ps
import strategy_fingerprint as sf

VALIDATED = "PROBABILITY_OVERLAY_VALIDATED"


class PromotionRefused(Exception):
    pass


def _load(path, what, required=True):
    if not os.path.exists(path):
        if required:
            raise PromotionRefused(f"{what} not found: {path}")
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except ValueError as e:
        raise PromotionRefused(f"{what} is not valid JSON: {e}")


def eligible_candidates(doc):
    cands = (doc or {}).get("candidates", []) if isinstance(doc, dict) else []
    return [c for c in cands if c.get("status") == VALIDATED and c.get("mode") == "VALIDATED_CANDIDATE_ONLY"
            and c.get("activation_allowed") is False]


def verify_chain(cand, experiments, policies, report, baseline, dashboard, allow_synthetic):
    errs = []
    exp = next((e for e in experiments.get("experiments", []) if e.get("experiment_id") == cand.get("experiment_id")), None)
    if exp is None:
        raise PromotionRefused("no Step 5 experiment matches the candidate")
    pol = next((p for p in policies.get("policies", []) if p.get("policy_id") == cand.get("source_policy_id")), None)
    if pol is None:
        raise PromotionRefused("no Step 4 policy matches the candidate")
    for a, b, why in ((cand.get("source_policy_id"), exp.get("source_policy_id"), "policy id"),
                      (cand.get("source_policy_hash"), exp.get("source_policy_hash"), "policy hash"),
                      (cand.get("feature_name"), exp.get("feature_name"), "feature"),
                      (cand.get("coin_or_group"), exp.get("coin_or_group"), "coin/group"),
                      (cand.get("step5_start_cutoff"), exp.get("step5_start_cutoff"), "step 5 cutoff"),
                      (cand.get("max_abs_perp_delta"), exp.get("max_abs_perp_delta"), "delta cap"),
                      (cand.get("source_step4_report_hash"), exp.get("source_hashes", {}).get("shadow_report"),
                       "step 4 report hash")):
        if a != b:
            errs.append(f"candidate/experiment {why} mismatch")
    if pol.get("policy_hash") != cand.get("source_policy_hash"):
        errs.append("policy registry hash differs from the candidate")
    if ps.compute_policy_hash(pol) != pol.get("policy_hash"):
        errs.append("Step 4 policy content does not match its own hash")
    import build_perp_integration_experiment as bx
    if bx.compute_experiment_hash(exp) != exp.get("experiment_hash"):
        errs.append("Step 5 experiment content does not match its own hash")
    if cand.get("selected_alpha") not in pp.ALPHA_GRID:
        errs.append("selected alpha is not in the frozen grid")
    if cand.get("max_abs_perp_delta") != pp.MAX_ABS_PERP_DELTA:
        errs.append("delta cap differs from the frozen overlay definition")
    if report is not None:
        res = next((r for r in report.get("policies", []) if r.get("policy_id") == cand.get("source_policy_id")), None)
        if res is None or res.get("status") != "SHADOW_VALIDATED_CANDIDATE":
            errs.append("Step 4 report does not show SHADOW_VALIDATED_CANDIDATE for this policy")
    if cand.get("dashboard_sha256") != baseline.get("step5_dashboard_sha256"):
        errs.append("candidate was validated against a different Step 5 dashboard than the baseline manifest")
    ok, why = sf.verify(dashboard, baseline)
    if not ok:
        errs += [w if w.startswith(sf.UNSUPPORTED_FORMAT) else f"strategy drift: {w}" for w in why]
    synthetic = bool(cand.get("synthetic") or exp.get("synthetic") or pol.get("synthetic"))
    if synthetic and not allow_synthetic:
        errs.append("synthetic source artifacts cannot be promoted")
    if errs:
        raise PromotionRefused("; ".join(errs))
    return exp, pol, synthetic


def make_promotion_id(cand_sha, exp, pol, fingerprint, alpha, cap, expires):
    ident = {"candidate_sha256": cand_sha, "experiment_id": exp["experiment_id"], "policy_hash": pol["policy_hash"],
             "legacy_strategy_fingerprint": fingerprint, "selected_alpha": alpha, "max_abs_perp_delta": cap,
             "expires_at_utc": expires}
    return hashlib.sha256(pl.canonical_json(ident).encode()).hexdigest()[:16]


def build(candidate_path, experiments_path, policies_path, report_path, baseline_path, dashboard_path,
          output, confirm=None, experiment_id=None, expiry_days=pl.DEFAULT_EXPIRY_DAYS, allow_synthetic=False,
          now=None, log=print):
    now = time.time() if now is None else now
    cand_doc = _load(candidate_path, "Step 5 candidate artifact", required=False)
    cands = eligible_candidates(cand_doc)
    if experiment_id:
        cands = [c for c in cands if c.get("experiment_id") == experiment_id]
    summary = {"eligible_candidates": len(cands), "promotions_created": 0, "promotion_id": None, "refused": None}
    if not cands:
        if log:
            log("STEP 6 OF 6 — LIVE VETO PROMOTION\n")
            log("Eligible real Step 5 candidates: 0")
            log("Promotions created: 0\n")
            log("No real PROBABILITY_OVERLAY_VALIDATED candidate exists.")
            log("Live veto remains unavailable.")
        return summary
    if len(cands) > 1 and not experiment_id:
        raise PromotionRefused(f"{len(cands)} validated candidates: pass --experiment-id to choose one explicitly")
    if confirm != pl.CONFIRM_PHRASE:
        raise PromotionRefused(f"confirmation phrase required: --confirm {pl.CONFIRM_PHRASE}")
    if not (0 < expiry_days <= pl.MAX_EXPIRY_DAYS):
        raise PromotionRefused(f"expiry must be > 0 and <= {pl.MAX_EXPIRY_DAYS} days")
    cand = cands[0]
    experiments = _load(experiments_path, "Step 5 experiment registry")
    policies = _load(policies_path, "Step 4 policy registry")
    report = _load(report_path, "Step 4 shadow report", required=False)
    baseline = _load(baseline_path, "Step 5 baseline manifest")
    exp, pol, synthetic = verify_chain(cand, experiments, policies, report, baseline, dashboard_path,
                                       allow_synthetic)
    created = dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat()
    expires = dt.datetime.fromtimestamp(now + expiry_days * 86400.0, dt.timezone.utc).isoformat()
    cand_sha = pl.sha256_file(candidate_path)
    fingerprint = baseline["legacy_strategy_fingerprint"]
    promo = {"promotion_schema_version": pl.PROMOTION_SCHEMA_VERSION,
             "promotion_id": make_promotion_id(cand_sha, exp, pol, fingerprint, cand["selected_alpha"],
                                               cand["max_abs_perp_delta"], expires),
             "mode": pl.MODE, "experiment_id": exp["experiment_id"], "candidate_sha256": cand_sha,
             "source_policy_id": pol["policy_id"], "source_policy_hash": pol["policy_hash"],
             "feature_name": cand["feature_name"], "coin_or_group": cand["coin_or_group"],
             "selected_alpha": cand["selected_alpha"], "max_abs_perp_delta": cand["max_abs_perp_delta"],
             "conflict_threshold": pol["conflict_threshold"],
             "step5_baseline_dashboard_sha256": baseline["step5_dashboard_sha256"],
             "legacy_strategy_fingerprint": fingerprint, "strategy_config_hash": baseline["strategy_config_hash"],
             "fingerprint_format_version": baseline.get("fingerprint_format_version"),
             "fingerprint_algorithm": baseline.get("fingerprint_algorithm"),
             "created_at_utc": created, "expires_at_utc": expires, "expiry_days": expiry_days,
             "synthetic": bool(synthetic),
             "scope": dict(pl.REQUIRED_SCOPE),
             "step5_holdout_summary": {k: cand.get(k) for k in ("holdout_brier_improvement", "brier_ci95",
                                                                "holdout_logloss_improvement", "logloss_ci95",
                                                                "holdout_pc_improvement", "pc_ci95", "retention",
                                                                "additional_block_fraction")},
             "source_hashes": {"candidate": cand_sha, "experiments": pl.sha256_file(experiments_path),
                               "policies": pl.sha256_file(policies_path),
                               "shadow_report": pl.sha256_file(report_path) if report is not None else None,
                               "baseline_manifest": pl.sha256_file(baseline_path),
                               "step5_dashboard": baseline["step5_dashboard_sha256"]}}
    promo["promotion_hash"] = pl.compute_promotion_hash(promo)
    problems = pl.validate_promotion(promo, now, allow_synthetic=allow_synthetic)
    if problems:
        raise PromotionRefused("; ".join(problems))
    if os.path.exists(output):
        os.replace(output, f"{output}.prev-{int(now)}.json")
    d = os.path.dirname(os.path.abspath(output)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".promotion-", suffix=".json", dir=d)
    with os.fdopen(fd, "w") as f:
        json.dump(promo, f, indent=1, sort_keys=True)
    os.replace(tmp, output)
    summary.update(promotions_created=1, promotion_id=promo["promotion_id"])
    if log:
        log("STEP 6 OF 6 — LIVE VETO PROMOTION\n")
        log(f"Eligible real Step 5 candidates: {len(cands)}")
        log("Promotions created: 1\n")
        log(f"  promotion_id {promo['promotion_id']}  policy {pol['policy_id']}  alpha {promo['selected_alpha']}")
        log(f"  mode {promo['mode']} (veto only)   expires {expires}")
        if synthetic:
            log("  WARNING: SYNTHETIC test promotion — the live loader will refuse it.")
        log("\nThe promotion artifact has been written. NOTHING is active yet.")
        log("To activate: set PERP_LIVE_VETO_ENABLED=1 and restart the bot, then check the startup banner.")
    return summary


def main(argv=None):
    a_ = argparse.ArgumentParser(description="Manually promote a validated Step 5 candidate to LIVE_VETO_ONLY.")
    a_.add_argument("--candidate", default=os.path.join("analysis_output", "perp_integration_candidate.json"))
    a_.add_argument("--experiments", default="perp_integration_experiments.json")
    a_.add_argument("--policies", default="perp_shadow_policies.json")
    a_.add_argument("--shadow-report", default=os.path.join("analysis_output", "perp_shadow_report.json"))
    a_.add_argument("--baseline", default="step5_baseline_manifest.json")
    a_.add_argument("--dashboard", default="kalshi_dashboard.py")
    a_.add_argument("--output", default="perp_live_promotion.json")
    a_.add_argument("--experiment-id", default=None)
    a_.add_argument("--confirm", default=None, help=f"must be exactly {pl.CONFIRM_PHRASE}")
    a_.add_argument("--expiry-days", type=int, default=pl.DEFAULT_EXPIRY_DAYS)
    a_.add_argument("--allow-synthetic-test-data", action="store_true", help="TESTS ONLY")
    a = a_.parse_args(argv)
    try:
        build(a.candidate, a.experiments, a.policies, a.shadow_report, a.baseline, a.dashboard, a.output,
              a.confirm, a.experiment_id, a.expiry_days, a.allow_synthetic_test_data)
    except PromotionRefused as e:
        print(f"PROMOTION_REFUSED: {e}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
