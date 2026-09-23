#!/usr/bin/env python3
"""
check_perp_deployment.py — offline deployment status for the perp live veto. Reads local files
only; changes nothing, activates nothing, makes no network request.

    python check_perp_deployment.py

FINAL STATUS is one of:
    INACTIVE            nothing promoted (the normal state while collecting data)
    READY_BUT_DISABLED  a valid promotion exists but PERP_LIVE_VETO_ENABLED is not 1
    ACTIVE_VETO_ONLY    valid promotion + env flag + no kill file/latch: calls may be suppressed
    REFUSED             a promotion exists but failed validation; legacy behaviour continues
"""
import argparse
import json
import os
import platform
import sys
import time

import perp_live as pl
import strategy_fingerprint as sf


def _load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def status(promotion_path="perp_live_promotion.json", policies_path="perp_shadow_policies.json",
           baseline_path="step5_baseline_manifest.json", dashboard_path="kalshi_dashboard.py",
           candidate_path=os.path.join("analysis_output", "perp_integration_candidate.json"),
           kill_file=pl.DEFAULT_KILL_FILE, env_enabled=None, now=None):
    now = time.time() if now is None else now
    env_enabled = (os.environ.get("PERP_LIVE_VETO_ENABLED", "0") == "1") if env_enabled is None else env_enabled
    cand_doc = _load(candidate_path)
    cands = (cand_doc or {}).get("candidates", []) if isinstance(cand_doc, dict) else []
    validated = [c for c in cands if c.get("status") == "PROBABILITY_OVERLAY_VALIDATED"]
    promo = _load(promotion_path)
    out = {"candidate_file_exists": cand_doc is not None, "validated_candidates": len(validated),
           "promotion_exists": promo is not None, "promotion_hash_valid": None, "promotion_expired": None,
           "promotion_synthetic": None, "env_enabled": env_enabled,
           "kill_file_present": bool(kill_file) and os.path.exists(kill_file),
           "strategy_fingerprint_valid": None, "policy_valid": None, "problems": [],
           "runtime_python": platform.python_version(), "baseline_generated_python": None,
           "baseline_fingerprint_format": None}
    baseline = _load(baseline_path)
    if baseline:
        out["baseline_generated_python"] = baseline.get("baseline_generated_python")
        out["baseline_fingerprint_format"] = baseline.get("fingerprint_format_version", 1)
        ok, why = sf.verify(dashboard_path, baseline)
        out["strategy_fingerprint_valid"] = ok
        out["problems"] += [w if w.startswith(sf.UNSUPPORTED_FORMAT) else f"strategy drift: {w}" for w in why]
    else:
        out["problems"].append("baseline manifest missing")
    if promo is None:
        out["final_status"] = "INACTIVE"
        return out
    out["promotion_hash_valid"] = pl.compute_promotion_hash(promo) == promo.get("promotion_hash")
    out["promotion_synthetic"] = bool(promo.get("synthetic"))
    exp = pl._parse_iso(promo.get("expires_at_utc"))
    out["promotion_expired"] = (exp is None or exp <= now)
    problems = pl.validate_promotion(promo, now)
    reg = _load(policies_path) or {}
    pol = next((p for p in reg.get("policies", []) if p.get("policy_id") == promo.get("source_policy_id")), None)
    out["policy_valid"] = bool(pol and pol.get("policy_hash") == promo.get("source_policy_hash"))
    if not out["policy_valid"]:
        problems.append("frozen Step 4 policy missing or changed")
    if baseline:
        if promo.get("legacy_strategy_fingerprint") != baseline.get("legacy_strategy_fingerprint"):
            problems.append("promotion fingerprint differs from baseline")
        if not out["strategy_fingerprint_valid"]:
            problems.append("current dashboard strategy differs from the validated baseline")
    out["problems"] += problems
    if problems:
        out["final_status"] = "REFUSED"
    elif out["kill_file_present"]:
        out["final_status"] = "READY_BUT_DISABLED"
        out["problems"].append(f"kill file present: {kill_file}")
    elif not env_enabled:
        out["final_status"] = "READY_BUT_DISABLED"
    else:
        out["final_status"] = "ACTIVE_VETO_ONLY"
    return out


def render(s):
    L = ["STEP 6 — PERP DEPLOYMENT STATUS (offline check; nothing was changed)", ""]
    for k, label in (("candidate_file_exists", "candidate artifact exists"),
                     ("validated_candidates", "validated Step 5 candidates"),
                     ("promotion_exists", "promotion exists"), ("promotion_hash_valid", "promotion hash valid"),
                     ("promotion_expired", "promotion expired"), ("promotion_synthetic", "promotion synthetic"),
                     ("env_enabled", "PERP_LIVE_VETO_ENABLED"), ("strategy_fingerprint_valid", "strategy fingerprint valid"),
                     ("policy_valid", "frozen policy valid"), ("kill_file_present", "kill file present"),
                     ("baseline_fingerprint_format", "baseline fingerprint format"),
                     ("runtime_python", "runtime Python (diagnostic)"),
                     ("baseline_generated_python", "baseline built on Python (diagnostic)")):
        L.append(f"  {label:32s} {s.get(k)}")
    for p in s["problems"]:
        L.append(f"  - {p}")
    L += ["", f"FINAL STATUS: {s['final_status']}"]
    if s["final_status"] == "ACTIVE_VETO_ONLY":
        L.append("(veto only: an existing valid call may be suppressed; no orders are ever placed)")
    return "\n".join(L)


def main(argv=None):
    a_ = argparse.ArgumentParser(description="Offline perp deployment status.")
    a_.add_argument("--promotion", default="perp_live_promotion.json")
    a_.add_argument("--policies", default="perp_shadow_policies.json")
    a_.add_argument("--baseline", default="step5_baseline_manifest.json")
    a_.add_argument("--dashboard", default="kalshi_dashboard.py")
    a_.add_argument("--candidate", default=os.path.join("analysis_output", "perp_integration_candidate.json"))
    a_.add_argument("--json", action="store_true")
    a = a_.parse_args(argv)
    s = status(a.promotion, a.policies, a.baseline, a.dashboard, a.candidate)
    print(json.dumps(s, indent=1, sort_keys=True) if a.json else render(s))
    return 0


if __name__ == "__main__":
    sys.exit(main())
