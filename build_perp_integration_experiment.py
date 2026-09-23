#!/usr/bin/env python3
"""
build_perp_integration_experiment.py — STEP 5: freeze a probability-overlay EXPERIMENT for
each Step 4 policy whose prospective status is exactly SHADOW_VALIDATED_CANDIDATE.
Offline (no network). Changes nothing in the bot.

    python build_perp_integration_experiment.py \
        --policies perp_shadow_policies.json \
        --shadow-report analysis_output/perp_shadow_report.json \
        --journal kalshi_perp_shadow.csv \
        --dashboard kalshi_dashboard.py \
        --output perp_integration_experiments.json

Chain: Step 3 PROMISING_CANDIDATE -> Step 4 frozen policy -> Step 4 prospective validation ->
SHADOW_VALIDATED_CANDIDATE -> Step 5 experiment. Nothing else is eligible (the Step 3 manifest
is never read here). Policy and report must agree on id / feature / group / cutoff / threshold;
the Step 4 report does not store the policy hash, so the hash is bound through the shadow
journal: every journal row the report validated must carry the registry policy's hash.
Any disagreement fails closed. step5_start_cutoff = the report's prospective_last_ts, so no
Step 4 validation observation can ever be reused as Step 5 evidence.
"""
import argparse
import ast
import csv
import datetime as dt
import hashlib
import json
import os
import sys
import tempfile
import time

import perp_probability as pp
import perp_shadow as ps

EXPERIMENT_SCHEMA_VERSION = 1
VALIDATED = "SHADOW_VALIDATED_CANDIDATE"
ST_CREATED = "EXPERIMENT_CREATED"
ST_ALREADY = "ALREADY_FROZEN"
ST_NOT_VALIDATED = "NOT_SHADOW_VALIDATED"
ST_SYNTHETIC = "REJECTED_SYNTHETIC"


class BuildError(Exception):
    """Artifacts missing/inconsistent: fail closed, write nothing."""


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_strategy_constants(dashboard_path):
    """Read MIN_CONF / MIN_PRICE / EDGE_THRESH / ENTRY_COST_CENTS WITHOUT executing the
    dashboard. Each must be exactly one top-level literal assignment and must never be
    reassigned or declared global anywhere; otherwise fail closed."""
    if not os.path.exists(dashboard_path):
        raise BuildError(f"dashboard not found: {dashboard_path}")
    tree = ast.parse(open(dashboard_path).read())
    names = set(pp.REQUIRED_CONSTANTS)
    top, other = {}, []
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) \
                and node.targets[0].id in names:
            if node.targets[0].id in top:
                raise BuildError(f"{node.targets[0].id} assigned more than once")
            try:
                top[node.targets[0].id] = ast.literal_eval(node.value)
            except ValueError:
                raise BuildError(f"{node.targets[0].id} is not a literal; cannot verify safely")
    top_nodes = {id(n) for n in tree.body}
    for node in ast.walk(tree):
        if isinstance(node, ast.Global) and names & set(node.names):
            other.append(f"global {sorted(names & set(node.names))}")
        targets = []
        if isinstance(node, ast.Assign) and id(node) not in top_nodes:
            targets = node.targets
        elif isinstance(node, (ast.AugAssign, ast.AnnAssign)):
            targets = [node.target]
        for t in targets:
            for n in ast.walk(t):
                if isinstance(n, ast.Name) and n.id in names:
                    other.append(f"reassignment of {n.id}")
    if other:
        raise BuildError(f"strategy constants may change at runtime: {other}")
    try:
        return pp.check_constants(top)
    except ValueError as e:
        raise BuildError(str(e))


def load_registry(path):
    if not os.path.exists(path):
        return []
    try:
        reg = json.load(open(path))
    except ValueError as e:
        raise BuildError(f"policy registry is not valid JSON: {e}")
    if not isinstance(reg, dict) or reg.get("policy_schema_version") != ps.POLICY_SCHEMA_VERSION \
            or not isinstance(reg.get("policies"), list):
        raise BuildError("unsupported Step 4 policy registry format")
    return reg["policies"]


def journal_binding(journal_path, pol, res):
    """Replicate the Step 4 analyzer's row selection (dedupe by ticker, post-discovery) up to
    prospective_last_ts and require every row to carry this policy's hash and threshold."""
    if not os.path.exists(journal_path):
        return ["shadow journal not found; cannot bind the policy hash to the Step 4 report"]
    rows = [r for r in csv.DictReader(open(journal_path, newline="")) if r.get("policy_id") == pol["policy_id"]]
    def ts(r):
        try:
            return float(r.get("signal_ts_epoch_ms")) / 1000.0
        except (TypeError, ValueError):
            return None
    rows.sort(key=lambda r: (ts(r) or 0.0, r.get("binary_ticker") or ""))
    seen, used = set(), []
    for r in rows:
        if r.get("binary_ticker") in seen:
            continue
        seen.add(r.get("binary_ticker"))
        t = ts(r)
        if t is None or t <= float(pol["discovery_cutoff_epoch"]) or t > float(res["prospective_last_ts"]):
            continue
        used.append(r)
    errs = []
    if len(used) != res.get("shadow_total"):
        errs.append(f"journal rows validated by the report ({len(used)}) != report shadow_total ({res.get('shadow_total')})")
    bad_hash = sum(1 for r in used if r.get("policy_hash") != pol["policy_hash"])
    bad_thr = sum(1 for r in used if r.get("conflict_threshold") in (None, "")
                  or abs(float(r["conflict_threshold"]) - float(pol["conflict_threshold"])) > 1e-12)
    if bad_hash:
        errs.append(f"{bad_hash} validated journal rows carry a different policy_hash")
    if bad_thr:
        errs.append(f"{bad_thr} validated journal rows carry a different threshold")
    return errs


def verify_pair(pol, res):
    errs = ps.validate_policy(pol, allow_synthetic=True)          # content/hash integrity (synthetic judged separately)
    for a, b in (("feature_name", "feature"), ("coin_or_group", "coin_or_group"),
                 ("discovery_cutoff_utc", "discovery_cutoff_utc")):
        if pol.get(a) != res.get(b):
            errs.append(f"{a} differs between policy and report")
    if res.get("frozen_threshold") is None or abs(float(res["frozen_threshold"]) - float(pol["conflict_threshold"])) > 1e-12:
        errs.append("frozen threshold differs between policy and report")
    if res.get("threshold_used") != res.get("frozen_threshold"):
        errs.append("report threshold_used differs from frozen threshold")
    integ = res.get("integrity") or {}
    for k in ("policy_hash_mismatch", "threshold_mismatch", "decision_mismatch"):
        if integ.get(k):
            errs.append(f"Step 4 report integrity {k} = {integ.get(k)}")
    if res.get("prospective_last_ts") is None:
        errs.append("report has no prospective_last_ts")
    return errs


def make_experiment_id(pol, report_sha, cutoff):
    ident = {"policy_id": pol["policy_id"], "policy_hash": pol["policy_hash"], "step4_report_sha256": report_sha,
             "step5_start_cutoff": cutoff, "overlay_version": pp.OVERLAY_VERSION,
             "alpha_grid": list(pp.ALPHA_GRID), "max_abs_perp_delta": pp.MAX_ABS_PERP_DELTA}
    return hashlib.sha256(ps.canonical_json(ident).encode()).hexdigest()[:16]


def compute_experiment_hash(exp):
    return hashlib.sha256(ps.canonical_json({k: v for k, v in exp.items() if k != "experiment_hash"}).encode()).hexdigest()


def assemble(pol, res, hashes, constants, dash_sha, synthetic, now_utc):
    cutoff = float(res["prospective_last_ts"])
    e = {"experiment_schema_version": EXPERIMENT_SCHEMA_VERSION,
         "experiment_id": make_experiment_id(pol, hashes["shadow_report"], cutoff),
         "mode": "SHADOW_ANALYSIS_ONLY", "synthetic": bool(synthetic), "created_at_utc": now_utc,
         "source_policy_id": pol["policy_id"], "source_policy_hash": pol["policy_hash"],
         "source_step4_status": res["status"], "feature_name": pol["feature_name"],
         "coin_or_group": pol["coin_or_group"], "discovery_cutoff_utc": pol["discovery_cutoff_utc"],
         "frozen_threshold": pol["conflict_threshold"],
         "step5_start_cutoff": cutoff,
         "step5_start_cutoff_utc": dt.datetime.fromtimestamp(cutoff, dt.timezone.utc).isoformat(),
         "overlay_version": pp.OVERLAY_VERSION, "alpha_grid": list(pp.ALPHA_GRID),
         "max_abs_perp_delta": pp.MAX_ABS_PERP_DELTA, "strategy_constants": constants,
         "dashboard_sha256": dash_sha, "source_hashes": dict(hashes, step4_policy_hash=pol["policy_hash"]),
         "step4_validation_snapshot": {k: res.get(k) for k in (
             "status", "prospective_first_ts", "prospective_last_ts", "prospective_days", "shadow_total",
             "settled_total", "would_block", "would_allow", "coverage_pct", "loss_enrichment_ci95_pp",
             "per_contract_improvement_ci95", "frozen_threshold", "flags")}}
    e["experiment_hash"] = compute_experiment_hash(e)
    return e


def _write(path, reg):
    d = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(prefix=".experiments-", suffix=".json", dir=d)
    with os.fdopen(fd, "w") as f:
        json.dump(reg, f, indent=1, sort_keys=True)
    os.replace(tmp, path)


def build(policies_path, report_path, journal_path, dashboard_path, output, allow_synthetic=False, now=None, log=print):
    now_utc = dt.datetime.fromtimestamp(time.time() if now is None else now, dt.timezone.utc).isoformat()
    policies = load_registry(policies_path)
    report = None
    if os.path.exists(report_path):
        try:
            report = json.load(open(report_path))
        except ValueError as e:
            raise BuildError(f"Step 4 report is not valid JSON: {e}")
    if os.path.exists(output):
        reg = json.load(open(output))
        if not isinstance(reg, dict) or reg.get("experiment_schema_version") != EXPERIMENT_SCHEMA_VERSION \
                or not isinstance(reg.get("experiments"), list):
            raise BuildError("existing experiment registry has an unsupported format; refusing to overwrite")
    else:
        reg = {"experiment_schema_version": EXPERIMENT_SCHEMA_VERSION, "experiments": []}
    results = (report or {}).get("policies", []) if isinstance(report, dict) else []
    summary = {"step4_policies": len(policies), "step4_validated": 0, "experiments_created": 0, "entries": []}
    by_id = {p.get("policy_id"): p for p in policies if isinstance(p, dict)}
    validated = [r for r in results if r.get("status") == VALIDATED]
    for r in results:
        if r.get("status") != VALIDATED:
            summary["entries"].append({"policy_id": r.get("policy_id"), "status": ST_NOT_VALIDATED,
                                       "step4_status": r.get("status")})
    if validated:
        constants = extract_strategy_constants(dashboard_path)
        dash_sha = sha256_file(dashboard_path)
        hashes = {"policies": sha256_file(policies_path), "shadow_report": sha256_file(report_path)}
        for r in validated:
            pol = by_id.get(r.get("policy_id"))
            if pol is None:
                raise BuildError(f"validated report policy {r.get('policy_id')} is not in the policy registry")
            errs = verify_pair(pol, r) + journal_binding(journal_path, pol, r)
            if errs:
                raise BuildError(f"policy {pol['policy_id']}: " + "; ".join(errs))
            synthetic = bool(pol.get("synthetic")) or bool(r.get("synthetic"))
            if synthetic and not allow_synthetic:
                summary["entries"].append({"policy_id": pol["policy_id"], "status": ST_SYNTHETIC})
                continue
            if not synthetic:
                summary["step4_validated"] += 1
            exp = assemble(pol, r, hashes, constants, dash_sha, synthetic, now_utc)
            if any(x.get("experiment_id") == exp["experiment_id"] for x in reg["experiments"]):
                summary["entries"].append({"policy_id": pol["policy_id"], "status": ST_ALREADY,
                                           "experiment_id": exp["experiment_id"]})
                continue
            reg["experiments"].append(exp)
            summary["experiments_created"] += 1
            summary["entries"].append({"policy_id": pol["policy_id"], "status": ST_CREATED,
                                       "experiment_id": exp["experiment_id"], "synthetic": synthetic})
    _write(output, reg)
    if log:
        log("STEP 5 OF 6 — PROBABILITY INTEGRATION EXPERIMENT BUILDER\n")
        log(f"Step 4 validated candidates: {summary['step4_validated']}")
        log(f"Experiments created: {summary['experiments_created']}\n")
        for e in summary["entries"]:
            log(f"  policy {e.get('policy_id')}: {e['status']}"
                + (f" (Step 4 status {e['step4_status']})" if e.get("step4_status") else "")
                + (f" experiment {e['experiment_id']}" if e.get("experiment_id") else ""))
        if not summary["experiments_created"] and not any(e["status"] == ST_ALREADY for e in summary["entries"]):
            log("No real SHADOW_VALIDATED_CANDIDATE policy exists.")
            log("No probability overlay experiment created.")
            log("Continue collecting real telemetry/shadow outcomes.")
    return summary


def main(argv=None):
    a_ = argparse.ArgumentParser(description="Freeze Step 5 probability-overlay experiments (offline).")
    a_.add_argument("--policies", default="perp_shadow_policies.json")
    a_.add_argument("--shadow-report", default=os.path.join("analysis_output", "perp_shadow_report.json"))
    a_.add_argument("--journal", default="kalshi_perp_shadow.csv")
    a_.add_argument("--dashboard", default="kalshi_dashboard.py")
    a_.add_argument("--output", default="perp_integration_experiments.json")
    a_.add_argument("--allow-synthetic-test-data", action="store_true", help="TESTS ONLY")
    a = a_.parse_args(argv)
    try:
        build(a.policies, a.shadow_report, a.journal, a.dashboard, a.output, a.allow_synthetic_test_data)
    except BuildError as e:
        print(f"EXPERIMENT BUILD FAILED CLOSED: {e}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
