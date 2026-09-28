#!/usr/bin/env python3
"""
run_step6_research.py - STEP 6 real-data evaluation, feature-family ablation and model comparison. RESEARCH ONLY.

    py run_step6_research.py --sessions market_data_sessions --assets BTC,ETH,SOL,XRP
    py run_step6_research.py --dry-run                 # plan, frozen universe check, configs; nothing computed
    py run_step6_research.py --validate-only           # quality + Coinbase sequence + settlement provenance only
    py run_step6_research.py --dataset-only            # + build (or load) the immutable research matrix
    py run_step6_research.py --family-ablation         # + ablation / model comparison only (later stages NOT_RUN)
    py run_step6_research.py --calibration             # + calibration, confidence buckets and economics (all stages)
    py run_step6_research.py --report                  # print a summary of the existing step6_*.json outputs
    py run_step6_research.py --synthetic-selftest      # code self-test on a SYNTHETIC matrix (separate output file)
    [--workers N]  deterministic process pool (Windows-safe spawn); results are identical for any N

Outputs (analysis_output/): step6_data_quality.json, step6_family_ablation.json, step6_calibration.json,
step6_confidence_buckets.json, step6_economic_metrics.json, step6_research_ledger.json (+ the hash-chained
step6_research_ledger.jsonl). Without enough REAL data every file says INSUFFICIENT_DATA and names what is missing.

It never modifies the production predictor, thresholds, windows, stops, sizing, the perp veto or execution, never
places orders and never promotes a model. The FINAL_HOLDOUT is evaluated only with --final-holdout
--confirm-single-use (once per dataset, recorded permanently in the research ledger).
"""
import argparse
import hashlib
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from feature_eval import APP_VERSION, STEP6_SCHEMA_VERSION  # noqa: E402
from feature_eval.dataset import CHECKPOINT_GRID_S           # noqa: E402
from feature_eval.ledger import Ledger                       # noqa: E402
from feature_eval.report import LEDGER_FILE, OUTPUT_FILES    # noqa: E402

ALL_ASSETS = ("BTC", "ETH", "SOL", "XRP")


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="Step-6 research evaluation (read only, research only).")
    ap.add_argument("--sessions", default=os.path.join(HERE, "market_data_sessions"))
    ap.add_argument("--assets", default=",".join(ALL_ASSETS))
    ap.add_argument("--out", default=os.path.join(HERE, "analysis_output"))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--validate-only", action="store_true")
    ap.add_argument("--dataset-only", action="store_true")
    ap.add_argument("--family-ablation", action="store_true")
    ap.add_argument("--calibration", action="store_true")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--synthetic-selftest", action="store_true")
    ap.add_argument("--selftest-slots", type=int, default=400)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--no-perp", action="store_true")
    ap.add_argument("--no-micro", action="store_true")
    ap.add_argument("--final-holdout", action="store_true")
    ap.add_argument("--confirm-single-use", action="store_true")
    a = ap.parse_args(argv)
    a.asset_list = tuple(x.strip().upper() for x in a.assets.split(",") if x.strip())
    bad = [x for x in a.asset_list if x not in ALL_ASSETS]
    if bad:
        ap.error(f"unknown assets {bad}")
    return a


def _ledger(out):
    return Ledger(os.path.join(out, LEDGER_FILE))


def dry_run(a):
    from feature_eval.ablation import LAYER_HYPOTHESES, AblationConfig
    from feature_eval.calibration import CalibrationGates
    from feature_eval.economics import FeeModel
    from feature_eval.gates import SampleGates
    from feature_eval.quality import discover_sessions, is_synthetic
    from feature_eval.splits import SplitConfig
    from feature_eval.universe import counts, load_frozen, verify_frozen
    ok, probs = verify_frozen()
    uni = load_frozen() if ok else None
    sess = discover_sessions(a.sessions)
    plan = {"app_version": APP_VERSION, "schema_version": STEP6_SCHEMA_VERSION, "sessions_root": a.sessions,
            "sessions_found": len(sess), "real_sessions": sum(1 for d in sess if not is_synthetic(d)),
            "assets": list(a.asset_list), "checkpoint_grid_s": list(CHECKPOINT_GRID_S),
            "feature_universe": {"ok": ok, "fingerprint": uni["fingerprint"] if uni else None, "problems": probs,
                                 "counts": counts(uni["records"]) if uni else None},
            "hypotheses": [h for h, _ in LAYER_HYPOTHESES], "split": SplitConfig().to_dict(),
            "ablation": AblationConfig().to_dict(), "sample_gates": SampleGates().to_dict(),
            "calibration_gates": CalibrationGates().to_dict(), "fee_model": FeeModel().to_dict(),
            "outputs": sorted(OUTPUT_FILES.values())}
    print(json.dumps(plan, indent=1, sort_keys=True, default=str))
    return 0 if ok else 3


def selftest(a):
    from feature_eval.ablation import AblationConfig
    from feature_eval.gates import ComplexityGates
    from feature_eval.pipeline import run
    from feature_eval.report import write_selftest
    from feature_eval.synthetic import make_matrix
    m, recs = make_matrix(n_slots=a.selftest_slots, checkpoints_s=(300, 60))
    cfg = AblationConfig(bootstrap_reps=300, permutation_reps=1000,
                         complexity=ComplexityGates(min_events_per_parameter=5, min_train_markets_per_parameter=8))
    res = run(m, recs, cfg=cfg, workers=a.workers)
    fa = res["family_ablation"]
    planted = next((r for r in fa["stage3_family_hypotheses"] if r["hypothesis"].endswith("SPOT_PRICE_MOMENTUM")), None)
    checks = {
        "planted_signal_family_promising": bool(planted and planted["result_class"] == "PROMISING_RESEARCH_ONLY"),
        "noise_layers_not_promising": all(r["result_class"] != "PROMISING_RESEARCH_ONLY" for r in
                                          fa["stage1_layer_hypotheses"] if r["hypothesis"] in ("H01", "H03", "H04", "H08")),
        "duplicate_removed": "mom.syn.z_dup" in fa["structural_pruning_train_partition"]["structurally_removed_features"],
        "constant_removed": "mom.syn.const" in fa["structural_pruning_train_partition"]["structurally_removed_features"],
        "coinbase_gated": fa["structural_pruning_train_partition"]["structurally_removed_features"].get(
            "micro.coinbase.syn_noise") == "GATED_COINBASE_SEQUENCE_UNVERIFIED",
        "overall_synthetic_only": fa["result_class"] == "SYNTHETIC_ONLY",
    }
    p = write_selftest(a.out, res, {"selftest_checks": checks, "config_note": "lenient complexity gates for the "
                                    "SYNTHETIC self-test only (real runs use the predeclared defaults)"})
    _ledger(a.out).append("SYNTHETIC_SELFTEST", dataset_fingerprint=m.fingerprint, synthetic_only=True, checks=checks)
    for k, v in checks.items():
        print(f"  {'PASS' if v else 'FAIL'}  {k}")
    print(f"SYNTHETIC self-test written to {p} (code correctness only)")
    return 0 if all(checks.values()) else 1


def report(a):
    for key, fn in sorted(OUTPUT_FILES.items()):
        p = os.path.join(a.out, fn)
        if not os.path.exists(p):
            print(f"{fn}: missing")
            continue
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
        print(f"{fn}: result_class={d.get('result_class', '-')} synthetic_only={d.get('synthetic_only')} "
              f"reason={str(d.get('reason', ''))[:160]}")
    return 0


def _cache_key(reports, universe_fp, a, cb_status):
    body = {"sessions": sorted((r["session_id"], r.get("raw_store_sha256")) for r in reports), "universe": universe_fp,
            "grid": list(CHECKPOINT_GRID_S), "assets": list(a.asset_list), "perp": not a.no_perp, "micro": not a.no_micro,
            "coinbase": cb_status, "schema": STEP6_SCHEMA_VERSION}
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


def main(argv=None):
    a = parse_args(argv)
    if a.dry_run:
        return dry_run(a)
    if a.report:
        return report(a)
    os.makedirs(a.out, exist_ok=True)
    if a.synthetic_selftest:
        return selftest(a)
    from feature_eval.coinbase_seq import analyse_sessions, coinbase_features_allowed
    from feature_eval.dataset import CacheInvalid, build_matrix, load_cache, save_cache
    from feature_eval.pipeline import evaluate_final_holdout, run
    from feature_eval.quality import discover_sessions, is_synthetic, validate_session
    from feature_eval.report import write_real_outputs
    from feature_eval.universe import load_frozen, verify_frozen
    ok, probs = verify_frozen()
    if not ok:
        print("STOP: the frozen feature universe does not verify:", probs)
        return 3
    uni = load_frozen()
    ledger = _ledger(a.out)
    sess = discover_sessions(a.sessions)
    real = [d for d in sess if not is_synthetic(d)]
    ignored_syn = [os.path.basename(d) for d in sess if is_synthetic(d)]
    cb = analyse_sessions(real) if real else {"status": "UNVERIFIED_REAL_FEED", "reason": "no real sessions"}
    reports = [validate_session(d, coinbase_status=cb["status"]) for d in real]
    dq = {"sessions_root": a.sessions, "real_sessions": len(real), "synthetic_sessions_ignored": ignored_syn,
          "session_reports": [{k: r.get(k) for k in ("session_id", "verdict", "real_data_verdict", "usable_for_research",
                                                     "span", "problems", "sources", "checks", "raw_store_sha256")}
                              for r in reports],
          "coinbase_sequence": cb, "coinbase_features_allowed": coinbase_features_allowed(cb["status"]),
          "assets": list(a.asset_list), "checkpoint_grid_s": list(CHECKPOINT_GRID_S),
          "feature_universe_fingerprint": uni["fingerprint"]}
    usable = [d for d, r in zip(real, reports) if r["verdict"] != "REJECT"]
    dq["usable_sessions"] = [os.path.basename(d) for d in usable]
    if a.validate_only:
        dq["result_class"] = "INSUFFICIENT_DATA" if not usable else "VALIDATED_ONLY"
        from feature_eval.report import _dump, _header
        _dump(os.path.join(a.out, OUTPUT_FILES["data_quality"]), dict(_header("data_quality", False), **dq))
        print(f"validated {len(real)} real session(s); usable {len(usable)}; coinbase sequence {cb['status']}")
        return 0
    if not usable:
        reason = "no real research sessions" if not real else "every real session was REJECTED by the quality validator"
        dq["result_class"] = "INSUFFICIENT_DATA"
        dq["reason"] = reason
        from feature_eval.gates import sample_gate
        sg = sample_gate([], assets=a.asset_list, checkpoints_s=CHECKPOINT_GRID_S)
        dq["sample_gate"] = sg
        ledger.append("RUN_INSUFFICIENT_DATA", reason=reason, real_sessions=len(real))
        w = write_real_outputs(a.out, dq, None, ledger.summary(), reason, sg["failed_requirements"])
        print(f"INSUFFICIENT_DATA: {reason}. Wrote {sorted(w.values())}")
        return 0
    key = _cache_key(reports, uni["fingerprint"], a, cb["status"])
    cache_root = os.path.join(a.out, "step6_cache")
    idx_p = os.path.join(cache_root, "index.json")
    index = {}
    if os.path.exists(idx_p):
        with open(idx_p, encoding="utf-8") as f:
            index = json.load(f)
    matrix = None
    if key in index:
        try:
            matrix = load_cache(os.path.join(cache_root, index[key]), usable)
            print(f"loaded cached research matrix {index[key][:12]}")
        except (CacheInvalid, OSError, ValueError) as e:
            print(f"cache not used ({e}); rebuilding")
    if matrix is None:
        matrix = build_matrix(usable, a.asset_list, CHECKPOINT_GRID_S, dict(zip(usable, reports)), cb["status"],
                              False, uni, not a.no_perp, not a.no_micro)
        d = save_cache(matrix, cache_root)
        index[key] = os.path.basename(d)
        with open(idx_p, "w", encoding="utf-8") as f:
            json.dump(index, f, indent=1, sort_keys=True)
        ledger.append("DATASET_BUILT", dataset_fingerprint=matrix.fingerprint, sessions=matrix.meta["sessions_used"],
                      rows=len(matrix.rows))
    dq["dataset"] = {k: matrix.meta.get(k) for k in ("dataset_fingerprint", "sessions_used", "sessions_skipped",
                                                      "settlement_gate", "rows", "checkpoint_grid_s", "assets")}
    if a.dataset_only:
        dq["result_class"] = "DATASET_ONLY"
        from feature_eval.report import _dump, _header
        _dump(os.path.join(a.out, OUTPUT_FILES["data_quality"]), dict(_header("data_quality", False), **dq))
        print(f"research matrix {matrix.fingerprint[:12]}: {len(matrix.rows)} rows")
        return 0
    stages = ("ablation",) if (a.family_ablation and not a.calibration) else ("ablation", "calibration", "economics")
    from feature_eval.leakage import LeakageError
    try:
        res = run(matrix, uni["records"], coinbase_allowed=coinbase_features_allowed(cb["status"]), workers=a.workers,
                  ledger=ledger, stages=stages)
    except LeakageError as e:                               # fail closed, but leave the evidence
        dq["result_class"] = "LEAKAGE_DETECTED"
        dq["reason"] = str(e)[:2000]
        ledger.append("LEAKAGE_DETECTED", dataset_fingerprint=matrix.fingerprint, reason=str(e)[:500])
        write_real_outputs(a.out, dq, None, ledger.summary(), "leakage guard failed - no model was evaluated")
        print(f"STOP: {e}")
        return 4
    dq["sample_gate"] = res["data"]["sample_gate"]
    dq["splits"] = res["data"]["splits"]
    dq["result_class"] = res["data"]["sample_gate"]["status"]
    results = {k: res.get(k) for k in ("family_ablation", "calibration", "confidence_buckets", "economic_metrics")}
    if a.final_holdout:
        best = (res.get("family_ablation") or {}).get("best_development_configuration")
        if not a.confirm_single_use or not best:
            print("the FINAL_HOLDOUT is single use: pass --confirm-single-use (and a best DEV configuration must exist)")
        else:
            ho = evaluate_final_holdout(matrix, uni["records"], best, ledger, coinbase_allowed=coinbase_features_allowed(
                cb["status"]), reason="--final-holdout")
            from feature_eval.report import _dump, _header
            _dump(os.path.join(a.out, "step6_final_holdout.json"), dict(_header("final_holdout", False), **ho))
    w = write_real_outputs(a.out, dq, results, ledger.summary())
    print(f"dataset {matrix.fingerprint[:12]}: sample gate {res['data']['sample_gate']['status']}; wrote {sorted(w.values())}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
