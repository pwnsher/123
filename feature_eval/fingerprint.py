#!/usr/bin/env python3
"""
Step-6 FEATURE-EVALUATION research fingerprint - separate from every earlier baseline (strategy, settlement,
market-data, perp-data, microstructure), none of which Step 6 changes.

    py -m feature_eval.fingerprint --verify
    py -m feature_eval.fingerprint --write --i-intend-to-change-the-step6-baseline

Pins every feature_eval module and the Step-6 CLIs (canonical AST: comments / docstrings / whitespace ignored), the
schema / universe / label versions, the frozen feature-universe fingerprint, the checkpoint grid, the result classes and
every predeclared default configuration (splits + purge, ablation, pruning, complexity and sample gates, calibration
gates, fee model, economics sizes, hypotheses). It also records the hashes of the existing perp-veto chain and the
production files so a change to either is reported. Stored in config/step6_baseline.json.
"""
import argparse
import ast
import hashlib
import json
import os
import sys

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import strategy_fingerprint as sf                                               # noqa: E402
from perp_data.fingerprint import perp_veto_fingerprint, perp_veto_hashes     # noqa: E402

BASELINE_PATH = os.path.join(HERE, "config", "step6_baseline.json")
PKG = os.path.join(HERE, "feature_eval")
CLIS = ("run_step6_research.py", "research_status.py", "validate_research_session.py")
FORMAT = "step6_fingerprint_v1/canonical_ast_v2_sha256"
PRODUCTION_FILES = ("kalshi_dashboard.py", "kalshi_backtest.py", "kalshi_bot.py", "run_local.py", "strategy_fingerprint.py")


def _ast_hash(p):
    with open(p, encoding="utf-8") as f:
        tree = sf._strip_docstrings(ast.parse(f.read()))
    return hashlib.sha256(sf.canonical_json(sf.canonicalize_ast(tree)).encode()).hexdigest()


def module_hashes(pkg_dir=PKG, repo=HERE):
    out = {}
    for name in sorted(os.listdir(pkg_dir)):
        if name.endswith(".py"):
            out[f"feature_eval/{name}"] = _ast_hash(os.path.join(pkg_dir, name))
    for c in CLIS:
        out[c] = _ast_hash(os.path.join(repo, c))
    return dict(sorted(out.items()))


def defaults():
    from feature_eval import (FEATURE_CLASSES, FEATURE_UNIVERSE_VERSION, LABEL_VERSION, RESULT_CLASSES,
                              STEP6_SCHEMA_VERSION)
    from feature_eval.ablation import LAYER_HYPOTHESES, REGIME_VARIABLES, AblationConfig
    from feature_eval.calibration import CalibrationGates
    from feature_eval.coinbase_seq import SequenceEvidenceConfig
    from feature_eval.dataset import CHECKPOINT_GRID_S
    from feature_eval.economics import RESEARCH_SIZES, FeeModel
    from feature_eval.gates import SampleGates
    from feature_eval.labels import LabelGateConfig
    from feature_eval.metrics import CONFIDENCE_BUCKETS
    from feature_eval.quality import QualityConfig
    from feature_eval.splits import SplitConfig
    from settlement.rules import RULE_SET_VERSION, rule_set_fingerprint
    return {"step6_schema_version": STEP6_SCHEMA_VERSION, "feature_universe_version": FEATURE_UNIVERSE_VERSION,
            "label_version": LABEL_VERSION, "result_classes": list(RESULT_CLASSES),
            "feature_classes": list(FEATURE_CLASSES), "checkpoint_grid_s": list(CHECKPOINT_GRID_S),
            "hypotheses": [[h, list(ls)] for h, ls in LAYER_HYPOTHESES], "regime_variables": list(REGIME_VARIABLES),
            "split": SplitConfig().to_dict(), "ablation": AblationConfig().to_dict(),
            "sample_gates": SampleGates().to_dict(), "calibration_gates": CalibrationGates().to_dict(),
            "fee_model": FeeModel().to_dict(), "fee_model_fingerprint": FeeModel().fingerprint(),
            "research_sizes": list(RESEARCH_SIZES), "confidence_buckets": [list(b) for b in CONFIDENCE_BUCKETS],
            "quality": QualityConfig().to_dict(), "coinbase_sequence": SequenceEvidenceConfig().to_dict(),
            "label_gate": LabelGateConfig().to_dict(),
            "settlement_rule_set": {"version": RULE_SET_VERSION, "fingerprint": rule_set_fingerprint()}}


def build(pkg_dir=PKG, repo=HERE):
    from feature_eval.universe import load_frozen
    uni = load_frozen()
    core = {"format": FORMAT, "modules": module_hashes(pkg_dir, repo),
            "feature_universe_fingerprint": uni["fingerprint"], "feature_universe_records": len(uni["records"]),
            "defaults_sha256": hashlib.sha256(sf.canonical_json(defaults()).encode()).hexdigest()}
    core["step6_fingerprint"] = hashlib.sha256(sf.canonical_json(core).encode()).hexdigest()
    core["defaults"] = defaults()
    core["existing_perp_veto_files"] = perp_veto_hashes(repo)
    core["existing_perp_veto_fingerprint"] = perp_veto_fingerprint(repo)
    core["production_files"] = {f: sf.sha256_file(os.path.join(repo, f)) for f in PRODUCTION_FILES}
    core["note"] = ("Research-only evaluation code; independent of every earlier baseline. existing_perp_veto_* and "
                    "production_files record files Step 6 must leave byte-identical.")
    return core


def verify(path=BASELINE_PATH, pkg_dir=PKG, repo=HERE):
    try:
        with open(path, encoding="utf-8") as f:
            stored = json.load(f)
    except (OSError, ValueError) as e:
        return False, [f"cannot read {path}: {e}"]
    cur = build(pkg_dir, repo)
    problems = []
    for k in ("format", "feature_universe_fingerprint", "feature_universe_records", "defaults_sha256"):
        if stored.get(k) != cur[k]:
            problems.append(f"{k} changed")
    for name in sorted(set(stored.get("modules", {})) | set(cur["modules"])):
        if stored.get("modules", {}).get(name) != cur["modules"].get(name):
            problems.append(f"modules/{name} changed")
    for group, label in (("existing_perp_veto_files", "EXISTING PERP VETO FILE CHANGED"),
                         ("production_files", "PRODUCTION FILE CHANGED")):
        for name in sorted(set(stored.get(group, {})) | set(cur[group])):
            if stored.get(group, {}).get(name) != cur[group].get(name):
                problems.append(f"{label}: {name}")
    if stored.get("step6_fingerprint") != cur["step6_fingerprint"] and not problems:
        problems.append("step-6 fingerprint differs")
    return not problems, problems


def main(argv=None):
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--verify", action="store_true")
    g.add_argument("--write", action="store_true")
    ap.add_argument("--i-intend-to-change-the-step6-baseline", action="store_true", dest="intend")
    a = ap.parse_args(argv)
    if a.write:
        if not a.intend:
            print("Refusing: pass --i-intend-to-change-the-step6-baseline and record why in "
                  "docs/STEP6_FEATURE_EVALUATION.md.", file=sys.stderr)
            return 2
        with open(BASELINE_PATH, "w", encoding="utf-8", newline="\n") as f:
            json.dump(build(), f, indent=1, sort_keys=True)
            f.write("\n")
        print(f"wrote {BASELINE_PATH}")
        return 0
    ok, problems = verify()
    b = build()
    print("step-6 fingerprint: " + ("MATCHES" if ok else "MISMATCH"))
    if ok:
        print(f"  {b['step6_fingerprint']}")
    print(f"existing perp-veto fingerprint: {b['existing_perp_veto_fingerprint']}")
    for p in problems:
        print(f"  - {p}")
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
