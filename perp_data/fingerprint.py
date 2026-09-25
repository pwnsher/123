#!/usr/bin/env python3
"""
Step-4 perp-data research fingerprint — separate from the Step-1 strategy, Step-2 settlement and Step-3
market-data baselines.

    py -m perp_data.fingerprint --verify
    py -m perp_data.fingerprint --write --i-intend-to-change-the-perp-data-baseline

Pins every perp_data module (canonical AST: comments / docstrings / whitespace ignored), the schema and
feature-set versions, the feature registry (names, families, windows, definitions), the default feature
config and the venue semantics table. It ALSO records the fingerprint of the EXISTING perp research / shadow /
promotion / live-veto chain (file hashes, read not imported), so any change to that chain is detected here
too - Step 4 must leave it byte-identical. Stored in config/perp_data_baseline.json. No network.
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

import strategy_fingerprint as sf                                                  # noqa: E402
from perp_data import PERP_FEATURE_SET_VERSION, PERP_SCHEMA_VERSION                # noqa: E402
from perp_data.features.definitions import FEATURES, PerpFeatureConfig             # noqa: E402
from perp_data.venues import VENUES                                                # noqa: E402

BASELINE_PATH = os.path.join(HERE, "config", "perp_data_baseline.json")
PKG = os.path.join(HERE, "perp_data")
FORMAT = "perp_data_fingerprint_v1/canonical_ast_v2_sha256"
# The existing Kalshi-perp chain (Steps 1-6 of the perp research) + the file its promotion binds to.
PERP_VETO_FILES = ("perp_telemetry.py", "perp_probability.py", "perp_shadow.py", "perp_live.py",
                   "analyze_perp_quality.py", "analyze_perp_predictive.py", "build_perp_shadow_policy.py",
                   "analyze_perp_shadow.py", "build_perp_integration_experiment.py", "analyze_perp_integration.py",
                   "promote_perp_integration.py", "check_perp_deployment.py", "label_binary_outcomes.py",
                   "step5_baseline_manifest.json")


def module_hashes(pkg_dir=PKG):
    out = {}
    for root, dirs, files in os.walk(pkg_dir):
        dirs[:] = sorted(d for d in dirs if d != "__pycache__")
        for name in sorted(files):
            if name.endswith(".py"):
                p = os.path.join(root, name)
                with open(p, encoding="utf-8") as f:
                    tree = sf._strip_docstrings(ast.parse(f.read()))
                out[os.path.relpath(p, pkg_dir).replace(os.sep, "/")] = hashlib.sha256(
                    sf.canonical_json(sf.canonicalize_ast(tree)).encode()).hexdigest()
    return dict(sorted(out.items()))


def perp_veto_hashes(repo=HERE):
    return {f: sf.sha256_file(os.path.join(repo, f)) for f in PERP_VETO_FILES}


def perp_veto_fingerprint(repo=HERE):
    return hashlib.sha256(sf.canonical_json(perp_veto_hashes(repo)).encode()).hexdigest()


def build(pkg_dir=PKG, repo=HERE):
    feats = [dict(f.__dict__) for f in FEATURES]
    venues = {n: {k: (dict(v) if isinstance(v, dict) else v) for k, v in s.__dict__.items()} for n, s in sorted(VENUES.items())}
    core = {"format": FORMAT, "perp_schema_version": PERP_SCHEMA_VERSION, "perp_feature_set_version": PERP_FEATURE_SET_VERSION,
            "modules": module_hashes(pkg_dir), "features_sha256": hashlib.sha256(sf.canonical_json(feats).encode()).hexdigest(),
            "feature_count": len(feats), "default_feature_config": PerpFeatureConfig().to_dict(),
            "venues_sha256": hashlib.sha256(sf.canonical_json(venues).encode()).hexdigest()}
    core["perp_data_fingerprint"] = hashlib.sha256(sf.canonical_json(core).encode()).hexdigest()
    core["existing_perp_veto_files"] = perp_veto_hashes(repo)
    core["existing_perp_veto_fingerprint"] = perp_veto_fingerprint(repo)
    core["note"] = ("Research/observation code only; independent of the strategy, settlement and market-data baselines. "
                    "existing_perp_veto_* records the untouched Kalshi-perp veto chain (Step 4 never feeds it).")
    return core


def verify(path=BASELINE_PATH, pkg_dir=PKG, repo=HERE):
    try:
        with open(path, encoding="utf-8") as f:
            stored = json.load(f)
    except (OSError, ValueError) as e:
        return False, [f"cannot read {path}: {e}"]
    cur = build(pkg_dir, repo)
    problems = []
    for k in ("format", "perp_schema_version", "perp_feature_set_version", "features_sha256", "feature_count",
              "default_feature_config", "venues_sha256"):
        if stored.get(k) != cur[k]:
            problems.append(f"{k} changed")
    for name in sorted(set(stored.get("modules", {})) | set(cur["modules"])):
        if stored.get("modules", {}).get(name) != cur["modules"].get(name):
            problems.append(f"modules/{name} changed")
    for name in sorted(set(stored.get("existing_perp_veto_files", {})) | set(cur["existing_perp_veto_files"])):
        if stored.get("existing_perp_veto_files", {}).get(name) != cur["existing_perp_veto_files"].get(name):
            problems.append(f"EXISTING PERP VETO FILE CHANGED: {name}")
    if stored.get("perp_data_fingerprint") != cur["perp_data_fingerprint"] and not problems:
        problems.append("perp-data fingerprint differs")
    return not problems, problems


def main(argv=None):
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--verify", action="store_true")
    g.add_argument("--write", action="store_true")
    ap.add_argument("--i-intend-to-change-the-perp-data-baseline", action="store_true", dest="intend")
    a = ap.parse_args(argv)
    if a.write:
        if not a.intend:
            print("Refusing: pass --i-intend-to-change-the-perp-data-baseline and record why in "
                  "docs/PERP_HIGH_RESOLUTION_DATA.md.", file=sys.stderr)
            return 2
        with open(BASELINE_PATH, "w", encoding="utf-8", newline="\n") as f:
            json.dump(build(), f, indent=1, sort_keys=True)
            f.write("\n")
        print(f"wrote {BASELINE_PATH}")
        return 0
    ok, problems = verify()
    b = build()
    print("perp-data fingerprint: " + ("MATCHES" if ok else "MISMATCH"))
    if ok:
        print(f"  {b['perp_data_fingerprint']}")
    print(f"existing perp-veto fingerprint: {b['existing_perp_veto_fingerprint']}")
    for p in problems:
        print(f"  - {p}")
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
