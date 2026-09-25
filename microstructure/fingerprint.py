#!/usr/bin/env python3
"""
Step-5 MICROSTRUCTURE research fingerprint - separate from the Step-1 strategy, Step-2 settlement, Step-3 market-data
and Step-4 perp-data baselines.

    py -m microstructure.fingerprint --verify
    py -m microstructure.fingerprint --write --i-intend-to-change-the-microstructure-baseline

Pins every microstructure module (canonical AST: comments / docstrings / whitespace ignored), the schema and
feature-set versions, the feature registry (names, families, windows, definitions), the default feature config and
the book-venue semantics table. It also records the hashes of the EXISTING perp-veto chain (read, not imported) and
the production files, so a change to either is reported here too. Stored in config/microstructure_baseline.json.
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

import strategy_fingerprint as sf                                                        # noqa: E402
from microstructure import MICRO_FEATURE_SET_VERSION, MICRO_SCHEMA_VERSION               # noqa: E402
from microstructure.features.definitions import FEATURES, MicroFeatureConfig             # noqa: E402
from microstructure.venues import VENUES                                                 # noqa: E402
from perp_data.fingerprint import perp_veto_fingerprint, perp_veto_hashes              # noqa: E402

BASELINE_PATH = os.path.join(HERE, "config", "microstructure_baseline.json")
PKG = os.path.join(HERE, "microstructure")
FORMAT = "microstructure_fingerprint_v1/canonical_ast_v2_sha256"
PRODUCTION_FILES = ("kalshi_dashboard.py", "kalshi_backtest.py", "kalshi_bot.py", "run_local.py", "strategy_fingerprint.py")


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


def production_hashes(repo=HERE):
    return {f: sf.sha256_file(os.path.join(repo, f)) for f in PRODUCTION_FILES}


def build(pkg_dir=PKG, repo=HERE):
    feats = [dict(f.__dict__) for f in FEATURES]
    venues = {n: {k: (dict(v) if isinstance(v, dict) else list(v) if isinstance(v, tuple) else v) for k, v in s.__dict__.items()}
              for n, s in sorted(VENUES.items())}
    core = {"format": FORMAT, "micro_schema_version": MICRO_SCHEMA_VERSION, "micro_feature_set_version": MICRO_FEATURE_SET_VERSION,
            "modules": module_hashes(pkg_dir), "features_sha256": hashlib.sha256(sf.canonical_json(feats).encode()).hexdigest(),
            "feature_count": len(feats), "default_feature_config": MicroFeatureConfig().to_dict(),
            "venues_sha256": hashlib.sha256(sf.canonical_json(venues).encode()).hexdigest()}
    core["microstructure_fingerprint"] = hashlib.sha256(sf.canonical_json(core).encode()).hexdigest()
    core["existing_perp_veto_files"] = perp_veto_hashes(repo)
    core["existing_perp_veto_fingerprint"] = perp_veto_fingerprint(repo)
    core["production_files"] = production_hashes(repo)
    core["note"] = ("Research/observation code only; independent of the strategy, settlement, market-data and perp-data "
                    "baselines. existing_perp_veto_* and production_files record files Step 5 must leave byte-identical.")
    return core


def verify(path=BASELINE_PATH, pkg_dir=PKG, repo=HERE):
    try:
        with open(path, encoding="utf-8") as f:
            stored = json.load(f)
    except (OSError, ValueError) as e:
        return False, [f"cannot read {path}: {e}"]
    cur = build(pkg_dir, repo)
    problems = []
    for k in ("format", "micro_schema_version", "micro_feature_set_version", "features_sha256", "feature_count",
              "default_feature_config", "venues_sha256"):
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
    if stored.get("microstructure_fingerprint") != cur["microstructure_fingerprint"] and not problems:
        problems.append("microstructure fingerprint differs")
    return not problems, problems


def main(argv=None):
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--verify", action="store_true")
    g.add_argument("--write", action="store_true")
    ap.add_argument("--i-intend-to-change-the-microstructure-baseline", action="store_true", dest="intend")
    a = ap.parse_args(argv)
    if a.write:
        if not a.intend:
            print("Refusing: pass --i-intend-to-change-the-microstructure-baseline and record why in docs/MICROSTRUCTURE.md.",
                  file=sys.stderr)
            return 2
        with open(BASELINE_PATH, "w", encoding="utf-8", newline="\n") as f:
            json.dump(build(), f, indent=1, sort_keys=True)
            f.write("\n")
        print(f"wrote {BASELINE_PATH}")
        return 0
    ok, problems = verify()
    b = build()
    print("microstructure fingerprint: " + ("MATCHES" if ok else "MISMATCH"))
    if ok:
        print(f"  {b['microstructure_fingerprint']}")
    print(f"existing perp-veto fingerprint: {b['existing_perp_veto_fingerprint']}")
    for p in problems:
        print(f"  - {p}")
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
