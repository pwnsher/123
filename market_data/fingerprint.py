#!/usr/bin/env python3
"""
Market-data research fingerprint — SEPARATE from the Step-1 strategy baseline and the Step-2
settlement baseline.

    py -m market_data.fingerprint --verify
    py -m market_data.fingerprint --write --i-intend-to-change-the-market-data-baseline

Pins every module of the market_data package (recursively; canonical AST, so comments / docstrings /
whitespace are ignored and any semantic change is detected), the schema / feature-set versions, the
feature registry (names, windows, definitions) and the default FeatureConfig. Stored in
config/market_data_baseline.json. Source is parsed, never imported for hashing. No network.
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
from market_data import FEATURE_SET_VERSION, MARKET_DATA_SCHEMA_VERSION                  # noqa: E402
from market_data.features.definitions import FEATURES, HORIZONS_MS, FeatureConfig        # noqa: E402

BASELINE_PATH = os.path.join(HERE, "config", "market_data_baseline.json")
PKG = os.path.join(HERE, "market_data")
FORMAT = "market_data_fingerprint_v1/canonical_ast_v2_sha256"


def module_hashes(pkg_dir=PKG):
    out = {}
    for root, dirs, files in os.walk(pkg_dir):
        dirs[:] = sorted(d for d in dirs if d != "__pycache__")
        for name in sorted(files):
            if name.endswith(".py"):
                p = os.path.join(root, name)
                with open(p, encoding="utf-8") as f:
                    tree = sf._strip_docstrings(ast.parse(f.read()))
                rel = os.path.relpath(p, pkg_dir).replace(os.sep, "/")
                out[rel] = hashlib.sha256(sf.canonical_json(sf.canonicalize_ast(tree)).encode()).hexdigest()
    return dict(sorted(out.items()))


def build(pkg_dir=PKG):
    feats = [dict(f.__dict__) for f in FEATURES]
    core = {"format": FORMAT, "market_data_schema_version": MARKET_DATA_SCHEMA_VERSION,
            "feature_set_version": FEATURE_SET_VERSION, "modules": module_hashes(pkg_dir),
            "features_sha256": hashlib.sha256(sf.canonical_json(feats).encode()).hexdigest(),
            "feature_count": len(feats), "windows_ms": dict(HORIZONS_MS),
            "default_feature_config": FeatureConfig().to_dict()}
    core["market_data_fingerprint"] = hashlib.sha256(sf.canonical_json(core).encode()).hexdigest()
    core["note"] = "Research/observation code only; independent of the strategy and settlement baselines."
    return core


def verify(path=BASELINE_PATH, pkg_dir=PKG):
    try:
        with open(path, encoding="utf-8") as f:
            stored = json.load(f)
    except (OSError, ValueError) as e:
        return False, [f"cannot read {path}: {e}"]
    cur = build(pkg_dir)
    problems = []
    for k in ("format", "market_data_schema_version", "feature_set_version", "features_sha256", "feature_count",
              "windows_ms", "default_feature_config"):
        if stored.get(k) != cur[k]:
            problems.append(f"{k} changed")
    for name in sorted(set(stored.get("modules", {})) | set(cur["modules"])):
        if stored.get("modules", {}).get(name) != cur["modules"].get(name):
            problems.append(f"modules/{name} changed")
    if stored.get("market_data_fingerprint") != cur["market_data_fingerprint"] and not problems:
        problems.append("market-data fingerprint differs")
    return not problems, problems


def main(argv=None):
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--verify", action="store_true")
    g.add_argument("--write", action="store_true")
    ap.add_argument("--i-intend-to-change-the-market-data-baseline", action="store_true", dest="intend")
    a = ap.parse_args(argv)
    if a.write:
        if not a.intend:
            print("Refusing: pass --i-intend-to-change-the-market-data-baseline and record why in "
                  "docs/HIGH_RESOLUTION_DATA.md.", file=sys.stderr)
            return 2
        os.makedirs(os.path.dirname(BASELINE_PATH), exist_ok=True)
        with open(BASELINE_PATH, "w", encoding="utf-8", newline="\n") as f:
            json.dump(build(), f, indent=1, sort_keys=True)
            f.write("\n")
        print(f"wrote {BASELINE_PATH}")
        return 0
    ok, problems = verify()
    print("market-data fingerprint: " + ("MATCHES" if ok else "MISMATCH"))
    if ok:
        print(f"  {build()['market_data_fingerprint']}")
    for p in problems:
        print(f"  - {p}")
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
