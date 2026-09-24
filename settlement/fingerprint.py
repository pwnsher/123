#!/usr/bin/env python3
"""
Settlement-research fingerprint — SEPARATE from the Step-1 strategy baseline.

    py -m settlement.fingerprint --verify
    py -m settlement.fingerprint --write --i-intend-to-change-the-settlement-baseline

Pins every module of the settlement package (canonical AST: comments/docstrings/whitespace ignored,
any semantic change detected) and every named window / reconstruction policy. Stored in
config/settlement_baseline.json. The Step-1 files (config/strategy_baseline.json,
step5_baseline_manifest.json, regression/strategy_cases.json) are neither read nor written here.
Source is parsed, never imported for hashing. No network.
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

import strategy_fingerprint as sf                                                    # noqa: E402
from settlement import ENGINE_VERSION, RECONSTRUCTION_VERSION, RECORD_SCHEMA_VERSION  # noqa: E402
from settlement.policy import RECONSTRUCTION_POLICIES, WINDOW_POLICIES               # noqa: E402
from settlement.schemas import SPECS                                                 # noqa: E402

BASELINE_PATH = os.path.join(HERE, "config", "settlement_baseline.json")
PKG = os.path.join(HERE, "settlement")
FORMAT = "settlement_fingerprint_v1/canonical_ast_v2_sha256"


def module_hashes(pkg_dir=PKG):
    out = {}
    for name in sorted(os.listdir(pkg_dir)):
        if name.endswith(".py"):
            with open(os.path.join(pkg_dir, name), encoding="utf-8") as f:
                tree = sf._strip_docstrings(ast.parse(f.read()))
            out[name] = hashlib.sha256(sf.canonical_json(sf.canonicalize_ast(tree)).encode()).hexdigest()
    return out


def build(pkg_dir=PKG):
    mods = module_hashes(pkg_dir)
    pols = {"window": {k: v.fingerprint() for k, v in sorted(WINDOW_POLICIES.items())},
            "reconstruction": {k: v.fingerprint() for k, v in sorted(RECONSTRUCTION_POLICIES.items())}}
    schemas = {k: {"version": v.version, "verification": v.verification} for k, v in sorted(SPECS.items())}
    core = {"format": FORMAT, "engine_version": ENGINE_VERSION, "reconstruction_version": RECONSTRUCTION_VERSION,
            "record_schema_version": RECORD_SCHEMA_VERSION, "modules": mods, "policies": pols, "schemas": schemas}
    core["settlement_fingerprint"] = hashlib.sha256(sf.canonical_json(core).encode()).hexdigest()
    core["note"] = "Research/observation code only; independent of the Step-1 strategy baseline."
    return core


def verify(path=BASELINE_PATH, pkg_dir=PKG):
    try:
        with open(path, encoding="utf-8") as f:
            stored = json.load(f)
    except (OSError, ValueError) as e:
        return False, [f"cannot read {path}: {e}"]
    cur = build(pkg_dir)
    problems = []
    for k in ("format", "engine_version", "reconstruction_version", "record_schema_version"):
        if stored.get(k) != cur[k]:
            problems.append(f"{k}: {stored.get(k)!r} -> {cur[k]!r}")
    for section in ("modules",):
        for name in sorted(set(stored.get(section, {})) | set(cur[section])):
            if stored.get(section, {}).get(name) != cur[section].get(name):
                problems.append(f"{section}/{name} changed")
    for kind in ("window", "reconstruction"):
        a, b = stored.get("policies", {}).get(kind, {}), cur["policies"][kind]
        for name in sorted(set(a) | set(b)):
            if a.get(name) != b.get(name):
                problems.append(f"policy {kind}/{name} changed")
    if stored.get("schemas") != cur["schemas"]:
        problems.append("schema specs changed")
    if stored.get("settlement_fingerprint") != cur["settlement_fingerprint"] and not problems:
        problems.append("settlement fingerprint differs")
    return not problems, problems


def main(argv=None):
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--verify", action="store_true")
    g.add_argument("--write", action="store_true")
    ap.add_argument("--i-intend-to-change-the-settlement-baseline", action="store_true", dest="intend")
    a = ap.parse_args(argv)
    if a.write:
        if not a.intend:
            print("Refusing: pass --i-intend-to-change-the-settlement-baseline and record why in "
                  "docs/SETTLEMENT_ENGINE.md.", file=sys.stderr)
            return 2
        os.makedirs(os.path.dirname(BASELINE_PATH), exist_ok=True)
        with open(BASELINE_PATH, "w", encoding="utf-8", newline="\n") as f:
            json.dump(build(), f, indent=1, sort_keys=True)
            f.write("\n")
        print(f"wrote {BASELINE_PATH}")
        return 0
    ok, problems = verify()
    print("settlement fingerprint: " + ("MATCHES" if ok else "MISMATCH"))
    if ok:
        print(f"  {build()['settlement_fingerprint']}")
    for p in problems:
        print(f"  - {p}")
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
