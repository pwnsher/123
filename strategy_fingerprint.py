#!/usr/bin/env python3
"""
strategy_fingerprint.py — pin the LEGACY strategy semantics of kalshi_dashboard.py.

Step 6 must add veto plumbing to the dashboard, so its whole-file SHA-256 changes. What must
NOT change is the legacy strategy itself. This module fingerprints the strategy-critical
constants and function bodies through a CANONICAL AST, so line numbers, whitespace, comments
and docstrings are ignored while semantics are pinned.

Fingerprint format v2 ("canonical_ast_v2_sha256") — portable across Python versions:
  v1 hashed the text of ast.dump(). That text is NOT stable across interpreters: Python 3.12
  added FunctionDef.type_params, and Python 3.13 changed ast.dump() to omit None/[] fields by
  default (show_empty=False). The same unchanged source therefore hashed differently.
  v2 walks the AST itself and emits plain dict/list/str/int/float/bool/None, where a field that
  is MISSING, None or [] is omitted (so interpreter-specific empty optional fields cannot change
  the hash) while every NON-EMPTY field is kept (so real syntax/semantic changes still do). The
  result is serialised as sorted, compact JSON and hashed with SHA-256.

    python strategy_fingerprint.py --dashboard kalshi_dashboard.py --output step5_baseline_manifest.json
    python strategy_fingerprint.py --verify step5_baseline_manifest.json

The source is parsed, never imported and never executed. No network.
"""
import argparse
import ast
import hashlib
import json
import os
import sys

MANIFEST_SCHEMA_VERSION = 1
FINGERPRINT_FORMAT_VERSION = 2
FINGERPRINT_ALGORITHM = "canonical_ast_v2_sha256"
UNSUPPORTED_FORMAT = "UNSUPPORTED_FINGERPRINT_FORMAT"

STRATEGY_CONSTANTS = ("INTERVAL_MIN", "MAX_ENTRY_MIN", "LAST_STOP_MIN", "MIN_PRICE", "MIN_CONF",
                     "EDGE_THRESH", "ENTRY_COST_CENTS", "CHOP_MAX", "VOL_LOOKBACK_MIN", "VOL_MULT",
                     "BANKROLL", "ENTRY_MODE", "FEE_CENTS", "COINS")
# BANKROLL and ENTRY_MODE are deliberately mutable at runtime (the web controls change them).
# They are still fingerprinted from their baseline literals, and the live gate re-checks their
# RUNTIME values before every activation, latching the veto off if they drift (spec 60).
MUTABLE_CONSTANTS = ("BANKROLL", "ENTRY_MODE")

STRATEGY_FUNCTIONS = ("norm_cdf", "conf_side", "crossings", "evaluate", "kalshi_fee_cents",
                      "_size_fraction", "_contracts", "_limit_price", "log_call", "settle_calls")


class FingerprintError(Exception):
    pass


def canonical_json(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _strip_docstrings(node):
    """Docstrings are comments in code form: ignore them like any other comment."""
    for n in ast.walk(node):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Module)) and n.body:
            first = n.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) \
                    and isinstance(first.value.value, str):
                n.body = n.body[1:] or [ast.Pass()]
    return node


def canonicalize_ast(value):
    """Version-tolerant canonical form of an AST (fingerprint format v2).

    * AST node  -> {"_type": ClassName, field: canonical(value), ...} over node._fields only, so
      source positions (lineno/col_offset/end_*) — which are attributes, not fields — never enter.
    * A field that is missing on this interpreter, None, or [] is OMITTED: "absent" and "empty"
      are the same thing, whichever Python produced the tree.
    * Every non-empty field is kept, including fields newer Pythons add (e.g. a non-empty
      type_params changes the hash).
    Output uses only dict/list/str/int/float/bool/None."""
    if isinstance(value, ast.AST):
        out = {"_type": type(value).__name__}
        for field in value._fields:
            v = getattr(value, field, None)
            if v is None or (isinstance(v, list) and not v):
                continue
            out[field] = canonicalize_ast(v)
        return out
    if isinstance(value, list):
        return [canonicalize_ast(v) for v in value]
    if isinstance(value, bool) or value is None or isinstance(value, (str, int)):
        return value
    if isinstance(value, float):
        return {"_float": repr(value)}                 # exact, interpreter-independent spelling
    if isinstance(value, complex):
        return {"_complex": [repr(value.real), repr(value.imag)]}
    if isinstance(value, bytes):
        return {"_bytes": value.hex()}
    if isinstance(value, (tuple, frozenset)):
        items = [canonicalize_ast(v) for v in value]
        return {"_frozenset": sorted(items, key=canonical_json)} if isinstance(value, frozenset) else {"_tuple": items}
    if value is Ellipsis:
        return {"_ellipsis": True}
    raise FingerprintError(f"cannot canonicalize AST value of type {type(value).__name__}")


def function_hash(fn_node):
    """SHA-256 of the canonical JSON of a docstring-stripped function AST (format v2)."""
    return hashlib.sha256(canonical_json(canonicalize_ast(_strip_docstrings(fn_node))).encode()).hexdigest()


def extract(dashboard_path, constants=STRATEGY_CONSTANTS, functions=STRATEGY_FUNCTIONS):
    """Returns (constant values, {function: normalized-AST sha256}). Fails closed if a
    strategy constant is missing, non-literal, reassigned, or declared global anywhere."""
    if not os.path.exists(dashboard_path):
        raise FingerprintError(f"dashboard not found: {dashboard_path}")
    tree = ast.parse(open(dashboard_path).read())
    names = set(constants)
    values, top_ids = {}, {id(n) for n in tree.body}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) \
                and node.targets[0].id in names:
            nm = node.targets[0].id
            if nm in values:
                raise FingerprintError(f"strategy constant {nm} assigned more than once")
            try:
                values[nm] = ast.literal_eval(node.value)
            except ValueError:
                raise FingerprintError(f"strategy constant {nm} is not a literal")
    missing = [c for c in constants if c not in values]
    if missing:
        raise FingerprintError(f"strategy constants missing: {missing}")
    for node in ast.walk(tree):
        immutable = names - set(MUTABLE_CONSTANTS)
        if isinstance(node, ast.Global) and immutable & set(node.names):
            raise FingerprintError(f"strategy constants declared global: {sorted(immutable & set(node.names))}")
        targets = []
        if isinstance(node, ast.Assign) and id(node) not in top_ids:
            targets = node.targets
        elif isinstance(node, (ast.AugAssign, ast.AnnAssign)):
            targets = [node.target]
        for t in targets:
            for n in ast.walk(t):
                if isinstance(n, ast.Name) and n.id in names - set(MUTABLE_CONSTANTS):
                    raise FingerprintError(f"strategy constant {n.id} is reassigned at runtime")
    fns = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in functions:
            if node.name in fns:
                raise FingerprintError(f"strategy function {node.name} defined more than once")
            fns[node.name] = function_hash(node)
    missing = [f for f in functions if f not in fns]
    if missing:
        raise FingerprintError(f"strategy functions missing: {missing}")
    return values, fns


def strategy_config_hash(values):
    return hashlib.sha256(canonical_json(values).encode()).hexdigest()


def legacy_strategy_fingerprint(values, fns):
    return hashlib.sha256(canonical_json({"fingerprint_format_version": FINGERPRINT_FORMAT_VERSION,
                                          "fingerprint_algorithm": FINGERPRINT_ALGORITHM,
                                          "strategy_config_hash": strategy_config_hash(values),
                                          "strategy_function_hashes": fns}).encode()).hexdigest()


def build_manifest(dashboard_path):
    values, fns = extract(dashboard_path)
    import platform
    return {"manifest_schema_version": MANIFEST_SCHEMA_VERSION,
            "fingerprint_format_version": FINGERPRINT_FORMAT_VERSION,
            "fingerprint_algorithm": FINGERPRINT_ALGORITHM,
            "baseline_generated_python": platform.python_version(),     # diagnostic only, never identity
            "step5_dashboard_sha256": sha256_file(dashboard_path),
            "strategy_constants": list(STRATEGY_CONSTANTS), "strategy_functions": list(STRATEGY_FUNCTIONS),
            "strategy_config_values": values, "strategy_config_hash": strategy_config_hash(values),
            "strategy_function_hashes": fns, "runtime_mutable_constants": list(MUTABLE_CONSTANTS),
            "legacy_strategy_fingerprint": legacy_strategy_fingerprint(values, fns)}


def current_fingerprint(dashboard_path):
    values, fns = extract(dashboard_path)
    return legacy_strategy_fingerprint(values, fns), strategy_config_hash(values), values, fns


def verify(dashboard_path, manifest):
    """Returns (ok, problems). The whole-file hash MAY differ (Step 6 plumbing); the legacy
    strategy fingerprint may NOT."""
    problems = []
    if not isinstance(manifest, dict) or manifest.get("manifest_schema_version") != MANIFEST_SCHEMA_VERSION:
        return False, ["unsupported baseline manifest schema"]
    if manifest.get("fingerprint_format_version") != FINGERPRINT_FORMAT_VERSION \
            or manifest.get("fingerprint_algorithm") != FINGERPRINT_ALGORITHM:
        return False, [f"{UNSUPPORTED_FORMAT}: manifest format "
                       f"{manifest.get('fingerprint_format_version', 1)!r}/{manifest.get('fingerprint_algorithm')!r}, "
                       f"this code requires {FINGERPRINT_FORMAT_VERSION}/{FINGERPRINT_ALGORITHM}"]
    try:
        fp, cfg, values, fns = current_fingerprint(dashboard_path)
    except FingerprintError as e:
        return False, [str(e)]
    if fp != manifest.get("legacy_strategy_fingerprint"):
        problems.append("legacy strategy fingerprint differs from the Step 5 baseline")
    if cfg != manifest.get("strategy_config_hash"):
        changed = [k for k, v in values.items() if manifest.get("strategy_config_values", {}).get(k) != v]
        problems.append(f"strategy constants changed: {changed}")
    for name, h in (manifest.get("strategy_function_hashes") or {}).items():
        if fns.get(name) != h:
            problems.append(f"strategy function changed: {name}")
    return not problems, problems


def main(argv=None):
    a_ = argparse.ArgumentParser(description="Fingerprint / verify the legacy strategy (AST only).")
    a_.add_argument("--dashboard", default="kalshi_dashboard.py")
    a_.add_argument("--output", default=None, help="write a baseline manifest")
    a_.add_argument("--verify", default=None, help="verify the dashboard against a baseline manifest")
    a = a_.parse_args(argv)
    try:
        if a.verify:
            with open(a.verify) as f:
                man = json.load(f)
            ok, problems = verify(a.dashboard, man)
            print("legacy strategy fingerprint: " + ("MATCHES baseline" if ok else "MISMATCH"))
            for p in problems:
                print(f"  - {p}")
            return 0 if ok else 3
        man = build_manifest(a.dashboard)
        if a.output:
            with open(a.output, "w") as f:
                json.dump(man, f, indent=1, sort_keys=True)
        print(f"step5_dashboard_sha256      {man['step5_dashboard_sha256']}")
        print(f"strategy_config_hash        {man['strategy_config_hash']}")
        print(f"legacy_strategy_fingerprint {man['legacy_strategy_fingerprint']}")
    except (FingerprintError, OSError, ValueError) as e:
        print(f"FINGERPRINT FAILED: {e}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
