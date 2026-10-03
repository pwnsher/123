"""
Execution architecture fingerprint (config/execution_baseline.json).

    py -m execution.fingerprint --verify
    py -m execution.fingerprint --write --i-intend-to-change-the-execution-baseline   (record OLD / NEW / WHY in
                                                                                    docs/EXECUTION_ARCHITECTURE.md)

It pins the deterministic execution SEMANTICS (OrderIntent schema, the transition graph, the execution-key and
client-order-id canonicalization, the journal schema / DDL, the reconciliation rules and verdicts, the adapter
protocol, the paper venue protocol, the safety invariants) and the canonical AST of every execution module (comments
/ docstrings / whitespace ignored, like the other layers). It never includes a database's contents. It also records
that live execution is unavailable.
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

import strategy_fingerprint as sf  # noqa: E402
from execution import EXECUTION_ARCHITECTURE_VERSION  # noqa: E402
from execution.adapter import ADAPTER_METHODS, ADAPTER_PROTOCOL_VERSION, ORDER_STATUSES  # noqa: E402
from execution.audit import AUDIT_SCHEMA_VERSION  # noqa: E402
from execution.faults import FAULT_POINTS  # noqa: E402
from execution.identity import (CLIENT_ORDER_ID_HEX, CLIENT_ORDER_ID_PREFIX, CLIENT_ORDER_ID_VERSION,  # noqa: E402
                                EXECUTION_KEY_FIELDS, EXECUTION_KEY_VERSION)
from execution.intent import ORDER_INTENT_SCHEMA_VERSION, SIDES, TIME_IN_FORCE, OrderIntent  # noqa: E402
from execution.invariants import INVARIANTS  # noqa: E402
from execution.journal import DDL, EVENT_COLUMNS, EVENT_KINDS, JOURNAL_SCHEMA_VERSION  # noqa: E402
from execution.ledger import LEDGER_VERSION  # noqa: E402
from execution.paper import CANCEL_MODES, PAPER_PROTOCOL_VERSION, SUBMIT_MODES  # noqa: E402
from execution.reconcile import RECONCILIATION_RULES, RECONCILIATION_VERSION, VERDICTS  # noqa: E402
from execution.states import transition_table  # noqa: E402
from kalshi_core.execution import LIVE_EXECUTION_AVAILABLE  # noqa: E402

PKG = os.path.join(HERE, "execution")
BASELINE_PATH = os.path.join(HERE, "config", "execution_baseline.json")
FORMAT = "execution_fingerprint_v1/canonical_ast_v2_sha256"


def _ast_hash(p):
    with open(p, encoding="utf-8") as f:
        tree = sf._strip_docstrings(ast.parse(f.read()))
    return hashlib.sha256(sf.canonical_json(sf.canonicalize_ast(tree)).encode()).hexdigest()


def module_hashes(pkg_dir=PKG):
    return {f"execution/{n}": _ast_hash(os.path.join(pkg_dir, n)) for n in sorted(os.listdir(pkg_dir))
            if n.endswith(".py")}


def semantics():
    return {
        "architecture": EXECUTION_ARCHITECTURE_VERSION,
        "order_intent": {"schema_version": ORDER_INTENT_SCHEMA_VERSION,
                         "fields": [f for f in OrderIntent.__dataclass_fields__], "sides": list(SIDES),
                         "time_in_force": list(TIME_IN_FORCE)},
        "transition_graph": transition_table(),
        "execution_key": {"version": EXECUTION_KEY_VERSION, "fields": list(EXECUTION_KEY_FIELDS),
                          "serialization": "json sort_keys separators(',',':') ascii; Decimal canonical text"},
        "client_order_id": {"version": CLIENT_ORDER_ID_VERSION, "prefix": CLIENT_ORDER_ID_PREFIX,
                            "hex_chars": CLIENT_ORDER_ID_HEX},
        "journal": {"schema_version": JOURNAL_SCHEMA_VERSION, "event_kinds": list(EVENT_KINDS),
                    "event_columns": list(EVENT_COLUMNS), "ddl_sha256": hashlib.sha256(DDL.encode()).hexdigest()},
        "reconciliation": {"version": RECONCILIATION_VERSION, "verdicts": list(VERDICTS),
                           "rules": list(RECONCILIATION_RULES)},
        "adapter_protocol": {"version": ADAPTER_PROTOCOL_VERSION, "methods": list(ADAPTER_METHODS),
                             "order_statuses": list(ORDER_STATUSES)},
        "paper_venue": {"version": PAPER_PROTOCOL_VERSION, "submit_modes": list(SUBMIT_MODES),
                        "cancel_modes": list(CANCEL_MODES)},
        "ledger": LEDGER_VERSION,
        "audit_schema_version": AUDIT_SCHEMA_VERSION,
        "invariants": list(INVARIANTS),
        "fault_points": list(FAULT_POINTS),
        "live_execution_available": LIVE_EXECUTION_AVAILABLE,
    }


def build(pkg_dir=PKG):
    sem = semantics()
    mods = module_hashes(pkg_dir)
    body = {"format": FORMAT, "semantics": sem, "modules": mods}
    fp = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"execution_fingerprint": fp, "format": FORMAT,
            "semantics_sha256": hashlib.sha256(json.dumps(sem, sort_keys=True).encode()).hexdigest(),
            "modules": mods, "semantics": sem}


def verify(pkg_dir=PKG, baseline_path=BASELINE_PATH):
    try:
        with open(baseline_path, encoding="utf-8") as f:
            stored = json.load(f)
    except (OSError, ValueError) as e:
        return False, [f"no execution baseline: {e}"]
    cur = build(pkg_dir)
    if stored.get("format") != FORMAT:
        return False, [f"unsupported baseline format {stored.get('format')!r}"]
    problems = []
    for k in sorted(set(stored.get("modules", {})) | set(cur["modules"])):
        if stored.get("modules", {}).get(k) != cur["modules"].get(k):
            problems.append(f"{k} changed")
    if stored.get("semantics_sha256") != cur["semantics_sha256"]:
        problems.append("execution semantics changed")
    if stored.get("execution_fingerprint") != cur["execution_fingerprint"] and not problems:
        problems.append("execution fingerprint changed")
    return not problems, problems


def main(argv=None):
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--verify", action="store_true")
    g.add_argument("--write", action="store_true")
    ap.add_argument("--i-intend-to-change-the-execution-baseline", action="store_true", dest="intend")
    a = ap.parse_args(argv)
    if a.write:
        if not a.intend:
            print("Refusing: pass --i-intend-to-change-the-execution-baseline and record why in "
                  "docs/EXECUTION_ARCHITECTURE.md.", file=sys.stderr)
            return 2
        with open(BASELINE_PATH, "w", encoding="utf-8", newline="\n") as f:
            json.dump(build(), f, indent=1, sort_keys=True)
            f.write("\n")
        print(f"wrote {BASELINE_PATH}")
        return 0
    ok, problems = verify()
    print("execution fingerprint: " + ("MATCHES" if ok else "MISMATCH"))
    if ok:
        print(f"  {build()['execution_fingerprint']}")
    for p in problems:
        print(f"  - {p}")
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
