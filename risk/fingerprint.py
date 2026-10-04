"""
Risk architecture fingerprint (config/risk_baseline.json), separate from the execution fingerprint.

    py -m risk.fingerprint --verify
    py -m risk.fingerprint --write --i-intend-to-change-the-risk-baseline   (record OLD / NEW / WHY in
                                                                           docs/RISK_ARCHITECTURE.md)

Covers the RiskCandidate / RiskSnapshot / RiskPolicy / RiskDecision / RiskApproval schemas and versions, the evaluation
order, hard-veto and size-cap codes, the worst-case-loss formula, breaker states / transitions / reset rules, the
approval binding, the persistence schema (DDL hash, event kinds), fault points, health / execution-state vocabularies,
the safety invariants, and the canonical AST of every risk module. Never any database content or runtime state.
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
from execution.risk import APPROVAL_BINDING_VERSION, BINDING_FIELDS  # noqa: E402
from kalshi_core.execution import LIVE_EXECUTION_AVAILABLE  # noqa: E402
from risk import RISK_ARCHITECTURE_VERSION  # noqa: E402
from risk import breakers as B  # noqa: E402
from risk import decision as D  # noqa: E402
from risk import evaluate as EV  # noqa: E402
from risk import policy as P  # noqa: E402
from risk import reasons as R  # noqa: E402
from risk import store as ST  # noqa: E402
from risk import types as T  # noqa: E402
from risk.faults import RISK_FAULT_POINTS  # noqa: E402

PKG = os.path.join(HERE, "risk")
BASELINE_PATH = os.path.join(HERE, "config", "risk_baseline.json")
FORMAT = "risk_fingerprint_v1/canonical_ast_v2_sha256"
INVARIANTS = (
    "approved_contracts >= 0", "approved_contracts <= requested_contracts",
    "approved_max_limit_price <= requested_max_limit_price", "VETO -> approved_contracts == 0",
    "REDUCE -> 0 < approved_contracts < requested_contracts", "APPROVE -> approved_contracts == requested_contracts",
    "NO_CALL -> VETO (no approval ever exists for NO_CALL)", "any hard veto -> VETO (never a reduction)",
    "side, signal, probabilities, EV and fingerprints are provenance: never changed by risk",
    "approved size = min(requested, every cap) floored to the contract quantum",
    "UNKNOWN is never zero and never safe", "breakers latch and survive restart; reset only by the documented rule",
    "an approval is bound, finite, single-use and verified by execution", "the risk package has no network path",
)


def _fields(cls):
    return [f for f in cls.__dataclass_fields__]


def _ast_hash(p):
    with open(p, encoding="utf-8") as f:
        tree = sf._strip_docstrings(ast.parse(f.read()))
    return hashlib.sha256(sf.canonical_json(sf.canonicalize_ast(tree)).encode()).hexdigest()


def module_hashes(pkg_dir=PKG):
    return {f"risk/{n}": _ast_hash(os.path.join(pkg_dir, n)) for n in sorted(os.listdir(pkg_dir)) if n.endswith(".py")}


def semantics():
    return {
        "architecture": RISK_ARCHITECTURE_VERSION,
        "versions": {"candidate": T.RISK_CANDIDATE_SCHEMA_VERSION, "snapshot": T.RISK_SNAPSHOT_SCHEMA_VERSION,
                     "snapshot_hash": T.SNAPSHOT_HASH_VERSION, "policy": P.RISK_POLICY_SCHEMA_VERSION,
                     "policy_fingerprint": P.POLICY_FINGERPRINT_VERSION, "rules": R.RISK_RULES_VERSION,
                     "decision": D.RISK_DECISION_SCHEMA_VERSION, "decision_id": D.DECISION_ID_VERSION,
                     "approval": D.RISK_APPROVAL_SCHEMA_VERSION, "approval_binding": APPROVAL_BINDING_VERSION,
                     "store": ST.RISK_STORE_SCHEMA_VERSION, "breakers": B.BREAKER_SCHEMA_VERSION},
        "schemas": {"candidate": _fields(T.RiskCandidate), "snapshot": _fields(T.RiskSnapshot),
                    "policy": _fields(P.RiskPolicy), "decision": _fields(D.RiskDecision),
                    "approval": _fields(D.RiskApproval), "approval_binding_fields": list(BINDING_FIELDS)},
        "vocabularies": {"health": list(T.HEALTH), "signal_status": list(T.SIGNAL_STATUSES),
                         "exec_states": list(T.EXEC_STATES), "decisions": list(D.DECISIONS)},
        "evaluation_order": list(R.EVALUATION_ORDER),
        "hard_veto_codes": list(R.HARD_VETO_CODES), "cap_codes": list(R.CAP_CODES),
        "worst_case_loss": "q * (approved_max_limit_price + extra_reserve_per_contract [+ unknown reserves]) + "
                           "estimated_fee_total + estimated_slippage_total",
        "contract_quantum": str(EV.CONTRACT_QUANTUM),
        "breakers": {"types": list(B.BREAKER_TYPES), "states": list(B.BREAKER_STATES),
                     "transitions": sorted([list(k) + [v] for k, v in B.BREAKER_TRANSITIONS.items()]),
                     "reset_rules": dict(B.RESET_RULE)},
        "store": {"ddl_sha256": hashlib.sha256(ST.RISK_DDL.encode()).hexdigest(), "event_kinds": list(ST.RISK_EVENT_KINDS)},
        "fault_points": list(RISK_FAULT_POINTS),
        "invariants": list(INVARIANTS),
        "live_execution_available": LIVE_EXECUTION_AVAILABLE,
    }


def build(pkg_dir=PKG):
    sem = semantics()
    mods = module_hashes(pkg_dir)
    body = {"format": FORMAT, "semantics": sem, "modules": mods}
    fp = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"risk_fingerprint": fp, "format": FORMAT,
            "semantics_sha256": hashlib.sha256(json.dumps(sem, sort_keys=True).encode()).hexdigest(),
            "modules": mods, "semantics": sem}


def verify(pkg_dir=PKG, baseline_path=BASELINE_PATH):
    try:
        with open(baseline_path, encoding="utf-8") as f:
            stored = json.load(f)
    except (OSError, ValueError) as e:
        return False, [f"no risk baseline: {e}"]
    cur = build(pkg_dir)
    if stored.get("format") != FORMAT:
        return False, [f"unsupported baseline format {stored.get('format')!r}"]
    problems = [f"{k} changed" for k in sorted(set(stored.get("modules", {})) | set(cur["modules"]))
                if stored.get("modules", {}).get(k) != cur["modules"].get(k)]
    if stored.get("semantics_sha256") != cur["semantics_sha256"]:
        problems.append("risk semantics changed")
    if stored.get("risk_fingerprint") != cur["risk_fingerprint"] and not problems:
        problems.append("risk fingerprint changed")
    return not problems, problems


def main(argv=None):
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--verify", action="store_true")
    g.add_argument("--write", action="store_true")
    ap.add_argument("--i-intend-to-change-the-risk-baseline", action="store_true", dest="intend")
    a = ap.parse_args(argv)
    if a.write:
        if not a.intend:
            print("Refusing: pass --i-intend-to-change-the-risk-baseline and record why in docs/RISK_ARCHITECTURE.md.",
                  file=sys.stderr)
            return 2
        with open(BASELINE_PATH, "w", encoding="utf-8", newline="\n") as f:
            json.dump(build(), f, indent=1, sort_keys=True)
            f.write("\n")
        print(f"wrote {BASELINE_PATH}")
        return 0
    ok, problems = verify()
    print("risk fingerprint: " + ("MATCHES" if ok else "MISMATCH"))
    if ok:
        print(f"  {build()['risk_fingerprint']}")
    for p in problems:
        print(f"  - {p}")
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
