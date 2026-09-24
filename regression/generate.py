#!/usr/bin/env python3
"""
Generate regression/strategy_cases.json from the CURRENT code.

    py -m regression.generate --check                    # recompute and compare (no write); exit 3 on drift
    py -m regression.generate --write --i-intend-to-change-the-baseline

The stored expectations are the behavioural contract of the legacy strategy. Rewriting them
ACCEPTS a behaviour change, so --write refuses to overwrite an existing file without the
explicit flag; record the reason in docs/BASELINE.md when you do.
"""
import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from regression import cases, harness          # noqa: E402

FIXTURE_PATH = os.path.join(HERE, "regression", "strategy_cases.json")


def _check_intent(case, out):
    """The case must exercise the branch it was designed for (guards against a mis-built fixture)."""
    kind, intent, cid = case["kind"], case.get("intent"), case["id"]
    if kind == "evaluate":
        r = out["result"]
        got = "RAISES" if "raises" in r else r.get("reason")
        assert got == intent, f"{cid}: intended {intent}, legacy code gave {got}"
        if got == "ENTER":
            assert r["signal"] is True and out["signal_decision"]["decision"] == "CALL", cid
        else:
            assert out["signal_decision"]["decision"] == "NO_CALL", cid
    elif kind == "poller":
        calls = [e for e in out["events"] if e[0] == "on_call"]
        want = {"P01_cycle_gate_inactive": 1, "P02_second_cycle_same_ticker": 1, "P03_perp_veto_blocks": 0,
                "P04_already_alerted": 0, "P05_paused": 0}[cid]
        assert len(calls) == want, f"{cid}: {len(calls)} calls, wanted {want}"
    elif kind == "gate_inactive":
        assert out["status"] == "INACTIVE_NO_PROMOTION" and out["active"] is False, cid


def build():
    out = []
    for case in cases.all_cases():
        case = harness.normalise(case)            # run on EXACTLY the input that is stored (key order matters:
        exp = harness.run_case(case)              # e.g. settle_calls processes _call_pending in dict order)
        _check_intent(case, exp)
        out.append(dict(case, expected=exp))
    return {"fixture": "kalshi_strategy_regression", "fixture_schema_version": harness.FIXTURE_SCHEMA_VERSION,
            "frozen_utc": harness.FROZEN_UTC.isoformat(),
            "note": "Expected outputs of the UNMODIFIED legacy code. Do not edit by hand.",
            "cases": out}


def dumps(doc):
    return json.dumps(doc, indent=1, sort_keys=True, ensure_ascii=True, allow_nan=True) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--check", action="store_true")
    g.add_argument("--write", action="store_true")
    ap.add_argument("--i-intend-to-change-the-baseline", action="store_true", dest="intend")
    a = ap.parse_args(argv)
    if a.write:
        if os.path.exists(FIXTURE_PATH) and not a.intend:
            print("Refusing to overwrite the behavioural contract. Re-run with "
                  "--i-intend-to-change-the-baseline if a strategy change is intended.", file=sys.stderr)
            return 2
        doc = build()
        with open(FIXTURE_PATH, "w", encoding="utf-8", newline="\n") as f:
            f.write(dumps(doc))
        print(f"wrote {FIXTURE_PATH} ({len(doc['cases'])} cases)")
        return 0
    # --check: re-run the STORED inputs with the current code (exactly what test_stage16 does)
    with open(FIXTURE_PATH, encoding="utf-8") as f:
        stored = json.load(f)
    diffs = []
    for case in stored["cases"]:
        diffs += [f"{case['id']}{d}" for d in harness.compare(case["expected"], harness.run_case(case))]
    print(f"regression fixtures: {'MATCH' if not diffs else 'DIFFER'} ({len(stored['cases'])} cases)")
    for d in diffs[:50]:
        print("  - " + d)
    return 0 if not diffs else 3


if __name__ == "__main__":
    sys.exit(main())
