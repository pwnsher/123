#!/usr/bin/env python3
"""
Mutation tests for the Step-4 perp pipeline's causal / semantic rules (offline, no network).

    py scripts/mutation_test_perp_data.py [--out analysis_output/perp_data_mutation_results.json] [--only P1,P5]

Each mutation breaks ONE rule in a TEMPORARY copy of the repository and runs the relevant Stage-20 tests there;
CAUGHT = those tests fail. Every mutation runs twice: as-is (the perp engine's causality guards active) and
with the guards disabled, so the tests themselves must catch it. Controls: the unmutated copy and the
guards-disabled-only copy must PASS. The working tree is never modified.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENG = "perp_data/features/engine.py"
GUARDS = [(ENG, "if self.max_receive is not None and ev.receive_ts_ms < self.max_receive:", "if False:", 1),
          (ENG, "if self.max_receive is not None and t < self.max_receive:", "if False:", 1)]
AVAIL = "for e in sorted((e for e in events if available(e, t)), key=event_key):"

MUTATIONS = [
    ("P1", "receive timestamps ignored (perp availability decided by event time)",
     [(ENG, AVAIL, "for e in sorted((e for e in events if (e.event_ts_ms if e.event_ts_ms is not None else e.receive_ts_ms) <= t), key=event_key):", 1)],
     ["causality", "leakage"]),
    ("P2", "future liquidations become visible",
     [(ENG, AVAIL, "for e in sorted((e for e in events if available(e, t) or getattr(e, 'event_type', None) == T.LIQUIDATION), key=event_key):", 1)],
     ["leakage", "liquidations"]),
    ("P3", "future open-interest updates become visible",
     [(ENG, AVAIL, "for e in sorted((e for e in events if available(e, t) or getattr(e, 'event_type', None) == T.OPEN_INTEREST), key=event_key):", 1)],
     ["leakage", "oi"]),
    ("P4", "backfilled trades become historically visible (receive time := event time)",
     [("perp_data/sources/base.py", "return PerpEvent(source=self.source, receive_ts_ms=ctx.receive_ts_ms,",
       "return PerpEvent(source=self.source, receive_ts_ms=(kw['event_ts_ms'] if kw.get('mode') == IngestMode.BACKFILLED "
       "and kw.get('event_ts_ms') else ctx.receive_ts_ms),", 1)],
     ["backfill", "trades"]),
    ("P5", "missing liquidation volume becomes zero",
     [(ENG, "row.set(n, None, st)                       # NOT observed -> never 0", "row.set(n, 0.0, S.READY)", 1)],
     ["liquidations"]),
    ("P6", "maker side interpreted as taker side (Binance aggTrade m)",
     [("perp_data/sources/binance.py", 'aggressor = "sell" if m else "buy"', 'aggressor = "buy" if m else "sell"', 1)],
     ["trades", "flow"]),
    ("P7", "raw contract OI compared across venues without normalization",
     [(ENG, 'self.oi[src].add(s, r, q, p["oi_coin"])', 'self.oi[src].add(s, r, q, p["oi_native"])', 1)],
     ["oi", "cross"]),
    ("P8", "settlement labels enter the perp features (official result merged into the row)",
     [("perp_data/dataset.py", "            r4 = e4[asset].features_at(t, m)\n",
       "            r4 = e4[asset].features_at(t, m)\n            r4.values = dict(r4.values, **_LEAK.get(ticker, {}))\n", 1),
      ("perp_data/dataset.py", "    for ev in events:\n        emit_until(ev.receive_ts_ms)",
       "    _LEAK = {e.payload['ticker']: {'official_result': e.payload['result']} for e in s3_events "
       "if e.event_type == EventType.RESOLUTION}\n    for ev in events:\n        emit_until(ev.receive_ts_ms)", 1)],
     ["dataset", "labels"]),
    ("P9", "the existing perp veto imports Step-4 features",
     [("perp_live.py", None, "\nimport perp_data.features.engine  # noqa: E402,F401\n", 1)],
     ["veto", "isolation"]),
]
IGNORE = shutil.ignore_patterns(".git", "__pycache__", "analysis_output", "market_data_sessions", "settlement_data",
                                ".venv", "venv*", "*.zip", ".mypy_cache", ".ruff_cache")


def apply(root, edits):
    for rel, old, new, count in edits:
        p = os.path.join(root, rel)
        s = open(p, encoding="utf-8").read()
        if old is None:                                   # append
            open(p, "w", encoding="utf-8", newline="\n").write(s + new)
            continue
        n = s.count(old)
        if n != count:
            raise RuntimeError(f"mutation anchor not found as expected in {rel}: {old[:60]!r} ({n} != {count})")
        open(p, "w", encoding="utf-8", newline="\n").write(s.replace(old, new))


def run_variant(edits, tests, timeout=1800):
    tmp = tempfile.mkdtemp(prefix="mut-perp-")
    root = os.path.join(tmp, "repo")
    try:
        shutil.copytree(REPO, root, ignore=IGNORE)
        apply(root, edits)
        t0 = time.time()
        p = subprocess.run([sys.executable, "test_stage20.py", "--only", ",".join(tests)], cwd=root, capture_output=True,
                           text=True, timeout=timeout, env=dict(os.environ, KALSHI_MASTER_TEST_RUN="1"))
        fails = [ln for ln in p.stdout.splitlines() if ln.startswith("FAIL")]
        return {"caught": p.returncode != 0, "returncode": p.returncode, "seconds": round(time.time() - t0, 1),
                "failed_test": fails[0][:300] if fails else None,
                "passed_tests": [ln[6:70] for ln in p.stdout.splitlines() if ln.startswith("PASS")]}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(REPO, "analysis_output", "perp_data_mutation_results.json"))
    ap.add_argument("--only", default=None)
    a = ap.parse_args(argv)
    only = set(a.only.split(",")) if a.only else None
    res = {"note": "Each mutation breaks one rule in a temporary copy; CAUGHT = the Stage-20 tests fail.",
           "controls": {}, "mutations": []}
    all_tests = sorted({t for m in MUTATIONS for t in m[3]})
    res["controls"]["unmutated"] = run_variant([], all_tests)
    res["controls"]["guards_disabled_only"] = run_variant(GUARDS, [t for t in all_tests if t != "causality"])
    for mid, desc, edits, tests in MUTATIONS:
        if only and mid not in only:
            continue
        r = {"id": mid, "mutation": desc, "tests": tests, "as_is": run_variant(edits, tests),
             "guards_disabled": run_variant(edits + GUARDS, [t for t in tests if t != "causality"] or tests)}
        res["mutations"].append(r)
        print(f"{mid} {desc}\n   as-is: {'CAUGHT' if r['as_is']['caught'] else 'NOT CAUGHT'}  ({r['as_is']['failed_test']})"
              f"\n   guards disabled: {'CAUGHT' if r['guards_disabled']['caught'] else 'NOT CAUGHT'}  "
              f"({r['guards_disabled']['failed_test']})", flush=True)
    c = res["controls"]
    ok = (not c["unmutated"]["caught"] and not c["guards_disabled_only"]["caught"]
          and all(m["as_is"]["caught"] and m["guards_disabled"]["caught"] for m in res["mutations"]))
    res["all_caught_and_controls_pass"] = ok
    print(f"controls: unmutated {'PASS' if not c['unmutated']['caught'] else 'FAIL ' + str(c['unmutated']['failed_test'])}, "
          f"guards-disabled-only {'PASS' if not c['guards_disabled_only']['caught'] else 'FAIL ' + str(c['guards_disabled_only']['failed_test'])}")
    print("RESULT: " + ("every mutation caught; controls pass" if ok else "NOT every mutation caught / a control failed"))
    if a.out:
        os.makedirs(os.path.dirname(a.out), exist_ok=True)
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            json.dump(res, f, indent=1)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
