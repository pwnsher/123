#!/usr/bin/env python3
"""
Mutation tests for the Step-5 microstructure pipeline's causal / semantic rules (offline, no network).

    py scripts/mutation_test_microstructure.py [--out analysis_output/microstructure_mutation_results.json] [--only M1,M7]

Each mutation breaks ONE rule in a TEMPORARY copy of the repository and runs the relevant Stage-21 tests there;
CAUGHT = those tests fail. Every mutation runs twice: as-is and with the micro engine's causality guards disabled,
so the tests themselves (not only the guards) must catch it. Controls: the unmutated copy and the guards-disabled-only
copy must PASS. The working tree is never modified.
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
ENG = "microstructure/features/engine.py"
REC = "microstructure/reconstruction.py"
DS = "microstructure/dataset.py"
GUARDS = [(ENG, "if self.max_receive is not None and ev.receive_ts_ms < self.max_receive:", "if False:", 1),
          (ENG, "if self.max_receive is not None and t < self.max_receive:", "if False:", 1)]
LOOP = ("    for ev in events:\n        emit_until(ev.receive_ts_ms)\n        fam = getattr(ev, \"family\", None)\n"
        "        if fam == \"micro\":\n            for eng in e5.values():\n                eng.ingest(ev)\n            continue\n")
LOOP_LEAK = ("    for ev in events:\n        fam = getattr(ev, \"family\", None)\n        if fam == \"micro\":\n"
             "            for eng in e5.values():\n                eng.ingest(ev)\n        emit_until(ev.receive_ts_ms)\n"
             "        if fam == \"micro\":\n            continue\n")

MUTATIONS = [
    ("M1", "event time used instead of receive time (availability decided by the venue's event timestamp)",
     [(ENG, "for e in sorted((e for e in events if available(e, t)), key=event_key):",
       "for e in sorted((e for e in events if (e.event_ts_ms if e.event_ts_ms is not None else e.receive_ts_ms) <= t), key=event_key):", 1)],
     ["causality"]),
    ("M2", "a future book update becomes visible (ingested before the checkpoint is emitted)",
     [(DS, LOOP, LOOP_LEAK, 1)] + GUARDS[1:], ["dataset"]),
    ("M3", "sequence gaps ignored (chain / prevSeqId / pu checks disabled)",
     [(REC, "return prev is not None and uid is not None and uid != prev + 1", "return False", 1),
      (REC, 'if tr.policy == "prev_chain" and prev is not None and prev != tr.update_id:', "if False:", 1),
      (REC, "if pu != tr.update_id:", "if False:", 1)],
     ["sequence", "faults"]),
    ("M4", "a resnapshot retroactively repairs history (the old valid interval is re-opened)",
     [(REC, "        tr.intervals.append([ev.receive_ts_ms, None])\n",
       "        if tr.intervals:\n            tr.intervals[-1][1] = None\n        else:\n            tr.intervals.append([ev.receive_ts_ms, None])\n", 1)],
     ["retro", "states"]),
    ("M5", "all removed liquidity labelled cancellation (trades not subtracted)",
     [(ENG, "max(0.0, sz(s.window(s.c_brem, t - _ms(w), t)) - sell_v)", "max(0.0, sz(s.window(s.c_brem, t - _ms(w), t)) - 0.0)", 1),
      (ENG, "max(0.0, sz(s.window(s.c_arem, t - _ms(w), t)) - buy_v)", "max(0.0, sz(s.window(s.c_arem, t - _ms(w), t)) - 0.0)", 1)],
     ["ofi"]),
    ("M6", "future price impact leaks into the features (post-event labels merged into the row)",
     [(DS, "    ds.provenance = _provenance(step3, perp, micro, assets,",
       "    for _r, _lab in zip(ds.rows, ds.micro_labels):\n"
       "        _r[\"micro_values\"].update({k: v for k, v in _lab.items() if k.startswith(\"micro_label\")})\n"
       "    ds.provenance = _provenance(step3, perp, micro, assets,", 1)],
     ["dataset"]),
    ("M7", "Kalshi YES / NO conversion reversed (a NO bid used as the YES ask without 100 - p)",
     [("microstructure/kalshi.py", 'return None if no_bid_cents is None else round(100.0 - _cents(no_bid_cents, "no_bid_cents"), 6)',
       'return None if no_bid_cents is None else round(_cents(no_bid_cents, "no_bid_cents"), 6)', 1)],
     ["kalshi"]),
    ("M8", "a missing book becomes zero imbalance",
     [(ENG, 'row.set(f"{p}.imbalance_{lv}", _imb(B, A) if lvl == S.READY else None, lvl)',
       'row.set(f"{p}.imbalance_{lv}", _imb(B, A) if lvl == S.READY else 0.0, S.READY)', 1)],
     ["missing"]),
    ("M9", "a crossed book is accepted as healthy",
     [(REC, "        if tr.book.crossed():", "        if False and tr.book.crossed():", 1)],
     ["states", "faults"]),
    ("M10", "a Step-5 feature is imported by production",
     [("run_local.py", None, "\nimport microstructure.features.engine  # noqa: E402,F401\n", 1)],
     ["isolation"]),
    ("M11", "a Step-5 feature is imported by the existing perp veto",
     [("perp_live.py", None, "\nimport microstructure.features.engine  # noqa: E402,F401\n", 1)],
     ["veto", "isolation"]),
]
IGNORE = shutil.ignore_patterns(".git", "__pycache__", "analysis_output", "market_data_sessions", "settlement_data",
                                ".venv", "venv*", "*.zip", ".mypy_cache", ".ruff_cache")


def apply(root, edits):
    for rel, old, new, count in edits:
        p = os.path.join(root, rel)
        s = open(p, encoding="utf-8").read()
        if old is None:
            open(p, "w", encoding="utf-8", newline="\n").write(s + new)
            continue
        n = s.count(old)
        if n != count:
            raise RuntimeError(f"mutation anchor not found as expected in {rel}: {old[:60]!r} ({n} != {count})")
        open(p, "w", encoding="utf-8", newline="\n").write(s.replace(old, new))


def run_variant(edits, tests, timeout=1800):
    tmp = tempfile.mkdtemp(prefix="mut-micro-")
    root = os.path.join(tmp, "repo")
    try:
        shutil.copytree(REPO, root, ignore=IGNORE)
        apply(root, edits)
        t0 = time.time()
        p = subprocess.run([sys.executable, "test_stage21.py", "--only", ",".join(tests)], cwd=root, capture_output=True,
                           text=True, timeout=timeout, env=dict(os.environ, KALSHI_MASTER_TEST_RUN="1"))
        fails = [ln for ln in p.stdout.splitlines() if ln.startswith("FAIL")]
        return {"caught": p.returncode != 0, "returncode": p.returncode, "seconds": round(time.time() - t0, 1),
                "failed_test": fails[0][:300] if fails else None,
                "passed_tests": [ln[6:70] for ln in p.stdout.splitlines() if ln.startswith("PASS")]}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(REPO, "analysis_output", "microstructure_mutation_results.json"))
    ap.add_argument("--only", default=None)
    a = ap.parse_args(argv)
    only = set(a.only.split(",")) if a.only else None
    res = {"note": "Each mutation breaks one rule in a temporary copy; CAUGHT = the Stage-21 tests fail.",
           "controls": {}, "mutations": []}
    all_tests = sorted({t for m in MUTATIONS for t in m[3]})
    res["controls"]["unmutated"] = run_variant([], all_tests)
    res["controls"]["guards_disabled_only"] = run_variant(GUARDS, [t for t in all_tests if t != "causality"])
    for mid, desc, edits, tests in MUTATIONS:
        if only and mid not in only:
            continue
        g = [x for x in GUARDS if x not in edits]
        r = {"id": mid, "mutation": desc, "tests": tests, "as_is": run_variant(edits, tests),
             "guards_disabled": run_variant(edits + g, [t for t in tests if t != "causality"] or tests)}
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
