#!/usr/bin/env python3
"""
Mutation tests for the CAUSAL rules of the market-data research pipeline (offline, no network).

    py scripts/mutation_test_market_data.py [--out analysis_output/market_data_mutation_results.json] [--only M1,M3]

Each mutation deliberately breaks one causal rule in a TEMPORARY copy of the repository and runs the
relevant Stage-19 tests there. A mutation is CAUGHT when those tests fail. Every mutation is run twice:
    as-is            the engine's causality guards (availability-order ingest, "no features before the
                     last ingested receive time") are active - they often stop the leak on their own;
    guards disabled  the same mutation with both guards switched off, so the leakage / alignment /
                     equivalence TESTS themselves must catch it (defence in depth is tested layer by layer).
A control run with only the guards disabled must PASS (so a catch is due to the mutation, not the guard edit).
The working tree is never modified.
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

ENGINE = "market_data/features/engine.py"
GUARDS = [(ENGINE, "if self.max_receive is not None and ev.receive_ts_ms < self.max_receive:", "if False:", 1),
          (ENGINE, "if self.max_receive is not None and t < self.max_receive:", "if False:", 2)]

MUTATIONS = [
    ("M1", "receive timestamps ignored (availability decided by event time)",
     [("market_data/alignment.py", "return event.receive_ts_ms <= t_ms",
       "return (event.event_ts_ms if event.event_ts_ms is not None else event.receive_ts_ms) <= t_ms", 1)],
     ["alignment", "leakage", "cross", "windows"]),
    ("M2", "future visible (dataset checkpoints emitted after 5 s of later data were ingested)",
     [("market_data/features/dataset.py", "sched[ci][0] < limit_exclusive)", "sched[ci][0] + 5000 < limit_exclusive)", 1)],
     ["equivalence", "leakage"]),
    ("M3", "missing values become zeros",
     [(ENGINE, "        else:\n            value = None\n", "        else:\n            value = 0.0\n", 1)],
     ["missing"]),
    ("M4", "backfilled data treated as live (visible from its event time instead of its retrieval time)",
     [("market_data/sources/base.py", "from market_data.types import MarketEvent, ParseFailure, ParseResult",
       "from market_data.types import IngestMode, MarketEvent, ParseFailure, ParseResult", 1),
      ("market_data/sources/base.py", "return MarketEvent(source=self.source, receive_ts_ms=ctx.receive_ts_ms,",
       "return MarketEvent(source=self.source, receive_ts_ms=(kw['event_ts_ms'] if kw.get('mode') == "
       "IngestMode.BACKFILLED and kw.get('event_ts_ms') else ctx.receive_ts_ms),", 1)],
     ["backfill", "coinbase", "kraken", "kalshi"]),
    ("M5", "rolling windows include the future (upper bound T + 5 s)",
     [("market_data/features/buffers.py", "b = bisect.bisect_right(self.times, hi_inclusive)",
       "b = bisect.bisect_right(self.times, hi_inclusive + 5000)", 1)],
     ["windows"]),
    ("M6", "cross-exchange quotes aligned by event time (a delayed venue quote used before it arrived)",
     [(ENGINE, "for e in sorted((e for e in events if available(e, t)),",
       "for e in sorted((e for e in events if available(e, t) or (e.event_type == EventType.QUOTE and "
       "e.event_ts_ms is not None and e.event_ts_ms <= t)),", 1)],
     ["cross", "leakage"]),
    ("M7", "settlement features computed in LABEL mode (final reconstruction instead of as-of T)",
     [(ENGINE, "st = reconstruct(market, obs, as_of_ms=t).state", "st = reconstruct(market, obs, as_of_ms=None).state", 1)],
     ["labels", "leakage"]),
]
IGNORE = shutil.ignore_patterns(".git", "__pycache__", "analysis_output", "market_data_sessions", "settlement_data",
                                ".venv", "venv*", "*.zip")


def apply(root, edits):
    for rel, old, new, count in edits:
        p = os.path.join(root, rel)
        s = open(p, encoding="utf-8").read()
        n = s.count(old)
        if n != count:
            raise RuntimeError(f"mutation anchor not found as expected in {rel}: {old[:60]!r} ({n} != {count})")
        open(p, "w", encoding="utf-8", newline="\n").write(s.replace(old, new))


def run_variant(edits, tests, timeout=1200):
    tmp = tempfile.mkdtemp(prefix="mut-md-")
    root = os.path.join(tmp, "repo")
    try:
        shutil.copytree(REPO, root, ignore=IGNORE)
        apply(root, edits)
        t0 = time.time()
        p = subprocess.run([sys.executable, "test_stage19.py", "--only", ",".join(tests)], cwd=root, capture_output=True,
                           text=True, timeout=timeout, env=dict(os.environ, KALSHI_MASTER_TEST_RUN="1"))
        fails = [ln for ln in p.stdout.splitlines() if ln.startswith("FAIL")]
        return {"caught": p.returncode != 0, "returncode": p.returncode, "seconds": round(time.time() - t0, 1),
                "failed_test": fails[0][:300] if fails else None,
                "passed_tests": [ln[6:60] for ln in p.stdout.splitlines() if ln.startswith("PASS")]}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(REPO, "analysis_output", "market_data_mutation_results.json"))
    ap.add_argument("--only", default=None)
    a = ap.parse_args(argv)
    only = set(a.only.split(",")) if a.only else None
    results = {"note": "Each mutation breaks one causal rule in a temporary copy; CAUGHT = the Stage-19 tests fail.",
               "controls": {}, "mutations": []}
    all_tests = sorted({t for m in MUTATIONS for t in m[3]})
    results["controls"]["unmutated"] = run_variant([], all_tests)
    results["controls"]["guards_disabled_only"] = run_variant(GUARDS, [t for t in all_tests if t != "alignment"])
    for mid, desc, edits, tests in MUTATIONS:
        if only and mid not in only:
            continue
        r = {"id": mid, "mutation": desc, "tests": tests,
             "as_is": run_variant(edits, tests),
             "guards_disabled": run_variant(edits + GUARDS, [t for t in tests if t != "alignment"] or tests)}
        results["mutations"].append(r)
        print(f"{mid} {desc}\n   as-is: {'CAUGHT' if r['as_is']['caught'] else 'NOT CAUGHT'}  "
              f"({r['as_is']['failed_test']})\n   guards disabled: "
              f"{'CAUGHT' if r['guards_disabled']['caught'] else 'NOT CAUGHT'}  ({r['guards_disabled']['failed_test']})",
              flush=True)
    c = results["controls"]
    ok = (not c["unmutated"]["caught"] and not c["guards_disabled_only"]["caught"]
          and all(m["as_is"]["caught"] and m["guards_disabled"]["caught"] for m in results["mutations"]))
    results["all_caught_and_controls_pass"] = ok
    print(f"controls: unmutated {'PASS' if not c['unmutated']['caught'] else 'FAIL'}, guards-disabled-only "
          f"{'PASS' if not c['guards_disabled_only']['caught'] else 'FAIL ' + str(c['guards_disabled_only']['failed_test'])}")
    print("RESULT: " + ("every mutation caught; controls pass" if ok else "NOT every mutation caught / a control failed"))
    if a.out:
        os.makedirs(os.path.dirname(a.out), exist_ok=True)
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            json.dump(results, f, indent=1)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
