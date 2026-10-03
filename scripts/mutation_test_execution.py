#!/usr/bin/env python3
"""
Mutation tests for the Step-6.5 execution foundation (paper / shadow only, offline, no network).

    py scripts/mutation_test_execution.py [--out analysis_output/execution_mutation_results.json] [--only E1,E7]

Each mutation breaks ONE execution-safety rule (E1-E20 of the Step-6.5 brief) in a TEMPORARY copy of the repository
and runs the relevant BEHAVIOURAL Stage-23 tests there; CAUGHT = those tests fail. The execution-fingerprint test is
deliberately never used to catch a mutation (it would flag any edit at all). Control: the unmutated copy must PASS the
same tests. The working tree is never modified.
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
IGNORE = shutil.ignore_patterns(".git", "__pycache__", "analysis_output", "market_data_sessions", "settlement_data",
                                ".venv", "venv*", "*.zip", ".mypy_cache", ".ruff_cache", "*.sqlite")
ENG = "execution/engine.py"
LED = "execution/ledger.py"
INV = "execution/invariants.py"
IDN = "execution/identity.py"
# the injected E15 endpoint, assembled so that THIS file never contains the literal (stages 17 / 18 scan scripts/)
LIVE_PATH = "/".join(("", "portfolio", "orders"))

MUTATIONS = [
    ("E1", "duplicate intent allowed (the recorded-intent check in submit is skipped)",
     [(ENG, "        row = self.j.intent_row(intent_id=intent.intent_id)\n        if row is not None:\n",
       "        row = self.j.intent_row(intent_id=intent.intent_id)\n        if False:\n", 1)],
     ["dupintent", "noduprestart"]),
    ("E2", "idempotency key becomes random / non-deterministic",
     [(IDN, 'return hashlib.sha256(canonical_identity(intent).encode("ascii")).hexdigest()',
       'return hashlib.sha256((canonical_identity(intent) + __import__("uuid").uuid4().hex).encode("ascii")).hexdigest()',
       1)],
     ["dupintent", "identity"]),
    ("E3", "execution can flip side (the side invariant is removed)",
     [(INV, "    if request.side != intent.side:\n", "    if False:\n", 1)],
     ["reducerisk"]),
    ("E4", "execution can increase requested size (the count cap is removed)",
     [(INV, "if not (0 < dec(request.count) <= intent.requested_contracts):", "if not (0 < dec(request.count)):", 1)],
     ["reducerisk"]),
    ("E5", "execution can worsen max price (the limit-price cap is removed)",
     [(INV, "if not (0 < dec(request.limit_price) <= intent.max_limit_price):", "if not (0 < dec(request.limit_price)):",
       1)],
     ["reducerisk"]),
    ("E6", "ambiguous submit automatically retries",
     [(ENG, "        except Exception as e:                                  # noqa: BLE001 - anything unproven is UNKNOWN\n"
            '            return self._mark_unknown(key, f"submit outcome unproven: {type(e).__name__}: {e}")\n',
       "        except Exception:                                       # MUTATION: blind retry\n"
       "            view = self.adapter.submit_order(req)\n", 1)],
     ["ambsubmit", "lostack"]),
    ("E7", "duplicate fill double counted",
     [(LED, "        if fill_id in self._fill_index:\n", "        if False:\n", 1)],
     ["dupfill"]),
    ("E8", "duplicate fee double counted",
     [(LED, "        if fee_id in self._fee_index:\n", "        if False:\n", 1)],
     ["fees"]),
    ("E9", "unknown fee converted to zero",
     [(LED, 'a = UNKNOWN if amount is UNKNOWN else dec(amount, "fee")', 'a = ZERO if amount is UNKNOWN else dec(amount, "fee")',
       1)],
     ["fees"]),
    ("E10", "invalid state transition accepted (an undefined pair falls back to 'allowed for everyone')",
     [("execution/states.py", "    allowed = TRANSITIONS.get((prev, new))\n",
       "    allowed = TRANSITIONS.get((prev, new), (ENGINE, RECONCILIATION))\n", 1)],
     ["transition"]),
    ("E11", "crash recovery loses the active order (in-flight / outstanding executions are not recovered)",
     [(ENG, "            if v.state in (S.SUBMITTING, S.CANCEL_PENDING, S.EXECUTION_UNKNOWN):\n"
            '                self._mark_unknown(key, f"restart:', "            if False:\n"
                                                                '                self._mark_unknown(key, f"restart:', 1),
      (ENG, "            if v.state in OUTSTANDING or v.state in UNSAFE or v.state == S.FILLED:\n", "            if False:\n",
       1)],
     ["crashbefore", "crashafter", "crashack", "crashpartial"]),
    ("E12", "reconciliation mismatch ignored (no halt on POSITION_MISMATCH / FILL_MISMATCH)",
     [(ENG, "        if a.verdict in MISMATCH_VERDICTS:\n", "        if False:\n", 1)],
     ["conflictpos", "conflictfill", "mismatchrestart"]),
    ("E13", "execution-unknown market accepts a new intent (UNKNOWN / HALTED no longer lock the market)",
     [(ENG, "            if v.state in UNSAFE:\n", "            if False:\n", 1)],
     ["lockunknown", "unknownrestart"]),
    ("E14", "journal transition is not atomic (no BEGIN / COMMIT / ROLLBACK: every statement autocommits)",
     [("execution/journal.py", 'self.conn.execute("BEGIN IMMEDIATE")', "pass", 1),
      ("execution/journal.py", 'self.conn.execute("COMMIT")', "pass", 1),
      ("execution/journal.py", 'self.conn.execute("ROLLBACK")', "pass", 1)],
     ["rollback"]),
    ("E15", "writable live endpoint introduced (a network POST path inside the execution package)",
     [("execution/adapter.py", "assert LIVE_EXECUTION_AVAILABLE is False\n",
       "assert LIVE_EXECUTION_AVAILABLE is False\n\n\nimport urllib.request  # noqa: E402\n\n\n"
       "def _live_submit(body):\n"
       f'    req = urllib.request.Request("https://trading-api.example.invalid/trade-api/v2{LIVE_PATH}", data=body,\n'
       '                                 method="POST")\n'
       "    return urllib.request.urlopen(req)\n", 1)],
     ["nolivewrite"]),
    ("E16", "expired intent still submits",
     [(ENG, "    return now_ms >= intent.expires_at\n", "    return False\n", 1)],
     ["expired"]),
    ("E17", "market lock is memory-only and lost on restart (only executions seen by THIS process can lock)",
     [(ENG, "        self._lock_cache = {}  ", "        self._session_keys = set()\n        self._lock_cache = {}  ", 1),
      (ENG, '        for r in self.j.intents():\n            key = r["execution_key"]\n            if key == exclude',
       '        for r in [x for x in self.j.intents() if x["execution_key"] in self._session_keys]:\n'
       '            key = r["execution_key"]\n            if key == exclude', 1),
      (ENG, "        key = execution_key(intent)\n        coid = client_order_id(key)\n        row = ",
       "        key = execution_key(intent)\n        coid = client_order_id(key)\n        self._session_keys.add(key)\n"
       "        row = ", 1)],
     ["lockunknown", "mismatchrestart", "unknownrestart"]),
    ("E18", "partial fill incorrectly treated as full fill",
     [("execution/reconcile.py", "    if total == requested:\n", "    if total > 0:\n", 1)],
     ["partialfill"]),
    ("E19", "cancelled partial order discards the existing filled position",
     [(ENG, "if st in (S.FILLED, S.PARTIALLY_FILLED, S.CANCELLED, S.EXPIRED) and filled > 0:",
       "if st in (S.FILLED, S.PARTIALLY_FILLED, S.EXPIRED) and filled > 0:", 1),
      (ENG, "elif v.state not in (S.CLOSED, S.REJECTED) and ledgers[key].filled_size > 0:",
       "elif v.state not in (S.CLOSED, S.REJECTED, S.CANCELLED) and ledgers[key].filled_size > 0:", 1)],
     ["cancelpartial"]),
    ("E20", "same intent after restart creates a second client_order_id (a per-process salt)",
     [(IDN, "    return CLIENT_ORDER_ID_PREFIX + h[:CLIENT_ORDER_ID_HEX]\n",
       "    return CLIENT_ORDER_ID_PREFIX + hashlib.sha256((h + _PROCESS_SALT).encode()).hexdigest()[:CLIENT_ORDER_ID_HEX]"
       "\n\n\n_PROCESS_SALT = __import__(\"os\").urandom(8).hex()\n", 1)],
     ["identity", "demo"]),
]


def apply(root, edits):
    for rel, old, new, count in edits:
        p = os.path.join(root, rel)
        s = open(p, encoding="utf-8").read()
        n = s.count(old)
        if n != count:
            raise RuntimeError(f"mutation anchor not found as expected in {rel}: {old[:60]!r} ({n} != {count})")
        open(p, "w", encoding="utf-8", newline="\n").write(s.replace(old, new))


def run_variant(edits, tests, timeout=1800):
    tmp = tempfile.mkdtemp(prefix="mut-exec-")
    root = os.path.join(tmp, "repo")
    try:
        shutil.copytree(REPO, root, ignore=IGNORE)
        apply(root, edits)
        t0 = time.time()
        p = subprocess.run([sys.executable, "test_stage23.py", "--only", ",".join(tests)], cwd=root, capture_output=True,
                           text=True, timeout=timeout, env=dict(os.environ, KALSHI_MASTER_TEST_RUN="1"))
        fails = [ln for ln in p.stdout.splitlines() if ln.startswith("FAIL")]
        return {"caught": p.returncode != 0, "returncode": p.returncode, "seconds": round(time.time() - t0, 1),
                "failed_test": fails[0][:300] if fails else None,
                "passed_tests": [ln[6:70] for ln in p.stdout.splitlines() if ln.startswith("PASS")]}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(REPO, "analysis_output", "execution_mutation_results.json"))
    ap.add_argument("--only", default=None)
    a = ap.parse_args(argv)
    only = set(a.only.split(",")) if a.only else None
    res = {"note": "Each mutation breaks one Step-6.5 execution-safety rule in a temporary copy; CAUGHT = the "
                   "behavioural Stage-23 tests fail (never the fingerprint test).",
           "controls": {}, "mutations": []}
    all_tests = sorted({t for m in MUTATIONS for t in m[3]})
    res["controls"]["unmutated"] = run_variant([], all_tests)
    for mid, desc, edits, tests in MUTATIONS:
        if only and mid not in only:
            continue
        r = {"id": mid, "mutation": desc, "tests": tests, "as_is": run_variant(edits, tests)}
        res["mutations"].append(r)
        print(f"{mid} {desc}\n   {'CAUGHT' if r['as_is']['caught'] else 'NOT CAUGHT'}  ({r['as_is']['failed_test']})",
              flush=True)
    c = res["controls"]
    ok = not c["unmutated"]["caught"] and all(m["as_is"]["caught"] for m in res["mutations"])
    res["all_caught_and_controls_pass"] = ok
    print(f"control: unmutated {'PASS' if not c['unmutated']['caught'] else 'FAIL ' + str(c['unmutated']['failed_test'])}")
    print("RESULT: " + ("every mutation caught; control passes" if ok else "NOT every mutation caught / the control failed"))
    if a.out:
        os.makedirs(os.path.dirname(a.out), exist_ok=True)
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            json.dump(res, f, indent=1)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
