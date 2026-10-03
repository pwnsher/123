#!/usr/bin/env python3
"""
Deterministic CLEAN-RESTART demonstration of the Step 6.5 execution foundation (PAPER ONLY - no network, no orders).

    py scripts/execution_restart_demo.py            (runs everything in a temporary directory, removes it afterwards)

Each phase runs in its OWN process; the only things that survive between processes are the execution journal
(SQLite) and the paper venue (the simulated outside world, saved as JSON).

A.  process 1: intent created -> validated -> paper submitted -> partial fill (2 of 5) -> process exits (state discarded)
    process 2: journal loaded -> paper venue restored -> fills reconciled -> correct position restored -> the same
               intent replayed -> no duplicate order (one submit attempt, one client_order_id)
B.  process 1: the submit result becomes ambiguous -> EXECUTION_UNKNOWN -> HALTED_FOR_RECONCILIATION -> exits
    process 2: restart -> recovery -> the same intent replayed -> no resubmit -> HALTED_FOR_RECONCILIATION remains
Exit code 0 only if every expectation holds.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)

T0 = 1_790_000_000_000
H = "d" * 64
TICKER = "KXBTC15M-26SEP292045-45"


def _intent(intent_id):
    from execution.intent import OrderIntent
    return OrderIntent(intent_id=intent_id, candidate_id="demo-candidate", created_at=T0, market_ticker=TICKER,
                       asset="BTC", side="YES", requested_contracts="5", max_limit_price="0.50",
                       time_in_force="GOOD_TIL_EXPIRY", decision_ts=T0 - 50, expires_at=T0 + 600_000,
                       raw_probability="0.60", calibrated_probability="0.57", market_price="0.48",
                       estimated_fee="0.02", estimated_slippage="0.00", estimated_net_ev="0.04", model_id="demo-model",
                       model_fingerprint="ab" * 16, calibration_fingerprint="cd" * 16, signal_fingerprint="ef" * 16,
                       risk_decision_id="demo-risk", risk_snapshot_hash=H)


def _engine(workdir, venue):
    from execution.engine import ExecutionEngine, FixedClock
    from execution.journal import Journal
    from execution.paper import PaperExecutionAdapter
    from execution.risk import RiskApproval, RiskApprovalBook
    book = RiskApprovalBook([RiskApproval("demo-risk", H, True, TICKER, "BTC", "YES", "5", "0.50", T0 + 900_000)])
    return ExecutionEngine(Journal(os.path.join(workdir, "journal.sqlite")), PaperExecutionAdapter(venue), book,
                           FixedClock(T0 + 1_000))


def phase(name, workdir):
    from execution.identity import client_order_id, execution_key
    from execution.paper import PaperVenue
    vpath = os.path.join(workdir, "venue.json")
    out = {"phase": name, "pid": os.getpid()}
    if name == "A1":
        venue = PaperVenue()
        eng = _engine(workdir, venue)
        it = _intent("demo-A")
        out["state_after_submit"] = eng.submit(it).value
        coid = client_order_id(execution_key(it))
        venue.fill(coid, "2", "0.40", fee="0.01")
        out["state_after_partial"] = eng.reconcile(execution_key(it)) and eng.state(execution_key(it)).value
        venue.fill(coid, "1", "0.50", fee="0.01")          # a fill the dying process never sees
        venue.save(vpath)
    elif name == "A2":
        venue = PaperVenue.load(vpath)
        eng = _engine(workdir, venue)
        it = _intent("demo-A")
        key = execution_key(it)
        out["recovery"] = eng.recover()
        out["state_after_recovery"] = eng.state(key).value
        snap = eng.ledger(key).snapshot()
        out["filled_size"], out["average_entry_price"] = str(snap["filled_size"]), str(snap["average_entry_price"])
        out["replay_state"] = eng.submit(it).value
        out["submit_attempts"] = venue.submit_attempts
        out["client_order_id"] = client_order_id(key)
        venue.save(vpath)
    elif name == "B1":
        venue = PaperVenue().script_submit("TIMEOUT_NOT_RECEIVED")
        eng = _engine(workdir, venue)
        out["state_after_submit"] = eng.submit(_intent("demo-B")).value
        venue.save(vpath)
    elif name == "B2":
        venue = PaperVenue.load(vpath)
        eng = _engine(workdir, venue)
        it = _intent("demo-B")
        out["recovery"] = eng.recover()
        out["replay_state"] = eng.submit(it).value
        out["submit_attempts"] = venue.submit_attempts
        out["market_locked"] = eng.market_lock_status("BTC", TICKER)[0]
    print(json.dumps(out, sort_keys=True))


def _run(name, workdir):
    p = subprocess.run([sys.executable, os.path.abspath(__file__), "--phase", name, workdir], capture_output=True,
                       text=True, cwd=HERE)
    if p.returncode != 0:
        raise SystemExit(f"phase {name} failed: {p.stderr[-800:]}")
    return json.loads(p.stdout.strip().splitlines()[-1])


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["--phase"]:
        phase(argv[1], argv[2])
        return 0
    root = tempfile.mkdtemp(prefix="exec-demo-")
    try:
        a_dir, b_dir = os.path.join(root, "A"), os.path.join(root, "B")
        os.makedirs(a_dir)
        os.makedirs(b_dir)
        a1, a2 = _run("A1", a_dir), _run("A2", a_dir)
        b1, b2 = _run("B1", b_dir), _run("B2", b_dir)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    checks = {
        "A: the two phases ran in different processes": a1["pid"] != a2["pid"],
        "A: partial fill before the restart": a1["state_after_partial"] == "PARTIALLY_FILLED",
        "A: fills reconciled after the restart (3 of 5)": a2["filled_size"] == "3"
                                                          and a2["state_after_recovery"] == "PARTIALLY_FILLED",
        "A: weighted average restored (2 @ 0.40 + 1 @ 0.50)": a2["average_entry_price"].startswith("0.4333"),
        "A: replay creates no duplicate order": list(a2["submit_attempts"].values()) == [1]
                                                and list(a2["submit_attempts"]) == [a2["client_order_id"]],
        "B: ambiguous submit halts": b1["state_after_submit"] == "HALTED_FOR_RECONCILIATION",
        "B: no resubmit after restart + replay": list(b2["submit_attempts"].values()) == [1],
        "B: HALTED_FOR_RECONCILIATION remains": b2["replay_state"] == "HALTED_FOR_RECONCILIATION",
        "B: the market stays locked": b2["market_locked"] is True,
    }
    print(json.dumps({"A": [a1, a2], "B": [b1, b2]}, sort_keys=True, indent=1))
    for k, v in checks.items():
        print(("PASS  " if v else "FAIL  ") + k)
    ok = all(checks.values())
    print("DEMO: " + ("every expectation holds" if ok else "an expectation FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
