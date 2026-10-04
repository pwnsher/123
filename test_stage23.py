#!/usr/bin/env python3
"""
Stage 23 - Step 6.5 EXECUTION FOUNDATION (paper / shadow only, zero live orders).

Run:  py test_stage23.py                 (or py run_all_tests.py for stages 1-23)
      py test_stage23.py --only lostack,dupfill   (a subset; used by scripts/mutation_test_execution.py)
Every test uses fake ids and temporary directories only; no network, no real order, no database is left behind.
"""
import ast
import json
import os
import shutil
import subprocess
import sys
import tempfile
from decimal import Decimal as D

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from execution.adapter import FutureKalshiExecutionAdapter  # noqa: E402
from execution.audit import AuditRecord, prediction_correct  # noqa: E402
from execution.engine import DuplicateIntentConflict, ExecutionEngine, FixedClock, SettlementReference  # noqa: E402
from execution.faults import FAULT_POINTS, FaultInjector, SimulatedCrash  # noqa: E402
from execution.identity import (EXECUTION_KEY_FIELDS, canonical_identity, client_order_id,  # noqa: E402
                                execution_key)
from execution.intent import IntentError, OrderIntent  # noqa: E402
from execution.invariants import InvariantViolation, assert_fill_within_intent, assert_order_within_intent  # noqa: E402
from execution.journal import Journal, JournalConflict, JournalError  # noqa: E402
from execution.ledger import ExecutionLedger, FillConflict  # noqa: E402
from execution.money import UNKNOWN  # noqa: E402
from execution.adapter import OrderRequest  # noqa: E402
from execution.paper import PaperExecutionAdapter, PaperVenue  # noqa: E402
from execution.risk import RiskApproval, RiskApprovalBook  # noqa: E402
from execution.states import TRANSITIONS, ExecState, InvalidTransition, check_transition  # noqa: E402
from kalshi_core.execution import LiveExecutionUnavailable  # noqa: E402

S = ExecState
T0 = 1_790_000_000_000
H = "a" * 64
BTC = "KXBTC15M-26SEP292045-45"
ETH = "KXETH15M-26SEP292045-45"
_DIRS = []


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


def tmpdir():
    d = tempfile.mkdtemp(prefix="stage23-")
    _DIRS.append(d)
    return d


def intent(iid="i1", **kw):
    d = dict(intent_id=iid, candidate_id="cand-1", created_at=T0, market_ticker=BTC, asset="BTC", side="YES",
             requested_contracts="5", max_limit_price="0.50", time_in_force="GOOD_TIL_EXPIRY", decision_ts=T0 - 10,
             expires_at=T0 + 60_000, raw_probability="0.61", calibrated_probability="0.58", market_price="0.48",
             estimated_fee="0.02", estimated_slippage="0.00", estimated_net_ev="0.05", model_id="model-1",
             model_fingerprint="ab" * 16, calibration_fingerprint="cd" * 16, signal_fingerprint="ef" * 16,
             risk_decision_id="risk-1", risk_snapshot_hash=H)
    d.update(kw)
    return OrderIntent(**d)


def approval(did="risk-1", ticker=BTC, asset="BTC", side="YES", max_c="5", max_p="0.50", approved=True, exp=T0 + 600_000,
             snap=H):
    return RiskApproval(did, snap, approved, ticker, asset, side, max_c, max_p, exp)


class Env:
    """One journal file + one paper venue (the outside world) + engine factories (a 'restart' = a new engine)."""

    def __init__(self, approvals=None, faults=None, now=T0 + 5):
        self.dir = tmpdir()
        self.path = os.path.join(self.dir, "journal.sqlite")
        self.venue = PaperVenue()
        self.book = RiskApprovalBook(approvals if approvals is not None else
                                     [approval(), approval("risk-eth", ETH, "ETH")])
        self.clock = FixedClock(now)
        self.faults = faults or FaultInjector()
        self.eng = self.new_engine()

    def new_engine(self):
        return ExecutionEngine(Journal(self.path, faults=self.faults), PaperExecutionAdapter(self.venue, self.faults),
                               self.book, self.clock, faults=self.faults)

    def restart(self):
        self.faults.disarm()
        self.eng = self.new_engine()
        return self.eng


def ids(it):
    k = execution_key(it)
    return k, client_order_id(k)


def crash(fn, *a):
    try:
        fn(*a)
    except SimulatedCrash as e:
        return str(e)
    raise AssertionError("expected a simulated crash")


# ═══════════════════ 1-2 duplicate intent / execution key ═══════════════════
def test_duplicate_intent():
    env = Env()
    it = intent()
    k, coid = ids(it)
    assert env.eng.submit(it) == S.ACKNOWLEDGED
    for _ in range(3):
        assert env.eng.submit(it) == S.ACKNOWLEDGED                       # idempotent replay
    assert env.venue.submit_attempts == {coid: 1} and len(env.eng.j.intents()) == 1
    try:
        env.eng.submit(intent(requested_contracts="4")); raise AssertionError("intent_id reuse with other content")
    except DuplicateIntentConflict:
        pass
    assert env.venue.submit_attempts == {coid: 1}


def test_deterministic_identity():
    a, b = intent(), intent(requested_contracts="5.00", max_limit_price="0.5000")
    assert execution_key(a) == execution_key(b) and client_order_id(execution_key(a)) == client_order_id(execution_key(b))
    rev = OrderIntent(**{k: v for k, v in reversed(list(intent().to_dict().items()))})       # field order irrelevant
    assert execution_key(rev) == execution_key(a)
    assert execution_key(intent("i2")) != execution_key(a)                                     # different intent_id
    assert execution_key(intent(side="NO")) != execution_key(a) and execution_key(intent(requested_contracts="4")) != \
        execution_key(a)
    can = json.loads(canonical_identity(a))
    assert can["v"] == "exec_key_v1" and set(can) == {"v"} | set(EXECUTION_KEY_FIELDS)
    assert can["requested_contracts"] == "5" and can["max_limit_price"] == "0.5"
    coid = client_order_id(execution_key(a))
    assert len(coid) == 32 and coid.startswith("x1") and all(c in "0123456789abcdefx" for c in coid)
    code = ("import sys; sys.path.insert(0, %r); sys.argv=['x']; import test_stage23 as t; "
            "from execution.identity import execution_key, client_order_id; k = execution_key(t.intent()); "
            "print(k, client_order_id(k))") % HERE
    outs = {subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=HERE).stdout.strip()
            for _ in range(2)}
    assert outs == {f"{execution_key(a)} {coid}"}, outs                        # identical in fresh processes
    env = Env()
    env.eng.submit(a)
    row = env.eng.j.intent_row(intent_id="i1")
    assert row["execution_key"] == execution_key(a) and row["client_order_id"] == coid  # persisted


# ═══════════════════ 3-7 fills, fees, marks ═══════════════════
def test_duplicate_fill():
    lg = ExecutionLedger("k", BTC, "BTC", "YES", "5", "0.50")
    assert lg.add_fill("f1", "2", "0.40") == "ADDED" and lg.add_fill("f1", "2", "0.40") == "DUPLICATE"
    assert lg.filled_size == D("2")
    try:
        lg.add_fill("f1", "3", "0.40"); raise AssertionError("conflicting duplicate accepted")
    except FillConflict:
        pass
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.venue.fill(coid, "2", "0.40")
    env.venue.duplicate_last_fill(coid)
    env.venue.duplicate_last_fill(coid)
    assert len(env.venue.fills[coid]) == 3
    env.eng.reconcile(k)
    assert env.eng.ledger(k).filled_size == D("2") and env.eng.state(k) == S.PARTIALLY_FILLED
    env.eng.reconcile(k)
    assert env.eng.ledger(k).filled_size == D("2") and env.eng.j.count("FILL") == 1


def test_partial_fills_weighted_average():
    lg = ExecutionLedger("k", BTC, "BTC", "YES", "5", "0.50")
    lg.add_fill("f1", "2", "0.40")
    lg.add_fill("f2", "3", "0.50")
    assert lg.average_entry_price == D("0.46") and lg.entry_notional == D("2.3") and lg.filled_size == D("5")
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.eng.reconcile(k)
    assert env.eng.state(k) == S.ACKNOWLEDGED                                 # no fill yet (no-fill / delayed fill)
    env.venue.fill(coid, "2", "0.40")
    env.eng.reconcile(k)
    assert env.eng.state(k) == S.PARTIALLY_FILLED
    env.venue.fill(coid, "1", "0.50")
    env.eng.reconcile(k)
    assert env.eng.state(k) == S.PARTIALLY_FILLED and env.eng.ledger(k).filled_size == D("3")   # never "full"
    env.venue.fill(coid, "2", "0.50")
    env.eng.reconcile(k)
    assert env.eng.state(k) == S.FILLED and env.eng.ledger(k).average_entry_price == D("0.46")
    path = [e["new_state"] for e in env.eng.j.events(k, "TRANSITION")]
    assert path[-4:] == ["ACKNOWLEDGED", "PARTIALLY_FILLED", "PARTIALLY_FILLED", "FILLED"], path


def test_missing_fee_and_duplicate_fee():
    lg = ExecutionLedger("k", BTC, "BTC", "YES", "5", "0.50")
    lg.add_fill("f1", "2", "0.40")
    assert lg.fees_paid is UNKNOWN                                            # a fill without a fee record
    lg.add_fee("fee-f1", UNKNOWN, "f1")
    assert lg.fees_paid is UNKNOWN and not lg.accounting_complete and lg.unrealized_pnl is UNKNOWN
    lg2 = ExecutionLedger("k", BTC, "BTC", "YES", "5", "0.50")
    lg2.add_fill("f1", "2", "0.40")
    assert lg2.add_fee("fee-f1", "0.02", "f1") == "ADDED" and lg2.add_fee("fee-f1", "0.02", "f1") == "DUPLICATE"
    assert lg2.fees_paid == D("0.02")
    try:
        lg2.add_fee("fee-f1", "0.03", "f1"); raise AssertionError("conflicting fee accepted")
    except FillConflict:
        pass
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.venue.fill(coid, "5", "0.45", fee=UNKNOWN)
    assert env.eng.reconcile(k) in ("RECOVERABLE_DIFFERENCE", "ACCOUNTING_INCOMPLETE")
    snap = env.eng.ledger(k).snapshot()
    assert snap["fees_paid"] is UNKNOWN and snap["pnl_authoritative"] is False and snap["fees_paid"] != D(0)
    env.eng.settle(SettlementReference(BTC, "yes"))
    assert env.eng.ledger(k).realized_pnl is UNKNOWN                          # never reported as authoritative


def test_missing_mark():
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.venue.fill(coid, "5", "0.40", fee="0.05")
    env.eng.reconcile(k)
    lg = env.eng.ledger(k)
    assert lg.mark_price is UNKNOWN and lg.unrealized_pnl is UNKNOWN and not lg.accounting_complete
    env.eng.set_mark(k, UNKNOWN)
    assert env.eng.ledger(k).unrealized_pnl is UNKNOWN
    env.eng.set_mark(k, "0.60")
    lg = env.eng.ledger(k)
    assert lg.unrealized_pnl == D("5") * D("0.60") - D("2.0") - D("0.05") and lg.accounting_complete


# ═══════════════════ 8 state machine ═══════════════════
def test_invalid_transition():
    for a, b in ((S.CREATED, S.FILLED), (S.READY, S.ACKNOWLEDGED), (S.FILLED, S.CREATED), (S.CLOSED, S.FILLED),
                 (S.REJECTED, S.READY), (S.EXECUTION_UNKNOWN, S.FILLED), (S.EXECUTION_UNKNOWN, S.SUBMITTING),
                 (S.SUBMITTING, S.FILLED), (None, S.READY)):
        try:
            check_transition(a, b); raise AssertionError(f"{a} -> {b} accepted")
        except InvalidTransition:
            pass
    try:
        check_transition(S.HALTED_FOR_RECONCILIATION, S.FILLED); raise AssertionError("engine released a halt")
    except InvalidTransition:
        pass
    try:
        check_transition(S.HALTED_FOR_RECONCILIATION, S.FILLED, "RECONCILIATION"); raise AssertionError("no evidence")
    except InvalidTransition:
        pass
    assert check_transition(S.HALTED_FOR_RECONCILIATION, S.FILLED, "RECONCILIATION", "r:1")
    for (a, b) in [(S.CREATED, S.VALIDATED), (S.VALIDATED, S.PRECHECK), (S.PRECHECK, S.READY), (S.READY, S.SUBMITTING),
                   (S.SUBMITTING, S.ACKNOWLEDGED), (S.SUBMITTING, S.REJECTED), (S.SUBMITTING, S.EXECUTION_UNKNOWN),
                   (S.ACKNOWLEDGED, S.PARTIALLY_FILLED), (S.ACKNOWLEDGED, S.FILLED), (S.ACKNOWLEDGED, S.CANCEL_PENDING),
                   (S.PARTIALLY_FILLED, S.PARTIALLY_FILLED), (S.PARTIALLY_FILLED, S.FILLED),
                   (S.PARTIALLY_FILLED, S.CANCEL_PENDING), (S.CANCEL_PENDING, S.CANCELLED), (S.CANCEL_PENDING, S.FILLED),
                   (S.CANCEL_PENDING, S.EXECUTION_UNKNOWN), (S.FILLED, S.SETTLEMENT_PENDING),
                   (S.SETTLEMENT_PENDING, S.CLOSED), (S.EXECUTION_UNKNOWN, S.HALTED_FOR_RECONCILIATION)]:
        assert (a, b) in TRANSITIONS, (a, b)                                  # every path the brief requires
    assert all(dst != S.SUBMITTING or src == S.READY for (src, dst) in TRANSITIONS)       # no path re-submits
    # a forged history is refused on reconstruction (the journal never trusts an invalid chain)
    env = Env()
    it = intent()
    k, _coid = ids(it)
    env.eng.submit(it)
    with env.eng.j.transaction():
        env.eng.j.append({"kind": "TRANSITION", "event_id": "forged", "execution_key": k, "intent_id": "i1",
                          "previous_state": "ACKNOWLEDGED", "new_state": "CLOSED", "ts_ms": T0, "actor": "ENGINE"})
    try:
        env.eng.j.reconstruct(); raise AssertionError("forged ACKNOWLEDGED -> CLOSED accepted")
    except InvalidTransition:
        pass


# ═══════════════════ 9-11 ambiguous outcomes ═══════════════════
def test_lost_ack():
    env = Env()
    env.venue.script_submit("LOST_ACK")
    it = intent()
    k, coid = ids(it)
    assert env.eng.submit(it) == S.HALTED_FOR_RECONCILIATION
    assert env.venue.submit_attempts == {coid: 1} and coid in env.venue.orders           # the venue HAS the order
    states = [e["new_state"] for e in env.eng.j.events(k, "TRANSITION")]
    assert states[-2:] == ["EXECUTION_UNKNOWN", "HALTED_FOR_RECONCILIATION"]
    assert env.eng.submit(it) == S.HALTED_FOR_RECONCILIATION and env.venue.submit_attempts == {coid: 1}
    assert env.eng.reconcile(k) == "RECOVERABLE_DIFFERENCE" and env.eng.state(k) == S.ACKNOWLEDGED  # proven by lookup
    assert env.venue.submit_attempts == {coid: 1}


def test_ambiguous_submit():
    env = Env()
    env.venue.script_submit("TIMEOUT_NOT_RECEIVED")
    it = intent()
    k, coid = ids(it)
    assert env.eng.submit(it) == S.HALTED_FOR_RECONCILIATION and coid not in env.venue.orders
    for _ in range(3):
        assert env.eng.submit(it) == S.HALTED_FOR_RECONCILIATION
        assert env.eng.reconcile(k) == "EXECUTION_UNKNOWN" and env.eng.state(k) == S.HALTED_FOR_RECONCILIATION
    assert env.venue.submit_attempts == {coid: 1}                            # never retried blindly
    env.venue.lookup_authoritative = True                                    # an authoritative absence proof
    assert env.eng.reconcile(k) == "RECOVERABLE_DIFFERENCE" and env.eng.state(k) == S.REJECTED
    assert env.venue.submit_attempts == {coid: 1}
    # a partial fill that happened during the ambiguous state is found, not lost
    env2 = Env()
    env2.venue.script_submit("LOST_ACK")
    it2 = intent("i2")
    k2, c2 = ids(it2)
    env2.eng.submit(it2)
    env2.venue.fill(c2, "2", "0.45")
    assert env2.eng.reconcile(k2) == "RECOVERABLE_DIFFERENCE" and env2.eng.state(k2) == S.PARTIALLY_FILLED
    assert env2.eng.ledger(k2).filled_size == D("2")


def test_ambiguous_cancel():
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.venue.script_cancel("LOST_ACK")                                      # cancelled, response lost
    assert env.eng.cancel(k) == S.HALTED_FOR_RECONCILIATION
    assert env.eng.reconcile(k) == "RECOVERABLE_DIFFERENCE" and env.eng.state(k) == S.CANCELLED
    env2 = Env()
    env2.eng.submit(intent())
    env2.venue.script_cancel("AMBIGUOUS_NOT_DONE")                           # nothing happened, outcome unknown
    assert env2.eng.cancel(k) == S.HALTED_FOR_RECONCILIATION
    assert env2.eng.reconcile(k) == "RECOVERABLE_DIFFERENCE" and env2.eng.state(k) == S.ACKNOWLEDGED   # still resting


# ═══════════════════ 12-17 crashes / restart ═══════════════════
def test_crash_before_submit():
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.faults.arm("before_journal_write", nth=5)            # dies while trying to persist READY -> SUBMITTING
    crash(env.eng.submit, it)
    assert env.restart().state(k) == S.READY and env.venue.submit_attempts == {}
    assert env.eng.recover()[k] == "READY"
    assert env.eng.submit(it) == S.ACKNOWLEDGED and env.venue.submit_attempts == {coid: 1}    # resumed exactly once
    env2 = Env()
    env2.faults.arm("before_submit")                         # SUBMITTING persisted, the adapter never called
    crash(env2.eng.submit, it)
    eng = env2.restart()
    assert eng.state(k) == S.SUBMITTING and env2.venue.submit_attempts == {}
    eng.recover()
    assert eng.state(k) == S.HALTED_FOR_RECONCILIATION                       # unproven -> never assumed failed
    assert eng.submit(it) == S.HALTED_FOR_RECONCILIATION and env2.venue.submit_attempts == {}
    env2.venue.lookup_authoritative = True
    assert eng.reconcile(k) == "RECOVERABLE_DIFFERENCE" and eng.state(k) == S.REJECTED


def test_crash_after_submit():
    env = Env()
    env.venue.script_submit("CRASH_AFTER_ACCEPT")            # the venue accepted, the process died before the ACK
    it = intent()
    k, coid = ids(it)
    crash(env.eng.submit, it)
    eng = env.restart()
    assert eng.state(k) == S.SUBMITTING
    eng.recover()
    assert eng.state(k) == S.ACKNOWLEDGED and env.venue.submit_attempts == {coid: 1}           # recovered, no resubmit
    assert eng.submit(it) == S.ACKNOWLEDGED and env.venue.submit_attempts == {coid: 1}
    env2 = Env()
    env2.faults.arm("after_submit_before_ack")
    crash(env2.eng.submit, it)
    env2.venue.fill(coid, "5", "0.48")                       # filled while we were down
    eng2 = env2.restart()
    eng2.recover()
    assert eng2.state(k) == S.FILLED and env2.venue.submit_attempts == {coid: 1}


def test_crash_after_ack():
    env = Env()
    env.faults.arm("after_ack")
    it = intent()
    k, coid = ids(it)
    crash(env.eng.submit, it)
    eng = env.restart()
    assert eng.state(k) == S.ACKNOWLEDGED
    env.venue.fill(coid, "5", "0.44")
    eng.recover()
    assert eng.state(k) == S.FILLED and env.venue.submit_attempts == {coid: 1}               # filled order, local FILLED
    assert eng.ledger(k).filled_size == D("5")                                                # restored safely


def test_crash_after_partial_fill():
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.venue.fill(coid, "2", "0.40", fee="0.01")
    env.faults.arm("after_partial_fill")
    crash(env.eng.reconcile, k)
    env.venue.fill(coid, "1", "0.50", fee="0.01")
    eng = env.restart()
    assert eng.state(k) == S.PARTIALLY_FILLED and eng.ledger(k).filled_size == D("2")
    eng.recover()
    assert eng.state(k) == S.PARTIALLY_FILLED and eng.ledger(k).filled_size == D("3")
    assert env.venue.submit_attempts == {coid: 1}
    env.faults.arm("before_fill_persist")                     # observed fills not yet persisted when the crash hits
    env.venue.fill(coid, "2", "0.50", fee="0.01")
    crash(eng.reconcile, k)
    eng = env.restart()
    assert eng.ledger(k).filled_size == D("3")                                 # nothing half-written
    eng.recover()
    assert eng.state(k) == S.FILLED and eng.ledger(k).filled_size == D("5") and eng.ledger(k).fees_paid == D("0.03")


def test_restart_reconstruction():
    env = Env()
    it, it2 = intent(), intent("i2", market_ticker=ETH, asset="ETH", risk_decision_id="risk-eth")
    k, coid = ids(it)
    k2, _c2 = ids(it2)
    env.eng.submit(it)
    env.eng.submit(it2)
    env.venue.fill(coid, "3", "0.45")
    env.eng.reconcile(k)
    before = ({kk: v.state for kk, v in env.eng.j.reconstruct().items()}, env.eng.positions())
    for _ in range(2):
        eng = env.restart()
        after = ({kk: v.state for kk, v in eng.j.reconstruct().items()}, eng.positions())
        assert after == before                                                 # deterministic replay, no memory needed
    assert before[0] == {k: S.PARTIALLY_FILLED, k2: S.ACKNOWLEDGED}
    try:
        eng.j.conn.execute("UPDATE events SET reason='x' WHERE seq=1"); raise AssertionError("journal rewritten")
    except Exception as e:                                                     # noqa: BLE001
        assert "append-only" in str(e)


def test_duplicate_order_prevented_after_restart():
    env = Env()
    env.venue.script_submit("LOST_ACK")
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    for _ in range(3):
        eng = env.restart()
        eng.recover()
        eng.submit(it)
        assert client_order_id(execution_key(it)) == coid
    assert env.venue.submit_attempts == {coid: 1} and len(env.venue.orders) == 1
    assert eng.state(k) == S.ACKNOWLEDGED                                      # proven by lookup, never resubmitted


# ═══════════════════ 18-20 reconciliation / locks ═══════════════════
def test_conflicting_position():
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.venue.fill(coid, "5", "0.45")
    env.venue.position_mode = ("CONFLICT", "YES", "3")                         # fills say 5, venue position says 3
    assert env.eng.reconcile(k) == "POSITION_MISMATCH" and env.eng.state(k) == S.HALTED_FOR_RECONCILIATION
    assert env.eng.market_lock_status("BTC", BTC)[0]
    env.venue.position_mode = "LIVE"
    assert env.eng.reconcile(k) == "RECOVERABLE_DIFFERENCE" and env.eng.state(k) == S.FILLED  # consistent evidence
    # brief example C exactly: journal FILLED size 5, fills total 5, adapter position 3 -> POSITION_MISMATCH -> halt
    env.venue.position_mode = ("CONFLICT", "YES", "3")
    assert env.eng.reconcile(k) == "POSITION_MISMATCH" and env.eng.state(k) == S.HALTED_FOR_RECONCILIATION
    assert env.eng.ledger(k).filled_size == D("5")                             # never "repaired" to the venue's 3


def test_conflicting_fills():
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.venue.fill(coid, "2", "0.60", force=True)                              # above the price cap
    assert env.eng.reconcile(k) == "FILL_MISMATCH" and env.eng.state(k) == S.HALTED_FOR_RECONCILIATION
    env2 = Env()
    env2.eng.submit(intent())
    env2.venue.fill(coid, "2", "0.45")
    env2.eng.reconcile(k)
    env2.venue.fills[coid][0] = dict(env2.venue.fills[coid][0], qty="3")      # same fill_id, different content
    assert env2.eng.reconcile(k) == "FILL_MISMATCH" and env2.eng.state(k) == S.HALTED_FOR_RECONCILIATION
    try:
        assert_fill_within_intent(intent(), "0.51", "1"); raise AssertionError("fill above cap accepted")
    except InvariantViolation:
        pass


def test_market_lock_while_unknown():
    env = Env(approvals=[approval(), approval("risk-2")])
    env.venue.script_submit("TIMEOUT_NOT_RECEIVED")
    env.eng.submit(intent())
    second = intent("i2", risk_decision_id="risk-2")
    k2, c2 = ids(second)
    st = env.eng.submit(second)
    assert st == S.REJECTED and c2 not in env.venue.submit_attempts
    reason = [e["reason"] for e in env.eng.j.events(k2, "TRANSITION")][-1]
    assert reason.startswith("MARKET_LOCKED") and "HALTED_FOR_RECONCILIATION" in reason
    eng = env.restart()                                                        # the lock survives a restart
    assert eng.market_lock_status("BTC", BTC)[0]
    third = intent("i3", risk_decision_id="risk-2")
    assert eng.submit(third) == S.REJECTED and ids(third)[1] not in env.venue.submit_attempts
    other = intent("i4", market_ticker=ETH, asset="ETH", risk_decision_id="risk-eth")
    env.book = RiskApprovalBook([approval(), approval("risk-2"), approval("risk-eth", ETH, "ETH")])
    eng = env.restart()
    assert eng.submit(other) == S.ACKNOWLEDGED                                 # an independent market is unaffected


# ═══════════════════ 21-24 expiry / invariants ═══════════════════
def test_expired_intent():
    env = Env(now=T0 + 60_000)
    it = intent()
    k, coid = ids(it)
    assert env.eng.submit(it) == S.REJECTED and env.venue.submit_attempts == {}
    assert "EXPIRED" in [e["reason"] for e in env.eng.j.events(k, "TRANSITION")][-1]
    env2 = Env()
    env2.faults.arm("before_journal_write", nth=5)
    crash(env2.eng.submit, it)                                                 # stuck in READY ...
    env2.clock.advance(70_000)                                                 # ... and the intent expires meanwhile
    eng = env2.restart()
    assert eng.submit(it) == S.EXPIRED and env2.venue.submit_attempts == {}
    env3 = Env()
    env3.faults.arm("before_journal_write", nth=5)
    crash(env3.eng.submit, intent("i9"))
    env3.clock.advance(70_000)
    eng3 = env3.restart()
    assert list(eng3.expire_due().values()) == [S.EXPIRED] and env3.venue.submit_attempts == {}


def test_execution_only_reduces_risk():
    it = intent()
    k, coid = ids(it)
    good = OrderRequest(coid, BTC, "YES", D("5"), D("0.50"), "GOOD_TIL_EXPIRY", it.expires_at)
    assert assert_order_within_intent(it, good, coid)
    assert assert_order_within_intent(it, OrderRequest(coid, BTC, "YES", D("3"), D("0.40"), "GOOD_TIL_EXPIRY",
                                                       it.expires_at), coid)              # fewer / better is fine
    for bad, what in ((OrderRequest(coid, BTC, "YES", D("6"), D("0.50"), "GOOD_TIL_EXPIRY", it.expires_at), "size"),
                      (OrderRequest(coid, BTC, "YES", D("5"), D("0.51"), "GOOD_TIL_EXPIRY", it.expires_at), "price"),
                      (OrderRequest(coid, BTC, "NO", D("5"), D("0.50"), "GOOD_TIL_EXPIRY", it.expires_at), "side"),
                      (OrderRequest(coid, ETH, "YES", D("5"), D("0.50"), "GOOD_TIL_EXPIRY", it.expires_at), "ticker"),
                      (OrderRequest("x1other", BTC, "YES", D("5"), D("0.50"), "GOOD_TIL_EXPIRY", it.expires_at), "id"),
                      (OrderRequest(coid, BTC, "YES", D("5"), D("0.50"), "GOOD_TIL_EXPIRY", it.expires_at + 1), "exp")):
        try:
            assert_order_within_intent(it, bad, coid); raise AssertionError(f"{what} increase accepted")
        except InvariantViolation:
            pass
    env = Env()
    env.eng.submit(it)
    o = env.venue.orders[coid]
    assert (o["side"], o["count"], o["limit_price"], o["market_ticker"]) == ("YES", "5", "0.5", BTC)
    # the risk approval caps size / price / side; execution never exceeds or overrides it
    for kw, why in (({"requested_contracts": "6"}, "SIZE_EXCEEDS_APPROVAL"), ({"max_limit_price": "0.55"},
                                                                             "PRICE_EXCEEDS_APPROVAL"),
                    ({"side": "NO"}, "RISK_APPROVAL_SCOPE_MISMATCH")):
        e2 = Env()
        bad_it = intent("ix", **kw)
        assert e2.eng.submit(bad_it) == S.REJECTED, kw
        assert [e["reason"] for e in e2.eng.j.events(ids(bad_it)[0], "TRANSITION")][-1] == why
        assert e2.venue.submit_attempts == {}
    for bad in ({"side": "UP"}, {"requested_contracts": "-1"}, {"max_limit_price": "1.5"}, {"requested_contracts": 5.0},
                {"estimated_fee": None}, {"model_fingerprint": "zz"}, {"risk_snapshot_hash": ""},
                {"expires_at": T0 - 1}):
        try:
            intent(**bad); raise AssertionError(f"invalid intent accepted: {bad}")
        except IntentError:
            pass
    assert intent(estimated_fee=UNKNOWN).estimated_fee is UNKNOWN              # unknown stays UNKNOWN, never zero
    try:
        setattr(it, "side", "NO"); raise AssertionError("intent mutated")
    except Exception as e:                                                     # noqa: BLE001
        assert "frozen" in type(e).__name__.lower() or "cannot assign" in str(e)


# ═══════════════════ 25-26 journal ═══════════════════
def test_journal_rollback():
    env = Env()
    env.venue.script_submit("TIMEOUT_NOT_RECEIVED")
    it = intent()
    k, _coid = ids(it)
    env.faults.arm("journal_mid_transaction")                # EXECUTION_UNKNOWN + HALTED are ONE atomic write
    crash(env.eng.submit, it)
    eng = env.restart()
    assert eng.state(k) == S.SUBMITTING                                        # neither half was persisted
    assert [e["new_state"] for e in eng.j.events(k, "TRANSITION")][-1] == "SUBMITTING"
    eng.recover()
    assert eng.state(k) == S.HALTED_FOR_RECONCILIATION
    j = eng.j
    n = j.count()
    try:
        with j.transaction():
            j.append({"kind": "MARK", "event_id": "rb-1", "execution_key": k, "ts_ms": T0, "payload": {"mark": "0.5"}})
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert j.count() == n                                                      # rolled back
    try:
        j.append({"kind": "MARK", "event_id": "rb-2", "ts_ms": T0}); raise AssertionError("write outside a transaction")
    except JournalError:
        pass


def test_duplicate_journal_event():
    j = Journal(os.path.join(tmpdir(), "j.sqlite"))
    ev = {"kind": "MARK", "event_id": "e-1", "execution_key": "k", "ts_ms": T0, "payload": {"mark": "0.5"}}
    with j.transaction():
        assert j.append(ev) == "APPENDED"
    with j.transaction():
        assert j.append(dict(ev)) == "DUPLICATE"
    try:
        with j.transaction():
            j.append(dict(ev, payload={"mark": "0.6"}))
        raise AssertionError("event rewritten")
    except JournalConflict:
        pass
    assert j.count() == 1
    it = intent()
    k, coid = ids(it)
    with j.transaction():
        assert j.record_intent(it, k, coid) == "APPENDED"
        assert j.record_intent(it, k, coid) == "DUPLICATE"
    try:
        with j.transaction():
            other = intent(requested_contracts="4")
            j.record_intent(other, execution_key(other), client_order_id(execution_key(other)))
        raise AssertionError("intent rewritten")
    except JournalConflict:
        pass
    try:
        j.conn.execute("DELETE FROM intents"); raise AssertionError("intent deleted")
    except Exception as e:                                                     # noqa: BLE001
        assert "immutable" in str(e)


# ═══════════════════ 27-30 cancel / settlement ═══════════════════
def test_cancel_partial_keeps_exposure():
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.venue.fill(coid, "2", "0.40", fee="0.01")
    env.eng.reconcile(k)
    assert env.eng.cancel(k) == S.CANCELLED
    lg = env.eng.ledger(k)
    assert lg.filled_size == D("2") and lg.open_size == D("2")                 # filled exposure retained
    assert env.eng.market_lock_status("BTC", BTC)[0]                           # an open position blocks a new entry
    out = env.eng.settle(SettlementReference(BTC, "yes", "83600.12", "kalshi_market_api"))
    assert out[k] == S.CLOSED
    lg = env.eng.ledger(k)
    assert lg.open_size == 0 and lg.realized_pnl == D("2") - D("0.80") - D("0.01")
    assert [e["payload"]["settled_size"] for e in env.eng.j.events(k, "SETTLEMENT")] == ["2"]
    assert not env.eng.market_lock_status("BTC", BTC)[0]


def test_cancel_never_filled():
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    assert env.eng.cancel(k) == S.CANCELLED
    assert env.eng.ledger(k).filled_size == 0 and not env.eng.market_lock_status("BTC", BTC)[0]
    env.eng.settle(SettlementReference(BTC, "no"))
    assert env.eng.state(k) == S.CANCELLED and env.eng.j.count("SETTLEMENT") == 0          # no position created
    assert env.eng.positions()[k]["open_size"] == 0


def test_fill_during_cancel():
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.venue.script_cancel("FILL_THEN_CANCEL")
    assert env.eng.cancel(k) == S.FILLED and env.eng.ledger(k).filled_size == D("5")
    env2 = Env()
    env2.eng.submit(intent())
    env2.venue.script_cancel(("FILL_THEN_CANCEL", "3", "0.45"))
    assert env2.eng.cancel(k) == S.CANCELLED and env2.eng.ledger(k).filled_size == D("3")


def test_settlement_filled_quantity_only():
    from settlement.types import OfficialResolution
    env = Env()
    it = intent(side="NO", risk_decision_id="risk-no")
    env.book = RiskApprovalBook([approval("risk-no", side="NO")])
    eng = env.restart()
    k, coid = ids(it)
    eng.submit(it)
    env.venue.fill(coid, "3", "0.40", fee="0.02")
    eng.reconcile(k)
    assert eng.state(k) == S.PARTIALLY_FILLED
    ref = SettlementReference.from_official(OfficialResolution(BTC, "yes", 83600.12, "kalshi_market_api"))
    env.faults.arm("before_settlement_update")
    crash(eng.settle, ref)
    eng = env.restart()
    assert eng.state(k) == S.PARTIALLY_FILLED
    assert eng.settle(ref)[k] == S.CLOSED
    lg = eng.ledger(k)
    assert lg.settlement_result == "yes" and lg.settlement_price == 0 and lg.open_size == 0      # NO loses
    assert lg.realized_pnl == D("0") - D("1.20") - D("0.02") and lg.accounting_complete
    path = [e["new_state"] for e in eng.j.events(k, "TRANSITION")]
    assert path[-2:] == ["SETTLEMENT_PENDING", "CLOSED"]
    assert eng.settle(ref)[k] == S.CLOSED and eng.j.count("SETTLEMENT") == 1                       # idempotent
    try:
        SettlementReference.from_official(OfficialResolution(BTC, None)); raise AssertionError("unsettled accepted")
    except ValueError:
        pass


# ═══════════════════ 31-34 risk, second intent, persistence of unsafe states ═══════════════════
def test_risk_approval_required():
    for book, why in (([], "RISK_APPROVAL_MISSING"), ([approval(approved=False)], "RISK_VETO"),
                      ([approval(snap="b" * 64)], "RISK_SNAPSHOT_MISMATCH"),
                      ([approval(exp=T0 + 1)], "RISK_APPROVAL_EXPIRED")):
        env = Env(approvals=book)
        it = intent()
        assert env.eng.submit(it) == S.REJECTED and env.venue.submit_attempts == {}
        assert [e["reason"] for e in env.eng.j.events(ids(it)[0], "TRANSITION")][-1].startswith(why), why


def test_second_active_intent_rejected():
    env = Env(approvals=[approval(), approval("risk-2")])
    a, b = intent(), intent("i2", risk_decision_id="risk-2")
    ka, ca = ids(a)
    assert env.eng.submit(a) == S.ACKNOWLEDGED
    assert env.eng.submit(b) == S.REJECTED and ids(b)[1] not in env.venue.submit_attempts
    env.eng.cancel(ka)
    c = intent("i3", risk_decision_id="risk-2")
    assert env.eng.submit(c) == S.ACKNOWLEDGED                                 # unlocked once the first ended flat


def test_unknown_survives_restart():
    env = Env()
    env.venue.script_submit("TIMEOUT_NOT_RECEIVED")
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    for _ in range(3):
        eng = env.restart()
        assert eng.state(k) == S.HALTED_FOR_RECONCILIATION
        eng.recover()
        assert eng.state(k) == S.HALTED_FOR_RECONCILIATION and eng.market_lock_status("BTC", BTC)[0]
    assert env.venue.submit_attempts == {coid: 1}


def test_mismatch_survives_restart():
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.venue.fill(coid, "5", "0.45")
    env.venue.position_mode = ("CONFLICT", "YES", "2")
    env.eng.reconcile(k)
    for _ in range(2):
        eng = env.restart()
        assert eng.state(k) == S.HALTED_FOR_RECONCILIATION
        assert eng.j.reconstruct()[k].last_verdict == "POSITION_MISMATCH"
        locked, why = eng.market_lock_status("BTC", BTC)
        assert locked and any("POSITION_MISMATCH" in w for w in why)
    env.venue.position_mode = "STALE"                                          # stale evidence never releases it
    env.venue.capture_position(BTC)
    assert eng.reconcile(k) == "ACCOUNTING_INCOMPLETE" and eng.state(k) == S.HALTED_FOR_RECONCILIATION


# ═══════════════════ other adapter scenarios ═══════════════════
def test_adapter_query_failures():
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.venue.fail_queries(1)                                                  # adapter timeout during reconcile
    assert env.eng.reconcile(k) == "ACCOUNTING_INCOMPLETE" and env.eng.state(k) == S.ACKNOWLEDGED
    assert env.eng.market_lock_status("BTC", BTC)[0]
    env.venue.position_mode = "FAIL"                                           # position UNKNOWN, never zero
    env.venue.fill(coid, "5", "0.45")
    assert env.eng.reconcile(k) == "ACCOUNTING_INCOMPLETE" and env.eng.state(k) == S.ACKNOWLEDGED
    env.venue.position_mode = "LIVE"
    assert env.eng.reconcile(k) == "RECOVERABLE_DIFFERENCE" and env.eng.state(k) == S.FILLED
    # a pre-submit check that cannot verify the venue position fails closed
    env2 = Env()
    env2.venue.position_mode = "FAIL"
    assert env2.eng.submit(intent()) == S.REJECTED and env2.venue.submit_attempts == {}
    env3 = Env()
    env3.venue.script_submit("REJECT")
    assert env3.eng.submit(intent()) == S.REJECTED                             # a definitive rejection
    # adapter restart: a NEW adapter instance on the same venue sees the same truth
    env.eng.adapter = PaperExecutionAdapter(env.venue)
    assert env.eng.reconcile(k) == "CONSISTENT"
    v2 = PaperVenue.load(_save(env.venue))
    assert v2.orders == env.venue.orders and v2.filled(coid) == D("5")


def _save(venue):
    p = os.path.join(tmpdir(), "venue.json")
    venue.save(p)
    return p


def test_audit_provenance():
    env = Env()
    it = intent()
    k, coid = ids(it)
    rec = AuditRecord(market=BTC, asset="BTC", checkpoint="T-300", raw_probability="0.61",
                      calibrated_probability="0.58", predicted_direction="UP", market_price="0.48", bid="0.47",
                      ask="0.49", available_depth="120", estimated_fee="0.02", estimated_slippage="0.00",
                      estimated_net_ev="0.05", decision="CALL", risk_approved=True, risk_reason="within limits")
    env.eng.submit(it, audit=rec)
    env.venue.fill(coid, "5", "0.48", fee="0.02")
    env.eng.reconcile(k)
    env.eng.settle(SettlementReference(BTC, "no"))
    v = env.eng.audit_view("i1")
    assert v["prediction"]["calibrated_probability"] == "0.58" and v["decision"]["decision"] == "CALL"
    assert v["execution"]["filled_size"] == "5" and v["execution"]["submitted_size"] == "5"
    assert v["outcome"]["prediction_correct"] is False and v["outcome"]["pnl_authoritative"] is True
    assert v["outcome"]["trade_pnl"] == str(D("0") - D("2.40") - D("0.02"))
    assert v["provenance"]["model_fingerprint"] == "ab" * 16 and v["provenance"]["risk_snapshot_hash"] == H
    assert prediction_correct("UP", None) is None and prediction_correct(None, "yes") is None
    assert AuditRecord(market=BTC, asset="BTC").to_dict()["calibrated_probability"] is None   # not supplied: None


# ═══════════════════ failure-injection harness ═══════════════════
def test_failure_injection_harness():
    """A crash at EVERY fault point, followed by a restart: the journal stays a valid chain and no client order id
    is ever submitted twice."""
    reached = set()
    for point in FAULT_POINTS:
        env = Env()
        it = intent()
        k, coid = ids(it)
        env.faults.arm(point)
        try:
            env.eng.submit(it)
            env.venue.fill(coid, "2", "0.45", fee="0.01")
            env.eng.reconcile(k)
            env.eng.cancel(k)
            env.eng.settle(SettlementReference(BTC, "yes"))
            env.eng.recover()
        except SimulatedCrash:
            reached.add(point)
        eng = env.restart()
        eng.j.reconstruct()                                                    # a valid chain after the crash
        eng.recover()
        eng.submit(it)
        eng.recover()
        assert env.venue.submit_attempts.get(coid, 0) <= 1, (point, env.venue.submit_attempts)
        assert eng.state(k) is not None
    assert reached == set(FAULT_POINTS), set(FAULT_POINTS) - reached


# ═══════════════════ 35 structural: no live write path ═══════════════════
FORBIDDEN_IMPORTS = {"requests", "http", "urllib", "urllib3", "socket", "ssl", "httpx", "aiohttp", "websocket",
                     "websockets", "asyncio", "cryptography", "hmac", "subprocess", "kalshi_dashboard", "kalshi_bot",
                     "kalshi_api_learn", "market_data", "perp_data", "microstructure", "feature_eval", "perp_live",
                     "discord"}
FORBIDDEN_LITERALS = ("http://", "https://", "wss://", "ws://", "/portfolio", "/orders", "/transfer", "trade-api",
                      "kalshi.com", "elections", "KALSHI-ACCESS", "api_key", "private_key", "api_secret")
FORBIDDEN_CALLS = {"post", "put", "patch", "delete", "request", "urlopen", "create_connection", "sign"}


def _exec_files():
    d = os.path.join(HERE, "execution")
    return sorted(os.path.join(d, f) for f in os.listdir(d) if f.endswith(".py"))


def test_no_live_write_path():
    for f in _exec_files():
        tree = ast.parse(open(f, encoding="utf-8").read())
        mods = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
               {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        bad = {m for m in mods if m.split(".")[0] in FORBIDDEN_IMPORTS or m in ("market_data.transport",
                                                                                "market_data.transport.kalshi_auth")}
        assert not bad, (f, bad)
        lits = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
        for s in lits:
            low = s.lower()
            assert not any(x.lower() in low for x in FORBIDDEN_LITERALS), (f, s[:60])
            assert s.strip().upper() not in ("POST", "PUT", "PATCH", "DELETE"), (f, s)
        called = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
        called |= {n.func.id for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        assert not called & FORBIDDEN_CALLS, (f, called & FORBIDDEN_CALLS)
    try:
        FutureKalshiExecutionAdapter(); raise AssertionError("live adapter constructed")
    except LiveExecutionUnavailable:
        pass
    stub = object.__new__(FutureKalshiExecutionAdapter)
    for m in ("submit_order", "cancel_order", "get_order", "get_fills", "get_positions", "get_balance", "reconcile"):
        try:
            getattr(stub, m)("x"); raise AssertionError(f"live {m} reachable")
        except LiveExecutionUnavailable:
            pass
    from kalshi_core import execution as kx
    assert kx.LIVE_EXECUTION_AVAILABLE is False
    try:
        kx.get_execution_engine("LIVE"); raise AssertionError("LIVE engine returned")
    except kx.LiveExecutionUnavailable:
        pass
    assert PaperExecutionAdapter.writable_venue is False
    # nothing in production (or in the data / research layers) imports the execution package
    prod = ["kalshi_dashboard.py", "kalshi_backtest.py", "kalshi_bot.py", "run_local.py", "strategy_fingerprint.py",
            "collect_market_data.py", "collect_research_data.py", "perp_live.py", "perp_shadow.py",
            "perp_probability.py"]
    for d in ("kalshi_core", "settlement", "market_data", "perp_data", "microstructure", "feature_eval"):
        for root, dirs, files in os.walk(os.path.join(HERE, d)):
            dirs[:] = [x for x in dirs if x != "__pycache__"]
            prod += [os.path.relpath(os.path.join(root, x), HERE) for x in files if x.endswith(".py")]
    for f in prod:
        tree = ast.parse(open(os.path.join(HERE, f), encoding="utf-8").read())
        mods = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
               {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        assert not any(m == "execution" or m.startswith("execution.") for m in mods), f"{f} imports execution"
    code = ("import sys; sys.path.insert(0, %r); import run_local, kalshi_core.signal, kalshi_core.adapter, perp_live; "
            "print(any(m == 'execution' or m.startswith('execution.') for m in sys.modules))") % HERE
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=HERE)
    assert p.returncode == 0 and p.stdout.strip() == "False", p.stdout + p.stderr


# ═══════════════════ demonstration, fingerprints, isolation, secrets, docs ═══════════════════
def test_restart_demonstration():
    p = subprocess.run([sys.executable, os.path.join("scripts", "execution_restart_demo.py")], capture_output=True,
                       text=True, cwd=HERE, timeout=300)
    assert p.returncode == 0 and "DEMO: every expectation holds" in p.stdout, p.stdout[-1500:] + p.stderr[-800:]
    assert p.stdout.count("PASS  ") == 9


def test_execution_fingerprint():
    from execution import fingerprint as XF
    ok, problems = XF.verify()
    assert ok, problems
    b = XF.build()
    assert b["semantics"]["live_execution_available"] is False
    assert b["semantics"]["transition_graph"] and b["semantics"]["execution_key"]["version"] == "exec_key_v1"
    d = tmpdir()
    pkg = os.path.join(d, "execution")
    shutil.copytree(os.path.join(HERE, "execution"), pkg, ignore=shutil.ignore_patterns("__pycache__"))
    assert XF.verify(pkg_dir=pkg)[0]
    with open(os.path.join(pkg, "states.py"), "a", encoding="utf-8") as f:
        f.write("\nMUTATED = 1\n")
    ok2, pr2 = XF.verify(pkg_dir=pkg)
    assert not ok2 and any("states.py" in x for x in pr2)
    p = subprocess.run([sys.executable, "-m", "execution.fingerprint", "--write"], cwd=HERE, capture_output=True,
                       text=True)
    assert p.returncode == 2                                                   # never silently rewritten
    stored = json.load(open(XF.BASELINE_PATH))
    assert "journal.sqlite" not in json.dumps(stored) and "events" not in stored   # no database contents


def test_production_isolation():
    from kalshi_core import baseline
    ok, problems = baseline.verify()
    assert ok, problems
    p = subprocess.run([sys.executable, "-m", "regression.generate", "--check"], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "MATCH (47 cases)" in p.stdout, p.stdout + p.stderr
    snap = json.load(open(os.path.join(HERE, "config", "history", "step6.4_approved_baseline_snapshot.json")))
    fp = snap["fingerprints"]
    import strategy_fingerprint as sf
    assert sf.current_fingerprint(os.path.join(HERE, "kalshi_dashboard.py"))[0] == fp["legacy_strategy"]
    assert baseline.build_manifest()["fingerprints"]["extended_strategy_fingerprint"] == fp["extended_strategy"]
    for mod, key in (("settlement", "settlement"), ("market_data", "market_data"), ("perp_data", "perp_data"),
                     ("microstructure", "microstructure")):
        m = __import__(f"{mod}.fingerprint", fromlist=["x"])
        okm, prm = m.verify()
        assert okm, (mod, prm)
        assert m.build()[f"{key}_fingerprint"] == fp[key], mod                 # Step-6.4 fingerprints unchanged
    from feature_eval import fingerprint as S6
    assert S6.verify()[0] and S6.build()["step6_fingerprint"] == fp["step6"]
    assert S6.build()["existing_perp_veto_fingerprint"] == fp["existing_perp_veto"]
    from feature_eval import universe as UV
    assert UV.load_frozen()["fingerprint"] == fp["feature_universe"]
    for f, h in snap["files_sha256"].items():                                  # every Step-6.4 baseline file unchanged
        import hashlib
        assert hashlib.sha256(open(os.path.join(HERE, f), "rb").read()).hexdigest() == h, f


def test_no_secrets_or_artifacts():
    import re
    tracked = subprocess.run(["git", "ls-files"], cwd=HERE, capture_output=True, text=True).stdout.split()
    files = tracked or [os.path.relpath(os.path.join(r, f), HERE) for r, ds, fs in os.walk(HERE)
                        for f in fs if ".git" not in r and "__pycache__" not in r]
    for f in files:
        low = f.lower()
        assert not low.endswith((".sqlite", ".sqlite3", ".db", ".pem", ".key", ".pyc")), f
        assert not low.endswith(".env") and "__pycache__" not in low, f
    pat = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----\s*\n(?!NOTAREALKEYMATERIAL)[A-Za-z0-9+/=]{40,}")
    for f in files:
        p = os.path.join(HERE, f)
        if not os.path.isfile(p) or os.path.getsize(p) > 3_000_000:
            continue
        try:
            txt = open(p, encoding="utf-8").read()
        except (UnicodeDecodeError, OSError):
            continue
        assert not pat.search(txt), f
        assert not re.search(r"\bsk_live_[A-Za-z0-9]{8,}|AKIA[0-9A-Z]{16}|ghp_[A-Za-z0-9]{30,}", txt), f
    user_path = re.compile("C:" + r"\\Users\\|/" + "home/[a-z]|/" + "Users/[A-Za-z]")   # built so this file never matches
    for f in files:                                                            # no local user path in any shipped file
        p = os.path.join(HERE, f)
        if not os.path.isfile(p) or os.path.getsize(p) > 3_000_000:
            continue
        try:
            txt = open(p, encoding="utf-8").read()
        except (UnicodeDecodeError, OSError):
            continue
        assert not user_path.search(txt), f


def test_docs():
    doc = open(os.path.join(HERE, "docs", "EXECUTION_ARCHITECTURE.md"), encoding="utf-8").read()
    low = doc.lower()
    for s in ("system boundary", "orderintent", "execution key", "client_order_id", "state diagram", "transition table",
              "journal", "position ledger", "paper", "reconciliation", "market lock", "restart recovery",
              "failure injection", "execution_unknown", "risk boundary", "settlement", "cannot place live orders",
              "step 6.6", "step 6.7", "live execution", "e1", "e20", "fingerprint"):
        assert s in low, s
    for (a, b) in TRANSITIONS:
        assert f"| {a.value if a else '(start)'} | {b.value} |" in doc, (a, b)
    for f in ("ARCHITECTURE.md", "BASELINE.md", "ROADMAP.md"):
        assert "EXECUTION_ARCHITECTURE" in open(os.path.join(HERE, "docs", f), encoding="utf-8").read(), f
    mut = json.load(open(os.path.join(HERE, "analysis_output", "execution_mutation_results.json")))
    assert mut["all_caught_and_controls_pass"] is True
    assert {m["id"] for m in mut["mutations"]} >= {f"E{i}" for i in range(1, 29)}
    for s in ("fee_resolution", "6.5.1", "fill identity", "authoritative absence", "central reachability", "e28"):
        assert s in low, s


# ═══════════════════ Step 6.5.1: reconciliation / accounting hardening ═══════════════════
def _filled_env(fee="0.05", qty="5"):
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.venue.fill(coid, qty, "0.45", fee=fee)
    env.eng.reconcile(k)
    return env, k, coid


def _halted_mismatch(env, k, verdict="FILL_MISMATCH"):
    assert env.eng.state(k) == S.HALTED_FOR_RECONCILIATION, env.eng.state(k)
    assert env.eng.j.reconstruct()[k].last_verdict == verdict
    assert env.eng.market_lock_status("BTC", BTC)[0]


def test_fill_identity():
    """651.1-3: a fill's immutable identity (order_id, client_order_id, fee_id, qty, price) is reconciled."""
    # 1. a previously journaled fill changes order_id (qty / price unchanged)
    env, k, coid = _filled_env()
    assert env.eng.state(k) == S.FILLED
    oid = env.venue.fills[coid][0]["order_id"]
    env.venue.fills[coid][0]["order_id"] = "po-someone-else"
    n_fill = env.eng.j.count("FILL")
    assert env.eng.reconcile(k) == "FILL_MISMATCH"
    _halted_mismatch(env, k)
    assert env.eng.j.count("FILL") == n_fill and env.eng.ledger(k).fill_identity(
        env.eng.ledger(k).fill_ids()[0])["order_id"] == oid                    # the journal is never rewritten
    # 2. a previously journaled fill changes client_order_id
    env, k, coid = _filled_env()
    env.venue.fills[coid][0]["client_order_id"] = "x1" + "0" * 30
    assert env.eng.reconcile(k) == "FILL_MISMATCH"
    _halted_mismatch(env, k)
    # 3. a NEW fill references the wrong order_id (looks valid otherwise) -> never accepted
    env, k, coid = _filled_env(qty="2")
    assert env.eng.state(k) == S.PARTIALLY_FILLED
    env.venue.fill(coid, "1", "0.45")
    env.venue.fills[coid][-1]["order_id"] = "po-another-order"
    assert env.eng.reconcile(k) == "FILL_MISMATCH" and env.eng.ledger(k).filled_size == D("2")
    _halted_mismatch(env, k)
    # 4. a NEW fill references the wrong client_order_id
    env, k, coid = _filled_env(qty="2")
    env.venue.fill(coid, "1", "0.45")
    env.venue.fills[coid][-1]["client_order_id"] = "x1" + "f" * 30
    assert env.eng.reconcile(k) == "FILL_MISMATCH" and env.eng.ledger(k).filled_size == D("2")
    _halted_mismatch(env, k)
    # the very first fill of an order is validated too (nothing journaled yet)
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.venue.fill(coid, "5", "0.45")
    env.venue.fills[coid][0]["order_id"] = "po-another-order"
    assert env.eng.reconcile(k) == "FILL_MISMATCH" and env.eng.ledger(k).filled_size == 0
    # the venue reporting a different order id for our client_order_id than the journal recorded
    env, k, coid = _filled_env(qty="2")
    env.venue.orders[coid]["order_id"] = "po-renamed"
    for f in env.venue.fills[coid]:
        f["order_id"] = "po-renamed"
    assert env.eng.reconcile(k) == "FILL_MISMATCH"
    _halted_mismatch(env, k)
    # ledger level: every identity field is checked
    lg = ExecutionLedger("k", BTC, "BTC", "YES", "5", "0.50")
    lg.add_fill("f1", "2", "0.40", "o1", "c1", "fee-f1")
    assert lg.add_fill("f1", "2", "0.40", "o1", "c1", "fee-f1") == "DUPLICATE"
    for args in (("o2", "c1", "fee-f1"), ("o1", "c2", "fee-f1"), ("o1", "c1", "fee-f2")):
        try:
            lg.add_fill("f1", "2", "0.40", *args); raise AssertionError(f"identity change accepted {args}")
        except FillConflict:
            pass


def test_known_fee_contradiction():
    """651.4 / 651.7 / 651.15: KNOWN -> SAME KNOWN is idempotent; KNOWN -> DIFFERENT KNOWN halts and survives restart."""
    env, k, coid = _filled_env(fee="0.05")
    env.eng.set_mark(k, "0.60")
    for _ in range(3):
        assert env.eng.reconcile(k) == "CONSISTENT"
    lg = env.eng.ledger(k)
    assert lg.fees_paid == D("0.05") and env.eng.j.count("FEE") == 1 and env.eng.j.count("FEE_RESOLUTION") == 0
    env.venue.fills[coid][0]["fee"] = "0.07"
    assert env.eng.reconcile(k) == "FILL_MISMATCH"
    _halted_mismatch(env, k)
    assert env.eng.ledger(k).fees_paid == D("0.05")                           # history never silently changed
    assert env.eng.j.count("FEE") == 1 and env.eng.j.count("FEE_RESOLUTION") == 0
    for _ in range(2):                                                         # 15: survives restart, locked
        eng = env.restart()
        assert eng.state(k) == S.HALTED_FOR_RECONCILIATION
        assert eng.market_lock_status("BTC", BTC)[0] and eng.ledger(k).fees_paid == D("0.05")
        assert eng.reconcile(k) == "FILL_MISMATCH" and eng.state(k) == S.HALTED_FOR_RECONCILIATION
    lg = ExecutionLedger("k", BTC, "BTC", "YES", "5", "0.50")
    lg.add_fill("f1", "2", "0.40", fee_id="fee-f1")
    lg.add_fee("fee-f1", "0.05", "f1")
    assert lg.resolve_fee("fee-f1", "0.05") == "DUPLICATE" and lg.fees_paid == D("0.05")
    try:
        lg.resolve_fee("fee-f1", "0.07"); raise AssertionError("known fee rewritten")
    except FillConflict:
        pass


def test_unknown_fee_resolution():
    """651.5 / 651.6 / 651.13 / 651.14: UNKNOWN -> KNOWN resolves the SAME fee (append-only), never a second charge."""
    env, k, coid = _filled_env(fee=UNKNOWN)
    assert env.eng.ledger(k).fees_paid is UNKNOWN
    env.venue.fills[coid][0]["fee"] = "UNKNOWN"
    assert env.eng.reconcile(k) == "ACCOUNTING_INCOMPLETE"                     # D: UNKNOWN -> UNKNOWN
    assert env.eng.ledger(k).fees_paid is UNKNOWN and env.eng.state(k) == S.FILLED
    env.venue.fills[coid][0]["fee"] = "0.05"
    assert env.eng.reconcile(k) == "RECOVERABLE_DIFFERENCE"                    # C: new evidence, not a contradiction
    lg = env.eng.ledger(k)
    assert lg.fees_paid == D("0.05") and lg.fees_paid != D("0.10") and env.eng.state(k) == S.FILLED
    fee_ev = env.eng.j.events(k, "FEE")
    res_ev = env.eng.j.events(k, "FEE_RESOLUTION")
    assert len(fee_ev) == 1 and fee_ev[0]["payload"]["amount"] == "UNKNOWN"   # original observation preserved
    assert len(res_ev) == 1 and res_ev[0]["payload"]["amount"] == "0.05"
    assert res_ev[0]["payload"]["resolves_event_id"] == fee_ev[0]["event_id"]
    env.eng.set_mark(k, "0.60")
    lg = env.eng.ledger(k)
    assert lg.accounting_complete and lg.unrealized_pnl == D("5") * D("0.60") - D("2.25") - D("0.05")
    for _ in range(3):                                                         # 14: repeated reconciliation
        assert env.eng.reconcile(k) == "CONSISTENT"
    assert env.eng.j.count("FEE_RESOLUTION") == 1 and env.eng.ledger(k).fees_paid == D("0.05")
    env.venue.fills[coid][0]["fee"] = "UNKNOWN"                                # the venue later omits it again
    assert env.eng.reconcile(k) == "CONSISTENT" and env.eng.ledger(k).fees_paid == D("0.05")
    env.venue.fills[coid][0]["fee"] = "0.05"
    for _ in range(2):                                                         # 13: survives restart
        eng = env.restart()
        assert eng.ledger(k).fees_paid == D("0.05") and eng.reconcile(k) == "CONSISTENT"
    env.venue.fills[coid][0]["fee"] = "0.06"                                   # a resolved fee is now KNOWN
    assert eng.reconcile(k) == "FILL_MISMATCH" and eng.state(k) == S.HALTED_FOR_RECONCILIATION
    # two fills, only one fee resolves: still UNKNOWN until both are known, never summed with UNKNOWN as 0
    env = Env()
    it = intent()
    k, coid = ids(it)
    env.eng.submit(it)
    env.venue.fill(coid, "2", "0.40", fee=UNKNOWN)
    env.venue.fill(coid, "3", "0.50", fee=UNKNOWN)
    env.eng.reconcile(k)
    env.venue.fills[coid][0]["fee"] = "0.02"
    env.eng.reconcile(k)
    assert env.eng.ledger(k).fees_paid is UNKNOWN
    env.venue.fills[coid][1]["fee"] = "0.03"
    env.eng.reconcile(k)
    assert env.eng.ledger(k).fees_paid == D("0.05") and env.eng.j.count("FEE_RESOLUTION") == 2
    env.eng.settle(SettlementReference(BTC, "yes"))
    assert env.eng.ledger(k).realized_pnl == D("5") - D("2.30") - D("0.05")
    # ledger level
    lg = ExecutionLedger("k", BTC, "BTC", "YES", "5", "0.50")
    lg.add_fill("f1", "2", "0.40", fee_id="fee-f1")
    lg.add_fee("fee-f1", UNKNOWN, "f1")
    assert lg.resolve_fee("fee-f1", "0.05") == "RESOLVED" and lg.fees_paid == D("0.05")
    assert lg.resolve_fee("fee-f1", "0.05") == "DUPLICATE" and lg.fees_paid == D("0.05")
    try:
        lg.resolve_fee("fee-unseen", "0.01"); raise AssertionError("resolution of an unobserved fee accepted")
    except FillConflict:
        pass


def test_fee_id_remap():
    """651.8: one logical fee per fill; the same fill switching fee_id fails closed."""
    env, k, coid = _filled_env(fee="0.05")
    env.venue.fills[coid][0]["fee_id"] = "fee-other"
    assert env.eng.reconcile(k) == "FILL_MISMATCH"
    _halted_mismatch(env, k)
    assert env.eng.ledger(k).fees_paid == D("0.05")
    lg = ExecutionLedger("k", BTC, "BTC", "YES", "5", "0.50")
    lg.add_fill("f1", "2", "0.40", fee_id="fee-a")
    lg.add_fee("fee-a", "0.01", "f1")
    try:
        lg.add_fee("fee-b", "0.01", "f1"); raise AssertionError("second fee for one fill accepted")
    except FillConflict:
        pass


def test_absence_after_ack():
    """651.9 / 651.10: authoritative absence of an order the journal acknowledged is a contradiction, not REJECTED."""
    env = Env()
    it = intent()
    k, coid = ids(it)
    assert env.eng.submit(it) == S.ACKNOWLEDGED
    del env.venue.orders[coid]
    env.venue.lookup_authoritative = True
    assert env.eng.reconcile(k) == "FILL_MISMATCH"                             # no InvalidTransition escapes
    _halted_mismatch(env, k)
    eng = env.restart()
    assert eng.recover()[k] == "FILL_MISMATCH" and eng.state(k) == S.HALTED_FOR_RECONCILIATION
    # HALTED after an acknowledged order (ambiguous cancel): HALTED -> REJECTED is a defined transition, but the
    # journal proves the venue acknowledged the order, so absence is still a contradiction
    env = Env()
    env.eng.submit(intent())
    env.venue.script_cancel("LOST_ACK")
    assert env.eng.cancel(k) == S.HALTED_FOR_RECONCILIATION
    del env.venue.orders[coid]
    env.venue.lookup_authoritative = True
    assert env.eng.reconcile(k) == "FILL_MISMATCH" and env.eng.state(k) == S.HALTED_FOR_RECONCILIATION
    assert "REJECTED" not in [e["new_state"] for e in env.eng.j.events(k, "TRANSITION")]
    # a partially filled order disappearing
    env, k, coid = _filled_env(qty="2")
    del env.venue.orders[coid]
    env.venue.lookup_authoritative = True
    assert env.eng.reconcile(k) == "FILL_MISMATCH"
    _halted_mismatch(env, k)
    # an intent that expired BEFORE submission is never queried (nothing was sent): no false halt, no lock
    env = Env()
    env.faults.arm("before_journal_write", nth=5)
    crash(env.eng.submit, it)
    env.clock.advance(70_000)
    eng = env.restart()
    assert eng.submit(it) == S.EXPIRED
    env.venue.lookup_authoritative = True
    assert eng.reconcile(k) == "CONSISTENT" and eng.state(k) == S.EXPIRED and not eng.market_lock_status("BTC", BTC)[0]


def test_absence_after_ambiguous_submit():
    """651.11: HALTED after an ambiguous submit (never acknowledged) + authoritative absence -> REJECTED is safe."""
    for mode in ("TIMEOUT_NOT_RECEIVED", "LOST_ACK"):
        env = Env()
        env.venue.script_submit(mode)
        it = intent()
        k, coid = ids(it)
        assert env.eng.submit(it) == S.HALTED_FOR_RECONCILIATION
        env.venue.orders.pop(coid, None)                                       # the venue proves nothing exists
        env.venue.lookup_authoritative = True
        assert env.eng.reconcile(k) == "RECOVERABLE_DIFFERENCE" and env.eng.state(k) == S.REJECTED, mode
        assert not env.eng.market_lock_status("BTC", BTC)[0] and env.venue.submit_attempts == {coid: 1}


def test_unreachable_implications():
    """651.12: every venue-implied state is validated centrally against the transition graph -> mismatch, never an
    InvalidTransition (several implications, not only ACKNOWLEDGED -> REJECTED)."""
    from execution.adapter import AdapterSnapshot, OrderLookup, OrderView, PositionReport
    from execution.reconcile import assess
    it = intent()
    k, coid = ids(it)
    zero = {"YES": D(0), "NO": D(0)}

    def snap(status, filled, yes, fills=()):
        o = OrderView("po-1", coid, BTC, "YES", D("5"), D("0.5"), status, D(filled))
        return AdapterSnapshot(OrderLookup(o, True), tuple(fills), PositionReport(BTC, D(yes), D(0)))

    def ledger_with(qty):
        lg = ExecutionLedger(k, BTC, "BTC", "YES", "5", "0.50")
        if qty:
            lg.add_fill("pf-1", qty, "0.45", "po-1", coid, "fee-1")
            lg.add_fee("fee-1", "0.01", "pf-1")
        return lg

    from execution.adapter import FillReport
    f5 = FillReport("pf-1", "po-1", coid, D("5"), D("0.45"), D("0.01"), "fee-1", T0)
    cases = [(S.CANCELLED, snap("RESTING", "0", "0"), ledger_with(None), "ACKNOWLEDGED"),
             (S.FILLED, snap("CANCELLED", "5", "5", [f5]), ledger_with("5"), "CANCELLED"),
             (S.EXPIRED, snap("FILLED", "5", "5", [f5]), ledger_with(None), "FILLED"),
             (S.SETTLEMENT_PENDING, snap("RESTING", "5", "5", [f5]), ledger_with("5"), "FILLED"),
             (S.CANCELLED, snap("REJECTED", "0", "0"), ledger_with(None), "REJECTED")]
    for st, sn, lg, implied in cases:
        a = assess(st, it, coid, sn, lg, zero, journal_order_id="po-1", acknowledged=True)
        assert a.verdict == "FILL_MISMATCH" and a.implied_state.value == implied and a.details.get("unreachable"), \
            (st, implied, a.verdict, a.reason)
    # authoritative absence proposing REJECTED from ACKNOWLEDGED when the caller supplies no history (legacy call)
    absent = AdapterSnapshot(OrderLookup(None, True), (), PositionReport(BTC, D(0), D(0)))
    a = assess(S.ACKNOWLEDGED, it, coid, absent, ledger_with(None), zero)
    assert a.verdict == "FILL_MISMATCH" and a.implied_state == S.REJECTED
    assert assess(S.HALTED_FOR_RECONCILIATION, it, coid, absent, ledger_with(None), zero).implied_state == S.REJECTED
    assert assess(S.EXECUTION_UNKNOWN, it, coid, absent, ledger_with(None), zero).verdict == "RECOVERABLE_DIFFERENCE"
    # through the engine: a CANCELLED order the venue later shows RESTING
    env = Env()
    env.eng.submit(it)
    assert env.eng.cancel(k) == S.CANCELLED
    env.venue.orders[coid]["status"] = "RESTING"
    assert env.eng.reconcile(k) == "FILL_MISMATCH"                             # no InvalidTransition
    _halted_mismatch(env, k)
    rec = env.eng.j.events(k, "RECONCILIATION")[-1]
    assert rec["reconciliation_status"] == "FILL_MISMATCH" and "unreachable" in rec["reconciliation_reason"]


def test_journal_schema_migration():
    """Journal schema v1 (Step 6.5) is migrated meta-only; an unknown / newer schema is refused."""
    d = tmpdir()
    p = os.path.join(d, "j.sqlite")
    env_j = Journal(p)
    it = intent()
    k, coid = ids(it)
    with env_j.transaction():
        env_j.record_intent(it, k, coid)
        env_j.append({"kind": "MARK", "event_id": "m-1", "execution_key": k, "ts_ms": T0, "payload": {"mark": "0.5"}})
    env_j.conn.execute("UPDATE meta SET value='1' WHERE key='schema_version'")    # a Step-6.5 journal
    before = [dict(e) for e in env_j.events()]
    env_j.close()
    j2 = Journal(p)
    meta = dict(j2.conn.execute("SELECT key, value FROM meta").fetchall())
    assert meta["schema_version"] == "2" and meta["migrated_from"] == "1"
    assert [dict(e) for e in j2.events()] == before and j2.intent_row(intent_id="i1") is not None
    j2.conn.execute("UPDATE meta SET value='3' WHERE key='schema_version'")
    j2.close()
    try:
        Journal(p); raise AssertionError("a newer journal schema was opened")
    except JournalError:
        pass


def test_previous_stages():
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 23):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-600:], p.stderr[-600:])


TESTS = [
    ("dupintent", "1 duplicate intent: idempotent replay, one order; reused intent_id with other content refused", test_duplicate_intent),
    ("identity", "2 deterministic execution key / client_order_id (order, decimal text, processes, persisted)", test_deterministic_identity),
    ("dupfill", "3 duplicate fill notifications never double-count; conflicting duplicate refused", test_duplicate_fill),
    ("partialfill", "4-5 partial / multiple / delayed / no fills; weighted average 2@0.40 + 3@0.50 = 0.46", test_partial_fills_weighted_average),
    ("fees", "6 missing fee = UNKNOWN (never 0); duplicate fee never double-counted", test_missing_fee_and_duplicate_fee),
    ("mark", "7 missing mark = UNKNOWN; PnL not authoritative", test_missing_mark),
    ("transition", "8 invalid transitions fail closed; forged history refused", test_invalid_transition),
    ("lostack", "9 lost acknowledgement -> UNKNOWN -> HALTED; proven by lookup; never resubmitted", test_lost_ack),
    ("ambsubmit", "10 ambiguous submit: halted, never retried; only proof releases it", test_ambiguous_submit),
    ("ambcancel", "11 ambiguous cancellation (done / not done) halts and reconciles", test_ambiguous_cancel),
    ("crashbefore", "12 crash before submit (before / after SUBMITTING is persisted)", test_crash_before_submit),
    ("crashafter", "13 crash after submit / before the ACK is persisted", test_crash_after_submit),
    ("crashack", "14 crash after ACK; filled while down -> FILLED restored", test_crash_after_ack),
    ("crashpartial", "15 crash after a partial fill / before fill persistence", test_crash_after_partial_fill),
    ("rebuild", "16 restart reconstruction is deterministic; journal append-only", test_restart_reconstruction),
    ("noduprestart", "17 duplicate order prevented after restarts", test_duplicate_order_prevented_after_restart),
    ("conflictpos", "18 conflicting observed position -> POSITION_MISMATCH, halted, locked", test_conflicting_position),
    ("conflictfill", "19 conflicting fills (price cap, rewritten fill) -> FILL_MISMATCH", test_conflicting_fills),
    ("lockunknown", "20 market lock while execution unknown; survives restart", test_market_lock_while_unknown),
    ("expired", "21 an expired intent cannot submit (also after a restart)", test_expired_intent),
    ("reducerisk", "22-24 execution never increases size, widens price or flips side; intent validation", test_execution_only_reduces_risk),
    ("rollback", "25 journal transaction rollback (atomic multi-event writes)", test_journal_rollback),
    ("dupevent", "26 duplicate journal event: identical no-op, different content refused", test_duplicate_journal_event),
    ("cancelpartial", "27 cancelled partial fill retains (and settles) the filled exposure", test_cancel_partial_keeps_exposure),
    ("cancelnofill", "28 a never-filled cancelled order creates no position", test_cancel_never_filled),
    ("fillcancel", "29 fill during the cancel race", test_fill_during_cancel),
    ("settlement", "30 settlement applies only to the filled quantity; idempotent; crash-safe", test_settlement_filled_quantity_only),
    ("risk", "31 missing / vetoed / mismatched / expired risk approval rejected", test_risk_approval_required),
    ("secondintent", "32 a second active intent for the same market is rejected", test_second_active_intent_rejected),
    ("unknownrestart", "33 UNKNOWN / HALTED survives restart", test_unknown_survives_restart),
    ("mismatchrestart", "34 reconciliation mismatch survives restart; stale evidence never releases it", test_mismatch_survives_restart),
    ("nolivewrite", "35 no live-write network path: structural scan, stub refuses, production never imports execution", test_no_live_write_path),
    ("queries", "36 adapter timeout / position failure / stale / restart / definitive rejection", test_adapter_query_failures),
    ("audit", "37 prediction / decision / execution provenance kept separate", test_audit_provenance),
    ("faults", "38 failure injection at every fault point keeps the journal valid and never double-submits", test_failure_injection_harness),
    ("demo", "39 clean-restart demonstration (separate processes)", test_restart_demonstration),
    ("fingerprint", "40 execution architecture fingerprint", test_execution_fingerprint),
    ("isolation", "41 production isolation: strategy / perp-veto / Step-6.4 fingerprints, 47 fixtures, LIVE", test_production_isolation),
    ("secrets", "42 no secrets / databases / artifacts shipped", test_no_secrets_or_artifacts),
    ("docs", "43 docs/EXECUTION_ARCHITECTURE.md + mutation results", test_docs),
    ("fillidentity", "45 [6.5.1] fill identity: changed order_id / client_order_id, wrong-order new fills -> FILL_MISMATCH", test_fill_identity),
    ("feecontra", "46 [6.5.1] known fee: same -> idempotent; changed -> FILL_MISMATCH, halted, survives restart", test_known_fee_contradiction),
    ("feeresolve", "47 [6.5.1] UNKNOWN fee resolves append-only to known; no double count; restart / repeat safe", test_unknown_fee_resolution),
    ("feeremap", "48 [6.5.1] the same fill switching fee_id fails closed", test_fee_id_remap),
    ("ackabsence", "49 [6.5.1] authoritative absence after ACK -> mismatch / halt, never InvalidTransition", test_absence_after_ack),
    ("haltabsence", "50 [6.5.1] HALTED ambiguous submit + authoritative absence -> REJECTED", test_absence_after_ambiguous_submit),
    ("unreachable", "51 [6.5.1] unreachable reconciliation implications -> mismatch (central reachability)", test_unreachable_implications),
    ("migration", "52 [6.5.1] journal schema v1 -> v2 meta-only migration; newer schema refused", test_journal_schema_migration),
    ("previous", "44 all previous stage suites", test_previous_stages),
]


if __name__ == "__main__":
    only = None
    if "--only" in sys.argv:
        only = set(sys.argv[sys.argv.index("--only") + 1].split(","))
    try:
        for key, name, fn in TESTS:
            if only is None or key in only:
                run(name, fn)
    finally:
        for d in _DIRS:
            shutil.rmtree(d, ignore_errors=True)
    if only is None:
        print("\nAll Stage 23 tests passed.")
    else:
        print(f"\nSelected Stage 23 tests passed: {','.join(sorted(only))}")
