"""
ExecutionEngine (PAPER / SHADOW ONLY): OrderIntent -> state machine -> journal -> adapter -> fills / positions ->
reconciliation -> settlement. It is not wired into any production decision.

Core rules
    * persist first: a transition is committed BEFORE the follow-on action (SUBMITTING before the adapter is called,
      CANCEL_PENDING before the cancel is sent); the in-memory engine holds no execution truth - every decision reads
      the journal, so discarding the process loses nothing;
    * idempotent: an intent is identified by its deterministic execution key; replaying the same intent (after a
      restart too) never creates a second order or a second client_order_id; an intent_id reused with different
      content is refused;
    * ambiguous outcomes fail closed: anything other than an acknowledgement or a definitive rejection of a submit
      (and anything other than a confirmed cancel) -> EXECUTION_UNKNOWN -> HALTED_FOR_RECONCILIATION; never retried,
      never assumed to have failed or filled; only reconciliation evidence releases the halt;
    * market locks (one active entry per asset + market_ticker) are DERIVED FROM THE JOURNAL, so they survive restarts;
    * execution may only reduce risk (invariants.py); a risk approval reference is required and never manufactured.
"""
from dataclasses import dataclass
from decimal import Decimal

from execution.adapter import AmbiguousOutcome, OrderRequest, SubmitRejected
from execution.audit import prediction_correct
from execution.faults import NO_FAULTS, SimulatedCrash
from execution.identity import client_order_id, execution_key
from execution.intent import IntentError, OrderIntent
from execution.invariants import InvariantViolation, assert_order_within_intent, assert_provenance_unchanged
from execution.journal import JournalConflict
from execution.ledger import ExecutionLedger
from execution.money import UNKNOWN, from_jsonable, jsonable
from execution.reconcile import MISMATCH_VERDICTS, assess
from execution.risk import check_intent_against_approval
from execution.states import (ENGINE, OUTSTANDING, PRE_SUBMIT, RECONCILIATION, TERMINAL, UNSAFE, ExecState,
                              check_transition)

S = ExecState
ZERO = Decimal(0)
LOCKING_VERDICTS = ("POSITION_MISMATCH", "FILL_MISMATCH", "EXECUTION_UNKNOWN")


class DuplicateIntentConflict(RuntimeError):
    """The same intent_id was presented again with different content."""


class FixedClock:
    """Deterministic clock (tests / demonstrations)."""

    def __init__(self, now_ms):
        self.t = int(now_ms)

    def now_ms(self):
        return self.t

    def advance(self, ms):
        self.t += int(ms)


@dataclass(frozen=True)
class SettlementReference:
    """The official outcome of a market, referenced (never recomputed) from the Step-2 settlement layer."""
    market_ticker: str
    result: str                      # "yes" | "no"
    settlement_value: object = None  # the official expiration value as text, when known (reference only)
    source: str = ""

    @classmethod
    def from_official(cls, resolution):
        """From settlement.types.OfficialResolution (the existing settlement methodology is untouched)."""
        if resolution.result not in ("yes", "no"):
            raise ValueError("the market has no official yes / no result yet")
        v = resolution.expiration_value
        return cls(resolution.ticker, resolution.result, None if v is None else repr(v), resolution.source)


def expired(intent, now_ms):
    return now_ms >= intent.expires_at


class ExecutionEngine:
    def __init__(self, journal, adapter, risk_book, clock, faults=None):
        self.j, self.adapter, self.risk_book, self.clock = journal, adapter, risk_book, clock
        self.faults = faults or NO_FAULTS
        self._lock_cache = {}            # runtime cache only - NEVER authoritative (locks come from the journal)

    # ---------------- journal-backed helpers ----------------
    def _intent(self, key):
        row = self.j.intent_row(execution_key=key)
        return OrderIntent.from_dict(row["intent"]), row["client_order_id"]

    def _views(self):
        return self.j.reconstruct()

    def state(self, key):
        v = self._views().get(key)
        return None if v is None else v.state

    def _ledgers(self):
        out = {}
        for r in self.j.intents():
            it = r["intent"]
            out[r["execution_key"]] = ExecutionLedger(r["execution_key"], it["market_ticker"], it["asset"], it["side"],
                                                      it["requested_contracts"], it["max_limit_price"])
        for ev in self.j.events():
            lg = out.get(ev["execution_key"])
            if lg is None:
                continue
            p = ev["payload"]
            if ev["kind"] == "FILL":
                lg.add_fill(p["fill_id"], p["qty"], p["price"])
            elif ev["kind"] == "FEE":
                lg.add_fee(p["fee_id"], from_jsonable(p["amount"]), p.get("fill_id"))
            elif ev["kind"] == "MARK":
                lg.set_mark(from_jsonable(p["mark"], "mark"))
            elif ev["kind"] == "SETTLEMENT":
                lg.settle(p["result"], p.get("settlement_value"))
        return out

    def ledger(self, key):
        return self._ledgers()[key]

    def _count(self, key, kind):
        return sum(1 for e in self.j.events(key, kind))

    def _event(self, key, intent, coid, kind, **kw):
        lg = kw.pop("ledger", None)
        ev = {"kind": kind, "intent_id": intent.intent_id, "execution_key": key, "ts_ms": kw.pop("ts_ms", None)
              or self.clock.now_ms(), "market_ticker": intent.market_ticker, "asset": intent.asset,
              "side": intent.side, "client_order_id": coid, "requested_size": intent.requested_contracts,
              "limit_price": intent.max_limit_price, "adapter_name": self.adapter.name}
        if lg is not None:
            avg = lg.average_entry_price
            ev.update(filled_size=lg.filled_size, remaining_size=lg.remaining_size,
                      average_fill_price=avg if avg is not None else None, fees=lg.fees_paid)
        ev.update(kw)
        return ev

    def _transition(self, key, new, reason, actor=ENGINE, evidence=None, expect=None, also=(), order_id=None,
                    adapter_event_id=None):
        """ONE atomic journal write: the transition (+ any accompanying events). The previous state is read from the
        journal, never from memory; `expect` guards against acting on a stale view."""
        intent, coid = self._intent(key)
        prev = self.state(key)
        if expect is not None and prev != expect:
            raise JournalConflict(f"{intent.intent_id}: expected {expect}, journal says {prev}")
        check_transition(prev, new, actor, evidence)
        lg = self._ledgers()[key]
        n = self._count(key, "TRANSITION")
        ev = self._event(key, intent, coid, "TRANSITION", ledger=lg, event_id=f"t:{key}:{n}",
                         previous_state=prev.value if prev else None, new_state=S(new).value, reason=reason,
                         actor=actor, order_id=order_id, adapter_event_id=adapter_event_id,
                         payload={"evidence": evidence} if evidence else {})
        with self.j.transaction():
            self.j.append_many([ev] + list(also))
        return S(new)

    # ---------------- market locks (journal-derived, restart-safe) ----------------
    def market_lock_status(self, asset, market_ticker, exclude=None):
        views, ledgers, reasons = self._views(), self._ledgers(), []
        for r in self.j.intents():
            key = r["execution_key"]
            if key == exclude or r["asset"] != asset or r["market_ticker"] != market_ticker:
                continue
            v = views[key]
            if v.state in UNSAFE:
                reasons.append(f"{r['intent_id']}: {v.state.value}")
            elif v.state in (PRE_SUBMIT | OUTSTANDING):
                reasons.append(f"{r['intent_id']}: active {v.state.value}")
            elif v.state not in (S.CLOSED, S.REJECTED) and ledgers[key].filled_size > 0:
                reasons.append(f"{r['intent_id']}: open position {ledgers[key].filled_size} ({v.state.value})")
            if v.last_verdict in LOCKING_VERDICTS:
                reasons.append(f"{r['intent_id']}: reconciliation {v.last_verdict}")
            elif v.last_verdict == "ACCOUNTING_INCOMPLETE" and (v.last_verdict_payload or {}).get("position_unsafe") \
                    and v.state not in TERMINAL:
                reasons.append(f"{r['intent_id']}: accounting incomplete with an unsafe position")
        self._lock_cache[(asset, market_ticker)] = list(reasons)
        return bool(reasons), reasons

    # ---------------- submission ----------------
    def submit(self, intent, audit=None):
        """Accept an intent and drive it as far as it can safely go. Idempotent for the same intent."""
        if not isinstance(intent, OrderIntent):
            raise IntentError("execution accepts only a validated OrderIntent")
        key = execution_key(intent)
        coid = client_order_id(key)
        row = self.j.intent_row(intent_id=intent.intent_id)
        if row is not None:
            if row["execution_key"] != key or row["content_hash"] != intent.content_hash():
                raise DuplicateIntentConflict(f"intent {intent.intent_id} already recorded with different content")
            return self._resume(key)
        n_audit = []
        if audit is not None:
            n_audit.append(self._event(key, intent, coid, "AUDIT", event_id=f"a:{intent.intent_id}:intent",
                                       payload={"audit": audit.to_dict()}))
        with self.j.transaction():
            self.j.record_intent(intent, key, coid)
            check_transition(None, S.CREATED)
            self.j.append_many([self._event(key, intent, coid, "TRANSITION", event_id=f"t:{key}:0",
                                            previous_state=None, new_state=S.CREATED.value, reason="intent received",
                                            actor=ENGINE, payload={})] + n_audit)
        return self._advance(key)

    def _resume(self, key):
        st = self.state(key)
        if st in PRE_SUBMIT:
            return self._advance(key)                 # nothing was ever sent: continuing is safe
        if st in (S.SUBMITTING, S.CANCEL_PENDING):    # an in-flight request from a previous process: unproven
            return self._mark_unknown(key, "replayed while a request was in flight; its outcome is unproven")
        return st                                     # never a second submission

    def _mark_unknown(self, key, reason):
        st = self.state(key)
        intent, coid = self._intent(key)
        if st in (S.SUBMITTING, S.CANCEL_PENDING, S.ACKNOWLEDGED, S.PARTIALLY_FILLED):
            n = self._count(key, "TRANSITION")
            lg = self._ledgers()[key]
            evs = [self._event(key, intent, coid, "TRANSITION", ledger=lg, event_id=f"t:{key}:{n}",
                               previous_state=st.value, new_state=S.EXECUTION_UNKNOWN.value, reason=reason,
                               actor=ENGINE, payload={}),
                   self._event(key, intent, coid, "TRANSITION", ledger=lg, event_id=f"t:{key}:{n + 1}",
                               previous_state=S.EXECUTION_UNKNOWN.value,
                               new_state=S.HALTED_FOR_RECONCILIATION.value, reason="halted: reconciliation required",
                               actor=ENGINE, payload={})]
            check_transition(st, S.EXECUTION_UNKNOWN)
            check_transition(S.EXECUTION_UNKNOWN, S.HALTED_FOR_RECONCILIATION)
            with self.j.transaction():
                self.j.append_many(evs)
        elif st == S.EXECUTION_UNKNOWN:
            self._transition(key, S.HALTED_FOR_RECONCILIATION, "halted: reconciliation required")
        return self.state(key)

    def _precheck(self, intent, key, coid):
        """-> (None, None) when the intent may proceed, else (terminal state, reason). No live call is made."""
        now = self.clock.now_ms()
        if expired(intent, now):
            return S.EXPIRED, "EXPIRED"
        locked, why = self.market_lock_status(intent.asset, intent.market_ticker, exclude=key)
        if locked:
            return S.REJECTED, "MARKET_LOCKED: " + "; ".join(why)[:300]
        ok, reason = check_intent_against_approval(intent, self.risk_book.get(intent.risk_decision_id), now)
        if not ok:
            return S.REJECTED, reason
        if execution_key(intent) != key or client_order_id(key) != coid:
            return S.REJECTED, "IDENTITY_CHANGED"
        try:
            assert_provenance_unchanged(intent, self.j.intent_row(execution_key=key)["intent"])
        except InvariantViolation as e:
            return S.REJECTED, f"INTENT_CHANGED: {e}"
        try:
            pos = self.adapter.get_positions(intent.market_ticker)
        except Exception as e:                                  # noqa: BLE001 - unverifiable -> fail closed
            return S.REJECTED, f"PRECHECK_UNVERIFIABLE: position query failed ({type(e).__name__})"
        if pos.stale:
            return S.REJECTED, "PRECHECK_UNVERIFIABLE: stale position"
        exp = {"YES": ZERO, "NO": ZERO}
        for lg in self._ledgers().values():
            if lg.market_ticker == intent.market_ticker and lg.execution_key != key:
                exp[lg.side] += lg.filled_size
        if (pos.yes, pos.no) != (exp["YES"], exp["NO"]):
            return S.REJECTED, f"UNEXPLAINED_EXISTING_POSITION yes={pos.yes} no={pos.no}"
        return None, None

    def _advance(self, key):
        intent, coid = self._intent(key)
        while True:
            st = self.state(key)
            now = self.clock.now_ms()
            if st == S.CREATED:
                if expired(intent, now):
                    self._transition(key, S.REJECTED, "EXPIRED_ON_ARRIVAL")
                elif not (intent.risk_decision_id and intent.risk_snapshot_hash):
                    self._transition(key, S.REJECTED, "RISK_APPROVAL_MISSING")
                else:
                    self._transition(key, S.VALIDATED, "intent validated")
            elif st == S.VALIDATED:
                self._transition(key, S.EXPIRED if expired(intent, now) else S.PRECHECK,
                                 "EXPIRED" if expired(intent, now) else "precheck")
            elif st in (S.PRECHECK, S.READY):
                bad, reason = self._precheck(intent, key, coid)
                if bad is not None:
                    self._transition(key, bad, reason)
                elif st == S.PRECHECK:
                    self._transition(key, S.READY, "prechecks passed")
                else:
                    return self._submit(key, intent, coid)
            else:
                return st

    def _submit(self, key, intent, coid):
        req = OrderRequest(client_order_id=coid, market_ticker=intent.market_ticker, side=intent.side,
                           count=intent.requested_contracts, limit_price=intent.max_limit_price,
                           time_in_force=intent.time_in_force, expires_at=intent.expires_at)
        try:
            assert_order_within_intent(intent, req, coid)
        except InvariantViolation as e:
            return self._transition(key, S.REJECTED, f"INVARIANT: {e}")
        self._transition(key, S.SUBMITTING, "submitting", expect=S.READY)     # persisted BEFORE the adapter call
        self.faults.hit("before_submit")
        try:
            view = self.adapter.submit_order(req)
        except SubmitRejected as e:
            return self._transition(key, S.REJECTED, f"VENUE_REJECTED: {e}")
        except SimulatedCrash:
            raise
        except Exception as e:                                  # noqa: BLE001 - anything unproven is UNKNOWN
            return self._mark_unknown(key, f"submit outcome unproven: {type(e).__name__}: {e}")
        self.faults.hit("after_submit_before_ack")
        self._transition(key, S.ACKNOWLEDGED, "acknowledged by the adapter", order_id=view.order_id,
                         adapter_event_id=view.adapter_event_id)
        self.faults.hit("after_ack")
        if view.filled > 0 or view.status != "RESTING":
            self.reconcile(key)
        return self.state(key)

    # ---------------- cancellation / expiry ----------------
    def cancel(self, key, reason="cancel requested"):
        st = self.state(key)
        if st in PRE_SUBMIT:
            return self._transition(key, S.EXPIRED if st != S.CREATED else S.REJECTED, f"withdrawn before submit: "
                                    f"{reason}")
        if st not in (S.ACKNOWLEDGED, S.PARTIALLY_FILLED):
            return st
        intent, coid = self._intent(key)
        self._transition(key, S.CANCEL_PENDING, reason)                      # persisted BEFORE the cancel is sent
        self.faults.hit("during_cancel")
        try:
            self.adapter.cancel_order(coid)
        except SimulatedCrash:
            raise
        except Exception as e:                                  # noqa: BLE001 - unproven cancel -> UNKNOWN
            return self._mark_unknown(key, f"cancel outcome unproven: {type(e).__name__}: {e}")
        self.reconcile(key)
        self.faults.hit("after_cancel")
        return self.state(key)

    def expire_due(self):
        """Pre-submit intents past their expiry -> EXPIRED; outstanding orders past expiry are cancelled."""
        now, out = self.clock.now_ms(), {}
        for r in self.j.intents():
            key = r["execution_key"]
            intent, _coid = self._intent(key)
            st = self.state(key)
            if not expired(intent, now):
                continue
            if st in (S.VALIDATED, S.PRECHECK, S.READY):
                out[key] = self._transition(key, S.EXPIRED, "EXPIRED")
            elif st == S.CREATED:
                out[key] = self._transition(key, S.REJECTED, "EXPIRED_ON_ARRIVAL")
            elif st in (S.ACKNOWLEDGED, S.PARTIALLY_FILLED):
                out[key] = self.cancel(key, "expired")
        return out

    # ---------------- reconciliation ----------------
    def reconcile(self, key):
        """Compare journal / venue order / fills / positions; persist the verdict; apply only PROVEN transitions."""
        st = self.state(key)
        intent, coid = self._intent(key)
        if st in TERMINAL or st in PRE_SUBMIT:
            return "CONSISTENT"
        snap = self.adapter.reconcile(coid, intent.market_ticker)
        ledgers = self._ledgers()
        lg = ledgers[key]
        other = {"YES": ZERO, "NO": ZERO}
        for k2, l2 in ledgers.items():
            if k2 != key and l2.market_ticker == intent.market_ticker:
                other[l2.side] += l2.filled_size
        a = assess(st, intent, coid, snap, lg, other)
        if a.new_fills:
            self.faults.hit("before_fill_persist")
        n = self._count(key, "RECONCILIATION")
        rid = f"r:{key}:{n}"
        evs = [self._event(key, intent, coid, "RECONCILIATION", event_id=rid, reconciliation_status=a.verdict,
                           reconciliation_reason=a.reason[:300], previous_state=st.value,
                           new_state=a.implied_state.value if a.implied_state else None,
                           payload={"position_unsafe": a.position_unsafe, "details": {k: jsonable(v) for k, v in
                                                                                      a.details.items()}})]
        for f in a.new_fills:
            evs.append(self._event(key, intent, coid, "FILL", event_id=f"fill:{self.adapter.name}:{f.fill_id}",
                                   ts_ms=f.ts_ms, order_id=f.order_id, payload={"fill_id": f.fill_id, "qty": f.qty,
                                                                               "price": f.price, "fee_id": f.fee_id}))
            evs.append(self._event(key, intent, coid, "FEE", event_id=f"fee:{self.adapter.name}:{f.fee_id}",
                                   ts_ms=f.ts_ms, payload={"fee_id": f.fee_id, "amount": jsonable(f.fee),
                                                           "fill_id": f.fill_id}))
        with self.j.transaction():
            self.j.append_many(evs)
        self._apply(key, st, a, rid)
        if self.state(key) == S.PARTIALLY_FILLED and a.new_fills:
            self.faults.hit("after_partial_fill")
        return a.verdict

    def _apply(self, key, st, a, rid):
        if a.verdict in MISMATCH_VERDICTS:
            if st == S.EXECUTION_UNKNOWN:
                self._transition(key, S.HALTED_FOR_RECONCILIATION, f"{a.verdict}: {a.reason}"[:300], RECONCILIATION, rid)
            elif st != S.HALTED_FOR_RECONCILIATION:
                self._transition(key, S.HALTED_FOR_RECONCILIATION, f"{a.verdict}: {a.reason}"[:300], RECONCILIATION, rid)
            return
        if a.verdict == "EXECUTION_UNKNOWN":
            if st in (S.SUBMITTING, S.CANCEL_PENDING, S.EXECUTION_UNKNOWN):
                self._mark_unknown(key, a.reason[:300])
            return
        if a.verdict == "RECOVERABLE_DIFFERENCE" and a.implied_state is not None:
            if st == S.EXECUTION_UNKNOWN:
                self._transition(key, S.HALTED_FOR_RECONCILIATION, "halted before recovery", RECONCILIATION, rid)
                st = S.HALTED_FOR_RECONCILIATION
            if st in (S.SUBMITTING,):
                return                                          # never resolved here: _submit owns this state
            self._transition(key, a.implied_state, f"reconciled: {a.reason}"[:300], RECONCILIATION, rid)

    # ---------------- marks ----------------
    def set_mark(self, key, mark):
        intent, coid = self._intent(key)
        n = self._count(key, "MARK")
        ev = self._event(key, intent, coid, "MARK", event_id=f"m:{key}:{n}",
                         payload={"mark": jsonable(UNKNOWN if mark is UNKNOWN else from_jsonable(str(mark)))})
        with self.j.transaction():
            self.j.append(ev)

    # ---------------- settlement ----------------
    def settle(self, ref):
        """Apply an official market result. Only FILLED quantity settles; never-filled orders create no position;
        halted executions are not settled until reconciled."""
        out = {}
        for r in self.j.intents():
            if r["market_ticker"] != ref.market_ticker:
                continue
            key = r["execution_key"]
            st = self.state(key)
            if st in (S.ACKNOWLEDGED, S.PARTIALLY_FILLED, S.CANCEL_PENDING):
                self.reconcile(key)
                st = self.state(key)
            if st in (S.VALIDATED, S.PRECHECK, S.READY):
                st = self._transition(key, S.EXPIRED, "market resolved before submission")
            elif st == S.CREATED:
                st = self._transition(key, S.REJECTED, "market resolved before validation")
            elif st == S.ACKNOWLEDGED:
                st = self._transition(key, S.EXPIRED, "market resolved before any fill")
            filled = self._ledgers()[key].filled_size
            if st in (S.FILLED, S.PARTIALLY_FILLED, S.CANCELLED, S.EXPIRED) and filled > 0:
                self.faults.hit("before_settlement_update")
                intent, coid = self._intent(key)
                sev = self._event(key, intent, coid, "SETTLEMENT", event_id=f"s:{key}",
                                  payload={"result": ref.result, "settlement_value": ref.settlement_value,
                                           "source": ref.source, "settled_size": filled})
                st = self._transition(key, S.SETTLEMENT_PENDING, f"market resolved {ref.result}", also=[sev])
            if st == S.SETTLEMENT_PENDING:
                st = self._transition(key, S.CLOSED, "position settled and closed")
            out[key] = st
        return out

    # ---------------- restart recovery ----------------
    def recover(self):
        """After a (simulated) restart: rebuild from the journal, make every in-flight request UNKNOWN -> HALTED, then
        reconcile every execution that may have exposure. Pre-submit executions are left to resume on replay."""
        self.faults.hit("during_recovery")
        summary = {}
        for key, v in self._views().items():
            if v.state in (S.SUBMITTING, S.CANCEL_PENDING, S.EXECUTION_UNKNOWN):
                self._mark_unknown(key, f"restart: the outcome of {v.state.value} is unproven")
        for key, v in self._views().items():
            if v.state in OUTSTANDING or v.state in UNSAFE or v.state == S.FILLED:
                summary[key] = self.reconcile(key)
            else:
                summary[key] = v.state.value if v.state else None
        return summary

    # ---------------- views ----------------
    def positions(self):
        return {k: lg.snapshot() for k, lg in self._ledgers().items()}

    def audit_view(self, intent_id):
        """Prediction / decision / execution provenance of one intent, kept separate (no aggregate metrics)."""
        row = self.j.intent_row(intent_id=intent_id)
        if row is None:
            return None
        key = row["execution_key"]
        audit = next((e["payload"]["audit"] for e in self.j.events(key, "AUDIT")), {})
        lg = self._ledgers()[key]
        snap = lg.snapshot()
        settle = next((e["payload"] for e in self.j.events(key, "SETTLEMENT")), None)
        direction = audit.get("predicted_direction")
        it = row["intent"]
        return {"prediction": {k: audit.get(k) for k in ("raw_probability", "calibrated_probability",
                                                         "predicted_direction", "checkpoint")},
                "decision": {k: audit.get(k) for k in ("decision", "market_price", "bid", "ask", "available_depth",
                                                       "estimated_fee", "estimated_slippage", "estimated_net_ev",
                                                       "risk_approved", "risk_reason")},
                "execution": {"requested_size": it["requested_contracts"],
                              "submitted_size": it["requested_contracts"] if any(
                                  e["new_state"] == "SUBMITTING" for e in self.j.events(key, "TRANSITION")) else None,
                              "filled_size": jsonable(snap["filled_size"]),
                              "average_fill": jsonable(snap["average_entry_price"]),
                              "actual_fees": jsonable(snap["fees_paid"]), "state": self.state(key).value},
                "outcome": {"settlement_result": None if settle is None else settle["result"],
                            "prediction_correct": prediction_correct(direction, None if settle is None
                                                                     else settle["result"]),
                            "trade_pnl": jsonable(snap["realized_pnl"]),
                            "pnl_authoritative": snap["pnl_authoritative"]},
                "provenance": {k: it[k] for k in ("model_id", "model_fingerprint", "calibration_fingerprint",
                                                  "signal_fingerprint", "risk_decision_id", "risk_snapshot_hash")}}
