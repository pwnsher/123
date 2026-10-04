"""
RiskManager (Step 6.6): orchestrates the pure evaluation and owns persistence. PAPER / SHADOW ONLY - it never fetches
data, never calls a network API and never places or cancels an order. It is not wired into any production path.

    evaluate(candidate, snapshot) -> (RiskDecision, RiskApproval | None)
        one risk-store transaction:
          1. UTC day-boundary RESETs of latched daily / drawdown breakers (LATCHED -> CLEAR, logged);
          2. the persisted RiskState (breakers, loss streak, day peak equity, candidate history) is read;
          3. risk.evaluate.evaluate(...) (pure);
          4. newly observed breaker triggers are persisted (CLEAR -> TRIGGERED -> LATCHED);
          5. the DECISION is recorded (idempotent: the same logical decision id returns the stored decision / approval);
          6. an APPROVE / REDUCE decision issues ONE RiskApproval; an older, unexpired, unconsumed approval of the
             same candidate is SUPERSEDED (at most one valid approval per candidate; never silently renewed).
        A crash anywhere inside rolls everything back: no approval exists without its decision, no trigger without
        its latch, and no veto ever becomes an approval.

    approval_book() -> StoreApprovalBook: the durable book execution consults (verify + single-use consume, both
    restart-safe), requiring the CURRENT policy fingerprint (an approval issued under another policy is rejected).
"""
from execution.risk import check_intent_against_approval
from risk.breakers import BREAKER_TYPES, RESET_RULE, utc_day
from risk.decision import RiskDecision, approval_for, decision_id
from risk.evaluate import RiskState, breaker_snapshot_problems, breaker_triggers, evaluate
from risk.policy import risk_policy_fingerprint
from risk.types import risk_snapshot_hash


class RiskManager:
    def __init__(self, store, policy, clock):
        self.store, self.policy, self.clock = store, policy, clock
        self.policy_fingerprint = risk_policy_fingerprint(policy)

    # ---------------- persisted state ----------------
    def state(self, candidate=None, now_ms=None):
        now_ms = self.clock.now_ms() if now_ms is None else now_ms
        n, unresolved = self.store.loss_streak()
        conflict = consumed = False
        if candidate is not None:
            prior = self.store.candidate_hash(candidate.candidate_id)
            conflict = prior is not None and prior != candidate.content_hash()
            consumed = self.store.candidate_consumed(candidate.candidate_id)
        return RiskState(breakers=tuple((t, s) for t, (s, _d) in sorted(self.store.breaker_states().items())),
                         consecutive_losses=n, unresolved_trade_results=unresolved,
                         day_peak_equity=self.store.day_peak_equity(utc_day(now_ms)), candidate_consumed=consumed,
                         candidate_conflict=conflict)

    def _day_resets(self, now_ms):
        today = utc_day(now_ms)
        for t, (st, day) in sorted(self.store.breaker_states().items()):
            if st == "LATCHED" and RESET_RULE[t] == "UTC_DAY_BOUNDARY" and day is not None and day < today:
                self.store.breaker_transition(t, "CLEAR", now_ms, f"RESET at UTC day boundary ({day} -> {today})",
                                              policy_fingerprint=self.policy_fingerprint)

    def _latch(self, triggers, now_ms, snap_hash):
        states = self.store.breaker_states()
        for t, reason in triggers:
            if states[t][0] != "CLEAR":
                continue
            self.store.breaker_transition(t, "TRIGGERED", now_ms, reason, snap_hash, self.policy_fingerprint)
            self.store.faults.hit("during_breaker_transition")
            self.store.breaker_transition(t, "LATCHED", now_ms, reason, snap_hash, self.policy_fingerprint)

    def reset_breaker(self, breaker_type, operator_reason):
        """Explicit operator reset - ONLY for breakers whose RESET_RULE is OPERATOR (CONSECUTIVE_LOSS). Daily and
        drawdown breakers (RESET_RULE UTC_DAY_BOUNDARY) are refused: they clear only at the UTC day boundary. A refused
        attempt changes no breaker state; it is logged as a BREAKER_RESET_REJECTED audit event."""
        now = self.clock.now_ms()
        reason_ok = isinstance(operator_reason, str) and bool(operator_reason.strip())
        if RESET_RULE.get(breaker_type) != "OPERATOR" or not reason_ok:
            why = (f"{breaker_type} resets only by rule {RESET_RULE.get(breaker_type)!r}, never by an operator"
                   if RESET_RULE.get(breaker_type) != "OPERATOR" else "an operator reset needs a non-empty reason")
            with self.store.transaction():
                n = self.store.count("BREAKER_RESET_REJECTED")
                self.store.append("BREAKER_RESET_REJECTED", f"rr:{n}", now,
                                  {"breaker_type": breaker_type, "operator_reason": operator_reason, "why": why,
                                   "state": self.store.breaker_states().get(breaker_type, ("?", None))[0]},
                                  breaker_type=breaker_type if breaker_type in BREAKER_TYPES else None)
            raise RiskBreakerResetError(why)
        with self.store.transaction():
            self.store.breaker_transition(breaker_type, "CLEAR", now, f"OPERATOR RESET: {operator_reason}",
                                          policy_fingerprint=self.policy_fingerprint)

    def set_policy(self, policy):
        """Activate another policy. Approvals issued under the previous policy fingerprint are rejected from now on."""
        self.policy, self.policy_fingerprint = policy, risk_policy_fingerprint(policy)

    def _observe_breakers(self, snapshot, now_ms, snap_hash):
        """Breaker OBSERVATION, independent of any candidate (CALL or NO_CALL): an authoritative snapshot
        (risk.evaluate.breaker_snapshot_problems is empty) that trips a breaker latches it."""
        if breaker_snapshot_problems(snapshot, now_ms):
            return []
        triggers, _unknown = breaker_triggers(snapshot, self.policy, self.state(None, now_ms))
        self._latch(triggers, now_ms, snap_hash)
        return triggers

    # ---------------- observation (breakers / day peak without a candidate) ----------------
    def observe(self, snapshot):
        """Record a snapshot for breaker / day-peak tracking and latch any breaker it triggers."""
        now = self.clock.now_ms()
        h = risk_snapshot_hash(snapshot)
        with self.store.transaction(after="after_breaker_trigger"):
            self._day_resets(now)
            self.store.append("OBSERVATION", f"o:{h}", now, {"snapshot_hash": h, "snapshot": snapshot.to_dict()})
            self._observe_breakers(snapshot, now, h)
        return self.breakers()

    def breakers(self):
        return {t: s for t, (s, _d) in self.store.breaker_states().items()}

    # ---------------- evaluation ----------------
    def evaluate(self, candidate, snapshot):
        now = self.clock.now_ms()
        snap_hash = risk_snapshot_hash(snapshot)
        with self.store.transaction(before="before_decision_write", after="after_decision_write"):
            self._day_resets(now)
            observed = self._observe_breakers(snapshot, now, snap_hash)       # A. observation (never by signal)
            state = self.state(candidate, now)                                # B. authorisation sees the result
            ev = evaluate(candidate, snapshot, self.policy, state, now)
            rid = decision_id(candidate.candidate_id, candidate.content_hash(), snap_hash, self.policy_fingerprint,
                              ev.decision, ev.approved_contracts, ev.approved_max_limit_price, ev.reason_codes)
            stored = self.store.decision_record(rid)
            if stored is not None:                               # the same logical decision: idempotent
                return self._load(rid)
            self._latch(ev.triggers, now, snap_hash)
            d = RiskDecision(risk_decision_id=rid, candidate_id=candidate.candidate_id,
                             candidate_hash=candidate.content_hash(), decision=ev.decision, decision_ts=now,
                             expires_at=ev.expires_at, market_ticker=candidate.market_ticker, asset=candidate.asset,
                             side=candidate.side, signal_status=candidate.signal_status,
                             requested_contracts=candidate.requested_contracts,
                             approved_contracts=ev.approved_contracts,
                             requested_max_limit_price=candidate.requested_max_limit_price,
                             approved_max_limit_price=ev.approved_max_limit_price,
                             calculated_worst_case_loss=ev.worst_case_loss, risk_snapshot_hash=snap_hash,
                             risk_policy_fingerprint=self.policy_fingerprint, reason_codes=ev.reason_codes,
                             human_readable_reasons=ev.reasons)
            approval = approval_for(d, candidate)
            self.store.append("DECISION", f"d:{rid}", now,
                              {"decision": d.to_dict(), "candidate": candidate.to_dict(), "snapshot": snapshot.to_dict(),
                               "snapshot_hash": snap_hash, "policy_fingerprint": self.policy_fingerprint,
                               "caps": ev.caps, "triggers": [list(t) for t in ev.triggers],
                               "observed_triggers": [list(t) for t in observed],
                               "approval_id": rid if approval else None},
                              candidate_id=candidate.candidate_id, risk_decision_id=rid)
            self.store.faults.hit("during_decision_transaction")
            if approval is not None:
                self.store.faults.hit("before_approval_creation")
                for old in self.store.approvals_for_candidate(candidate.candidate_id):
                    if (not self.store.superseded(old.risk_decision_id) and old.expires_at > now
                            and self.store.consumption(old.risk_decision_id) is None):
                        self.store.append("APPROVAL_SUPERSEDED", f"x:{old.risk_decision_id}", now,
                                          {"superseded_by": rid}, candidate_id=candidate.candidate_id,
                                          risk_decision_id=old.risk_decision_id)
                self.store.append("APPROVAL", f"a:{rid}", now,
                                  {"approval": approval.to_dict(),
                                   "binding_hash": approval.to_execution().binding_hash},
                                  candidate_id=candidate.candidate_id, risk_decision_id=rid)
        if approval is not None:
            self.store.faults.hit("after_approval_creation")
        return d, approval

    def _load(self, rid):
        rec = self.store.decision_record(rid)["decision"]
        from execution.money import from_jsonable
        kw = dict(rec)
        for k in ("requested_contracts", "approved_contracts", "requested_max_limit_price",
                  "approved_max_limit_price", "calculated_worst_case_loss"):
            kw[k] = from_jsonable(kw[k], k)
        kw["reason_codes"], kw["human_readable_reasons"] = tuple(kw["reason_codes"]), tuple(kw["human_readable_reasons"])
        return RiskDecision(**kw), self.store.approval(rid)

    def decision(self, rid):
        return self._load(rid) if self.store.decision_record(rid) else (None, None)

    def approval_book(self):
        return StoreApprovalBook(self)


class RiskBreakerResetError(RuntimeError):
    """A breaker reset that its RESET_RULE does not allow (fail closed; breaker state unchanged)."""


class StoreApprovalBook:
    """The durable approval book execution consults (Step 6.6 / 6.6.1).

    verify(intent, now_ms)                    READ-ONLY (execution PRECHECK and READY). Never consumes.
    verify_and_consume(intent, key, now_ms)   the FINAL authorisation, called by execution immediately before
                                              READY -> SUBMITTING. In ONE risk-store transaction (BEGIN IMMEDIATE,
                                              so it is serialised with every breaker / approval write) it re-checks
                                              EVERYTHING against the CURRENT clock and the persisted risk state, then
                                              appends the CONSUMPTION event. That commit is the LINEARISATION POINT:
                                              a breaker transition, supersession or expiry ordered before it makes the
                                              authorisation fail; one ordered after it cannot affect an execution that
                                              is already authorised.
    Checks (both): approval exists; not superseded; no breaker TRIGGERED or LATCHED; and
    execution.risk.check_intent_against_approval at `now`: binding hash, candidate, CURRENT policy fingerprint,
    provenance, snapshot hash, expiry (now >= expires_at -> expired), market / asset / side, size, price.
    Consumption: the same intent_id + execution_key replays idempotently (e.g. after a crash between consumption
    and SUBMITTING); any other intent or key is rejected."""

    def __init__(self, manager):
        self.mgr = manager
        self.store, self.clock = manager.store, manager.clock

    @property
    def required_policy_fingerprint(self):
        return self.mgr.policy_fingerprint                  # the CURRENT policy, read at check time

    def get(self, decision_id):
        a = self.store.approval(decision_id)
        return None if a is None else a.to_execution()

    def _authorize(self, intent, now_ms):
        if intent.risk_decision_id and self.store.superseded(intent.risk_decision_id):
            return False, "RISK_APPROVAL_SUPERSEDED"
        latched = sorted(t for t, (st, _d) in self.store.breaker_states().items() if st != "CLEAR")
        if latched:                                  # a breaker latched AFTER issuance still stops the entry
            return False, "RISK_BREAKER_LATCHED: " + ", ".join(latched)
        return check_intent_against_approval(intent, self.get(intent.risk_decision_id), now_ms,
                                             self.required_policy_fingerprint)

    def verify(self, intent, now_ms):
        return self._authorize(intent, now_ms)

    def verify_and_consume(self, intent, execution_key, now_ms):
        st = self.store
        with st.transaction(after="after_approval_consumption"):
            now = max(now_ms, self.clock.now_ms())          # the CURRENT time, read inside the transaction
            ok, reason = self._authorize(intent, now)
            if not ok:
                return False, reason
            prior = st.consumption(intent.risk_decision_id)
            if prior is not None:
                if prior["intent_id"] == intent.intent_id and prior["execution_key"] == execution_key:
                    return True, "RISK_APPROVAL_REPLAY"
                return False, f"RISK_APPROVAL_ALREADY_CONSUMED by {prior['intent_id']}"
            st.faults.hit("during_approval_consumption")
            appr = st.approval(intent.risk_decision_id)
            st.append("CONSUMPTION", f"c:{intent.risk_decision_id}", now,
                      {"intent_id": intent.intent_id, "execution_key": execution_key, "candidate_id": appr.candidate_id,
                       "authorized_at": now},
                      candidate_id=appr.candidate_id, risk_decision_id=intent.risk_decision_id)
        return True, "RISK_APPROVAL_CONSUMED"
