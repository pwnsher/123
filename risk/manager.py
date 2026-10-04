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
from risk.evaluate import RiskState, breaker_triggers, evaluate
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
        """Explicit operator reset (the only reset of CONSECUTIVE_LOSS). Logged like every transition."""
        if not operator_reason:
            raise ValueError("an operator reset needs a reason")
        now = self.clock.now_ms()
        with self.store.transaction():
            self.store.breaker_transition(breaker_type, "CLEAR", now, f"OPERATOR RESET: {operator_reason}",
                                          policy_fingerprint=self.policy_fingerprint)

    # ---------------- observation (breakers / day peak without a candidate) ----------------
    def observe(self, snapshot):
        """Record a snapshot for breaker / day-peak tracking and latch any breaker it triggers."""
        now = self.clock.now_ms()
        h = risk_snapshot_hash(snapshot)
        with self.store.transaction(after="after_breaker_trigger"):
            self._day_resets(now)
            self.store.append("OBSERVATION", f"o:{h}", now, {"snapshot_hash": h, "snapshot": snapshot.to_dict()})
            self._latch(breaker_triggers(snapshot, self.policy, self.state(None, now))[0], now, h)
        return self.breakers()

    def breakers(self):
        return {t: s for t, (s, _d) in self.store.breaker_states().items()}

    # ---------------- evaluation ----------------
    def evaluate(self, candidate, snapshot):
        now = self.clock.now_ms()
        snap_hash = risk_snapshot_hash(snapshot)
        with self.store.transaction(before="before_decision_write", after="after_decision_write"):
            self._day_resets(now)
            state = self.state(candidate, now)
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
        return StoreApprovalBook(self.store, self.policy_fingerprint, self.clock)


class StoreApprovalBook:
    """The durable approval book execution consults (Step 6.6). verify(): the approval exists, is not superseded, no
    safety breaker is latched NOW (a breaker that latched after issuance still stops the entry), it was issued under
    the CURRENT policy and binds this exact intent (execution.risk.check_intent_against_approval);
    consume(): single logical use, persisted in the risk journal (restart-safe; the same intent replays)."""

    def __init__(self, store, required_policy_fingerprint, clock):
        self.store, self.required_policy_fingerprint, self.clock = store, required_policy_fingerprint, clock

    def get(self, decision_id):
        a = self.store.approval(decision_id)
        return None if a is None else a.to_execution()

    def verify(self, intent, now_ms):
        if intent.risk_decision_id and self.store.superseded(intent.risk_decision_id):
            return False, "RISK_APPROVAL_SUPERSEDED"
        latched = sorted(t for t, (st, _d) in self.store.breaker_states().items() if st != "CLEAR")
        if latched:                                  # a breaker latched AFTER issuance still stops the entry
            return False, "RISK_BREAKER_LATCHED: " + ", ".join(latched)
        return check_intent_against_approval(intent, self.get(intent.risk_decision_id), now_ms,
                                             self.required_policy_fingerprint)

    def consume(self, intent, execution_key):
        return self.store.consume(intent.risk_decision_id, intent.intent_id, execution_key, self.clock.now_ms())
