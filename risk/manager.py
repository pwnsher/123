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

ACTIVE POLICY (Step 6.6.2): the active risk policy is DURABLE state of the risk journal (POLICY_ACTIVATED events,
append-only: previous / new fingerprint, policy id / version, the full policy, timestamp, reason). It is changed only
by activate_policy() inside a risk-store transaction (BEGIN IMMEDIATE), so a policy change is totally ordered with every
evaluation and every final authorisation, across processes, RiskManager instances and SQLite connections. Each
transaction resolves ONE (policy, fingerprint) pair from the journal and uses it for everything it does (day resets,
breaker observation, evaluation, decision id, approval). Construction reconciles the supplied RiskPolicy with the
persisted active policy:
    fresh store (no POLICY_ACTIVATED)  -> the supplied policy is activated (audited "initial activation");
    same fingerprint                   -> continue;
    different fingerprint              -> RiskPolicyMismatchError, unless the caller passes activate_reason (an
                                          explicit, audited activation). Never a silent overwrite or reversion.
RiskManager.from_store(store, clock) reopens with the persisted active policy (its fingerprint re-verified).
"""
from execution.risk import check_intent_against_approval
from risk.breakers import BREAKER_TYPES, RESET_RULE, utc_day
from risk.decision import RiskDecision, approval_for, decision_id
from risk.evaluate import RiskState, breaker_snapshot_problems, breaker_triggers, evaluate
from risk.policy import RiskPolicy, risk_policy_fingerprint
from risk.store import RiskStoreError
from risk.types import RiskInputError, risk_snapshot_hash


class RiskPolicyMismatchError(RuntimeError):
    """The supplied policy differs from the durable active policy and no explicit activation was requested."""


class RiskManager:
    def __init__(self, store, policy, clock, activate_reason=None):
        self.store, self.clock = store, clock
        problems = policy.problems()
        if problems:
            raise RiskInputError(f"invalid risk policy: {problems}")
        self.policy_fingerprint_supplied = risk_policy_fingerprint(policy)
        with store.transaction():
            active = store.active_policy()
            if active is None:
                store.append_policy_activation(clock.now_ms(), policy.to_dict(), self.policy_fingerprint_supplied,
                                               "initial activation (no recorded active policy)")
            elif active["new_policy_fingerprint"] != self.policy_fingerprint_supplied:
                if not (isinstance(activate_reason, str) and activate_reason.strip()):
                    raise RiskPolicyMismatchError(
                        f"the risk journal's active policy is {active['new_policy_fingerprint'][:16]}... "
                        f"({active['policy_id']} v{active['policy_version']}); the supplied policy is "
                        f"{self.policy_fingerprint_supplied[:16]}...: activate it explicitly (activate_reason)")
                store.append_policy_activation(clock.now_ms(), policy.to_dict(), self.policy_fingerprint_supplied,
                                               activate_reason)

    @classmethod
    def from_store(cls, store, clock):
        """Reopen with the durable active policy (no reconfiguration)."""
        pol, _fp = _resolve_active(store)
        return cls(store, pol, clock)

    # ---------------- active policy (durable, ordered) ----------------
    def _active(self):
        """ONE authoritative (policy, fingerprint) pair, read from the risk journal (call inside a transaction)."""
        return _resolve_active(self.store)

    @property
    def policy(self):
        return self._active()[0]

    @property
    def policy_fingerprint(self):
        return self.store.active_policy_fingerprint()

    def activate_policy(self, policy, reason):
        """Durably activate another policy (POLICY_ACTIVATED, ordered with every authorisation). Approvals issued under
        the previous fingerprint are rejected from the moment this commits."""
        if not (isinstance(reason, str) and reason.strip()):
            raise ValueError("a policy activation needs a reason")
        problems = policy.problems()
        if problems:
            raise RiskInputError(f"invalid risk policy: {problems}")
        fp = risk_policy_fingerprint(policy)
        with self.store.transaction():
            if self.store.active_policy_fingerprint() != fp:
                self.store.append_policy_activation(self.clock.now_ms(), policy.to_dict(), fp, reason)
        return fp

    def set_policy(self, policy, reason="set_policy"):
        """Compatibility alias of activate_policy (durable, ordered)."""
        return self.activate_policy(policy, reason)

    # ---------------- persisted state ----------------
    def state(self, candidate=None, now_ms=None):
        now_ms = self.clock.now_ms() if now_ms is None else now_ms
        n, unresolved = self.store.loss_streak()
        epoch = self.store.loss_streak_epoch()
        conflict = consumed = False
        if candidate is not None:
            prior = self.store.candidate_hash(candidate.candidate_id)
            conflict = prior is not None and prior != candidate.content_hash()
            consumed = self.store.candidate_consumed(candidate.candidate_id)
        return RiskState(breakers=tuple((t, s) for t, (s, _d) in sorted(self.store.breaker_states().items())),
                         consecutive_losses=n, unresolved_trade_results=unresolved, loss_streak_epoch=epoch,
                         day_peak_equity=self.store.day_peak_equity(utc_day(now_ms)), candidate_consumed=consumed,
                         candidate_conflict=conflict)

    def _day_resets(self, now_ms, fp):
        today = utc_day(now_ms)
        for t, (st, day) in sorted(self.store.breaker_states().items()):
            if st == "LATCHED" and RESET_RULE[t] == "UTC_DAY_BOUNDARY" and day is not None and day < today:
                self.store.breaker_transition(t, "CLEAR", now_ms, f"RESET at UTC day boundary ({day} -> {today})",
                                              policy_fingerprint=fp)

    def _latch(self, triggers, now_ms, snap_hash, fp):
        states = self.store.breaker_states()
        for t, reason in triggers:
            if states[t][0] != "CLEAR":
                continue
            self.store.breaker_transition(t, "TRIGGERED", now_ms, reason, snap_hash, fp)
            self.store.faults.hit("during_breaker_transition")
            self.store.breaker_transition(t, "LATCHED", now_ms, reason, snap_hash, fp)

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
        with self.store.transaction():                     # ONE transaction: streak baseline + breaker clear
            _pol, fp = self._active()
            if breaker_type == "CONSECUTIVE_LOSS":
                self.store.append_loss_streak_reset(now, operator_reason, fp)
                self.store.faults.hit("after_streak_reset_before_breaker_clear")
            self.store.breaker_transition(breaker_type, "CLEAR", now, f"OPERATOR RESET: {operator_reason}",
                                          policy_fingerprint=fp)
            self.store.faults.hit("after_breaker_clear_before_commit")

    def _observe_breakers(self, snapshot, now_ms, snap_hash, pol, fp):
        """Breaker OBSERVATION, independent of any candidate (CALL or NO_CALL): an authoritative snapshot
        (risk.evaluate.breaker_snapshot_problems is empty) that trips a breaker latches it."""
        if breaker_snapshot_problems(snapshot, now_ms):
            return []
        triggers, _unknown = breaker_triggers(snapshot, pol, self.state(None, now_ms))
        self._latch(triggers, now_ms, snap_hash, fp)
        return triggers

    # ---------------- observation (breakers / day peak without a candidate) ----------------
    def observe(self, snapshot):
        """Record a snapshot for breaker / day-peak tracking and latch any breaker it triggers."""
        now = self.clock.now_ms()
        h = risk_snapshot_hash(snapshot)
        with self.store.transaction(after="after_breaker_trigger"):
            pol, fp = self._active()
            self._day_resets(now, fp)
            self.store.append("OBSERVATION", f"o:{h}", now, {"snapshot_hash": h, "snapshot": snapshot.to_dict()})
            self._observe_breakers(snapshot, now, h, pol, fp)
        return self.breakers()

    def breakers(self):
        return {t: s for t, (s, _d) in self.store.breaker_states().items()}

    # ---------------- evaluation ----------------
    def evaluate(self, candidate, snapshot):
        now = self.clock.now_ms()
        snap_hash = risk_snapshot_hash(snapshot)
        with self.store.transaction(before="before_decision_write", after="after_decision_write"):
            pol, fp = self._active()        # ONE policy snapshot for this whole evaluation (thresholds + identity)
            self._day_resets(now, fp)
            observed = self._observe_breakers(snapshot, now, snap_hash, pol, fp)   # A. observation (never by signal)
            state = self.state(candidate, now)                                # B. authorisation sees the result
            ev = evaluate(candidate, snapshot, pol, state, now)
            rid = decision_id(candidate.candidate_id, candidate.content_hash(), snap_hash, fp,
                              ev.decision, ev.approved_contracts, ev.approved_max_limit_price, ev.reason_codes)
            stored = self.store.decision_record(rid)
            if stored is not None:                               # the same logical decision: idempotent
                return self._load(rid)
            self._latch(ev.triggers, now, snap_hash, fp)
            d = RiskDecision(risk_decision_id=rid, candidate_id=candidate.candidate_id,
                             candidate_hash=candidate.content_hash(), decision=ev.decision, decision_ts=now,
                             expires_at=ev.expires_at, market_ticker=candidate.market_ticker, asset=candidate.asset,
                             side=candidate.side, signal_status=candidate.signal_status,
                             requested_contracts=candidate.requested_contracts,
                             approved_contracts=ev.approved_contracts,
                             requested_max_limit_price=candidate.requested_max_limit_price,
                             approved_max_limit_price=ev.approved_max_limit_price,
                             calculated_worst_case_loss=ev.worst_case_loss, risk_snapshot_hash=snap_hash,
                             risk_policy_fingerprint=fp, reason_codes=ev.reason_codes,
                             human_readable_reasons=ev.reasons)
            approval = approval_for(d, candidate)
            self.store.append("DECISION", f"d:{rid}", now,
                              {"decision": d.to_dict(), "candidate": candidate.to_dict(), "snapshot": snapshot.to_dict(),
                               "snapshot_hash": snap_hash, "policy_fingerprint": fp,
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
        return self.store.active_policy_fingerprint()       # the DURABLE active policy, read at check time

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
            if prior is not None and (prior["intent_id"], prior["execution_key"]) != (intent.intent_id, execution_key):
                return False, f"RISK_APPROVAL_ALREADY_CONSUMED by {prior['intent_id']}"
            st.faults.hit("during_approval_consumption")
            appr = st.approval(intent.risk_decision_id)
            # ---- FINAL critical section: no callback / hook / fault point between these checks and the append ----
            authorized_at = max(now, self.clock.now_ms())          # FINAL time read = the TIME authorisation point
            if st.active_policy_fingerprint() != appr.risk_policy_fingerprint:
                return False, "RISK_POLICY_CHANGED"
            if authorized_at >= appr.expires_at:
                return False, "RISK_APPROVAL_EXPIRED"
            if any(s != "CLEAR" for s, _d in st.breaker_states().values()):
                return False, "RISK_BREAKER_LATCHED"
            if prior is not None:
                return True, "RISK_APPROVAL_REPLAY"
            st.append("CONSUMPTION", f"c:{intent.risk_decision_id}", authorized_at,
                      {"intent_id": intent.intent_id, "execution_key": execution_key, "candidate_id": appr.candidate_id,
                       "authorized_at": authorized_at, "policy_fingerprint": appr.risk_policy_fingerprint},
                      candidate_id=appr.candidate_id, risk_decision_id=intent.risk_decision_id)
        return True, "RISK_APPROVAL_CONSUMED"


def _resolve_active(store):
    a = store.active_policy()
    if a is None:
        raise RiskStoreError("the risk journal has no active policy")
    pol = RiskPolicy.from_dict(a["policy"])
    fp = risk_policy_fingerprint(pol)
    if fp != a["new_policy_fingerprint"]:
        raise RiskStoreError("the persisted active policy does not match its recorded fingerprint (fail closed)")
    return pol, fp
