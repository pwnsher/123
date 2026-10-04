"""
SHADOW-ONLY integration helpers (Step 6.6). Nothing in production imports them; they connect

    RiskCandidate -> RiskManager.evaluate() -> RiskDecision -> RiskApproval -> OrderIntent -> PaperExecutionAdapter

for demos and tests. They READ execution state (journal-derived) to describe it to risk; they never repair it, never
submit, never cancel and never call a network API.
"""
from dataclasses import dataclass
from decimal import Decimal

from execution.intent import DIRECTION_OF_SIDE, OrderIntent
from execution.money import UNKNOWN
from execution.states import OUTSTANDING, PRE_SUBMIT, ExecState

ZERO = Decimal(0)


def intent_from_approval(candidate, approval, intent_id, time_in_force="GOOD_TIL_EXPIRY"):
    """An OrderIntent that cannot exceed its approval: size / price / side / market / expiry / provenance are COPIED
    from the approval and the candidate (execution re-verifies every one of them)."""
    return OrderIntent(intent_id=intent_id, candidate_id=candidate.candidate_id, created_at=approval.issued_at,
                       market_ticker=approval.market_ticker, asset=approval.asset, side=approval.approved_side,
                       requested_contracts=approval.approved_contracts,
                       max_limit_price=approval.approved_max_limit_price, time_in_force=time_in_force,
                       decision_ts=candidate.decision_ts, expires_at=approval.expires_at,
                       raw_probability=candidate.raw_probability,
                       calibrated_probability=candidate.calibrated_probability,
                       market_price=candidate.current_executable_price, estimated_fee=candidate.estimated_fee,
                       estimated_slippage=candidate.estimated_slippage, estimated_net_ev=candidate.estimated_net_ev,
                       model_id=candidate.model_id, model_fingerprint=candidate.model_fingerprint,
                       calibration_fingerprint=candidate.calibration_fingerprint,
                       signal_fingerprint=candidate.signal_fingerprint, risk_decision_id=approval.risk_decision_id,
                       risk_snapshot_hash=approval.risk_snapshot_hash)


@dataclass(frozen=True)
class Exposure:
    """One open exposure: worst-case open risk and gross notional, in a direction (UP / DOWN)."""
    asset: str
    market_ticker: str
    direction: str
    open_risk: Decimal
    gross_notional: Decimal


_SEVERITY = ("CLEAR", "OPEN_POSITION", "ACTIVE_ENTRY", "ACCOUNTING_INCOMPLETE_UNSAFE", "EXECUTION_UNKNOWN",
             "HALTED_FOR_RECONCILIATION", "FILL_MISMATCH", "POSITION_MISMATCH")


def _execution_state(view, ledger):
    if view.last_verdict in ("POSITION_MISMATCH", "FILL_MISMATCH") and view.state not in (ExecState.CLOSED,
                                                                                            ExecState.REJECTED):
        return view.last_verdict
    if view.state == ExecState.HALTED_FOR_RECONCILIATION:
        return "HALTED_FOR_RECONCILIATION"
    if view.state == ExecState.EXECUTION_UNKNOWN:
        return "EXECUTION_UNKNOWN"
    if view.last_verdict == "ACCOUNTING_INCOMPLETE" and (view.last_verdict_payload or {}).get("position_unsafe") \
            and view.state not in (ExecState.CLOSED, ExecState.REJECTED):
        return "ACCOUNTING_INCOMPLETE_UNSAFE"
    if view.state in PRE_SUBMIT or view.state in OUTSTANDING:
        return "ACTIVE_ENTRY"
    if view.state not in (ExecState.CLOSED, ExecState.REJECTED) and ledger.open_size > 0:
        return "OPEN_POSITION"
    return "CLEAR"


def execution_facts(engine, asset, market_ticker):
    """-> (market_execution_state, asset_execution_state) from the execution journal (worst state wins)."""
    views, ledgers = engine.j.reconstruct(), engine.positions()
    lg_objs = {k: engine.ledger(k) for k in ledgers}
    market, other = "CLEAR", "CLEAR"
    for r in engine.j.intents():
        if r["asset"] != asset:
            continue
        st = _execution_state(views[r["execution_key"]], lg_objs[r["execution_key"]])
        if r["market_ticker"] == market_ticker:
            market = max(market, st, key=_SEVERITY.index)
        else:
            other = max(other, st, key=_SEVERITY.index)
    return market, other


def exposures_from_engine(engine):
    """Conservative open exposure per execution: filled cost basis (entry notional + known fees, or UNKNOWN) plus the
    worst case of any still-outstanding remainder (remaining size * limit price)."""
    out = []
    views = engine.j.reconstruct()
    for r in engine.j.intents():
        key, it = r["execution_key"], r["intent"]
        v, lg = views[key], engine.ledger(key)
        if v.state in (ExecState.CLOSED, ExecState.REJECTED) or v.state is None:
            continue
        fees = lg.fees_paid
        filled_risk = UNKNOWN if fees is UNKNOWN else lg.entry_notional + fees
        pending = ZERO
        if v.state in PRE_SUBMIT or v.state in OUTSTANDING or v.state in (ExecState.EXECUTION_UNKNOWN,
                                                                          ExecState.HALTED_FOR_RECONCILIATION):
            pending = lg.remaining_size * lg.max_limit_price
        risk = UNKNOWN if filled_risk is UNKNOWN else filled_risk + pending
        out.append(Exposure(it["asset"], it["market_ticker"], DIRECTION_OF_SIDE[it["side"]], risk,
                            lg.entry_notional + pending))
    return out


def exposure_fields(exposures, policy, asset, market_ticker, direction):
    """Snapshot exposure fields for a candidate (any UNKNOWN risk makes the dependent field UNKNOWN)."""
    group = policy.group_of(asset)

    def total(sel, attr):
        vals = [getattr(e, attr) for e in exposures if sel(e)]
        return UNKNOWN if any(v is UNKNOWN for v in vals) else sum(vals, ZERO)

    in_group = lambda e: policy.group_of(e.asset) == group  # noqa: E731
    return {"open_positions_count": len(exposures),
            "total_open_risk": total(lambda e: True, "open_risk"),
            "total_gross_notional": total(lambda e: True, "gross_notional"),
            "asset_open_risk": total(lambda e: e.asset == asset, "open_risk"),
            "asset_gross_notional": total(lambda e: e.asset == asset, "gross_notional"),
            "same_direction_crypto_risk": total(lambda e: in_group(e) and e.direction == direction, "open_risk"),
            "opposite_direction_crypto_risk": total(lambda e: in_group(e) and e.direction != direction, "open_risk"),
            "current_market_open_risk": total(lambda e: e.market_ticker == market_ticker, "open_risk"),
            "exposure_group": group}
