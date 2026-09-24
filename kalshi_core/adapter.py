"""
Legacy strategy adapter: kalshi_dashboard.evaluate() output  ->  SignalDecision.

READ-ONLY translation. It contains no strategy logic: no probability, no threshold, no gate.
The legacy result dict is authoritative; this module copies its fields and names its reason.

It fails closed: the adapter reports CALL only when the legacy dict says BOTH
signal is True AND reason == "ENTER" (the two are equivalent by construction in evaluate();
if they ever disagreed the record would be NO_CALL / EVALUATION_ERROR, never a call).

Poller-level outcomes (first signal per ticker, live perp veto) are not visible in the
evaluate() dict; callers that know them pass already_called / gate_result.
"""
from kalshi_core.no_call import NoCallReason, reasons_for_evaluation, LEGACY_ENTER
from kalshi_core.data_health import health_from_evaluation
from kalshi_core.signal import SignalDecision, Decision, Side

# Identifier of the legacy probability model (a label, not a version bump of anything).
LEGACY_MODEL_ID = "legacy_lognormal_strike_cdf_v1"


def _f(v):
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def from_evaluation(r, coin=None, series=None, strategy_fingerprint=None, model_id=LEGACY_MODEL_ID,
                    already_called=False, gate_result=None):
    """Build a SignalDecision from one legacy evaluate() result (a dict). Never raises on
    malformed input: anything unusable becomes NO_CALL / EVALUATION_ERROR."""
    if not isinstance(r, dict):
        r = {"status": "error: result is not a dict"}
    asset = coin or r.get("coin") or "?"
    reasons = reasons_for_evaluation(r)
    legacy_call = (r.get("status") == "ok" and r.get("signal") is True and r.get("reason") == LEGACY_ENTER)
    if not reasons and not legacy_call:
        reasons = [NoCallReason.EVALUATION_ERROR]           # inconsistent legacy dict: fail closed
    if not reasons and already_called:
        reasons = [NoCallReason.ALREADY_CALLED_THIS_MARKET]
    if not reasons and isinstance(gate_result, dict) and gate_result.get("decision") == "BLOCK":
        reasons = [NoCallReason.PERP_VETO_SUPPRESSED]
    decision = Decision.NO_CALL if reasons else Decision.CALL

    p_up_pct = _f(r.get("p_up"))
    p_up = p_up_pct / 100.0 if p_up_pct is not None else None
    fav = r.get("fav")
    ts = _f(r.get("spot_observed_ts"))
    sl = _f(r.get("sl_pct"))
    regime = {k: r[k] for k in ("stoch", "stoch_arrow", "bb", "atr") if k in r}
    return SignalDecision(
        asset=asset, decision=decision, market_ticker=r.get("ticker") or None, series=series,
        timestamp_epoch_ms=int(round(ts * 1000)) if ts is not None else None,
        market_close_time=r.get("close"), time_remaining_min=_f(r.get("remain")),
        strike=_f(r.get("strike")), underlying_price=_f(r.get("spot_raw", r.get("spot"))),
        yes_bid=_f(r.get("up_bid")), yes_ask=_f(r.get("up_ask")),
        no_bid=_f(r.get("dn_bid")), no_ask=_f(r.get("dn_ask")), side_ask=_f(r.get("side_ask")),
        side=Side(fav) if fav in ("UP", "DOWN") else None,
        raw_probability_up=p_up, raw_probability_down=(1.0 - p_up) if p_up is not None else None,
        calibrated_probability_up=None, calibration_method="NONE",
        confidence_pct=_f(r.get("conf")), raw_edge_cents=_f(r.get("raw_edge")),
        net_edge_cents=_f(r.get("net_edge")), recommended_stop_cents=_f(r.get("rec_stop")),
        stop_loss_fraction=sl / 100.0 if sl is not None else None,
        volatility_per_min=None, regime=regime,
        model_id=model_id, strategy_fingerprint=strategy_fingerprint,
        no_call_reasons=reasons, legacy_status=r.get("status"), legacy_reason=r.get("reason"),
        legacy_verdict=r.get("verdict"), data_health=list(health_from_evaluation(r)),
        perp_overlay=dict(gate_result) if isinstance(gate_result, dict) else None)
