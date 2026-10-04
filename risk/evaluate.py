"""
The PURE risk evaluation (Step 6.6): evaluate(candidate, snapshot, policy, state, now_ms) -> Evaluation.

Deterministic and side-effect free: no clock read, no I/O, no network, no refresh of any input. The caller
(risk.manager) supplies `now_ms` and the persisted RiskState, and persists the result (decision, approval, breaker
triggers) atomically. The evaluation order is risk.reasons.EVALUATION_ORDER.

Risk may only approve, reduce size, tighten price, expire, veto or latch a breaker. It never increases size, widens
price, flips side, creates or changes a signal / probability / EV, overrides NO_CALL or missing data, or manufactures
an approval.

Worst-case loss of a LONG binary contract (no naked short exposure exists in this design), using the MAXIMUM
authorised price - never the bid, mid, model probability or EV:
    per_contract   = approved_max_limit_price + extra_reserve_per_contract
                     (+ unknown_fee_reserve_per_contract / unknown_slippage_reserve_per_contract when that estimate is
                     UNKNOWN and the policy explicitly allows a reserve instead of a veto)
    fixed_reserve  = estimated_fee + estimated_slippage   (the candidate's TOTALS for its requested size, kept in full
                     even when the size is reduced - conservative, never scaled down)
    worst_case(q)  = q * per_contract + fixed_reserve        e.g. 10 * 0.40 + 0.20 = 4.20
Size caps are solved for the largest q with worst_case(q) (or q * price for notional caps) <= the remaining budget,
floored to the contract quantum (0.01, the repository's fixed-point contract granularity): rounding only ever
reduces size. approved = min(requested, every cap) - never an average, median, first match or the largest.
"""
from dataclasses import dataclass, field
from decimal import ROUND_FLOOR, Decimal
from typing import Optional

from execution.intent import DIRECTION_OF_SIDE, SIDES
from execution.money import UNKNOWN
from risk.breakers import BREAKER_REASON, BREAKER_TYPES
from risk.reasons import ordered
from risk.types import (DIRECTIONS, EXEC_EXPOSURE, EXEC_MISMATCH, EXEC_STATES, EXEC_UNRESOLVED, HEALTH,
                        HEALTH_FIELDS, SIGNAL_STATUSES)

CONTRACT_QUANTUM = Decimal("0.01")
ZERO = Decimal(0)
_HEX = set("0123456789abcdef")


@dataclass(frozen=True)
class RiskState:
    """Persisted risk state the evaluation needs (built by RiskStore.state)."""
    breakers: tuple = ()                 # ((breaker_type, state), ...)
    consecutive_losses: int = 0
    unresolved_trade_results: int = 0
    day_peak_equity: Optional[Decimal] = None
    candidate_consumed: bool = False
    candidate_conflict: bool = False     # this candidate_id was evaluated before with DIFFERENT content

    def breaker(self, t):
        return dict(self.breakers).get(t, "CLEAR")


@dataclass
class Evaluation:
    decision: str
    approved_contracts: Decimal
    approved_max_limit_price: object
    worst_case_loss: Decimal
    expires_at: int
    reason_codes: tuple
    reasons: tuple                       # human-readable, same order as reason_codes
    triggers: tuple = ()                 # ((breaker_type, reason), ...) newly observed in this snapshot
    caps: dict = field(default_factory=dict)


def _known(*vs):
    return all(v is not UNKNOWN for v in vs)


def _is_hex(v):
    return isinstance(v, str) and len(v) >= 16 and set(v) <= _HEX


def floor_q(x):
    return x.quantize(CONTRACT_QUANTUM, rounding=ROUND_FLOOR)


def candidate_problems(c, policy):
    out = []
    if not c.candidate_id or not c.market_ticker or not c.checkpoint:
        out.append("candidate_id / market_ticker / checkpoint missing")
    if c.side not in SIDES:
        out.append(f"side {c.side!r}")
    if c.signal_status not in SIGNAL_STATUSES:
        out.append(f"signal_status {c.signal_status!r}")
    if policy.group_of(c.asset) is None:
        out.append(f"asset {c.asset!r} is not in the policy asset_group_map")
    if not (c.decision_ts <= c.created_at < c.expires_at):
        out.append("timestamps must satisfy decision_ts <= created_at < expires_at")
    if c.market_close_ts is UNKNOWN:
        out.append("market_close_ts UNKNOWN")
    if c.signal_status != "CALL":
        return out
    if c.predicted_direction not in DIRECTIONS or DIRECTION_OF_SIDE.get(c.side) != c.predicted_direction:
        out.append(f"predicted_direction {c.predicted_direction!r} inconsistent with side {c.side!r}")
    r, p = c.requested_contracts, c.requested_max_limit_price
    if r is UNKNOWN or r <= 0 or floor_q(r) != r:
        out.append(f"requested_contracts {r!r} must be > 0 with at most 2 decimals")
    if p is UNKNOWN or not (ZERO < p <= 1) or p.quantize(Decimal("0.0001"), rounding=ROUND_FLOOR) != p:
        out.append(f"requested_max_limit_price {p!r} must be in (0, 1] with at most 4 decimals")
    for n in ("raw_probability", "calibrated_probability"):
        v = getattr(c, n)
        if v is UNKNOWN or not (ZERO <= v <= 1):
            out.append(f"{n} must be a known probability for a CALL")
    for n in ("market_bid", "market_ask", "current_executable_price"):
        v = getattr(c, n)
        if v is not UNKNOWN and not (ZERO <= v <= 1):
            out.append(f"{n} outside [0, 1]")
    for n in ("available_depth", "estimated_fee", "estimated_slippage"):
        v = getattr(c, n)
        if v is not UNKNOWN and v < 0:
            out.append(f"{n} < 0")
    if not isinstance(c.model_id, str) or not c.model_id:
        out.append("model_id missing")
    for n in ("model_fingerprint", "calibration_fingerprint", "signal_fingerprint"):
        if not _is_hex(getattr(c, n)):
            out.append(f"{n} missing / not a hex fingerprint (provenance is never manufactured)")
    return out


def snapshot_problems(s, c, policy, now_ms):
    out = []
    if not s.snapshot_id:
        out.append("snapshot_id missing")
    if (s.market_ticker, s.asset, s.side) != (c.market_ticker, c.asset, c.side):
        out.append("snapshot was captured for another market / asset / side")
    for f in HEALTH_FIELDS:
        if getattr(s, f) not in HEALTH:
            out.append(f"{f} {getattr(s, f)!r}")
    for f in ("market_execution_state", "asset_execution_state"):
        if getattr(s, f) not in EXEC_STATES:
            out.append(f"{f} {getattr(s, f)!r}")
    if s.exposure_group != policy.group_of(c.asset):
        out.append(f"exposure_group {s.exposure_group!r} != policy group {policy.group_of(c.asset)!r}")
    for f in ("total_open_risk", "total_gross_notional", "asset_open_risk", "asset_gross_notional",
              "same_direction_crypto_risk", "opposite_direction_crypto_risk", "current_position_size",
              "current_market_open_risk", "spread", "available_depth"):
        v = getattr(s, f)
        if v is not UNKNOWN and v < 0:
            out.append(f"{f} < 0")
    for f in ("open_positions_count", "consecutive_losses", "quote_age_ms", "feature_age_ms", "model_decision_age_ms"):
        v = getattr(s, f)
        if v is not UNKNOWN and v < 0:
            out.append(f"{f} < 0")
    if s.captured_at > now_ms:
        out.append("snapshot captured in the future")
    return out


def breaker_triggers(s, policy, state):
    """-> (triggers [(breaker_type, reason)], unknown [(reason_code, message)]). The ONE definition of when a breaker
    trips, used by evaluate() and RiskManager.observe(). Boundaries: a loss / drawdown > its maximum trips; equal is
    allowed; the N-th consecutive loss (count >= max_consecutive_losses) trips. UNKNOWN inputs never trip a breaker
    (no PnL is invented) but always veto (fail closed)."""
    triggers, unknown = [], []
    r, u = s.daily_realized_pnl, s.daily_unrealized_pnl
    if r is UNKNOWN:
        unknown.append(("DAILY_PNL_UNKNOWN", "daily realized PnL is UNKNOWN (fail closed; no PnL is invented)"))
    elif -r > policy.max_daily_realized_loss:
        triggers.append(("DAILY_REALIZED_LOSS", f"daily realized loss {-r} > {policy.max_daily_realized_loss}"))
    if r is UNKNOWN or u is UNKNOWN:
        unknown.append(("DAILY_PNL_UNKNOWN", "daily total PnL needs authoritative realized AND unrealized PnL"))
    elif -(r + u) > policy.max_daily_total_loss:
        triggers.append(("DAILY_TOTAL_LOSS", f"daily total loss {-(r + u)} > {policy.max_daily_total_loss}"))
    eq, peak = s.account_equity, s.day_peak_equity
    if eq is UNKNOWN or peak is UNKNOWN:
        unknown.append(("EQUITY_STATE_UNKNOWN", "equity / day peak equity UNKNOWN: drawdown cannot be evaluated"))
    else:
        peak = max(peak, eq, state.day_peak_equity if state.day_peak_equity is not None else peak)
        if peak - eq > policy.max_rolling_drawdown:
            triggers.append(("ROLLING_DRAWDOWN", f"drawdown {peak - eq} (peak {peak}) > {policy.max_rolling_drawdown}"))
    if s.consecutive_losses is UNKNOWN or state.unresolved_trade_results > 0:
        unknown.append(("CONSECUTIVE_LOSS_STATE_UNKNOWN", "consecutive-loss state UNKNOWN (unresolved trade results)"))
    else:
        n = max(s.consecutive_losses, state.consecutive_losses)
        if n >= policy.max_consecutive_losses:
            triggers.append(("CONSECUTIVE_LOSS", f"{n} consecutive losses >= {policy.max_consecutive_losses}"))
    return triggers, unknown


def _veto_result(codes, now_ms, triggers=(), caps=None):
    oc = ordered(codes)
    return Evaluation("VETO", ZERO, None, ZERO, now_ms, oc, tuple(f"{k}: {codes[k]}" for k in oc), tuple(triggers),
                      caps or {})


def evaluate(c, s, policy, state, now_ms):
    codes = {}

    def veto(code, msg):
        codes.setdefault(code, msg)

    # 1 validity, 2 signal status
    for p in policy.problems():
        veto("INVALID_POLICY", p)
    for p in candidate_problems(c, policy):
        veto("INVALID_CANDIDATE", p)
    for p in snapshot_problems(s, c, policy, now_ms):
        veto("INVALID_SNAPSHOT", p)
    if state.candidate_conflict:
        veto("INVALID_CANDIDATE", "candidate_id was already evaluated with different content")
    if c.signal_status == "NO_CALL":
        veto("NO_CALL", "the signal is NO_CALL: risk never converts NO_CALL into a trade")
    if codes:
        return _veto_result(codes, now_ms)

    # 3 candidate expiry / market close; already executed
    if now_ms >= c.expires_at:
        veto("EXPIRED_CANDIDATE", f"candidate expired at {c.expires_at}")
    if now_ms >= c.market_close_ts:
        veto("EXPIRED_CANDIDATE", f"market closed at {c.market_close_ts}")
    if state.candidate_consumed:
        veto("CANDIDATE_ALREADY_EXECUTED", "an approval for this candidate was already consumed by an order intent")

    # 4 snapshot freshness
    if now_ms - s.captured_at > policy.max_snapshot_age_ms:
        veto("STALE_RISK_SNAPSHOT", f"snapshot age {now_ms - s.captured_at} ms > {policy.max_snapshot_age_ms}")

    # 5 health (FAIL / UNKNOWN / UNAVAILABLE always veto; DEGRADED only if the policy allows it)
    from risk.policy import HEALTH_ALLOW
    for f in HEALTH_FIELDS:
        h, prefix = getattr(s, f), f.split("_")[0].upper()
        if h == "PASS" or (h == "DEGRADED" and getattr(policy, HEALTH_ALLOW[f])):
            continue
        veto(f"{prefix}_HEALTH_{h}", f"{f} is {h}")

    # 6 breakers: latched state + triggers observed in this snapshot (never cleared by better PnL)
    triggers, unknown = breaker_triggers(s, policy, state)
    for code, msg in unknown:
        veto(code, msg)
    eq = s.account_equity
    triggered = {t for t, _ in triggers}
    for t in BREAKER_TYPES:
        if state.breaker(t) != "CLEAR" or t in triggered:
            veto(BREAKER_REASON[t], f"{t} breaker {'latched' if state.breaker(t) != 'CLEAR' else 'triggered'}")

    # 7 required account information
    if eq is UNKNOWN:
        veto("ACCOUNT_EQUITY_UNKNOWN", "account equity UNKNOWN")
    elif eq < policy.minimum_account_equity:
        veto("ACCOUNT_EQUITY_TOO_LOW", f"equity {eq} < minimum {policy.minimum_account_equity}")
    if s.available_cash is UNKNOWN:
        veto("ACCOUNT_CASH_UNKNOWN", "available cash UNKNOWN")

    # 8 staleness (UNKNOWN age = stale)
    for f, cap, code in (("quote_age_ms", policy.max_quote_age_ms, "STALE_QUOTE"),
                         ("feature_age_ms", policy.max_feature_age_ms, "STALE_FEATURES"),
                         ("model_decision_age_ms", policy.max_decision_age_ms, "STALE_MODEL_DECISION")):
        age = getattr(s, f)
        if age is UNKNOWN or age > cap:
            veto(code, f"{f} {age} > {cap}")
    if now_ms - c.decision_ts > policy.max_decision_age_ms:
        veto("STALE_MODEL_DECISION", f"candidate decision age {now_ms - c.decision_ts} ms > {policy.max_decision_age_ms}")

    # 9 market: execution facts, existing exposure, executable price, spread, depth
    for f in ("market_execution_state", "asset_execution_state"):
        st = getattr(s, f)
        if st in EXEC_UNRESOLVED:
            veto("EXECUTION_UNRESOLVED", f"{f} {st}: execution reconciliation owns it; risk vetoes")
        elif st in EXEC_MISMATCH:
            veto("EXECUTION_MISMATCH", f"{f} {st}: execution reconciliation owns it; risk vetoes")
    if s.market_execution_state in EXEC_EXPOSURE:
        veto("EXISTING_MARKET_EXPOSURE", f"market already has {s.market_execution_state}")
    if not _known(s.current_position_size, s.current_market_open_risk):
        veto("EXPOSURE_STATE_UNKNOWN", "current market position / open risk UNKNOWN")
    elif s.current_position_size > 0 or s.current_market_open_risk > 0:
        veto("EXISTING_MARKET_EXPOSURE", "the market already has open entry exposure (no netting is assumed)")
    if not _known(s.total_open_risk, s.total_gross_notional, s.asset_open_risk, s.asset_gross_notional,
                  s.same_direction_crypto_risk, s.opposite_direction_crypto_risk):
        veto("EXPOSURE_STATE_UNKNOWN", "portfolio / asset / group exposure UNKNOWN")
    price_cap = c.requested_max_limit_price
    if policy.max_entry_price is not None:
        price_cap = min(price_cap, policy.max_entry_price)          # tighten only, never widen
    if c.current_executable_price is UNKNOWN:
        veto("PRICE_UNKNOWN", "current executable price UNKNOWN")
    elif c.current_executable_price > price_cap:
        veto("PRICE_ABOVE_CAP", f"executable price {c.current_executable_price} > approved cap {price_cap}")
    if s.spread is UNKNOWN:
        if policy.require_known_spread:
            veto("SPREAD_UNKNOWN", "spread UNKNOWN")
    elif s.spread > policy.max_spread:
        veto("SPREAD_TOO_WIDE", f"spread {s.spread} > {policy.max_spread}")
    depth = UNKNOWN if not _known(c.available_depth, s.available_depth) else min(c.available_depth, s.available_depth)
    if depth is UNKNOWN:
        if policy.require_known_depth:
            veto("DEPTH_UNKNOWN", "executable depth UNKNOWN (never treated as infinite)")
    elif depth < policy.min_executable_depth:
        veto("INSUFFICIENT_DEPTH", f"depth {depth} < minimum {policy.min_executable_depth}")

    # 10 fee / slippage / EV
    per_extra = policy.extra_reserve_per_contract
    fixed = ZERO
    if c.estimated_fee is UNKNOWN:
        if policy.require_known_fee or policy.unknown_fee_reserve_per_contract is None:
            veto("FEE_UNKNOWN", "estimated_fee UNKNOWN (never treated as zero)")
        else:
            per_extra += policy.unknown_fee_reserve_per_contract
    else:
        fixed += c.estimated_fee
    if c.estimated_slippage is UNKNOWN:
        if policy.require_known_slippage or policy.unknown_slippage_reserve_per_contract is None:
            veto("SLIPPAGE_UNKNOWN", "estimated_slippage UNKNOWN (never treated as zero)")
        else:
            per_extra += policy.unknown_slippage_reserve_per_contract
    else:
        fixed += c.estimated_slippage
    ev = c.estimated_net_ev
    if ev is UNKNOWN:
        if policy.require_known_ev:
            veto("EV_UNKNOWN", "estimated net EV UNKNOWN")
    elif ev <= 0:
        veto("NON_POSITIVE_EV", f"estimated net EV {ev} <= 0")

    if codes:
        return _veto_result(codes, now_ms, triggers)

    # 11-16 size caps (each: the largest q whose exposure stays <= the remaining budget)
    P = price_cap
    per = P + per_extra
    req = c.requested_contracts

    def by_risk(budget):
        return (budget - fixed) / per

    group_risk = s.same_direction_crypto_risk + s.opposite_direction_crypto_risk
    caps = {
        "PER_TRADE_CONTRACT_LIMIT": policy.max_contracts_per_trade,
        "PER_TRADE_NOTIONAL_LIMIT": policy.max_notional_per_trade / P,
        "PER_TRADE_LOSS_LIMIT": by_risk(policy.max_loss_per_trade),
        "PER_TRADE_EQUITY_FRACTION_LIMIT": by_risk(policy.max_fraction_equity_per_trade * eq),
        "INSUFFICIENT_CASH": by_risk(s.available_cash),
        "ASSET_OPEN_RISK_LIMIT": by_risk(policy.max_asset_open_risk - s.asset_open_risk),
        "ASSET_NOTIONAL_LIMIT": (policy.max_asset_gross_notional - s.asset_gross_notional) / P,
        "PORTFOLIO_OPEN_RISK_LIMIT": by_risk(policy.max_portfolio_open_risk - s.total_open_risk),
        "PORTFOLIO_NOTIONAL_LIMIT": (policy.max_portfolio_gross_notional - s.total_gross_notional) / P,
        "CRYPTO_GROUP_RISK_LIMIT": by_risk(policy.max_crypto_group_open_risk - group_risk),
        "SAME_DIRECTION_CRYPTO_LIMIT": by_risk(policy.max_same_direction_crypto_risk - s.same_direction_crypto_risk),
    }
    if depth is not UNKNOWN:
        caps["DEPTH_CAP"] = depth
    caps = {k: max(ZERO, floor_q(v)) for k, v in caps.items()}
    q = min([req] + list(caps.values()))
    binding = {k: f"cap {v} < requested {req}" for k, v in caps.items() if v < req}
    if q <= 0:
        return _veto_result(binding, now_ms, triggers, caps)
    decision = "APPROVE" if q == req else "REDUCE"
    worst = q * per + fixed
    expires = min(now_ms + policy.approval_ttl_ms, c.expires_at, c.market_close_ts)
    oc = ordered(binding)
    return Evaluation(decision, q, P, worst, expires, oc, tuple(f"{k}: {binding[k]}" for k in oc), tuple(triggers),
                      caps)


def worst_case_loss(contracts, max_price, fee_total, slippage_total, extra_per_contract=ZERO):
    """The documented formula, exposed for audit / tests."""
    return contracts * (max_price + extra_per_contract) + fee_total + slippage_total
