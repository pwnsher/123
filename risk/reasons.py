"""
Risk reason codes, their DETERMINISTIC order, and the documented evaluation order (Step 6.6).

Reason codes are always reported sorted by their index in REASON_CODES (never by dict / set iteration order).
Every code is either a HARD VETO (the decision is VETO whatever sizing could allow) or a SIZE CAP (the code names a
limit that reduced the approved size; a cap that leaves <= 0 contracts turns the decision into VETO).
"""

RISK_RULES_VERSION = "risk_rules_v1"

EVALUATION_ORDER = (
    "1 input validity (policy, candidate, snapshot, candidate/snapshot scope)",
    "2 signal status (NO_CALL)",
    "3 candidate expiry / market close",
    "4 risk-snapshot freshness",
    "5 system health (feed, model, calibration, settlement, execution)",
    "6 breakers (latched state + triggers observed in this snapshot)",
    "7 required account information (equity, cash, daily PnL, consecutive-loss state)",
    "8 quote / feature / model-decision staleness",
    "9 market: execution state, existing exposure, executable price, spread, depth",
    "10 fee / slippage / EV known; EV > 0",
    "11 per-trade caps (contracts, notional, worst-case loss, equity fraction, cash)",
    "12 asset caps (open risk, gross notional)",
    "13 portfolio caps (open risk, gross notional)",
    "14 exposure-group cap (crypto group open risk)",
    "15 same-direction group cap",
    "16 executable-depth cap",
    "17 decision: any hard veto -> VETO; else min(all caps) floored to the contract quantum: == requested -> APPROVE,"
    " 0 < q < requested -> REDUCE, <= 0 -> VETO",
)

HARD_VETO_CODES = (
    "INVALID_POLICY", "INVALID_CANDIDATE", "INVALID_SNAPSHOT",
    "NO_CALL",
    "EXPIRED_CANDIDATE",
    "CANDIDATE_ALREADY_EXECUTED",
    "STALE_RISK_SNAPSHOT",
    "FEED_HEALTH_FAIL", "FEED_HEALTH_UNKNOWN", "FEED_HEALTH_UNAVAILABLE", "FEED_HEALTH_DEGRADED",
    "MODEL_HEALTH_FAIL", "MODEL_HEALTH_UNKNOWN", "MODEL_HEALTH_UNAVAILABLE", "MODEL_HEALTH_DEGRADED",
    "CALIBRATION_HEALTH_FAIL", "CALIBRATION_HEALTH_UNKNOWN", "CALIBRATION_HEALTH_UNAVAILABLE",
    "CALIBRATION_HEALTH_DEGRADED",
    "SETTLEMENT_HEALTH_FAIL", "SETTLEMENT_HEALTH_UNKNOWN", "SETTLEMENT_HEALTH_UNAVAILABLE",
    "SETTLEMENT_HEALTH_DEGRADED",
    "EXECUTION_HEALTH_FAIL", "EXECUTION_HEALTH_UNKNOWN", "EXECUTION_HEALTH_UNAVAILABLE", "EXECUTION_HEALTH_DEGRADED",
    "DAILY_REALIZED_LOSS_BREAKER", "DAILY_TOTAL_LOSS_BREAKER", "ROLLING_DRAWDOWN_BREAKER", "CONSECUTIVE_LOSS_BREAKER",
    "ACCOUNT_EQUITY_UNKNOWN", "ACCOUNT_EQUITY_TOO_LOW", "ACCOUNT_CASH_UNKNOWN",
    "DAILY_PNL_UNKNOWN", "EQUITY_STATE_UNKNOWN", "CONSECUTIVE_LOSS_STATE_UNKNOWN",
    "STALE_QUOTE", "STALE_FEATURES", "STALE_MODEL_DECISION",
    "EXECUTION_UNRESOLVED", "EXECUTION_MISMATCH", "EXISTING_MARKET_EXPOSURE", "EXPOSURE_STATE_UNKNOWN",
    "PRICE_UNKNOWN", "PRICE_ABOVE_CAP",
    "SPREAD_UNKNOWN", "SPREAD_TOO_WIDE",
    "DEPTH_UNKNOWN", "INSUFFICIENT_DEPTH",
    "FEE_UNKNOWN", "SLIPPAGE_UNKNOWN", "EV_UNKNOWN", "NON_POSITIVE_EV",
)

CAP_CODES = (
    "PER_TRADE_CONTRACT_LIMIT", "PER_TRADE_NOTIONAL_LIMIT", "PER_TRADE_LOSS_LIMIT", "PER_TRADE_EQUITY_FRACTION_LIMIT",
    "INSUFFICIENT_CASH",
    "ASSET_OPEN_RISK_LIMIT", "ASSET_NOTIONAL_LIMIT",
    "PORTFOLIO_OPEN_RISK_LIMIT", "PORTFOLIO_NOTIONAL_LIMIT",
    "CRYPTO_GROUP_RISK_LIMIT", "SAME_DIRECTION_CRYPTO_LIMIT",
    "DEPTH_CAP",
)

REASON_CODES = HARD_VETO_CODES + CAP_CODES
_INDEX = {c: i for i, c in enumerate(REASON_CODES)}
assert len(_INDEX) == len(REASON_CODES), "duplicate reason code"


def ordered(codes):
    """Deterministic order (index in REASON_CODES), duplicates removed. An unknown code is a programming error."""
    return tuple(sorted(set(codes), key=lambda c: _INDEX[c]))
