"""
Perp-specific normalization: funding units / intervals, open-interest units, contract values, sides.
General price / size / time parsing is Step 3's (market_data.normalization) - one set of rules.

Funding
    Every supported venue publishes funding as a FRACTION of notional PER FUNDING INTERVAL
    (0.0001 = 0.01 % per interval). Intervals differ (8 h default; some symbols 4 h / 1 h). The native value
    is always stored; the normalized forms are
        rate_per_hour      = native / interval_hours
        rate_8h            = rate_per_hour * 8           (the conventional quoting basis)
        rate_annual_simple = rate_per_hour * 24 * 365    (simple, NOT compounded)
    They are computed only when the interval is known (published, documented default, or derived from the
    venue's own settlement times). Unknown units/interval -> None (never a guess).

Open interest
    oi_native + oi_unit are always stored. oi_coin (base-coin quantity) is filled only when the unit is the
    base coin, or contracts x a VERIFIED contract value. Raw contract counts are never compared across venues.
"""
from market_data.normalization import NormalizationError, optional_price, price, size  # noqa: F401  (re-export)

HOUR_MS = 3_600_000
EIGHT_HOURS_MS = 8 * HOUR_MS


def funding_normalized(rate_native, interval_ms):
    """(rate_per_hour, rate_8h, rate_annual_simple) or (None, None, None) if the interval is unknown."""
    if rate_native is None or not interval_ms or interval_ms <= 0:
        return None, None, None
    per_hour = rate_native / (interval_ms / HOUR_MS)
    return per_hour, per_hour * 8.0, per_hour * 24.0 * 365.0


def funding_rate(v, name="funding rate"):
    """Funding may legitimately be 0 or negative; it must be finite and plausibly a fraction (|r| < 1)."""
    if v is None or v == "" or isinstance(v, bool):
        raise NormalizationError(f"{name} missing")
    try:
        x = float(v)
    except (TypeError, ValueError):
        raise NormalizationError(f"{name} not numeric: {str(v)[:30]!r}")
    if x != x or x in (float("inf"), float("-inf")) or abs(x) >= 1.0:
        raise NormalizationError(f"{name} not a plausible fraction: {x}")
    return x


def optional_funding_rate(v, name="funding rate"):
    return None if v is None or v == "" else funding_rate(v, name)


def oi_to_coin(oi_native, unit, contract_value=None, contract_value_verified=False):
    """Base-coin open interest, or None when the conversion is not reliable."""
    if oi_native is None:
        return None
    if unit == "coin":
        return oi_native
    if unit == "contracts" and contract_value is not None and contract_value_verified:
        return oi_native * contract_value
    return None


def qty_to_coin(qty_native, unit, contract_value=None, contract_value_verified=False):
    return oi_to_coin(qty_native, unit, contract_value, contract_value_verified)


def side(v, name="side"):
    s = str(v).strip().lower() if v is not None else ""
    if s not in ("buy", "sell"):
        raise NormalizationError(f"{name} not buy/sell: {v!r}")
    return s


OPPOSITE = {"buy": "sell", "sell": "buy"}
# A forced (liquidation) SELL order can only reduce a LONG position; a forced BUY only a SHORT one.
POSITION_CLOSED_BY_FORCED = {"sell": "long", "buy": "short"}
FORCED_SIDE_FOR_POSITION = {"long": "sell", "short": "buy"}
