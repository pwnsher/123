"""
Machine-readable NO_CALL reasons.

Every code below corresponds to a gate that ALREADY EXISTS in the repository. None of them
creates, removes or reorders a gate: the legacy code decides, and this module only names the
decision so it can be counted.

Where each legacy gate lives
    kalshi_dashboard.evaluate()   ->  r["reason"] / r["status"]   (the per-coin decision)
    kalshi_dashboard.poller()     ->  first-signal-per-ticker, live perp veto, pause switch

Legacy evaluate() reports ONE reason: the first failing gate in this fixed order
    BLOCK_COIN, BLOCK_DIRECTION, WAIT_TOO_EARLY, WAIT_LATE, BLOCK_CHOP,
    WAIT_LOW_CONFIDENCE, WAIT_BAD_PRICE, WAIT_NO_EDGE, else ENTER.
ENTER is equivalent to r["signal"] is True (every gate passed). Later failing gates are not
reported by the legacy code, so they are not reported here either (they cannot be recovered
exactly from the rounded outputs).
"""
from enum import Enum


class NoCallReason(str, Enum):
    # ---- evaluate(): r["reason"] ----
    COIN_DISABLED = "COIN_DISABLED"                          # BLOCK_COIN (web toggle ACTIVE_COINS)
    DIRECTION_DISABLED = "DIRECTION_DISABLED"                # BLOCK_DIRECTION (web toggle DIRECTION)
    OUTSIDE_ENTRY_WINDOW_EARLY = "OUTSIDE_ENTRY_WINDOW_EARLY"  # WAIT_TOO_EARLY: remain > MAX_ENTRY_MIN
    OUTSIDE_ENTRY_WINDOW_LATE = "OUTSIDE_ENTRY_WINDOW_LATE"    # WAIT_LATE: remain < LAST_STOP_MIN
    CHOPPY_MARKET = "CHOPPY_MARKET"                          # BLOCK_CHOP: strike crossings > CHOP_MAX
    CONFIDENCE_BELOW_THRESHOLD = "CONFIDENCE_BELOW_THRESHOLD"  # WAIT_LOW_CONFIDENCE: conf < MIN_CONF
    PRICE_BELOW_THRESHOLD = "PRICE_BELOW_THRESHOLD"          # WAIT_BAD_PRICE: side ask < MIN_PRICE
    INSUFFICIENT_EDGE = "INSUFFICIENT_EDGE"                  # WAIT_NO_EDGE: net edge <= 0 or < EDGE_THRESH
    DATA_STALE = "DATA_STALE"                                # status "stale", reason BLOCK_STALE_DATA
    # ---- evaluate() did not return a decision (poller turns these into a status string) ----
    MARKET_UNAVAILABLE = "MARKET_UNAVAILABLE"                # status "no market"
    DATA_UNAVAILABLE = "DATA_UNAVAILABLE"                    # status "net error" (requests exception)
    EVALUATION_ERROR = "EVALUATION_ERROR"                    # status "error: ..." (any other exception)
    # ---- poller(): applied AFTER evaluate() said signal=True ----
    ALREADY_CALLED_THIS_MARKET = "ALREADY_CALLED_THIS_MARKET"  # ticker already in _alerted (first signal only)
    PERP_VETO_SUPPRESSED = "PERP_VETO_SUPPRESSED"            # live veto gate returned BLOCK (default OFF)
    WATCHER_PAUSED = "WATCHER_PAUSED"                        # RUNNING cleared: nothing is evaluated


# legacy evaluate() reason code -> NoCallReason. "ENTER" is not a NO_CALL.
LEGACY_REASON_MAP = {
    "BLOCK_COIN": NoCallReason.COIN_DISABLED,
    "BLOCK_DIRECTION": NoCallReason.DIRECTION_DISABLED,
    "WAIT_TOO_EARLY": NoCallReason.OUTSIDE_ENTRY_WINDOW_EARLY,
    "WAIT_LATE": NoCallReason.OUTSIDE_ENTRY_WINDOW_LATE,
    "BLOCK_CHOP": NoCallReason.CHOPPY_MARKET,
    "WAIT_LOW_CONFIDENCE": NoCallReason.CONFIDENCE_BELOW_THRESHOLD,
    "WAIT_BAD_PRICE": NoCallReason.PRICE_BELOW_THRESHOLD,
    "WAIT_NO_EDGE": NoCallReason.INSUFFICIENT_EDGE,
    "BLOCK_STALE_DATA": NoCallReason.DATA_STALE,
}
LEGACY_ENTER = "ENTER"

# The exact order in which evaluate() tests its gates (documentation + tests only).
LEGACY_GATE_ORDER = ("BLOCK_COIN", "BLOCK_DIRECTION", "WAIT_TOO_EARLY", "WAIT_LATE", "BLOCK_CHOP",
                     "WAIT_LOW_CONFIDENCE", "WAIT_BAD_PRICE", "WAIT_NO_EDGE")


def from_legacy_status(status):
    """Map a non-"ok" legacy status string (as produced by evaluate()/poller) to a reason."""
    s = str(status or "")
    if s == "stale":
        return NoCallReason.DATA_STALE
    if s == "no market":
        return NoCallReason.MARKET_UNAVAILABLE
    if s == "net error":
        return NoCallReason.DATA_UNAVAILABLE
    return NoCallReason.EVALUATION_ERROR


def reasons_for_evaluation(r):
    """NO_CALL reasons for one legacy evaluate() result dict (empty list == legacy ENTER).

    Pure read of the dict; applies no threshold of its own."""
    if not isinstance(r, dict):
        return [NoCallReason.EVALUATION_ERROR]
    if r.get("status") != "ok":
        code = r.get("reason")
        if code in LEGACY_REASON_MAP:
            return [LEGACY_REASON_MAP[code]]
        return [from_legacy_status(r.get("status"))]
    code = r.get("reason")
    if code == LEGACY_ENTER:
        return []
    if code in LEGACY_REASON_MAP:
        return [LEGACY_REASON_MAP[code]]
    return [NoCallReason.EVALUATION_ERROR]         # unknown code: never report it as a call
