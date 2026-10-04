"""
Safety breakers (Step 6.6): an explicit, persisted state machine.

    CLEAR --TRIGGER--> TRIGGERED --LATCH--> LATCHED --RESET--> CLEAR

TRIGGER and LATCH are written in ONE risk-store transaction (a trigger is never visible un-latched after a crash).
A latched breaker vetoes every new candidate. It is NEVER cleared because PnL / equity later improves intraday, and
never by a process restart (its state is the replay of the append-only breaker history). RESET rules:
    DAILY_REALIZED_LOSS, DAILY_TOTAL_LOSS, ROLLING_DRAWDOWN: at the first evaluation on a LATER UTC day than the day the
        breaker latched (UTC calendar day of the evaluation timestamp; never the local machine time zone);
    CONSECUTIVE_LOSS: only by an explicit operator reset (a loss streak does not end at midnight); the loss count itself
        resets on a winning trade (see RiskStore.record_trade_result).
Any other transition raises InvalidBreakerTransition (fail closed).
"""
from datetime import datetime, timezone

BREAKER_SCHEMA_VERSION = 1
BREAKER_TYPES = ("DAILY_REALIZED_LOSS", "DAILY_TOTAL_LOSS", "ROLLING_DRAWDOWN", "CONSECUTIVE_LOSS")
BREAKER_STATES = ("CLEAR", "TRIGGERED", "LATCHED")
BREAKER_REASON = {"DAILY_REALIZED_LOSS": "DAILY_REALIZED_LOSS_BREAKER", "DAILY_TOTAL_LOSS": "DAILY_TOTAL_LOSS_BREAKER",
                  "ROLLING_DRAWDOWN": "ROLLING_DRAWDOWN_BREAKER", "CONSECUTIVE_LOSS": "CONSECUTIVE_LOSS_BREAKER"}
BREAKER_TRANSITIONS = {("CLEAR", "TRIGGERED"): "TRIGGER", ("TRIGGERED", "LATCHED"): "LATCH",
                       ("LATCHED", "CLEAR"): "RESET"}
RESET_RULE = {"DAILY_REALIZED_LOSS": "UTC_DAY_BOUNDARY", "DAILY_TOTAL_LOSS": "UTC_DAY_BOUNDARY",
              "ROLLING_DRAWDOWN": "UTC_DAY_BOUNDARY", "CONSECUTIVE_LOSS": "OPERATOR"}


class InvalidBreakerTransition(RuntimeError):
    pass


def check_breaker_transition(breaker_type, prev, new):
    if breaker_type not in BREAKER_TYPES:
        raise InvalidBreakerTransition(f"unknown breaker {breaker_type!r}")
    if (prev, new) not in BREAKER_TRANSITIONS:
        raise InvalidBreakerTransition(f"{breaker_type}: {prev} -> {new} is not a defined breaker transition")
    return BREAKER_TRANSITIONS[(prev, new)]


def utc_day(ts_ms):
    """The UTC calendar day of a millisecond timestamp ('YYYY-MM-DD'); never the local time zone."""
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date().isoformat()
