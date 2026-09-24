"""
Execution SAFETY BOUNDARY.

This repository has NO live order execution, and this step adds none. This module exists so
that the future execution layer has exactly one, explicit, fenced entry point:

  * The only way to reach an ExecutionEngine is get_execution_engine(mode).
  * Mode LIVE always raises LiveExecutionUnavailable. There is no environment variable, flag,
    file or argument that changes this: enabling live trading will require new code, review and
    tests in a later phase (see docs/ROADMAP.md, phases 13-17).
  * Mode DISABLED returns an engine whose submit() always raises ExecutionDisabled.
  * An engine accepts only an OrderIntent built from a SignalDecision AND an approving
    RiskDecision. A SignalDecision alone can never be submitted (prediction/signal code cannot
    place orders).
  * This module imports no network library and contains no API endpoint.

Existing PAPER behaviour is untouched: kalshi_dashboard.log_call()/_append_paper() keep
journaling the order each call WOULD place (kalshi_paper_orders.csv), exactly as before; that
legacy paper journal does not go through this module.
kalshi_api_learn.py (a separate, demo-locked, DRY_RUN learning client) is not imported by any
core module and is not an execution engine.
"""
from dataclasses import dataclass
from enum import Enum

from kalshi_core.signal import SignalDecision, Decision
from kalshi_core.interfaces import RiskDecision

LIVE_EXECUTION_AVAILABLE = False     # a constant, not a setting


class ExecutionMode(str, Enum):
    DISABLED = "DISABLED"
    PAPER = "PAPER"
    LIVE = "LIVE"


class ExecutionDisabled(RuntimeError):
    pass


class LiveExecutionUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class OrderIntent:
    """An order a future engine MAY place: only constructible from a CALL plus risk approval."""
    decision: SignalDecision
    risk: RiskDecision
    contracts: int
    limit_price_cents: float

    def __post_init__(self):
        if not isinstance(self.decision, SignalDecision) or self.decision.decision != Decision.CALL:
            raise ValueError("an OrderIntent requires a CALL SignalDecision")
        if not isinstance(self.risk, RiskDecision) or not self.risk.approved:
            raise ValueError("an OrderIntent requires an approving RiskDecision")
        if not (isinstance(self.contracts, int) and 0 < self.contracts <= self.risk.max_contracts):
            raise ValueError("contracts must be positive and within the risk limit")
        if not (0 < float(self.limit_price_cents) < 100):
            raise ValueError("limit price must be inside (0, 100) cents")


class DisabledExecutionEngine:
    mode = ExecutionMode.DISABLED

    def submit(self, intent):
        raise ExecutionDisabled("execution is disabled: this build places no orders")


def get_execution_engine(mode=ExecutionMode.DISABLED):
    mode = ExecutionMode(mode)
    if mode == ExecutionMode.LIVE:
        raise LiveExecutionUnavailable("live execution does not exist in this build and cannot be enabled")
    if mode == ExecutionMode.PAPER:
        raise ExecutionDisabled("no paper execution engine exists yet; the legacy paper journal in "
                                "kalshi_dashboard.py is unchanged and does not use this module")
    return DisabledExecutionEngine()
