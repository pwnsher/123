"""
Risk-layer failure injection (Step 6.6). Same mechanism as execution.faults (a SimulatedCrash - a BaseException - at
an armed point), with the risk persistence points. A crash inside a risk-store transaction rolls the whole
transaction back: a decision, its approval and any breaker trigger are persisted together or not at all.
"""
from execution.faults import SimulatedCrash

RISK_FAULT_POINTS = (
    "before_decision_write",          # a risk evaluation is about to persist its decision
    "during_decision_transaction",    # between two writes of the decision transaction (atomicity / rollback)
    "after_decision_write",           # the decision transaction committed
    "before_approval_creation",       # inside the transaction: decision written, approval not yet
    "after_approval_creation",        # the approval was committed
    "during_breaker_transition",      # inside the transaction: breaker TRIGGERED written, LATCHED not yet
    "after_breaker_trigger",          # a breaker trigger + latch committed
    "during_approval_consumption",    # inside the consumption transaction
    "after_approval_consumption",     # the consumption committed
    "after_streak_reset_before_breaker_clear",   # inside the CONSECUTIVE_LOSS reset transaction (Step 6.6.2)
    "after_breaker_clear_before_commit",         # inside the CONSECUTIVE_LOSS reset transaction (Step 6.6.2)
)

__all__ = ["RISK_FAULT_POINTS", "RiskFaultInjector", "SimulatedCrash", "NO_RISK_FAULTS"]


class RiskFaultInjector:
    def __init__(self):
        self._armed = {}
        self.hits = {}

    def arm(self, point, nth=1):
        if point not in RISK_FAULT_POINTS:
            raise ValueError(f"unknown risk fault point {point!r}")
        self._armed[point] = int(nth)
        return self

    def disarm(self):
        self._armed.clear()

    def hit(self, point):
        if point not in RISK_FAULT_POINTS:
            raise ValueError(f"unknown risk fault point {point!r}")
        self.hits[point] = self.hits.get(point, 0) + 1
        n = self._armed.get(point)
        if n is None:
            return
        if n <= 1:
            del self._armed[point]
            raise SimulatedCrash(point)
        self._armed[point] = n - 1


NO_RISK_FAULTS = RiskFaultInjector()
