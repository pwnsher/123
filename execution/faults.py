"""
Deterministic failure injection. A test arms a named point; the engine / journal / paper adapter call hit(point) at
that point and a SimulatedCrash is raised exactly there (optionally only on the n-th hit). SimulatedCrash derives
from BaseException so ordinary `except Exception` handlers can never swallow a simulated process death: the
in-process state is abandoned exactly as a real crash would abandon it, and only the journal (and the external
paper venue) survive.
"""

FAULT_POINTS = (
    "before_journal_write",       # a journal transaction is about to start
    "journal_mid_transaction",    # between two writes of ONE journal transaction (atomicity / rollback)
    "after_journal_write",        # right after a journal transaction committed
    "before_submit",              # SUBMITTING is persisted, the adapter has not been called
    "during_submit",              # the venue accepted the order, the adapter call never returned
    "after_submit_before_ack",    # the adapter returned an acknowledgement, ACKNOWLEDGED not yet persisted
    "after_ack",                  # ACKNOWLEDGED persisted
    "before_fill_persist",        # fills were observed, not yet persisted
    "after_partial_fill",         # a partial fill was persisted
    "during_cancel",              # CANCEL_PENDING persisted, the cancel request is in flight
    "after_cancel",               # the cancel outcome was persisted
    "before_settlement_update",   # a settlement is about to be applied
    "during_recovery",            # restart recovery is running
)


class SimulatedCrash(BaseException):
    """A simulated process death at an injected fault point."""


class FaultInjector:
    def __init__(self):
        self._armed = {}              # point -> remaining hits before the crash (1 = the next hit)
        self.hits = {}

    def arm(self, point, nth=1):
        if point not in FAULT_POINTS:
            raise ValueError(f"unknown fault point {point!r}")
        self._armed[point] = int(nth)
        return self

    def disarm(self):
        self._armed.clear()

    def hit(self, point):
        if point not in FAULT_POINTS:
            raise ValueError(f"unknown fault point {point!r}")
        self.hits[point] = self.hits.get(point, 0) + 1
        n = self._armed.get(point)
        if n is None:
            return
        if n <= 1:
            del self._armed[point]
            raise SimulatedCrash(point)
        self._armed[point] = n - 1


NO_FAULTS = FaultInjector()
