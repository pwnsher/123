"""
The execution state machine: ONE authoritative, explicitly enumerated transition table. No arbitrary assignment:
every change goes through check_transition(), and an unlisted transition raises InvalidTransition (fail closed).

Actors
    ENGINE          normal lifecycle steps
    RECONCILIATION  the only actor allowed to leave HALTED_FOR_RECONCILIATION, and the only one allowed to halt an
                    execution because of contradictory evidence (POSITION_MISMATCH / FILL_MISMATCH). Every
                    reconciliation transition must carry evidence (a reconciliation event id).
"""
from enum import Enum


class ExecState(str, Enum):
    CREATED = "CREATED"
    VALIDATED = "VALIDATED"
    PRECHECK = "PRECHECK"
    READY = "READY"
    SUBMITTING = "SUBMITTING"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    SETTLEMENT_PENDING = "SETTLEMENT_PENDING"
    CLOSED = "CLOSED"
    EXECUTION_UNKNOWN = "EXECUTION_UNKNOWN"
    HALTED_FOR_RECONCILIATION = "HALTED_FOR_RECONCILIATION"


S = ExecState
ENGINE = "ENGINE"
RECONCILIATION = "RECONCILIATION"
ACTORS = (ENGINE, RECONCILIATION)

# (from, to) -> actors allowed. None = "no previous state" (the first event of an execution).
TRANSITIONS = {
    (None, S.CREATED): (ENGINE,),
    (S.CREATED, S.VALIDATED): (ENGINE,),
    (S.CREATED, S.REJECTED): (ENGINE,),
    (S.VALIDATED, S.PRECHECK): (ENGINE,),
    (S.VALIDATED, S.REJECTED): (ENGINE,),
    (S.VALIDATED, S.EXPIRED): (ENGINE,),
    (S.PRECHECK, S.READY): (ENGINE,),
    (S.PRECHECK, S.REJECTED): (ENGINE,),
    (S.PRECHECK, S.EXPIRED): (ENGINE,),
    (S.READY, S.SUBMITTING): (ENGINE,),
    (S.READY, S.REJECTED): (ENGINE,),
    (S.READY, S.EXPIRED): (ENGINE,),
    (S.SUBMITTING, S.ACKNOWLEDGED): (ENGINE,),
    (S.SUBMITTING, S.REJECTED): (ENGINE,),
    (S.SUBMITTING, S.EXECUTION_UNKNOWN): (ENGINE,),
    (S.ACKNOWLEDGED, S.PARTIALLY_FILLED): (ENGINE, RECONCILIATION),
    (S.ACKNOWLEDGED, S.FILLED): (ENGINE, RECONCILIATION),
    (S.ACKNOWLEDGED, S.CANCEL_PENDING): (ENGINE,),
    (S.ACKNOWLEDGED, S.CANCELLED): (ENGINE, RECONCILIATION),      # e.g. an unfilled IOC remainder cancelled by venue
    (S.ACKNOWLEDGED, S.EXPIRED): (ENGINE, RECONCILIATION),
    (S.ACKNOWLEDGED, S.EXECUTION_UNKNOWN): (ENGINE,),
    (S.PARTIALLY_FILLED, S.PARTIALLY_FILLED): (ENGINE, RECONCILIATION),
    (S.PARTIALLY_FILLED, S.FILLED): (ENGINE, RECONCILIATION),
    (S.PARTIALLY_FILLED, S.CANCEL_PENDING): (ENGINE,),
    (S.PARTIALLY_FILLED, S.CANCELLED): (ENGINE, RECONCILIATION),  # remainder cancelled: filled exposure is KEPT
    (S.PARTIALLY_FILLED, S.EXPIRED): (ENGINE, RECONCILIATION),
    (S.PARTIALLY_FILLED, S.SETTLEMENT_PENDING): (ENGINE,),         # market resolved: only the filled quantity settles
    (S.PARTIALLY_FILLED, S.EXECUTION_UNKNOWN): (ENGINE,),
    (S.CANCEL_PENDING, S.CANCELLED): (ENGINE, RECONCILIATION),
    (S.CANCEL_PENDING, S.FILLED): (ENGINE, RECONCILIATION),        # filled during the cancel request
    (S.CANCEL_PENDING, S.PARTIALLY_FILLED): (ENGINE, RECONCILIATION),
    (S.CANCEL_PENDING, S.EXECUTION_UNKNOWN): (ENGINE,),
    (S.FILLED, S.SETTLEMENT_PENDING): (ENGINE,),
    (S.CANCELLED, S.SETTLEMENT_PENDING): (ENGINE,),               # guard: only with filled quantity > 0
    (S.EXPIRED, S.SETTLEMENT_PENDING): (ENGINE,),                 # guard: only with filled quantity > 0
    (S.SETTLEMENT_PENDING, S.CLOSED): (ENGINE,),
    (S.EXECUTION_UNKNOWN, S.HALTED_FOR_RECONCILIATION): (ENGINE, RECONCILIATION),
    # contradictory evidence found by reconciliation halts an execution that is past submission
    (S.ACKNOWLEDGED, S.HALTED_FOR_RECONCILIATION): (RECONCILIATION,),
    (S.PARTIALLY_FILLED, S.HALTED_FOR_RECONCILIATION): (RECONCILIATION,),
    (S.FILLED, S.HALTED_FOR_RECONCILIATION): (RECONCILIATION,),
    (S.CANCEL_PENDING, S.HALTED_FOR_RECONCILIATION): (RECONCILIATION,),
    (S.CANCELLED, S.HALTED_FOR_RECONCILIATION): (RECONCILIATION,),
    (S.EXPIRED, S.HALTED_FOR_RECONCILIATION): (RECONCILIATION,),
    (S.SETTLEMENT_PENDING, S.HALTED_FOR_RECONCILIATION): (RECONCILIATION,),
    # only reconciliation, with evidence, releases a halt - to the state the evidence PROVES
    (S.HALTED_FOR_RECONCILIATION, S.ACKNOWLEDGED): (RECONCILIATION,),
    (S.HALTED_FOR_RECONCILIATION, S.PARTIALLY_FILLED): (RECONCILIATION,),
    (S.HALTED_FOR_RECONCILIATION, S.FILLED): (RECONCILIATION,),
    (S.HALTED_FOR_RECONCILIATION, S.CANCELLED): (RECONCILIATION,),
    (S.HALTED_FOR_RECONCILIATION, S.EXPIRED): (RECONCILIATION,),
    (S.HALTED_FOR_RECONCILIATION, S.REJECTED): (RECONCILIATION,),  # only with authoritative proof of no order
    (S.HALTED_FOR_RECONCILIATION, S.SETTLEMENT_PENDING): (RECONCILIATION,),
}

TERMINAL = frozenset({S.REJECTED, S.CLOSED})
PRE_SUBMIT = frozenset({S.CREATED, S.VALIDATED, S.PRECHECK, S.READY})
OUTSTANDING = frozenset({S.SUBMITTING, S.ACKNOWLEDGED, S.PARTIALLY_FILLED, S.CANCEL_PENDING})
UNSAFE = frozenset({S.EXECUTION_UNKNOWN, S.HALTED_FOR_RECONCILIATION})
# an execution in any of these states keeps its market locked (one active entry per asset + market_ticker)
ACTIVE = PRE_SUBMIT | OUTSTANDING | UNSAFE


class InvalidTransition(RuntimeError):
    """A transition that is not in TRANSITIONS (or not allowed for the actor / without evidence): fail closed."""


def check_transition(prev, new, actor=ENGINE, evidence=None):
    prev = None if prev is None else S(prev)
    new = S(new)
    allowed = TRANSITIONS.get((prev, new))
    if allowed is None:
        raise InvalidTransition(f"{prev.value if prev else None} -> {new.value} is not a defined transition")
    if actor not in allowed:
        raise InvalidTransition(f"{prev.value if prev else None} -> {new.value} is not allowed for actor {actor}")
    if actor == RECONCILIATION and not evidence:
        raise InvalidTransition(f"{prev.value if prev else None} -> {new.value}: reconciliation needs evidence")
    return True


def transition_table():
    """Deterministic, serializable form of the table (docs and the execution fingerprint)."""
    return sorted([[a.value if a else None, b.value, list(v)] for (a, b), v in TRANSITIONS.items()],
                  key=lambda r: (r[0] or "", r[1]))
