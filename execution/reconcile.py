"""
Reconciliation (RECONCILIATION_VERSION "reconciliation_v2"): a PURE assessment comparing
    journal state  <->  expected order (the recorded intent)  <->  adapter-observed order
    fills (deduplicated by fill_id)  <->  calculated position (ledger)  <->  adapter-reported position
and returning one verdict. Contradictory evidence is NEVER silently repaired: it becomes a mismatch verdict, the
engine halts the execution and the market stays locked until later evidence is consistent.

Verdicts
    CONSISTENT              everything agrees with the journal
    RECOVERABLE_DIFFERENCE  the venue PROVES a state the journal has not recorded yet (e.g. journal ACKNOWLEDGED,
                            venue FILLED with fills totalling the size) -> the engine records the proven state
    EXECUTION_UNKNOWN       the outcome cannot be proven (lost acknowledgement, order not found without an
                            authoritative absence proof, lookup failed while a request was in flight)
    POSITION_MISMATCH       the venue position contradicts the fills
    FILL_MISMATCH           fills / fees / the order contradict the intent or the journal (size, price, totals, the
                            immutable fill identity, a changed known fee, a fill re-mapped to another fee, an order
                            the venue denies although the journal recorded its acknowledgement, or a venue-implied
                            state the transition graph cannot reach from the journal state)
    ACCOUNTING_INCOMPLETE   evidence is missing (position / fills query failed, stale position, unknown fees);
                            position_unsafe says whether the market must stay locked
"""
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from execution.ledger import FillConflict
from execution.money import UNKNOWN
from execution.states import PRE_SUBMIT, RECONCILIATION, TRANSITIONS, UNSAFE, ExecState

RECONCILIATION_VERSION = "reconciliation_v2"
VERDICTS = ("CONSISTENT", "RECOVERABLE_DIFFERENCE", "EXECUTION_UNKNOWN", "POSITION_MISMATCH", "FILL_MISMATCH",
            "ACCOUNTING_INCOMPLETE")
MISMATCH_VERDICTS = ("POSITION_MISMATCH", "FILL_MISMATCH")
RECONCILIATION_RULES = (
    "pre-submit executions are never queried and are CONSISTENT (nothing was sent)",
    "order lookup failed while a request was in flight (SUBMITTING / CANCEL_PENDING / UNKNOWN / HALTED) -> "
    "EXECUTION_UNKNOWN; otherwise -> ACCOUNTING_INCOMPLETE (unsafe)",
    "order not found + authoritative absence while the journal recorded an acknowledgement / order id / fill -> "
    "FILL_MISMATCH (contradiction, halt)",
    "order not found: authoritative absence + never acknowledged + no fills + flat position -> RECOVERABLE_DIFFERENCE "
    "to REJECTED; otherwise EXECUTION_UNKNOWN (never assume the submit failed)",
    "order parameters differing from the recorded intent, or an order_id differing from the journaled one -> "
    "FILL_MISMATCH",
    "fills query failed -> ACCOUNTING_INCOMPLETE (unsafe)",
    "every venue fill must carry this execution's client_order_id and the observed order_id -> else FILL_MISMATCH",
    "fills deduplicated by fill_id; the same id with any identity field changed (qty, price, order_id, "
    "client_order_id, fee_id) -> FILL_MISMATCH",
    "a journaled fill the venue no longer reports -> FILL_MISMATCH",
    "fees: one logical fee per fill; known == known -> no change; known != known -> FILL_MISMATCH; UNKNOWN -> known "
    "-> an append-only FEE_RESOLUTION (the same fee, never a second charge); known -> UNKNOWN or UNKNOWN -> UNKNOWN "
    "-> no new evidence",
    "fill total > requested, a fill price > max_limit_price, or fill total != the order's filled size -> FILL_MISMATCH",
    "position query failed or stale -> ACCOUNTING_INCOMPLETE (unsafe)",
    "reported position != ledger exposure of the market (both sides) -> POSITION_MISMATCH",
    "venue-implied state != journal state -> RECOVERABLE_DIFFERENCE; a fee resolution alone -> RECOVERABLE_DIFFERENCE "
    "(no state change); otherwise CONSISTENT (or ACCOUNTING_INCOMPLETE, safe, while a fee is UNKNOWN)",
    "CENTRAL: any CONSISTENT / RECOVERABLE_DIFFERENCE whose implied state the transition graph cannot reach from the "
    "journal state by a reconciliation transition -> FILL_MISMATCH (the engine never attempts an undefined transition)",
)


@dataclass
class Assessment:
    verdict: str
    reason: str
    implied_state: Optional[ExecState] = None
    new_fills: list = field(default_factory=list)          # FillReports not yet in the ledger (deduplicated)
    position_unsafe: bool = False
    details: dict = field(default_factory=dict)
    fee_resolutions: list = field(default_factory=list)    # FillReports whose UNKNOWN fee the venue now resolves


def implied_state(order, total, requested):
    """The state the venue's evidence proves."""
    if order.status == "REJECTED":
        return ExecState.REJECTED
    if order.status == "CANCELLED":
        return ExecState.CANCELLED
    if order.status == "EXPIRED":
        return ExecState.EXPIRED
    if total == requested:
        return ExecState.FILLED
    if total > 0:
        return ExecState.PARTIALLY_FILLED
    return ExecState.ACKNOWLEDGED


def _reachable(prev, new):
    """Can a RECONCILIATION transition move the journal from `prev` to `new`? EXECUTION_UNKNOWN is always halted first
    by the engine, so it is judged as HALTED_FOR_RECONCILIATION (mirrors ExecutionEngine._apply)."""
    if prev == ExecState.EXECUTION_UNKNOWN:
        prev = ExecState.HALTED_FOR_RECONCILIATION
    if prev == new:
        return True
    allowed = TRANSITIONS.get((prev, new))
    return allowed is not None and RECONCILIATION in allowed


def assess(state, intent, client_order_id, snapshot, ledger, other_exposure, journal_order_id=None,
           acknowledged=False):
    """state: journal ExecState; ledger: this execution's ExecutionLedger (not mutated); other_exposure:
    {"YES": Decimal, "NO": Decimal} filled exposure of OTHER executions in the same market; journal_order_id: the
    venue order id the journal recorded (None if never recorded); acknowledged: the journal history contains an
    acknowledgement (any ACKNOWLEDGED-or-later state).

    Every verdict that would make the engine record a state (CONSISTENT / RECOVERABLE_DIFFERENCE) is validated HERE,
    centrally, against the authoritative transition graph: an unreachable implication becomes FILL_MISMATCH."""
    a = _assess(state, intent, client_order_id, snapshot, ledger, other_exposure, journal_order_id, acknowledged)
    if (a.verdict in ("CONSISTENT", "RECOVERABLE_DIFFERENCE") and a.implied_state is not None
            and not _reachable(state, a.implied_state)):
        return Assessment("FILL_MISMATCH", f"venue evidence implies {a.implied_state.value}, which is unreachable from "
                          f"journal state {state.value} ({a.reason})", a.implied_state, a.new_fills, True,
                          dict(a.details, unreachable=True))
    return a


def _assess(state, intent, client_order_id, snapshot, ledger, other_exposure, journal_order_id, acknowledged):
    if state in PRE_SUBMIT:
        return Assessment("CONSISTENT", "pre-submit: nothing was sent", state)
    in_flight = state in (ExecState.SUBMITTING, ExecState.CANCEL_PENDING) or state in UNSAFE
    lookup = snapshot.lookup
    if lookup is None:
        if in_flight:
            return Assessment("EXECUTION_UNKNOWN", "order lookup failed while the outcome is unproven: "
                              + "; ".join(snapshot.errors), None, position_unsafe=True)
        return Assessment("ACCOUNTING_INCOMPLETE", "order lookup failed: " + "; ".join(snapshot.errors), None,
                          position_unsafe=True)
    pos = snapshot.position
    if lookup.order is None:
        if lookup.authoritative and (acknowledged or journal_order_id is not None or ledger.filled_size > 0):
            return Assessment("FILL_MISMATCH", "the venue authoritatively denies an order the journal recorded as "
                              f"acknowledged (order_id={journal_order_id}, filled={ledger.filled_size})", None,
                              position_unsafe=True)
        flat = (pos is not None and not pos.stale and pos.yes == other_exposure["YES"]
                and pos.no == other_exposure["NO"])
        if lookup.authoritative and snapshot.fills == () and flat and ledger.filled_size == 0:
            return Assessment("RECOVERABLE_DIFFERENCE", "authoritative lookup proves the order was never accepted",
                              ExecState.REJECTED)
        return Assessment("EXECUTION_UNKNOWN", "order not found and its absence is not proven "
                          f"(authoritative={lookup.authoritative})", None, position_unsafe=True)
    o = lookup.order
    if (o.client_order_id != client_order_id or o.side != intent.side or o.market_ticker != intent.market_ticker
            or o.count != intent.requested_contracts or o.limit_price != intent.max_limit_price):
        return Assessment("FILL_MISMATCH", "the venue order's parameters contradict the recorded intent", None,
                          position_unsafe=True)
    if journal_order_id is not None and o.order_id != journal_order_id:
        return Assessment("FILL_MISMATCH", f"the venue reports order {o.order_id} for this client_order_id; the journal "
                          f"recorded {journal_order_id}", None, position_unsafe=True)
    if snapshot.fills is None:
        return Assessment("ACCOUNTING_INCOMPLETE", "fills unavailable: " + "; ".join(snapshot.errors), None,
                          position_unsafe=True)
    for f in snapshot.fills:                     # a fill of ANOTHER order is never accepted, however plausible it looks
        if f.client_order_id != client_order_id:
            return Assessment("FILL_MISMATCH", f"fill {f.fill_id} carries client_order_id {f.client_order_id}, not "
                              f"{client_order_id}", None, position_unsafe=True)
        if f.order_id != o.order_id:
            return Assessment("FILL_MISMATCH", f"fill {f.fill_id} references order {f.order_id}, not {o.order_id}",
                              None, position_unsafe=True)
    scratch = ledger.copy()
    new, resolutions = [], []
    try:
        for f in snapshot.fills:
            if scratch.add_fill(f.fill_id, f.qty, f.price, f.order_id, f.client_order_id, f.fee_id) == "ADDED":
                scratch.add_fee(f.fee_id, f.fee, f.fill_id)
                new.append(f)
        for f in snapshot.fills:                 # fee observations of every reported fill (new and journaled)
            if scratch.fee_for_fill.get(f.fill_id) != f.fee_id:
                return Assessment("FILL_MISMATCH", f"fill {f.fill_id} maps to fee {scratch.fee_for_fill.get(f.fill_id)}"
                                  f" in the journal, the venue now says {f.fee_id}", None, position_unsafe=True)
            cur = scratch.fee_amount(scratch.fee_for_fill[f.fill_id])
            if f.fee is UNKNOWN:
                continue                         # no new evidence (UNKNOWN -> UNKNOWN, or a known fee not re-reported)
            if cur is UNKNOWN:
                scratch.resolve_fee(f.fee_id, f.fee)
                resolutions.append(f)
            elif cur != f.fee:
                return Assessment("FILL_MISMATCH", f"fee {f.fee_id} of fill {f.fill_id} is journaled as {cur}; the "
                                  f"venue now reports {f.fee} (immutable accounting evidence contradicts)", None,
                                  position_unsafe=True)
    except FillConflict as e:
        return Assessment("FILL_MISMATCH", f"conflicting fill / fee notifications: {e}", None, position_unsafe=True)
    venue_ids = {f.fill_id for f in snapshot.fills}
    missing = [fid for fid in ledger.fill_ids() if fid not in venue_ids]
    if missing:
        return Assessment("FILL_MISMATCH", f"journaled fills not reported by the venue: {missing[:3]}", None,
                          position_unsafe=True)
    total = scratch.filled_size
    if total > intent.requested_contracts or any(p > intent.max_limit_price for _q, p in scratch.fill_values()):
        return Assessment("FILL_MISMATCH", "fills exceed the intent's size or price cap", None, new, True)
    if total != o.filled:
        return Assessment("FILL_MISMATCH", f"fills total {total} != venue filled {o.filled}", None, new, True)
    implied = implied_state(o, total, intent.requested_contracts)
    if pos is None or pos.stale:
        return Assessment("ACCOUNTING_INCOMPLETE", "position unavailable" if pos is None else "position report stale",
                          implied, new, True, {"filled": total}, resolutions)
    expected = dict(other_exposure)
    expected[intent.side] = expected[intent.side] + total
    if (pos.yes, pos.no) != (expected["YES"], expected["NO"]):
        return Assessment("POSITION_MISMATCH", f"venue position yes={pos.yes} no={pos.no} != ledger "
                          f"yes={expected['YES']} no={expected['NO']}", implied, new, True, {"filled": total},
                          resolutions)
    fees_unknown = not scratch.fees_known
    details = {"filled": total, "fees_unknown": fees_unknown, "order_id": o.order_id,
               "fees_resolved": [f.fee_id for f in resolutions]}
    if implied != state or (new and implied == ExecState.PARTIALLY_FILLED):
        return Assessment("RECOVERABLE_DIFFERENCE", f"venue proves {implied.value} (filled {total})", implied, new,
                          False, details, resolutions)
    if resolutions:
        return Assessment("RECOVERABLE_DIFFERENCE", f"venue resolves UNKNOWN fee(s) {details['fees_resolved']}",
                          implied, new, False, details, resolutions)
    if fees_unknown:
        return Assessment("ACCOUNTING_INCOMPLETE", "a fee is UNKNOWN (PnL not authoritative)", implied, new, False,
                          details)
    return Assessment("CONSISTENT", "journal, venue order, fills, fees and position agree", implied, new, False,
                      details)


ZERO = Decimal(0)
