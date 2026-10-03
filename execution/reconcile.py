"""
Reconciliation (RECONCILIATION_VERSION "reconciliation_v1"): a PURE assessment comparing
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
    FILL_MISMATCH           fills contradict the order / the intent / the journal (size, price, totals, ids)
    ACCOUNTING_INCOMPLETE   evidence is missing (position / fills query failed, stale position, unknown fees);
                            position_unsafe says whether the market must stay locked
"""
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from execution.ledger import FillConflict
from execution.money import UNKNOWN
from execution.states import OUTSTANDING, PRE_SUBMIT, RECONCILIATION, TRANSITIONS, UNSAFE, ExecState

RECONCILIATION_VERSION = "reconciliation_v1"
VERDICTS = ("CONSISTENT", "RECOVERABLE_DIFFERENCE", "EXECUTION_UNKNOWN", "POSITION_MISMATCH", "FILL_MISMATCH",
            "ACCOUNTING_INCOMPLETE")
MISMATCH_VERDICTS = ("POSITION_MISMATCH", "FILL_MISMATCH")
RECONCILIATION_RULES = (
    "pre-submit executions are never queried and are CONSISTENT (nothing was sent)",
    "order lookup failed while a request was in flight (SUBMITTING / CANCEL_PENDING / UNKNOWN / HALTED) -> "
    "EXECUTION_UNKNOWN; otherwise -> ACCOUNTING_INCOMPLETE (unsafe)",
    "order not found: authoritative absence + no fills + flat position -> RECOVERABLE_DIFFERENCE to REJECTED; "
    "otherwise EXECUTION_UNKNOWN (never assume the submit failed)",
    "order parameters differing from the recorded intent -> FILL_MISMATCH",
    "fills query failed -> ACCOUNTING_INCOMPLETE (unsafe)",
    "fills deduplicated by fill_id; the same id with different content -> FILL_MISMATCH",
    "a journaled fill the venue no longer reports -> FILL_MISMATCH",
    "fill total > requested, a fill price > max_limit_price, or fill total != the order's filled size -> FILL_MISMATCH",
    "position query failed or stale -> ACCOUNTING_INCOMPLETE (unsafe)",
    "reported position != ledger exposure of the market (both sides) -> POSITION_MISMATCH",
    "venue-implied state unreachable from the journal state by a reconciliation transition -> FILL_MISMATCH",
    "venue-implied state != journal state and reachable -> RECOVERABLE_DIFFERENCE; equal -> CONSISTENT "
    "(or ACCOUNTING_INCOMPLETE, safe, when a fee is UNKNOWN)",
)


@dataclass
class Assessment:
    verdict: str
    reason: str
    implied_state: Optional[ExecState] = None
    new_fills: list = field(default_factory=list)          # FillReports not yet in the ledger (deduplicated)
    position_unsafe: bool = False
    details: dict = field(default_factory=dict)


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
    if prev == new:
        return True
    allowed = TRANSITIONS.get((prev, new))
    return allowed is not None and RECONCILIATION in allowed


def assess(state, intent, client_order_id, snapshot, ledger, other_exposure):
    """state: journal ExecState; ledger: this execution's ExecutionLedger (not mutated); other_exposure:
    {"YES": Decimal, "NO": Decimal} filled exposure of OTHER executions in the same market."""
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
    if snapshot.fills is None:
        return Assessment("ACCOUNTING_INCOMPLETE", "fills unavailable: " + "; ".join(snapshot.errors), None,
                          position_unsafe=True)
    scratch = ledger.copy()
    new = []
    try:
        for f in snapshot.fills:
            if scratch.add_fill(f.fill_id, f.qty, f.price) == "ADDED":
                new.append(f)
    except FillConflict as e:
        return Assessment("FILL_MISMATCH", f"conflicting fill notifications: {e}", None, position_unsafe=True)
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
                          implied, new, True, {"filled": total})
    expected = dict(other_exposure)
    expected[intent.side] = expected[intent.side] + total
    if (pos.yes, pos.no) != (expected["YES"], expected["NO"]):
        return Assessment("POSITION_MISMATCH", f"venue position yes={pos.yes} no={pos.no} != ledger "
                          f"yes={expected['YES']} no={expected['NO']}", implied, new, True, {"filled": total})
    if not _reachable(state, implied):
        return Assessment("FILL_MISMATCH", f"venue state {implied.value} is unreachable from journal state "
                          f"{state.value}", implied, new, True, {"filled": total})
    fees_unknown = any(f.fee is UNKNOWN for f in snapshot.fills)
    if implied != state or (new and implied == ExecState.PARTIALLY_FILLED):
        return Assessment("RECOVERABLE_DIFFERENCE", f"venue proves {implied.value} (filled {total})", implied, new,
                          False, {"filled": total, "fees_unknown": fees_unknown})
    if fees_unknown:
        return Assessment("ACCOUNTING_INCOMPLETE", "a fee is UNKNOWN (PnL not authoritative)", implied, new, False,
                          {"filled": total})
    return Assessment("CONSISTENT", "journal, venue order, fills and position agree", implied, new, False,
                      {"filled": total})


ZERO = Decimal(0)
