"""
The execution adapter boundary (ADAPTER_PROTOCOL_VERSION "adapter_protocol_v1").

    ExecutionAdapter                     the interface every adapter implements
        PaperExecutionAdapter            (execution/paper.py) deterministic, scripted, in-process simulation - Step 6.5
        FutureKalshiExecutionAdapter     an interface STUB only: constructing it or calling any method raises
                                         LiveExecutionUnavailable (the existing repository-wide LIVE refusal from
                                         kalshi_core.execution, reused unchanged). It contains no endpoint, no
                                         network import, no request signing and no writable call of any kind.

Outcome semantics an adapter must honour:
    submit_order  -> OrderView (accepted)   | raises SubmitRejected (DEFINITIVE: the venue refused, nothing exists)
                                            | raises anything else  (the outcome is UNPROVEN -> EXECUTION_UNKNOWN)
    cancel_order  -> OrderView              | raises anything        (the outcome is UNPROVEN -> EXECUTION_UNKNOWN)
    get_order     -> OrderLookup(order | None, authoritative): a NOT-FOUND proves "never accepted" only when
                     authoritative=True (an idempotency-key lookup the venue guarantees); otherwise it proves nothing
    get_fills / get_positions / get_balance -> data | raises AdapterUnavailable (no evidence -> UNKNOWN, never zero)
"""
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional, Tuple

from kalshi_core.execution import LIVE_EXECUTION_AVAILABLE, LiveExecutionUnavailable

ADAPTER_PROTOCOL_VERSION = "adapter_protocol_v1"
ADAPTER_METHODS = ("submit_order", "cancel_order", "get_order", "get_fills", "get_positions", "get_balance",
                   "reconcile")
ORDER_STATUSES = ("RESTING", "FILLED", "CANCELLED", "EXPIRED", "REJECTED")


class AdapterError(Exception):
    pass


class SubmitRejected(AdapterError):
    """DEFINITIVE rejection: the venue refused the order; no order exists."""


class AmbiguousOutcome(AdapterError):
    """The request may or may not have taken effect (lost acknowledgement, timeout after sending)."""


class AdapterUnavailable(AdapterError):
    """A query failed: no evidence either way."""


@dataclass(frozen=True)
class OrderRequest:
    client_order_id: str
    market_ticker: str
    side: str
    count: Decimal
    limit_price: Decimal
    time_in_force: str
    expires_at: int


@dataclass(frozen=True)
class OrderView:
    order_id: str
    client_order_id: str
    market_ticker: str
    side: str
    count: Decimal
    limit_price: Decimal
    status: str
    filled: Decimal
    adapter_event_id: str = ""


@dataclass(frozen=True)
class OrderLookup:
    order: Optional[OrderView]
    authoritative: bool = False


@dataclass(frozen=True)
class FillReport:
    fill_id: str
    order_id: str
    client_order_id: str
    qty: Decimal
    price: Decimal
    fee: object                      # Decimal | UNKNOWN
    fee_id: str
    ts_ms: int


@dataclass(frozen=True)
class PositionReport:
    market_ticker: str
    yes: Decimal
    no: Decimal
    stale: bool = False


@dataclass(frozen=True)
class AdapterSnapshot:
    lookup: Optional[OrderLookup]                 # None = the order query failed
    fills: Optional[Tuple[FillReport, ...]]       # None = the fills query failed (never read as "no fills")
    position: Optional[PositionReport]            # None = the position query failed (UNKNOWN, never zero)
    errors: Tuple[str, ...] = field(default_factory=tuple)


class ExecutionAdapter:
    """Interface. `name` identifies the adapter in the journal."""
    name = "abstract"
    writable_venue = False          # True would mean a real venue; no such adapter exists in this build

    def submit_order(self, request):
        raise NotImplementedError

    def cancel_order(self, client_order_id):
        raise NotImplementedError

    def get_order(self, client_order_id):
        raise NotImplementedError

    def get_fills(self, client_order_id):
        raise NotImplementedError

    def get_positions(self, market_ticker):
        raise NotImplementedError

    def get_balance(self):
        raise NotImplementedError

    def reconcile(self, client_order_id, market_ticker):
        """Everything reconciliation needs; each query is independently fallible (a failure is recorded, not guessed)."""
        errors, lookup, fills, pos = [], None, None, None
        try:
            lookup = self.get_order(client_order_id)
        except AdapterError as e:
            errors.append(f"get_order: {e}")
        try:
            fills = tuple(self.get_fills(client_order_id))
        except AdapterError as e:
            errors.append(f"get_fills: {e}")
        try:
            pos = self.get_positions(market_ticker)
        except AdapterError as e:
            errors.append(f"get_positions: {e}")
        return AdapterSnapshot(lookup, fills, pos, tuple(errors))


class FutureKalshiExecutionAdapter(ExecutionAdapter):
    """INTERFACE STUB for an eventual live adapter. Every entry point refuses: live execution does not exist."""
    name = "kalshi_live_stub"

    def __init__(self, *_a, **_k):
        raise LiveExecutionUnavailable("the live Kalshi execution adapter does not exist in this build")

    def _refuse(self, *_a, **_k):
        raise LiveExecutionUnavailable("live Kalshi execution is not implemented and cannot be enabled")

    submit_order = cancel_order = get_order = get_fills = get_positions = get_balance = reconcile = _refuse


assert LIVE_EXECUTION_AVAILABLE is False
