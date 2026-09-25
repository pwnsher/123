"""
MicroEvent: the Step-5 envelope. Same timestamp / ordering fields as Step 3's MarketEvent and Step 4's PerpEvent
(event_ts_ms, receive_ts_ms, receive_mono_ns, ingest_seq, raw_seq, source_seq, mode, flags, channel), so the
Step-3 alignment rule and replay ordering apply unchanged.

Event types
    BOOK_SNAPSHOT   a complete price-level book (initial or re-snapshot): bids / asks [[price, qty], ...]
    BOOK_DELTA      incremental price-level changes: changes [[side, price, qty, mode]]
                        mode "abs": qty is the new ABSOLUTE size at that price (0 deletes the level)
                        mode "rel": qty is a signed CHANGE of the size (Kalshi)
                    plus the venue's sequence fields (update_id, prev_update_id, first_update_id) and checksum
    BOOK_RESET      the collector's statement that the local book can no longer be trusted from receive_ts on
                    (transport outage, forced resubscription, overload); the book is INVALID until a snapshot
    TRADE           a trade on a venue whose trades are not already captured by Steps 3/4 (Kalshi websocket)
    INSTRUMENT      metadata needed to interpret sizes (tick / contract value / checksum precision)

Units are explicit in every payload: `price_unit` ("USD", "USDT", "YES_CENTS") and `qty_unit` ("coin",
"contracts"). Kalshi books are normalized to YES terms (bids = YES bids; asks = YES asks = 100 - NO bids, in
cents) and the venue's own side / price are kept under `native` (see microstructure/kalshi.py).
All books are PRICE-LEVEL aggregates: no venue here publishes order-level identifiers, so no individual queue
position exists anywhere in this package.
"""
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, Optional, Tuple

from market_data.types import IngestMode
from microstructure import MICRO_SCHEMA_VERSION


class MicroEventType(str, Enum):
    BOOK_SNAPSHOT = "BOOK_SNAPSHOT"
    BOOK_DELTA = "BOOK_DELTA"
    BOOK_RESET = "BOOK_RESET"
    TRADE = "TRADE"
    INSTRUMENT = "INSTRUMENT"


PAYLOAD_FIELDS = {
    MicroEventType.BOOK_SNAPSHOT: ("book", "bids", "asks", "price_unit", "qty_unit", "update_id", "depth"),
    MicroEventType.BOOK_DELTA: ("book", "changes", "price_unit", "qty_unit", "update_id", "prev_update_id",
                                "first_update_id", "checksum"),
    MicroEventType.BOOK_RESET: ("book", "reason"),
    MicroEventType.TRADE: ("price", "qty", "price_unit", "qty_unit", "aggressor", "aggressor_semantics", "trade_id"),
    MicroEventType.INSTRUMENT: ("book", "info"),
}


@dataclass(frozen=True)
class MicroEvent:
    source: str
    asset: str
    event_type: MicroEventType
    symbol: str
    event_ts_ms: Optional[int]
    receive_ts_ms: int
    ingest_seq: int
    payload: Dict[str, Any]
    receive_mono_ns: Optional[int] = None
    raw_seq: Optional[int] = None
    source_seq: Optional[Any] = None
    mode: IngestMode = IngestMode.LIVE
    flags: Tuple[str, ...] = ()
    session_id: str = ""
    schema_version: int = MICRO_SCHEMA_VERSION
    family: str = "micro"
    channel: str = "ws"

    def __post_init__(self):
        missing = [k for k in PAYLOAD_FIELDS[self.event_type] if k not in self.payload]
        if missing:
            raise ValueError(f"{self.event_type.value} payload missing {missing}")
        if not isinstance(self.receive_ts_ms, int) or isinstance(self.receive_ts_ms, bool):
            raise ValueError("receive_ts_ms must be an integer epoch ms")
        if self.event_ts_ms is not None and (not isinstance(self.event_ts_ms, int) or isinstance(self.event_ts_ms, bool)):
            raise ValueError("event_ts_ms must be an integer epoch ms or None")

    @property
    def series_ts_ms(self):
        if self.event_ts_ms is None:
            return self.receive_ts_ms
        return min(self.event_ts_ms, self.receive_ts_ms)

    def to_dict(self):
        d = asdict(self)
        d["event_type"] = self.event_type.value
        d["mode"] = self.mode.value
        d["flags"] = list(self.flags)
        return d

    @classmethod
    def from_dict(cls, d):
        kw = dict(d)
        kw["event_type"] = MicroEventType(d["event_type"])
        kw["mode"] = IngestMode(d.get("mode", "LIVE"))
        kw["flags"] = tuple(d.get("flags") or ())
        return cls(**kw)

    def dedup_key(self):
        if self.event_type == MicroEventType.TRADE and self.payload.get("trade_id") is not None:
            return (self.source, self.symbol, "T", str(self.payload["trade_id"]))
        return None


def book_key(ev):
    """Identity of a local book: (source, symbol)."""
    return (ev.source, ev.symbol)
