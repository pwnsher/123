"""
Common event envelope and normalized payloads.

MarketEvent
    source          "coinbase" | "kraken" | "kalshi" | "cf_via_kalshi" | "cf_direct" | "collector"
    asset           "BTC" | "ETH" | "SOL" | "XRP"
    event_type      EventType
    symbol          the venue's own symbol / ticker / index id
    event_ts_ms     SOURCE event time, UTC epoch ms; None when the source provides none (never filled
                    from the receive time)
    receive_ts_ms   LOCAL UTC wall-clock time the message was received (for backfill: when the
                    backfill response was received). The only time used for causal availability.
    receive_mono_ns local monotonic clock at receipt (latency / staleness arithmetic only)
    ingest_seq      global arrival counter within a session: replay order == original arrival order
    source_seq      venue sequence / trade id when the source provides one
    mode            LIVE | BACKFILLED
    payload         normalized fields for the event type (see PAYLOAD_FIELDS)
    flags           per-event flags (EVENT_TIME_MISSING, DERIVED_ASK, ...)
    session_id / schema_version

Payload conventions: prices and sizes are floats; missing values are None, never 0.
"""
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional, Tuple, Dict, Any

from market_data import MARKET_DATA_SCHEMA_VERSION


class EventType(str, Enum):
    TRADE = "TRADE"                      # an executed trade (spot venue or Kalshi)
    QUOTE = "QUOTE"                      # best bid / best ask (top of book)
    BOOK = "BOOK"                        # depth snapshot (top N levels)
    INDEX_VALUE = "INDEX_VALUE"          # CF Benchmarks RTI value (wraps a settlement observation)
    PUBLISHED_AVERAGE = "PUBLISHED_AVERAGE"  # provider-published average (validation only)
    MARKET_STATE = "MARKET_STATE"        # Kalshi market object (strike, close, bid/ask)
    RESOLUTION = "RESOLUTION"            # Kalshi settled result (LABEL; never a feature)
    HEARTBEAT = "HEARTBEAT"
    FEED_STATUS = "FEED_STATUS"          # connection lifecycle (connect/disconnect/reconnect)


class IngestMode(str, Enum):
    LIVE = "LIVE"
    BACKFILLED = "BACKFILLED"            # retrieved after the fact (REST backfill, subscription snapshot)


class EventFlag(str, Enum):
    EVENT_TIME_MISSING = "EVENT_TIME_MISSING"
    DERIVED_ASK = "DERIVED_ASK"              # ask derived as 100 - opposite bid (Kalshi)
    AGGRESSOR_UNAVAILABLE = "AGGRESSOR_UNAVAILABLE"
    ONE_SIDED_BOOK = "ONE_SIDED_BOOK"
    CROSSED_BOOK = "CROSSED_BOOK"
    SNAPSHOT = "SNAPSHOT"                    # delivered as part of a subscription snapshot


class AggressorSemantics(str, Enum):
    TAKER_SIDE_FIELD = "TAKER_SIDE_FIELD"        # the source's field IS the taker (aggressor) side
    INVERTED_MAKER_SIDE = "INVERTED_MAKER_SIDE"  # the source reports the maker side; aggressor = opposite
    UNAVAILABLE = "UNAVAILABLE"                  # not determinable from documented source semantics


PAYLOAD_FIELDS = {
    EventType.TRADE: ("price", "size", "aggressor", "aggressor_semantics", "trade_id"),
    EventType.QUOTE: ("bid", "bid_size", "ask", "ask_size", "mid"),
    EventType.BOOK: ("bids", "asks", "depth_levels"),
    EventType.INDEX_VALUE: ("index_id", "value", "amend_ts_ms", "repeat_of_previous", "observation"),
    EventType.PUBLISHED_AVERAGE: ("index_id", "kind", "value"),
    EventType.MARKET_STATE: ("ticker", "strike", "strike_source", "close_ts_ms", "open_ts_ms", "yes_bid", "yes_ask",
                             "no_bid", "no_ask", "status"),
    EventType.RESOLUTION: ("ticker", "result", "expiration_value"),
    EventType.HEARTBEAT: ("last_trade_id",),
    EventType.FEED_STATUS: ("state", "detail"),
}


@dataclass(frozen=True)
class MarketEvent:
    source: str
    asset: str
    event_type: EventType
    symbol: str
    event_ts_ms: Optional[int]
    receive_ts_ms: int
    ingest_seq: int
    payload: Dict[str, Any]
    receive_mono_ns: Optional[int] = None
    source_seq: Optional[int] = None
    mode: IngestMode = IngestMode.LIVE
    flags: Tuple[str, ...] = ()
    session_id: str = ""
    schema_version: int = MARKET_DATA_SCHEMA_VERSION

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
        """Ordering time within a stream: the source event time when given (clamped to the receive time -
        an event cannot have happened after we received it; a later venue timestamp is clock skew),
        else the receive time."""
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
        kw["event_type"] = EventType(d["event_type"])
        kw["mode"] = IngestMode(d.get("mode", "LIVE"))
        kw["flags"] = tuple(d.get("flags") or ())
        return cls(**kw)

    def dedup_key(self):
        """Identity of the underlying market fact, so reconnect/backfill duplicates collapse (earliest receive wins)."""
        p = self.payload
        if self.event_type == EventType.TRADE and p.get("trade_id") is not None:
            return (self.source, self.symbol, "T", str(p["trade_id"]))
        if self.event_type == EventType.INDEX_VALUE:
            return (self.source, self.symbol, "I", self.event_ts_ms, repr(p.get("value")), p.get("amend_ts_ms"))
        if self.event_type == EventType.RESOLUTION:
            return (self.source, self.symbol, "R", p.get("result"), repr(p.get("expiration_value")))
        return None                                  # quotes/books/market states: every observation counts


@dataclass(frozen=True)
class RawMessage:
    """The exact text received (bounded), so normalization can be recomputed later."""
    source: str
    stream: str
    receive_ts_ms: int
    ingest_seq: int
    text: str
    receive_mono_ns: Optional[int] = None
    session_id: str = ""

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


@dataclass(frozen=True)
class ParseFailure:
    source: str
    stream: str
    reason: str
    receive_ts_ms: int
    ingest_seq: int
    excerpt: str = ""

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


@dataclass
class ParseResult:
    events: list = field(default_factory=list)
    failures: list = field(default_factory=list)
    control: list = field(default_factory=list)      # acks / subscriptions / errors (not market data)
