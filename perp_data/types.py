"""
Normalized perpetual-futures event envelope.

PerpEvent has the SAME timestamp / ordering fields as Step 3's MarketEvent, so Step-3 alignment
(available(ev, T) = ev.receive_ts_ms <= T), buffers, replay ordering and storage apply unchanged:

    source          venue adapter: "binance_usdm" | "bybit_linear" | "okx_swap" | "kalshi_perp" | "coinbase_usdt"
    asset           "BTC" | "ETH" | "SOL" | "XRP" ("*" for venue-wide messages; "USDT" for the stablecoin rate)
    event_type      PerpEventType
    symbol          the venue's own contract / instrument id (BTCUSDT, BTC-USDT-SWAP, ...)
    event_ts_ms     VENUE event time (None when the venue gives none; never filled from receive time)
    receive_ts_ms   local UTC wall time the message arrived (backfill: when the response arrived)
    receive_mono_ns local monotonic clock at receipt
    ingest_seq      global arrival counter (shared with Step 3 when collected together)
    raw_seq         ingest_seq of the RAW message this event was normalized from (provenance)
    channel         "ws" | "rest": the transport the raw message arrived on (websocket liveness counts only "ws")
    source_seq      venue sequence / update / trade id where provided
    mode            LIVE | BACKFILLED
    quality         OK | ESTIMATE | SAMPLED | UNVERIFIED_UNITS | DERIVED (see QUALITY)
    flags           per-event flags (Step-3 EventFlag values plus PerpFlag values)
    payload         normalized fields (PAYLOAD_FIELDS) + `native` (the venue's own fields kept for verification)

Prices are floats in the contract's QUOTE currency (USDT for the USDT-margined venues, USD for Kalshi
perps and Coinbase); `quote_ccy` in the payload says which. Sizes are normalized to BASE-COIN units
(`qty_coin`) only when the conversion is documented/verified; the native quantity and unit are always
kept. Missing values are None, never 0.
"""
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, Optional, Tuple

from market_data.types import IngestMode
from perp_data import PERP_SCHEMA_VERSION


class PerpEventType(str, Enum):
    PERP_TRADE = "PERP_TRADE"                  # one executed trade (aggregated trade on Binance)
    PERP_QUOTE = "PERP_QUOTE"                  # best bid / ask (+ sizes)
    PERP_MARK_PRICE = "PERP_MARK_PRICE"        # the venue's mark price (NOT the last trade)
    PERP_INDEX_PRICE = "PERP_INDEX_PRICE"      # the venue's spot index (NOT the mark)
    FUNDING_RATE = "FUNDING_RATE"              # the rate that applies at the UPCOMING settlement, as published
    PREDICTED_FUNDING = "PREDICTED_FUNDING"    # a published forecast for the FOLLOWING period (only if published)
    FUNDING_SETTLED = "FUNDING_SETTLED"        # a settled (historical) funding rate from the venue's history API
    OPEN_INTEREST = "OPEN_INTEREST"
    LIQUIDATION = "LIQUIDATION"
    ORDERBOOK_TOP = "ORDERBOOK_TOP"            # top-N levels after the update (N configurable, venue-limited)
    ORDERBOOK_UPDATE = "ORDERBOOK_UPDATE"      # reserved: incremental diffs are applied in the adapter, not stored
    INSTRUMENT = "INSTRUMENT"                  # contract metadata (contract value, funding interval) from REST
    STABLECOIN_RATE = "STABLECOIN_RATE"        # USDT/USD mid (to convert USDT-quoted perps to USD)
    HEARTBEAT = "HEARTBEAT"


class PerpFlag(str, Enum):
    ESTIMATE = "ESTIMATE"                          # value may still change (e.g. in-progress funding)
    SAMPLED_STREAM = "SAMPLED_STREAM"              # venue pushes a SAMPLE (e.g. largest liquidation per second)
    CONTRACT_SIZE_UNVERIFIED = "CONTRACT_SIZE_UNVERIFIED"
    UNITS_UNVERIFIED = "UNITS_UNVERIFIED"
    POSITION_SIDE_DERIVED = "POSITION_SIDE_DERIVED"  # liquidated position derived from the forced-order side
    DEFAULT_INTERVAL = "DEFAULT_INTERVAL"          # funding interval = documented venue default (not published per symbol)
    DELTA_MERGED = "DELTA_MERGED"                  # value carried from a venue snapshot+delta stream


QUALITY = ("OK", "ESTIMATE", "SAMPLED", "UNVERIFIED_UNITS", "DERIVED")

PAYLOAD_FIELDS = {
    PerpEventType.PERP_TRADE: ("price", "qty_native", "qty_unit", "qty_coin", "notional_quote", "aggressor",
                               "aggressor_semantics", "trade_id", "quote_ccy"),
    PerpEventType.PERP_QUOTE: ("bid", "bid_qty_coin", "ask", "ask_qty_coin", "mid", "quote_ccy"),
    PerpEventType.PERP_MARK_PRICE: ("mark", "quote_ccy"),
    PerpEventType.PERP_INDEX_PRICE: ("index", "quote_ccy"),
    PerpEventType.FUNDING_RATE: ("rate_native", "interval_ms", "interval_source", "next_funding_ts_ms",
                                 "rate_per_hour", "rate_8h", "rate_annual_simple", "is_estimate"),
    PerpEventType.PREDICTED_FUNDING: ("rate_native", "interval_ms", "applies_at_ts_ms", "rate_per_hour", "rate_8h"),
    PerpEventType.FUNDING_SETTLED: ("rate_native", "funding_ts_ms", "interval_ms", "rate_8h"),
    PerpEventType.OPEN_INTEREST: ("oi_native", "oi_unit", "oi_coin", "oi_quote_native"),
    PerpEventType.LIQUIDATION: ("price", "qty_native", "qty_unit", "qty_coin", "notional_quote", "forced_side",
                                "liquidated_position", "side_semantics", "quote_ccy"),
    PerpEventType.ORDERBOOK_TOP: ("bids", "asks", "depth", "update_id", "prev_update_id", "quote_ccy"),
    PerpEventType.ORDERBOOK_UPDATE: ("changes",),
    PerpEventType.INSTRUMENT: ("contract_value", "contract_value_ccy", "funding_interval_ms", "verified"),
    PerpEventType.STABLECOIN_RATE: ("pair", "bid", "ask", "mid"),
    PerpEventType.HEARTBEAT: (),
}


@dataclass(frozen=True)
class PerpEvent:
    source: str
    asset: str
    event_type: PerpEventType
    symbol: str
    event_ts_ms: Optional[int]
    receive_ts_ms: int
    ingest_seq: int
    payload: Dict[str, Any]
    receive_mono_ns: Optional[int] = None
    raw_seq: Optional[int] = None
    source_seq: Optional[Any] = None
    mode: IngestMode = IngestMode.LIVE
    quality: str = "OK"
    flags: Tuple[str, ...] = ()
    session_id: str = ""
    schema_version: int = PERP_SCHEMA_VERSION
    family: str = "perp"
    channel: str = "ws"

    def __post_init__(self):
        missing = [k for k in PAYLOAD_FIELDS[self.event_type] if k not in self.payload]
        if missing:
            raise ValueError(f"{self.event_type.value} payload missing {missing}")
        if not isinstance(self.receive_ts_ms, int) or isinstance(self.receive_ts_ms, bool):
            raise ValueError("receive_ts_ms must be an integer epoch ms")
        if self.event_ts_ms is not None and (not isinstance(self.event_ts_ms, int) or isinstance(self.event_ts_ms, bool)):
            raise ValueError("event_ts_ms must be an integer epoch ms or None")
        if self.quality not in QUALITY:
            raise ValueError(f"unknown quality {self.quality!r}")

    @property
    def series_ts_ms(self):
        """Ordering time inside a stream: venue event time clamped to the receive time (Step-3 rule)."""
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
        kw["event_type"] = PerpEventType(d["event_type"])
        kw["mode"] = IngestMode(d.get("mode", "LIVE"))
        kw["flags"] = tuple(d.get("flags") or ())
        return cls(**kw)

    def dedup_key(self):
        """Identity of the underlying fact (reconnect / backfill overlaps collapse; first arrival wins)."""
        p = self.payload
        if self.event_type == PerpEventType.PERP_TRADE and p.get("trade_id") is not None:
            return (self.source, self.symbol, "T", str(p["trade_id"]))
        if self.event_type == PerpEventType.FUNDING_SETTLED:
            return (self.source, self.symbol, "FS", p.get("funding_ts_ms"))
        if self.event_type == PerpEventType.LIQUIDATION:
            return (self.source, self.symbol, "L", self.event_ts_ms, p.get("forced_side"), repr(p.get("price")),
                    repr(p.get("qty_native")))
        return None
