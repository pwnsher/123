"""
Kraken (spot) — public websocket API v2. The secondary spot venue. No authentication.

Why Kraken: a long-running, regulated USD venue with deep BTC/ETH/SOL/XRP-USD books, a documented
public v2 websocket with explicit taker-side semantics and RFC3339 microsecond trade timestamps. It is
one of the constituent-exchange families of CF Benchmarks' RTIs, which makes it a meaningful second
view of the market (the goal is not to treat Coinbase's microstructure as the whole market).

Transport   wss://ws.kraken.com/v2  (env KRAKEN_WS_URL overrides)
Subscribe   {"method":"subscribe","params":{"channel":"trade","symbol":[...],"snapshot":true}}
            {"method":"subscribe","params":{"channel":"ticker","symbol":[...],"event_trigger":"bbo"}}
Symbols     BTC/USD, ETH/USD, SOL/USD, XRP/USD

Messages -> events
    channel trade   TRADE  per data[] item: price, qty, side, trade_id, timestamp (RFC3339 µs, UTC).
                    `side` is the side of the TAKER order (Kraken v2 docs) -> aggressor = side.
                    type "snapshot" = the most recent 50 trades at subscription -> BACKFILLED + SNAPSHOT
                    (they happened before we connected). Several trades in one message are separate
                    events and do not necessarily share a taker order.
    channel ticker  QUOTE  bid, bid_qty, ask, ask_qty. The v2 ticker carries no event timestamp in the
                    documented schema -> event_ts None + EVENT_TIME_MISSING (a "timestamp" field is used
                    if present). Such quotes are ordered by receive time.
    channel heartbeat  HEARTBEAT (about once per second while subscribed)
    status / method responses  control (subscription errors are recorded as control, not data)
Trade ids are per-pair counters; jumps are recorded as TRADE_ID gaps (not recoverable here; a
re-subscription snapshot may cover part of it).
"""
import json
import os

from market_data.normalization import iso_ms, mid, optional_price, optional_size, price, size
from market_data.sources.base import SourceAdapter, new_result
from market_data.types import AggressorSemantics, EventFlag, EventType, IngestMode

WS_URL = os.environ.get("KRAKEN_WS_URL", "wss://ws.kraken.com/v2")
SYMBOLS = {"BTC": "BTC/USD", "ETH": "ETH/USD", "SOL": "SOL/USD", "XRP": "XRP/USD"}
_ASSET = {v: k for k, v in SYMBOLS.items()}


class KrakenAdapter(SourceAdapter):
    source = "kraken"
    stream = "ws"
    transport = "ws"
    documentation = "Kraken websocket v2: trade (snapshot), ticker (event_trigger=bbo), heartbeat (public)"

    def __init__(self, assets):
        self.assets = [a for a in assets if a in SYMBOLS]
        self.url = WS_URL

    def subscribe_messages(self):
        syms = [SYMBOLS[a] for a in self.assets]
        return [json.dumps({"method": "subscribe", "params": {"channel": "trade", "symbol": syms, "snapshot": True}}),
                json.dumps({"method": "subscribe", "params": {"channel": "ticker", "symbol": syms, "event_trigger": "bbo"}})]

    def parse(self, text, ctx):
        res = new_result()
        m = self.load(text, ctx, res)
        if m is None:
            return res
        if not isinstance(m, dict):
            self.fail(res, ctx, "message is not an object", text)
            return res
        if "method" in m:
            res.control.append(m)
            return res
        ch = m.get("channel")
        if ch in ("status",):
            res.control.append(m)
            return res
        if ch == "heartbeat":
            res.events.append(self.event(ctx, asset="*", event_type=EventType.HEARTBEAT, symbol="*", event_ts_ms=None,
                                         flags=(EventFlag.EVENT_TIME_MISSING.value,), payload={"last_trade_id": None}))
            return res
        if ch not in ("trade", "ticker") or not isinstance(m.get("data"), list):
            self.fail(res, ctx, f"unexpected message channel {ch!r}", text)
            return res
        snapshot = m.get("type") == "snapshot"
        for item in m["data"]:
            asset = _ASSET.get(item.get("symbol")) if isinstance(item, dict) else None
            if asset is None:
                self.fail(res, ctx, f"unknown symbol {item.get('symbol') if isinstance(item, dict) else item!r}", item)
                continue
            if ch == "trade":
                self.guarded(res, ctx, item, lambda it=item, a=asset: res.events.append(self._trade(it, a, ctx, snapshot)))
            else:
                self.guarded(res, ctx, item, lambda it=item, a=asset: res.events.append(self._quote(it, a, ctx)))
        return res

    def _trade(self, it, asset, ctx, snapshot):
        side = it.get("side")
        aggressor = side if side in ("buy", "sell") else None
        flags = [] if aggressor else [EventFlag.AGGRESSOR_UNAVAILABLE.value]
        if snapshot:
            flags.append(EventFlag.SNAPSHOT.value)
        return self.event(ctx, asset=asset, event_type=EventType.TRADE, symbol=SYMBOLS[asset],
                          event_ts_ms=iso_ms(it["timestamp"]), source_seq=it.get("trade_id"),
                          mode=IngestMode.BACKFILLED if snapshot else IngestMode.LIVE, flags=tuple(flags),
                          payload={"price": price(it["price"]), "size": size(it["qty"], "qty"), "aggressor": aggressor,
                                   "aggressor_semantics": (AggressorSemantics.TAKER_SIDE_FIELD.value if aggressor
                                                           else AggressorSemantics.UNAVAILABLE.value),
                                   "trade_id": int(it["trade_id"]) if it.get("trade_id") is not None else None})

    def _quote(self, it, asset, ctx):
        bid, ask = optional_price(it.get("bid"), "bid"), optional_price(it.get("ask"), "ask")
        ts = iso_ms(it["timestamp"]) if it.get("timestamp") else None
        flags = [] if ts is not None else [EventFlag.EVENT_TIME_MISSING.value]
        if bid is None or ask is None:
            flags.append(EventFlag.ONE_SIDED_BOOK.value)
        elif ask < bid:
            flags.append(EventFlag.CROSSED_BOOK.value)
        return self.event(ctx, asset=asset, event_type=EventType.QUOTE, symbol=SYMBOLS[asset], event_ts_ms=ts,
                          flags=tuple(flags),
                          payload={"bid": bid, "bid_size": optional_size(it.get("bid_qty"), "bid_qty"), "ask": ask,
                                   "ask_size": optional_size(it.get("ask_qty"), "ask_qty"), "mid": mid(bid, ask)})
