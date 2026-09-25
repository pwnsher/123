"""
Coinbase Exchange (spot) — public websocket feed + REST trade backfill. No authentication.

Transport   wss://ws-feed.exchange.coinbase.com  (env COINBASE_WS_URL overrides)
Subscribe   {"type": "subscribe", "product_ids": [...], "channels": ["ticker", "matches", "heartbeat"]}
Symbols     BTC-USD, ETH-USD, SOL-USD, XRP-USD (the same products the production model uses)

Messages -> events
    match / last_match   TRADE  price, size, trade_id, time (event time, RFC3339 µs, UTC), sequence
                         `side` is the MAKER order side (Coinbase docs), so the AGGRESSOR is the opposite
                         side: side "sell" -> aggressive BUY. last_match (sent on subscribe) is the
                         most recent trade BEFORE the subscription -> BACKFILLED + SNAPSHOT.
    ticker               QUOTE  best_bid / best_bid_size / best_ask / best_ask_size at `time`.
                         (Coinbase's ticker updates on trades; a full order book (level2) needs
                         authentication and is not used.)
    heartbeat            HEARTBEAT last_trade_id (lets the collector detect missed trades even when
                         no trade arrives)
    subscriptions/error  control

REST backfill  GET https://api.exchange.coinbase.com/products/{p}/trades?limit=..  (newest first)
               entries {time, trade_id, price, size, side}; `side` again the MAKER side.
               Backfilled trades are BACKFILLED with receive_ts = when the response arrived.
Trade ids are contiguous per product, so id jumps are exact, recoverable gaps.
"""
import os

from market_data.normalization import iso_ms, mid, optional_price, optional_size, price, size
from market_data.sources.base import SourceAdapter, new_result
from market_data.types import AggressorSemantics, EventFlag, EventType, IngestMode

WS_URL = os.environ.get("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
REST_URL = os.environ.get("COINBASE_REST_URL", "https://api.exchange.coinbase.com")
SYMBOLS = {"BTC": "BTC-USD", "ETH": "ETH-USD", "SOL": "SOL-USD", "XRP": "XRP-USD"}
_ASSET = {v: k for k, v in SYMBOLS.items()}
_AGGRESSOR_FROM_MAKER = {"sell": "buy", "buy": "sell"}


class CoinbaseAdapter(SourceAdapter):
    source = "coinbase"
    stream = "ws"
    transport = "ws"
    documentation = "Coinbase Exchange websocket: ticker, matches, heartbeat (public)"

    def __init__(self, assets):
        self.assets = [a for a in assets if a in SYMBOLS]
        self.url = WS_URL

    def subscribe_messages(self):
        import json
        return [json.dumps({"type": "subscribe", "product_ids": [SYMBOLS[a] for a in self.assets],
                            "channels": ["ticker", "matches", "heartbeat"]})]

    def parse(self, text, ctx):
        res = new_result()
        m = self.load(text, ctx, res)
        if m is None:
            return res
        if not isinstance(m, dict) or "type" not in m:
            self.fail(res, ctx, "message without type", text)
            return res
        t = m["type"]
        if t in ("subscriptions", "error"):
            res.control.append(m)
            return res
        asset = _ASSET.get(m.get("product_id"))
        if asset is None:
            self.fail(res, ctx, f"unknown product {m.get('product_id')!r}", text)
            return res
        if t in ("match", "last_match"):
            self.guarded(res, ctx, text, lambda: res.events.append(self._trade(m, asset, ctx, snapshot=t == "last_match")))
        elif t == "ticker":
            self.guarded(res, ctx, text, lambda: res.events.append(self._quote(m, asset, ctx)))
        elif t == "heartbeat":
            self.guarded(res, ctx, text, lambda: res.events.append(self.event(
                ctx, asset=asset, event_type=EventType.HEARTBEAT, symbol=m["product_id"], event_ts_ms=iso_ms(m["time"]),
                source_seq=m.get("sequence"), payload={"last_trade_id": m.get("last_trade_id")})))
        else:
            self.fail(res, ctx, f"unexpected message type {t!r}", text)
        return res

    def _trade(self, m, asset, ctx, snapshot=False, backfill=False):
        side = m.get("side")
        aggressor = _AGGRESSOR_FROM_MAKER.get(side)
        flags = [] if aggressor else [EventFlag.AGGRESSOR_UNAVAILABLE.value]
        if snapshot:
            flags.append(EventFlag.SNAPSHOT.value)
        return self.event(ctx, asset=asset, event_type=EventType.TRADE, symbol=SYMBOLS[asset],
                          event_ts_ms=iso_ms(m["time"]), source_seq=m.get("sequence"),
                          mode=IngestMode.BACKFILLED if (snapshot or backfill) else IngestMode.LIVE,
                          flags=tuple(flags),
                          payload={"price": price(m["price"]), "size": size(m["size"]), "aggressor": aggressor,
                                   "aggressor_semantics": (AggressorSemantics.INVERTED_MAKER_SIDE.value if aggressor
                                                           else AggressorSemantics.UNAVAILABLE.value),
                                   "trade_id": int(m["trade_id"])})

    def _quote(self, m, asset, ctx):
        bid, ask = optional_price(m.get("best_bid"), "best_bid"), optional_price(m.get("best_ask"), "best_ask")
        flags = []
        if bid is None or ask is None:
            flags.append(EventFlag.ONE_SIDED_BOOK.value)
        elif ask < bid:
            flags.append(EventFlag.CROSSED_BOOK.value)
        return self.event(ctx, asset=asset, event_type=EventType.QUOTE, symbol=SYMBOLS[asset],
                          event_ts_ms=iso_ms(m["time"]), source_seq=m.get("sequence"), flags=tuple(flags),
                          payload={"bid": bid, "bid_size": optional_size(m.get("best_bid_size"), "best_bid_size"),
                                   "ask": ask, "ask_size": optional_size(m.get("best_ask_size"), "best_ask_size"),
                                   "mid": mid(bid, ask)})

    # ---------- REST backfill ----------
    def backfill_url(self, asset, limit=100):
        return f"{REST_URL}/products/{SYMBOLS[asset]}/trades", {"limit": limit}

    def parse_backfill(self, asset, body, ctx):
        """REST /trades response (list, newest first) -> BACKFILLED trades (receive_ts = retrieval time)."""
        res = new_result()
        if not isinstance(body, list):
            self.fail(res, ctx, "backfill response is not a list", body)
            return res
        for item in reversed(body):                                       # oldest first
            self.guarded(res, ctx, item, lambda it=item: res.events.append(self._trade(it, asset, ctx, backfill=True)))
        return res
