"""
Coinbase Advanced Trade market-data websocket — public level2 channel (+ heartbeats). No authentication.

Transport  wss://advanced-trade-ws.coinbase.com (env COINBASE_ADV_WS_URL overrides)
Subscribe  {"type":"subscribe","product_ids":[...],"channel":"level2"} and {"type":"subscribe","channel":"heartbeats"}

Messages
    l2_data         events[] of type "snapshot" (full book) or "update"; updates[] = {side: bid|offer, event_time,
                    price_level, new_quantity}. new_quantity is the ABSOLUTE size at that price (0 removes).
                    snapshot -> BOOK_SNAPSHOT, update -> BOOK_DELTA (mode abs). Event time = the message timestamp
                    (updates' event_time is kept per change when present).
    heartbeats / subscriptions   control (they still advance the connection's sequence_num)
Every message carries sequence_num, increasing by exactly 1 per message on the CONNECTION. The chain id is
"coinbase_l2:c<conn>"; each book event reports prev_update_id = the last sequence number seen before it, or the
last one before a discontinuity, so a message lost anywhere on the connection is detected at the next book event.
"""
import json
import os

from market_data.normalization import iso_ms
from microstructure.sources.base import MicroAdapter, new_result, num
from microstructure.types import MicroEventType as MT

WS_URL = os.environ.get("COINBASE_ADV_WS_URL", "wss://advanced-trade-ws.coinbase.com")
_SIDE = {"bid": "bid", "offer": "ask", "ask": "ask"}


class CoinbaseL2Adapter(MicroAdapter):
    venue = source = "coinbase_l2"
    stream = "ws"
    transport = "ws"
    documentation = "Coinbase Advanced Trade public level2 + heartbeats websocket"

    def __init__(self, assets, depth=None):
        super().__init__(assets, depth)
        self.url = WS_URL
        self.conn = 0
        self.last_seq = None
        self.broken_from = None

    def subscribe_messages(self):
        return [json.dumps({"type": "subscribe", "product_ids": [self.spec.symbols[a] for a in self.assets], "channel": "level2"}),
                json.dumps({"type": "subscribe", "channel": "heartbeats"})]

    def on_new_connection(self, conn):
        self.conn, self.last_seq, self.broken_from = conn, None, None

    def _track(self, seq):
        """Advance the connection's sequence tracking; return the prev id to report for a book event."""
        if seq is None:
            return None
        if self.last_seq is not None and seq != self.last_seq + 1 and self.broken_from is None:
            self.broken_from = self.last_seq
        prev = self.broken_from if self.broken_from is not None else self.last_seq
        self.last_seq = seq
        return prev

    def parse(self, text, ctx):
        res = new_result()
        m = self.load(text, ctx, res)
        if m is None:
            return res
        if not isinstance(m, dict):
            self.fail(res, ctx, "message is not an object", text)
            return res
        if getattr(ctx, "conn", 0) != self.conn:
            self.on_new_connection(getattr(ctx, "conn", 0))
        seq = m.get("sequence_num")
        seq = int(seq) if isinstance(seq, (int, float)) and not isinstance(seq, bool) else None
        ch = m.get("channel")
        if ch != "l2_data":
            self._track(seq)
            if m.get("type") == "error" or ch == "error":
                res.control.append({"error": str(m.get("message"))[:200]})
            else:
                res.control.append({"channel": ch, "sequence_num": seq})
            return res
        prev = self._track(seq)
        self.broken_from = None
        ts = None
        if m.get("timestamp"):
            try:
                ts = iso_ms(m["timestamp"], "timestamp")
            except ValueError:
                ts = None
        chain = f"{self.source}:c{self.conn}"
        for e in m.get("events") or []:
            def build(e=e):
                sym = e["product_id"]
                asset = self.by_symbol.get(sym)
                if asset is None:
                    raise KeyError(f"unexpected product {sym}")
                ups = e.get("updates") or []
                if e.get("type") == "snapshot":
                    bids = [[num(u["price_level"], "price", True), num(u["new_quantity"], "qty")] for u in ups if _SIDE[u["side"]] == "bid"]
                    asks = [[num(u["price_level"], "price", True), num(u["new_quantity"], "qty")] for u in ups if _SIDE[u["side"]] == "ask"]
                    res.events.append(self.event(ctx, asset=asset, event_type=MT.BOOK_SNAPSHOT, symbol=sym, event_ts_ms=ts,
                                                 source_seq=seq, payload=dict(book=sym, bids=bids, asks=asks, update_id=seq, depth=None,
                                                                              prev_update_id=prev, chain=chain, **self.units())))
                elif e.get("type") == "update":
                    ch_ = [[_SIDE[u["side"]], num(u["price_level"], "price", True), num(u["new_quantity"], "qty"), "abs"] for u in ups]
                    res.events.append(self.event(ctx, asset=asset, event_type=MT.BOOK_DELTA, symbol=sym, event_ts_ms=ts,
                                                 source_seq=seq, payload=dict(book=sym, changes=ch_, update_id=seq, prev_update_id=prev,
                                                                              first_update_id=seq, checksum=None, chain=chain,
                                                                              **self.units())))
                else:
                    raise ValueError(f"unknown l2 event type {e.get('type')!r}")
            self.guarded(res, ctx, e, build)
            prev = None if seq is None else seq - 1    # further events of the same message share its sequence number
        return res
