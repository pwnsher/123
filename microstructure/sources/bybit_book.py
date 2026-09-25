"""
Bybit v5 public linear — orderbook.<depth>.<symbol> (snapshot + deltas). No authentication.

Transport  wss://stream.bybit.com/v5/public/linear (env BYBIT_LINEAR_WS_URL overrides)
Subscribe  {"op":"subscribe","args":["orderbook.200.BTCUSDT", ...]} (<= 10 args per request);
           client keep-alive {"op":"ping"} every 20 s.
Messages   {"topic", "type": snapshot|delta, "ts", "cts", "data": {s, b: [[p, size]], a, u, seq}}
           sizes are ABSOLUTE (0 deletes). snapshot -> BOOK_SNAPSHOT (a "u": 1 snapshot is the venue's restart
           snapshot and also resets the book); delta -> BOOK_DELTA (update_id u). Event time = cts (matching-engine
           time) when present, else ts.
Continuity: u increases but contiguity is not documented -> policy monotonic_only (a lost delta is not detectable).
"""
import json
import os

from market_data.normalization import epoch_ms
from microstructure.sources.base import MicroAdapter, new_result, num
from microstructure.types import MicroEventType as MT

WS_URL = os.environ.get("BYBIT_LINEAR_WS_URL", "wss://stream.bybit.com/v5/public/linear")


class BybitBookAdapter(MicroAdapter):
    venue = source = "bybit_linear_book"
    stream = "ws"
    transport = "ws"
    keepalive_text = json.dumps({"op": "ping"})
    keepalive_s = 20
    documentation = "Bybit v5 public linear orderbook (snapshot + delta)"

    def __init__(self, assets, depth=None):
        super().__init__(assets, depth)
        if self.depth not in self.spec.depth_options:
            raise ValueError(f"Bybit book depth must be one of {self.spec.depth_options}")
        self.url = WS_URL

    def subscribe_messages(self):
        args = [f"orderbook.{self.depth}.{self.spec.symbols[a]}" for a in self.assets]
        return [json.dumps({"op": "subscribe", "args": args[i:i + 10]}) for i in range(0, len(args), 10)]

    def parse(self, text, ctx):
        res = new_result()
        m = self.load(text, ctx, res)
        if m is None:
            return res
        if not isinstance(m, dict) or "topic" not in m:
            res.control.append({k: m.get(k) for k in ("op", "success", "ret_msg") if k in m} if isinstance(m, dict) else str(m)[:100])
            return res
        if not str(m["topic"]).startswith("orderbook."):
            self.fail(res, ctx, f"unexpected topic {m['topic']!r}", text)
            return res

        def build():
            d = m["data"]
            sym = d["s"]
            asset = self.by_symbol.get(sym)
            if asset is None:
                raise KeyError(f"unexpected symbol {sym}")
            t = m.get("cts") or m.get("ts")
            ts = epoch_ms(t, "cts") if t else None
            bids = [[num(p, "price", True), num(q, "qty")] for p, q in d.get("b") or []]
            asks = [[num(p, "price", True), num(q, "qty")] for p, q in d.get("a") or []]
            u = int(d["u"])
            if m.get("type") == "snapshot":
                res.events.append(self.event(ctx, asset=asset, event_type=MT.BOOK_SNAPSHOT, symbol=sym, event_ts_ms=ts,
                                             source_seq=u, flags=(("VENUE_RESTART_SNAPSHOT",) if u == 1 else ()),
                                             payload=dict(book=sym, bids=bids, asks=asks, update_id=u, depth=self.depth, **self.units())))
            elif m.get("type") == "delta":
                ch = [["bid", p, q, "abs"] for p, q in bids] + [["ask", p, q, "abs"] for p, q in asks]
                res.events.append(self.event(ctx, asset=asset, event_type=MT.BOOK_DELTA, symbol=sym, event_ts_ms=ts, source_seq=u,
                                             payload=dict(book=sym, changes=ch, update_id=u, prev_update_id=None, first_update_id=None,
                                                          checksum=None, **self.units())))
            else:
                raise ValueError(f"unknown orderbook message type {m.get('type')!r}")
        self.guarded(res, ctx, m, build)
        return res
