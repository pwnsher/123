"""
OKX v5 public — books channel (400 levels, snapshot + incremental updates). No authentication.

Transport  wss://ws.okx.com:8443/ws/v5/public (env OKX_WS_URL overrides)
Subscribe  {"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT-SWAP"}, ...]}; keep-alive text "ping"
           every 25 s (the server answers "pong").
Messages   {"arg": {channel, instId}, "action": snapshot|update, "data": [{asks: [[px, sz, "0", n_orders]], bids,
           ts, checksum, prevSeqId, seqId}]}
           sz is the ABSOLUTE level size in CONTRACTS (0 deletes). snapshot -> BOOK_SNAPSHOT (update_id seqId);
           update -> BOOK_DELTA (update_id seqId, prev_update_id prevSeqId; prevSeqId must equal the previous seqId).
           The per-level ORDER COUNT (4th field) is a count of resting orders, not a queue position; it is not used.
           The OKX checksum is deprecated (always 0): continuity rests on the seqId chain.
"""
import json
import os

from market_data.normalization import epoch_ms
from microstructure.sources.base import MicroAdapter, new_result, num
from microstructure.types import MicroEventType as MT

WS_URL = os.environ.get("OKX_WS_URL", "wss://ws.okx.com:8443/ws/v5/public")


class OkxBooksAdapter(MicroAdapter):
    venue = source = "okx_swap_book"
    stream = "ws"
    transport = "ws"
    keepalive_text = "ping"
    keepalive_s = 25
    documentation = "OKX v5 public books (400 levels, seqId chain); sizes in contracts"

    def __init__(self, assets, depth=None):
        super().__init__(assets, depth)
        self.url = WS_URL

    def subscribe_messages(self):
        return [json.dumps({"op": "subscribe", "args": [{"channel": "books", "instId": self.spec.symbols[a]} for a in self.assets]})]

    def parse(self, text, ctx):
        res = new_result()
        if text == "pong":
            res.control.append("pong")
            return res
        m = self.load(text, ctx, res)
        if m is None:
            return res
        if not isinstance(m, dict) or "data" not in m:
            res.control.append({k: m.get(k) for k in ("event", "code", "msg", "arg") if k in m} if isinstance(m, dict) else str(m)[:100])
            return res
        arg = m.get("arg") or {}
        if arg.get("channel") != "books":
            self.fail(res, ctx, f"unexpected channel {arg.get('channel')!r}", text)
            return res
        sym = arg.get("instId")
        asset = self.by_symbol.get(sym)
        for d in m["data"]:
            def build(d=d):
                if asset is None:
                    raise KeyError(f"unexpected instrument {sym}")
                bids = [[num(x[0], "price", True), num(x[1], "qty")] for x in d.get("bids") or []]
                asks = [[num(x[0], "price", True), num(x[1], "qty")] for x in d.get("asks") or []]
                ts = epoch_ms(int(d["ts"]), "ts") if d.get("ts") else None
                seq, prev = int(d["seqId"]), int(d.get("prevSeqId", -1))
                if m.get("action") == "snapshot":
                    res.events.append(self.event(ctx, asset=asset, event_type=MT.BOOK_SNAPSHOT, symbol=sym, event_ts_ms=ts,
                                                 source_seq=seq, payload=dict(book=sym, bids=bids, asks=asks, update_id=seq,
                                                                              depth=self.depth, **self.units())))
                elif m.get("action") == "update":
                    ch = [["bid", p, q, "abs"] for p, q in bids] + [["ask", p, q, "abs"] for p, q in asks]
                    res.events.append(self.event(ctx, asset=asset, event_type=MT.BOOK_DELTA, symbol=sym, event_ts_ms=ts,
                                                 source_seq=seq, payload=dict(book=sym, changes=ch, update_id=seq, prev_update_id=prev,
                                                                              first_update_id=None, checksum=None, **self.units())))
                else:
                    raise ValueError(f"unknown books action {m.get('action')!r}")
            self.guarded(res, ctx, d, build)
        return res
