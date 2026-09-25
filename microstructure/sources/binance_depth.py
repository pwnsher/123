"""
Binance USD-M futures — public DIFF book depth stream + GET depth snapshot. No authentication, no order endpoint.

Websocket  wss://fstream.binance.com/stream?streams=<s>@depth@100ms/...   (env BINANCE_FUTURES_WS_URL overrides the base)
           combined-stream envelope {"stream": ..., "data": {"e": "depthUpdate", E, T, s, U, u, pu, b: [[p, q]], a}}
           quantities are ABSOLUTE (0 removes the level) -> BOOK_DELTA (mode abs; first_update_id U, update_id u,
           prev_update_id pu). Event time T (transaction time).
REST       GET /fapi/v1/depth?symbol=<S>&limit=<depth>  {lastUpdateId, E, T, bids, asks} -> BOOK_SNAPSHOT
           (update_id = lastUpdateId). Requested when a book has no snapshot or needs a resnapshot (see
           microstructure.poller.SnapshotPoller); the documented alignment lives in reconstruction.py.
"""
import os

from market_data.normalization import epoch_ms
from microstructure.sources.base import MicroAdapter, new_result, num
from microstructure.types import MicroEventType as MT

WS_BASE = os.environ.get("BINANCE_FUTURES_WS_URL", "wss://fstream.binance.com/stream")
REST_BASE = os.environ.get("BINANCE_FUTURES_REST_URL", "https://fapi.binance.com")


class BinanceDepthAdapter(MicroAdapter):
    venue = source = "binance_usdm_book"
    stream = "ws"
    transport = "ws"
    rest_snapshots = True
    documentation = "Binance USD-M diff depth @100ms + GET /fapi/v1/depth snapshots (local order book procedure)"

    def __init__(self, assets, depth=None):
        super().__init__(assets, depth)
        if self.depth not in self.spec.depth_options:
            raise ValueError(f"Binance snapshot depth must be one of {self.spec.depth_options}")
        self.url = WS_BASE + "?streams=" + "/".join(f"{self.spec.symbols[a].lower()}@depth@100ms" for a in self.assets)

    def snapshot_url(self, asset):
        return f"{REST_BASE}/fapi/v1/depth", {"symbol": self.spec.symbols[asset], "limit": self.depth}

    def parse(self, text, ctx):
        res = new_result()
        m = self.load(text, ctx, res)
        if m is None:
            return res
        d = m.get("data") if isinstance(m, dict) else None
        if not isinstance(d, dict):
            res.control.append({k: m.get(k) for k in ("result", "id", "code", "msg") if k in m} if isinstance(m, dict) else str(m)[:100])
            return res
        if d.get("e") != "depthUpdate":
            self.fail(res, ctx, f"unexpected event {d.get('e')!r}", text)
            return res

        def build():
            sym = d["s"]
            asset = self.by_symbol.get(sym)
            if asset is None:
                raise KeyError(f"unexpected symbol {sym}")
            ch = [["bid", num(p, "price", True), num(q, "qty"), "abs"] for p, q in d.get("b") or []] + \
                 [["ask", num(p, "price", True), num(q, "qty"), "abs"] for p, q in d.get("a") or []]
            res.events.append(self.event(ctx, asset=asset, event_type=MT.BOOK_DELTA, symbol=sym,
                                         event_ts_ms=epoch_ms(d.get("T") or d["E"], "T"), source_seq=int(d["u"]),
                                         payload=dict(book=sym, changes=ch, update_id=int(d["u"]), prev_update_id=int(d["pu"]),
                                                      first_update_id=int(d["U"]), checksum=None, **self.units())))
        self.guarded(res, ctx, d, build)
        return res

    def parse_rest(self, stream, body, ctx):
        res = new_result()
        kind, _, asset = stream.partition(":")
        if kind != "snapshot" or asset not in self.assets:
            self.fail(res, ctx, f"unknown REST stream {stream}", body)
            return res

        def build():
            sym = self.spec.symbols[asset]
            bids = [[num(p, "price", True), num(q, "qty")] for p, q in body["bids"]]
            asks = [[num(p, "price", True), num(q, "qty")] for p, q in body["asks"]]
            t = body.get("T") or body.get("E")
            res.events.append(self.event(ctx, asset=asset, event_type=MT.BOOK_SNAPSHOT, symbol=sym,
                                         event_ts_ms=epoch_ms(t, "T") if t else None, source_seq=int(body["lastUpdateId"]),
                                         payload=dict(book=sym, bids=bids, asks=asks, update_id=int(body["lastUpdateId"]),
                                                      depth=self.depth, **self.units())))
        self.guarded(res, ctx, body, build)
        return res
