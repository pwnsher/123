"""
Kraken spot websocket v2 — public book channel (+ instrument channel for precisions). No authentication.

Transport  wss://ws.kraken.com/v2 (env KRAKEN_WS_V2_URL overrides)
Subscribe  {"method":"subscribe","params":{"channel":"book","symbol":[...],"depth":<10|25|100|500|1000>,"snapshot":true}}
           {"method":"subscribe","params":{"channel":"instrument","snapshot":true}}

Messages
    book snapshot   data[] {symbol, bids[{price, qty}], asks[...], checksum} -> BOOK_SNAPSHOT (depth = subscribed)
    book update     data[] {symbol, bids, asks, checksum, timestamp}; qty is the new ABSOLUTE level size (0 deletes)
                    -> BOOK_DELTA (mode abs, checksum). The local book is TRUNCATED to the subscribed depth after
                    each update (levels pushed out of range get no delete message - documented behaviour).
    instrument      pairs[] {symbol, price_precision, qty_precision, ...} -> INSTRUMENT (only our symbols); the
                    precisions let the reconstructor verify the CRC32 checksum of the top 10 levels.
    heartbeat / status / method acks   control
There are no sequence numbers: continuity rests on the checksum.

EXACT DECIMALS. The checksum is defined on the decimal digits of the prices and quantities, so the message is parsed
with json parse_float=Decimal (the literal digits, never a binary float). Every book event carries the exact wire
decimals as strings (exact_bids / exact_asks / exact_changes, format 'f': trailing zeros kept); the reconstructor
keeps a separate Decimal-only ExactBook from them for the checksum. The float bids / asks / changes are derived from
the same Decimals and feed the generic feature engine only.
"""
import json
import os
from decimal import Decimal

from market_data.normalization import NormalizationError, iso_ms
from microstructure.book import exact_decimal
from microstructure.sources.base import MicroAdapter, new_result
from microstructure.types import MicroEventType as MT

WS_URL = os.environ.get("KRAKEN_WS_V2_URL", "wss://ws.kraken.com/v2")


class KrakenBookAdapter(MicroAdapter):
    venue = source = "kraken_book"
    stream = "ws"
    transport = "ws"
    documentation = "Kraken websocket v2 public book (checksummed) + instrument"

    def __init__(self, assets, depth=None):
        super().__init__(assets, depth)
        if self.depth not in self.spec.depth_options:
            raise ValueError(f"Kraken book depth must be one of {self.spec.depth_options}")
        self.url = WS_URL

    def subscribe_messages(self):
        syms = [self.spec.symbols[a] for a in self.assets]
        return [json.dumps({"method": "subscribe", "params": {"channel": "instrument", "snapshot": True}}),
                json.dumps({"method": "subscribe", "params": {"channel": "book", "symbol": syms, "depth": self.depth,
                                                              "snapshot": True}})]

    def parse(self, text, ctx):
        res = new_result()
        try:
            m = json.loads(text, parse_float=Decimal) if isinstance(text, (str, bytes)) else text
        except ValueError as e:
            self.fail(res, ctx, f"invalid JSON: {e}", text)
            return res
        if not isinstance(m, dict):
            self.fail(res, ctx, "message is not an object", text)
            return res
        ch = m.get("channel")
        if ch == "instrument":
            data = m.get("data") or {}
            for pr in (data.get("pairs") or []) if isinstance(data, dict) else []:
                sym = pr.get("symbol") if isinstance(pr, dict) else None
                if sym in self.by_symbol:
                    def ins(pr=pr, sym=sym):
                        info = {"price_precision": int(pr["price_precision"]), "qty_precision": int(pr["qty_precision"]),
                                "price_increment": _txt(pr.get("price_increment")), "qty_increment": _txt(pr.get("qty_increment"))}
                        res.events.append(self.event(ctx, asset=self.by_symbol[sym], event_type=MT.INSTRUMENT, symbol=sym,
                                                     event_ts_ms=None, payload={"book": sym, "info": info}))
                    self.guarded(res, ctx, pr, ins)
            return res
        if ch != "book":
            res.control.append({k: m.get(k) for k in ("channel", "method", "success", "error", "type") if k in m})
            return res
        typ = m.get("type")
        for d in m.get("data") or []:
            def build(d=d):
                sym = d["symbol"]
                asset = self.by_symbol.get(sym)
                if asset is None:
                    raise KeyError(f"unexpected symbol {sym}")
                xb = [(_exact(x["price"], "price", True), _exact(x["qty"], "qty")) for x in d.get("bids") or []]
                xa = [(_exact(x["price"], "price", True), _exact(x["qty"], "qty")) for x in d.get("asks") or []]
                bids = [[float(p), float(q)] for p, q in xb]          # feature book only
                asks = [[float(p), float(q)] for p, q in xa]
                ts = iso_ms(d["timestamp"], "timestamp") if d.get("timestamp") else None
                cs = d.get("checksum")
                if typ == "snapshot":
                    res.events.append(self.event(ctx, asset=asset, event_type=MT.BOOK_SNAPSHOT, symbol=sym, event_ts_ms=ts,
                                                 payload=dict(book=sym, bids=bids, asks=asks, update_id=None, depth=self.depth,
                                                              checksum=cs, exact_bids=[[_s(p), _s(q)] for p, q in xb],
                                                              exact_asks=[[_s(p), _s(q)] for p, q in xa], **self.units())))
                elif typ == "update":
                    ch_ = [["bid", p, q, "abs"] for p, q in bids] + [["ask", p, q, "abs"] for p, q in asks]
                    ex_ = [["bid", _s(p), _s(q)] for p, q in xb] + [["ask", _s(p), _s(q)] for p, q in xa]
                    res.events.append(self.event(ctx, asset=asset, event_type=MT.BOOK_DELTA, symbol=sym, event_ts_ms=ts,
                                                 payload=dict(book=sym, changes=ch_, update_id=None, prev_update_id=None,
                                                              first_update_id=None, checksum=cs, exact_changes=ex_,
                                                              **self.units())))
                else:
                    raise ValueError(f"unknown book message type {typ!r}")
            self.guarded(res, ctx, d, build)
        return res


def _exact(v, name, positive=False):
    """A wire number -> Decimal with its literal digits (no binary float anywhere)."""
    try:
        d = exact_decimal(v, name)
    except (TypeError, ValueError, ArithmeticError) as e:
        raise NormalizationError(str(e))
    if positive and d <= 0:
        raise NormalizationError(f"{name} must be positive: {v!r}")
    return d


def _s(d):
    """Decimal -> plain decimal string (trailing zeros kept, no exponent)."""
    return format(d, "f")


def _txt(v):
    """Instrument metadata kept JSON-safe and exact (a Decimal becomes its literal string)."""
    return format(v, "f") if isinstance(v, Decimal) else v
