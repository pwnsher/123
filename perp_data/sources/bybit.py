"""
Bybit USDT perpetuals (category "linear") — public v5 websocket + public REST. No authentication.

Websocket  wss://stream.bybit.com/v5/public/linear, subscribe {"op":"subscribe","args":[...]} (<= 10 args per
           request) to publicTrade.<S>, tickers.<S>, orderbook.<N>.<S>, allLiquidation.<S>.
           Client keep-alive: {"op":"ping"} every 20 s (server answers {"op":"pong"} - control).

Messages -> events (documented semantics)
    publicTrade      PERP_TRADE  T (trade time), S = side of the TAKER, v (base coin), p, i (trade id, not
                     contiguous -> no id-gap detection).
    tickers          snapshot, then DELTAS that carry only changed fields. The adapter merges them per symbol
                     and emits, for the fields present in each message: PERP_MARK_PRICE (markPrice),
                     PERP_INDEX_PRICE (indexPrice), FUNDING_RATE (fundingRate for the upcoming settlement at
                     nextFundingTime, interval = fundingIntervalHour; flagged ESTIMATE), OPEN_INTEREST
                     (openInterest = base coin, openInterestValue = USDT), PERP_QUOTE (bid1/ask1).
                     A delta before any snapshot is a recorded failure (never a partial state).
                     Bybit publishes no forecast for the following period -> no PREDICTED_FUNDING.
    orderbook.N      snapshot + deltas applied to a local book (size "0" deletes a level) -> ORDERBOOK_TOP
                     (top `depth` levels). A snapshot (also the u=1 restart snapshot) resets the book. The update
                     id `u` is recorded; contiguity of `u` is not documented, so no sequence-gap rule is applied.
    allLiquidation   LIQUIDATION (all liquidations, pushed in 500 ms batches): S is the POSITION side - "Buy" means
                     a LONG position was liquidated (so the forced order was a sell). v (base coin), p, T.
REST (GET)  /v5/market/funding/history (FUNDING_SETTLED), /v5/market/recent-trade (backfill; side = taker).
"""
import json
import os

from market_data.normalization import epoch_ms, mid, optional_price, optional_size, price, size
from market_data.types import AggressorSemantics, EventFlag
from perp_data.normalization import FORCED_SIDE_FOR_POSITION, HOUR_MS, funding_normalized, funding_rate, side
from perp_data.sources.base import PerpAdapter, new_result
from perp_data.types import PerpEventType as T, PerpFlag

WS_URL = os.environ.get("BYBIT_LINEAR_WS_URL", "wss://stream.bybit.com/v5/public/linear")
REST_BASE = os.environ.get("BYBIT_REST_URL", "https://api.bybit.com")


class BybitLinearAdapter(PerpAdapter):
    venue = source = "bybit_linear"
    stream = "ws"
    transport = "ws"
    keepalive_text = json.dumps({"op": "ping"})
    keepalive_s = 20
    documentation = "Bybit v5 public linear: publicTrade, tickers (snapshot+delta), orderbook, allLiquidation"

    def __init__(self, assets, depth=10):
        super().__init__(assets, depth=max(1, min(depth, 50)))
        self.url = WS_URL
        self.tickers = {}                       # symbol -> merged ticker fields
        self.books = {}                         # symbol -> {"b": {px: qty}, "a": {px: qty}}

    def subscribe_messages(self):
        args = []
        for a in self.assets:
            s = self.spec.symbols[a]
            args += [f"publicTrade.{s}", f"tickers.{s}", f"orderbook.50.{s}", f"allLiquidation.{s}"]
        return [json.dumps({"op": "subscribe", "args": args[i:i + 10]}) for i in range(0, len(args), 10)]

    def rest_urls(self):
        return {f"funding_hist:{a}": (f"{REST_BASE}/v5/market/funding/history",
                                      {"category": "linear", "symbol": self.spec.symbols[a], "limit": 3}, 60)
                for a in self.assets}

    def backfill_url(self, asset, limit=1000):
        return f"{REST_BASE}/v5/market/recent-trade", {"category": "linear", "symbol": self.spec.symbols[asset],
                                                       "limit": limit}

    # ---------------- websocket ----------------
    def parse(self, text, ctx):
        res = new_result()
        m = self.load(text, ctx, res)
        if m is None:
            return res
        if isinstance(m, dict) and "op" in m and "topic" not in m:
            res.control.append(m)                    # subscribe acks, pong
            return res
        topic = m.get("topic") if isinstance(m, dict) else None
        if not isinstance(topic, str) or "data" not in m:
            self.fail(res, ctx, "message without topic/data", text)
            return res
        kind, _, rest = topic.partition(".")
        sym = rest.rsplit(".", 1)[-1]
        asset = self.by_symbol.get(sym)
        if asset is None:
            self.fail(res, ctx, f"unknown symbol in topic {topic!r}", text)
            return res
        fn = {"publicTrade": self._trades, "tickers": self._ticker, "orderbook": self._book,
              "allLiquidation": self._liqs}.get(kind)
        if fn is None:
            self.fail(res, ctx, f"unexpected topic {topic!r}", text)
            return res
        self.guarded(res, ctx, text, lambda: res.events.extend(fn(m, asset, sym, ctx)))
        return res

    def _trade(self, x, asset, sym, ctx, backfill=False, rest=False):
        qty = size(x["size"] if rest else x["v"], "v")
        px = price(x["price"] if rest else x["p"], "p")
        agg = side(x["side"] if rest else x["S"], "S")
        tid = x["execId"] if rest else x["i"]
        ts = epoch_ms(int(x["time"]) if rest else x["T"], "T")
        return self.event(ctx, asset=asset, event_type=T.PERP_TRADE, symbol=sym, event_ts_ms=ts, source_seq=tid,
                          mode=self.mode(backfill),
                          payload={"price": px, "qty_native": qty, "qty_unit": "coin", "qty_coin": qty,
                                   "notional_quote": px * qty, "aggressor": agg,
                                   "aggressor_semantics": AggressorSemantics.TAKER_SIDE_FIELD.value,
                                   "trade_id": str(tid), "quote_ccy": "USDT",
                                   "native": {"S": x.get("side") if rest else x.get("S"),
                                              "BT": x.get("isBlockTrade") if rest else x.get("BT")}})

    def _trades(self, m, asset, sym, ctx):
        if not isinstance(m["data"], list):
            raise ValueError("publicTrade data is not a list")
        return [self._trade(x, asset, sym, ctx) for x in sorted(m["data"], key=lambda x: int(x["T"]))]

    def _ticker(self, m, asset, sym, ctx):
        d = m["data"]
        if not isinstance(d, dict):
            raise ValueError("tickers data is not an object")
        typ = m.get("type")
        if typ == "snapshot":
            self.tickers[sym] = dict(d)
        elif typ == "delta":
            if sym not in self.tickers:
                raise ValueError("tickers delta before snapshot")
            self.tickers[sym].update(d)
        else:
            raise ValueError(f"unknown tickers type {typ!r}")
        st = self.tickers[sym]
        ts = epoch_ms(m["ts"], "ts")
        flags = (PerpFlag.DELTA_MERGED.value,) if typ == "delta" else ()
        out = []
        ev = lambda et, payload, q="OK", fl=(): self.event(  # noqa: E731
            ctx, asset=asset, event_type=et, symbol=sym, event_ts_ms=ts, source_seq=m.get("cs"), quality=q,
            flags=flags + fl, payload=payload)
        if "markPrice" in d:
            out.append(ev(T.PERP_MARK_PRICE, {"mark": price(st["markPrice"], "markPrice"), "quote_ccy": "USDT"}))
        if "indexPrice" in d:
            out.append(ev(T.PERP_INDEX_PRICE, {"index": price(st["indexPrice"], "indexPrice"), "quote_ccy": "USDT"}))
        if {"fundingRate", "nextFundingTime", "fundingIntervalHour"} & set(d) and st.get("fundingRate") not in (None, ""):
            r = funding_rate(st["fundingRate"], "fundingRate")
            ih = st.get("fundingIntervalHour")
            ms = int(float(ih) * HOUR_MS) if ih not in (None, "") else None
            ph, r8, ra = funding_normalized(r, ms)
            nft = st.get("nextFundingTime")
            out.append(ev(T.FUNDING_RATE, {"rate_native": r, "interval_ms": ms,
                                           "interval_source": "ticker_fundingIntervalHour" if ms else "unknown",
                                           "next_funding_ts_ms": epoch_ms(int(nft), "nextFundingTime") if nft not in (None, "", "0") else None,
                                           "rate_per_hour": ph, "rate_8h": r8, "rate_annual_simple": ra,
                                           "is_estimate": True},
                          "ESTIMATE", (PerpFlag.ESTIMATE.value,)))
        if {"openInterest", "openInterestValue"} & set(d) and st.get("openInterest") not in (None, ""):
            q = size(st["openInterest"], "openInterest")
            out.append(ev(T.OPEN_INTEREST, {"oi_native": q, "oi_unit": "coin", "oi_coin": q,
                                            "oi_quote_native": optional_size(st.get("openInterestValue"), "openInterestValue")}))
        if {"bid1Price", "ask1Price", "bid1Size", "ask1Size"} & set(d):
            bid, ask = optional_price(st.get("bid1Price"), "bid1Price"), optional_price(st.get("ask1Price"), "ask1Price")
            fl = () if (bid is not None and ask is not None) else (EventFlag.ONE_SIDED_BOOK.value,)
            out.append(ev(T.PERP_QUOTE, {"bid": bid, "bid_qty_coin": optional_size(st.get("bid1Size"), "bid1Size"),
                                         "ask": ask, "ask_qty_coin": optional_size(st.get("ask1Size"), "ask1Size"),
                                         "mid": mid(bid, ask), "quote_ccy": "USDT"}, "OK", fl))
        return out

    def _book(self, m, asset, sym, ctx):
        d = m["data"]
        typ = m.get("type")
        if typ == "snapshot" or d.get("u") == 1:
            book = {"b": {}, "a": {}}
            self.books[sym] = book
        elif typ == "delta":
            if sym not in self.books:
                raise ValueError("orderbook delta before snapshot")
            book = self.books[sym]
        else:
            raise ValueError(f"unknown orderbook type {typ!r}")
        for key in ("b", "a"):
            for p, q in d.get(key) or []:
                px, qty = price(p, "level price"), size(q, "level qty")
                if qty == 0:
                    book[key].pop(px, None)
                else:
                    book[key][px] = qty
        bids = [[p, book["b"][p]] for p in sorted(book["b"], reverse=True)[:self.depth]]
        asks = [[p, book["a"][p]] for p in sorted(book["a"])[:self.depth]]
        return [self.event(ctx, asset=asset, event_type=T.ORDERBOOK_TOP, symbol=sym,
                           event_ts_ms=epoch_ms(m.get("cts") or m["ts"], "cts"), source_seq=d.get("u"),
                           flags=(PerpFlag.DELTA_MERGED.value,) if typ == "delta" else (),
                           payload={"bids": bids, "asks": asks, "depth": self.depth, "update_id": d.get("u"),
                                    "prev_update_id": None, "quote_ccy": "USDT", "native": {"seq": d.get("seq")}})]

    def _liqs(self, m, asset, sym, ctx):
        if not isinstance(m["data"], list):
            raise ValueError("allLiquidation data is not a list")
        out = []
        for x in sorted(m["data"], key=lambda x: int(x["T"])):
            pos_side = side(x["S"], "S")                              # POSITION side: buy = long liquidated
            liquidated = "long" if pos_side == "buy" else "short"
            qty, px = size(x["v"], "v"), price(x["p"], "p")
            out.append(self.event(ctx, asset=asset, event_type=T.LIQUIDATION, symbol=sym,
                                  event_ts_ms=epoch_ms(x["T"], "T"),
                                  payload={"price": px, "qty_native": qty, "qty_unit": "coin", "qty_coin": qty,
                                           "notional_quote": px * qty,
                                           "forced_side": FORCED_SIDE_FOR_POSITION[liquidated],
                                           "liquidated_position": liquidated,
                                           "side_semantics": "POSITION_SIDE (Buy = long liquidated)",
                                           "quote_ccy": "USDT", "native": {"S": x["S"]}}))
        return out

    # ---------------- REST ----------------
    def parse_rest(self, stream, body, ctx):
        res = new_result()
        kind, _, asset = stream.partition(":")
        try:
            if not isinstance(body, dict) or body.get("retCode") != 0:
                raise ValueError(f"retCode {body.get('retCode') if isinstance(body, dict) else '?'}")
            rows = (body.get("result") or {}).get("list")
            if not isinstance(rows, list):
                raise ValueError("result.list missing")
            sym = self.spec.symbols[asset]
            if kind == "funding_hist":
                st = self.tickers.get(sym, {})
                ih = st.get("fundingIntervalHour")
                ms = int(float(ih) * HOUR_MS) if ih not in (None, "") else None
                for x in sorted(rows, key=lambda x: int(x["fundingRateTimestamp"])):
                    r = funding_rate(x["fundingRate"])
                    t = int(x["fundingRateTimestamp"])
                    res.events.append(self.event(ctx, asset=asset, event_type=T.FUNDING_SETTLED, symbol=sym,
                                                 event_ts_ms=epoch_ms(t, "fundingRateTimestamp"), mode=self.mode(True),
                                                 payload={"rate_native": r, "funding_ts_ms": t, "interval_ms": ms,
                                                          "rate_8h": funding_normalized(r, ms)[1]}))
            elif kind == "backfill":
                for x in sorted(rows, key=lambda x: int(x["time"])):
                    res.events.append(self._trade(x, asset, sym, ctx, backfill=True, rest=True))
            else:
                raise ValueError(f"unknown REST stream {stream!r}")
        except (KeyError, TypeError, ValueError) as e:
            self.fail(res, ctx, f"{stream}: {type(e).__name__}: {e}", json.dumps(body, default=str)[:300])
        return res
