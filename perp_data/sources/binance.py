"""
Binance USDⓈ-M perpetuals — public market streams + public REST. No authentication, no order endpoint.

Websocket  wss://fstream.binance.com/stream?streams=<s>@aggTrade/<s>@bookTicker/<s>@markPrice@1s/
           <s>@forceOrder/<s>@depth<N>@100ms   (combined stream: {"stream": ..., "data": {...}})
           Binance sends pings; the websocket client answers them automatically.

Messages -> events (documented semantics)
    aggTrade      PERP_TRADE  p, q (BASE COIN), a (aggregate trade id, sequential), T (trade time).
                  `m` = "is the buyer the market maker" -> m true => the SELLER was the aggressor.
    bookTicker    PERP_QUOTE  b/B, a/A, u (update id), T (transaction time).
    markPriceUpdate  (every 1 s) PERP_MARK_PRICE p, PERP_INDEX_PRICE i, FUNDING_RATE r for the upcoming
                  settlement at T (an in-progress estimate: flagged ESTIMATE). Interval: /fapi/v1/fundingInfo
                  lists symbols with an ADJUSTED interval; any other symbol uses the documented 8 h default
                  (flag DEFAULT_INTERVAL). Binance publishes no forecast for the FOLLOWING period, so no
                  PREDICTED_FUNDING events are produced.
    forceOrder    LIQUIDATION o.S = side of the FORCED (liquidation) order; a forced SELL closes a LONG, so the
                  liquidated position is DERIVED (flag POSITION_SIDE_DERIVED). The stream is a SAMPLE: only the
                  largest liquidation per symbol per 1000 ms is pushed (quality SAMPLED) - totals are lower bounds.
    depthUpdate   ORDERBOOK_TOP  partial book (top N levels, each message complete); u / pu (previous u)
                  chain -> sequence gaps are detectable.
REST (GET)  /fapi/v1/openInterest (OPEN_INTEREST, base coin), /fapi/v1/fundingInfo (INSTRUMENT: interval),
            /fapi/v1/fundingRate (FUNDING_SETTLED), /fapi/v1/aggTrades?fromId= (exact trade backfill).
"""
import json
import os

from market_data.normalization import epoch_ms, mid, optional_size, price, size
from market_data.types import AggressorSemantics, EventFlag
from perp_data.normalization import (EIGHT_HOURS_MS, HOUR_MS, POSITION_CLOSED_BY_FORCED, funding_normalized,
                                     funding_rate, side)
from perp_data.sources.base import PerpAdapter, new_result
from perp_data.types import PerpEventType as T, PerpFlag

WS_BASE = os.environ.get("BINANCE_FUTURES_WS_URL", "wss://fstream.binance.com/stream")
REST_BASE = os.environ.get("BINANCE_FUTURES_REST_URL", "https://fapi.binance.com")


class BinanceUsdmAdapter(PerpAdapter):
    venue = source = "binance_usdm"
    stream = "ws"
    transport = "ws"
    documentation = "Binance USDⓈ-M public streams: aggTrade, bookTicker, markPrice@1s, forceOrder, partial depth"

    def __init__(self, assets, depth=10):
        super().__init__(assets, depth=depth if depth in (5, 10, 20) else 10)
        self.intervals = {}                     # asset -> (interval_ms, source)
        names = []
        for a in self.assets:
            s = self.spec.symbols[a].lower()
            names += [f"{s}@aggTrade", f"{s}@bookTicker", f"{s}@markPrice@1s", f"{s}@forceOrder",
                      f"{s}@depth{self.depth}@100ms"]
        self.url = f"{WS_BASE}?streams=" + "/".join(names)

    def rest_urls(self):
        out = {"funding_info": (f"{REST_BASE}/fapi/v1/fundingInfo", None, 600)}
        for a in self.assets:
            sym = self.spec.symbols[a]
            out[f"oi:{a}"] = (f"{REST_BASE}/fapi/v1/openInterest", {"symbol": sym}, 2)
            out[f"funding_hist:{a}"] = (f"{REST_BASE}/fapi/v1/fundingRate", {"symbol": sym, "limit": 3}, 60)
        return out

    def backfill_url(self, asset, from_id=None, limit=1000):
        p = {"symbol": self.spec.symbols[asset], "limit": limit}
        if from_id is not None:
            p["fromId"] = int(from_id)
        return f"{REST_BASE}/fapi/v1/aggTrades", p

    def _interval(self, asset):
        ms, src = self.intervals.get(asset, (EIGHT_HOURS_MS, "documented_default_8h"))
        return ms, src

    # ---------------- websocket ----------------
    def parse(self, text, ctx):
        res = new_result()
        m = self.load(text, ctx, res)
        if m is None:
            return res
        if isinstance(m, dict) and "result" in m and "id" in m:
            res.control.append(m)
            return res
        d = m.get("data") if isinstance(m, dict) else None
        if not isinstance(d, dict) or "e" not in d:
            self.fail(res, ctx, "message without data.e", text)
            return res
        asset = self.by_symbol.get(d.get("s") or (d.get("o") or {}).get("s"))
        if asset is None:
            self.fail(res, ctx, f"unknown symbol {d.get('s')!r}", text)
            return res
        e = d["e"]
        fn = {"aggTrade": self._trade, "bookTicker": self._quote, "markPriceUpdate": self._mark,
              "forceOrder": self._liq, "depthUpdate": self._depth}.get(e)
        if fn is None:
            self.fail(res, ctx, f"unexpected event type {e!r}", text)
            return res
        self.guarded(res, ctx, text, lambda: res.events.extend(fn(d, asset, ctx)))
        return res

    def _trade(self, d, asset, ctx, backfill=False):
        qty = size(d["q"], "q")
        px = price(d["p"], "p")
        m = d["m"]
        if not isinstance(m, bool):
            raise ValueError("aggTrade m is not a boolean")
        aggressor = "sell" if m else "buy"               # buyer was maker -> seller took liquidity
        return [self.event(ctx, asset=asset, event_type=T.PERP_TRADE, symbol=self.spec.symbols[asset],
                           event_ts_ms=epoch_ms(d["T"], "T"), source_seq=int(d["a"]), mode=self.mode(backfill),
                           payload={"price": px, "qty_native": qty, "qty_unit": "coin", "qty_coin": qty,
                                    "notional_quote": px * qty, "aggressor": aggressor,
                                    "aggressor_semantics": AggressorSemantics.INVERTED_MAKER_SIDE.value,
                                    "trade_id": int(d["a"]), "quote_ccy": "USDT",
                                    "native": {"m": m, "f": d.get("f"), "l": d.get("l")}})]

    def _quote(self, d, asset, ctx):
        bid, ask = price(d["b"], "b"), price(d["a"], "a")
        flags = (EventFlag.CROSSED_BOOK.value,) if ask < bid else ()
        return [self.event(ctx, asset=asset, event_type=T.PERP_QUOTE, symbol=self.spec.symbols[asset],
                           event_ts_ms=epoch_ms(d.get("T") or d["E"], "T"), source_seq=d.get("u"), flags=flags,
                           payload={"bid": bid, "bid_qty_coin": optional_size(d.get("B"), "B"), "ask": ask,
                                    "ask_qty_coin": optional_size(d.get("A"), "A"), "mid": mid(bid, ask),
                                    "quote_ccy": "USDT", "native": {"u": d.get("u")}})]

    def _mark(self, d, asset, ctx):
        sym, ts = self.spec.symbols[asset], epoch_ms(d["E"], "E")
        out = [self.event(ctx, asset=asset, event_type=T.PERP_MARK_PRICE, symbol=sym, event_ts_ms=ts,
                          payload={"mark": price(d["p"], "p"), "quote_ccy": "USDT"})]
        if d.get("i") not in (None, ""):
            out.append(self.event(ctx, asset=asset, event_type=T.PERP_INDEX_PRICE, symbol=sym, event_ts_ms=ts,
                                  payload={"index": price(d["i"], "i"), "quote_ccy": "USDT"}))
        if d.get("r") not in (None, ""):
            r = funding_rate(d["r"], "r")
            ms, src = self._interval(asset)
            ph, r8, ra = funding_normalized(r, ms)
            flags = (PerpFlag.ESTIMATE.value,) + ((PerpFlag.DEFAULT_INTERVAL.value,) if src.startswith("documented") else ())
            out.append(self.event(ctx, asset=asset, event_type=T.FUNDING_RATE, symbol=sym, event_ts_ms=ts,
                                  quality="ESTIMATE", flags=flags,
                                  payload={"rate_native": r, "interval_ms": ms, "interval_source": src,
                                           "next_funding_ts_ms": epoch_ms(d["T"], "T") if d.get("T") else None,
                                           "rate_per_hour": ph, "rate_8h": r8, "rate_annual_simple": ra,
                                           "is_estimate": True}))
        return out

    def _liq(self, d, asset, ctx):
        o = d["o"]
        forced = side(o["S"], "o.S")
        qty = size(o.get("z") if o.get("z") not in (None, "", "0") else o["q"], "qty")
        px = price(o["ap"] if o.get("ap") not in (None, "", "0", "0.00") else o["p"], "price")
        return [self.event(ctx, asset=asset, event_type=T.LIQUIDATION, symbol=self.spec.symbols[asset],
                           event_ts_ms=epoch_ms(o.get("T") or d["E"], "T"), quality="SAMPLED",
                           flags=(PerpFlag.SAMPLED_STREAM.value, PerpFlag.POSITION_SIDE_DERIVED.value),
                           payload={"price": px, "qty_native": qty, "qty_unit": "coin", "qty_coin": qty,
                                    "notional_quote": px * qty, "forced_side": forced,
                                    "liquidated_position": POSITION_CLOSED_BY_FORCED[forced],
                                    "side_semantics": "FORCED_ORDER_SIDE (position derived)", "quote_ccy": "USDT",
                                    "native": {"S": o["S"], "X": o.get("X"), "q": o.get("q"), "z": o.get("z"),
                                               "p": o.get("p"), "ap": o.get("ap")}})]

    def _depth(self, d, asset, ctx):
        bids = [[price(p, "bid"), size(q, "qty")] for p, q in d["b"]][:self.depth]
        asks = [[price(p, "ask"), size(q, "qty")] for p, q in d["a"]][:self.depth]
        bids.sort(key=lambda x: -x[0])
        asks.sort(key=lambda x: x[0])
        return [self.event(ctx, asset=asset, event_type=T.ORDERBOOK_TOP, symbol=self.spec.symbols[asset],
                           event_ts_ms=epoch_ms(d.get("T") or d["E"], "T"), source_seq=d.get("u"),
                           payload={"bids": bids, "asks": asks, "depth": self.depth, "update_id": d.get("u"),
                                    "prev_update_id": d.get("pu"), "quote_ccy": "USDT"})]

    # ---------------- REST ----------------
    def parse_rest(self, stream, body, ctx):
        res = new_result()
        kind, _, asset = stream.partition(":")
        try:
            if kind == "oi":
                if not isinstance(body, dict) or body.get("symbol") != self.spec.symbols.get(asset):
                    raise ValueError("openInterest response for another symbol")
                q = size(body["openInterest"], "openInterest")
                res.events.append(self.event(ctx, asset=asset, event_type=T.OPEN_INTEREST, symbol=body["symbol"],
                                             event_ts_ms=epoch_ms(body["time"], "time"),
                                             payload={"oi_native": q, "oi_unit": "coin", "oi_coin": q,
                                                      "oi_quote_native": None}))
            elif kind == "funding_info":
                if not isinstance(body, list):
                    raise ValueError("fundingInfo is not a list")
                listed = {x.get("symbol"): x for x in body if isinstance(x, dict)}
                for a in self.assets:
                    sym = self.spec.symbols[a]
                    x = listed.get(sym)
                    if x is not None and x.get("fundingIntervalHours") not in (None, ""):
                        ms, src, ver = int(float(x["fundingIntervalHours"]) * HOUR_MS), "fundingInfo", True
                    else:
                        ms, src, ver = EIGHT_HOURS_MS, "documented_default_8h", False
                    self.intervals[a] = (ms, src)
                    res.events.append(self.event(ctx, asset=a, event_type=T.INSTRUMENT, symbol=sym, event_ts_ms=None,
                                                 flags=(EventFlag.EVENT_TIME_MISSING.value,),
                                                 payload={"contract_value": None, "contract_value_ccy": a,
                                                          "funding_interval_ms": ms, "verified": ver,
                                                          "interval_source": src}))
            elif kind == "funding_hist":
                if not isinstance(body, list):
                    raise ValueError("fundingRate history is not a list")
                for x in sorted(body, key=lambda x: int(x["fundingTime"])):
                    ms, _src = self._interval(asset)
                    r = funding_rate(x["fundingRate"])
                    res.events.append(self.event(ctx, asset=asset, event_type=T.FUNDING_SETTLED, symbol=x["symbol"],
                                                 event_ts_ms=epoch_ms(int(x["fundingTime"]), "fundingTime"),
                                                 mode=self.mode(True),
                                                 payload={"rate_native": r, "funding_ts_ms": int(x["fundingTime"]),
                                                          "interval_ms": ms, "rate_8h": funding_normalized(r, ms)[1]}))
            elif kind == "backfill":
                if not isinstance(body, list):
                    raise ValueError("aggTrades is not a list")
                for x in sorted(body, key=lambda x: int(x["a"])):
                    res.events += self._trade(x, asset, ctx, backfill=True)
            else:
                raise ValueError(f"unknown REST stream {stream!r}")
        except (KeyError, TypeError, ValueError) as e:
            self.fail(res, ctx, f"{stream}: {type(e).__name__}: {e}", json.dumps(body, default=str)[:300])
        return res
