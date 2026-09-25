"""
OKX USDT swaps (instType SWAP) — public v5 websocket + public REST. No authentication.

Websocket  wss://ws.okx.com:8443/ws/v5/public, subscribe {"op":"subscribe","args":[{"channel":..,"instId":..}]}
           channels trades, books5, mark-price, funding-rate, open-interest (per swap), index-tickers (per index
           BTC-USDT ...), liquidation-orders (instType SWAP; filtered to our instruments).
           Client keep-alive: the text "ping" every 25 s (server answers "pong" - control).

SIZES ARE IN CONTRACTS. One contract = ctVal base coins (e.g. 0.01 BTC for BTC-USDT-SWAP). Coin quantities
are produced only after the contract value is VERIFIED from /api/v5/public/instruments (INSTRUMENT events);
until then qty_coin is None, quality UNVERIFIED_UNITS (the static table in venues.py is never trusted alone).

Messages -> events (documented semantics)
    trades            PERP_TRADE side = TAKER side; sz contracts; tradeId; ts.
    books5            ORDERBOOK_TOP (5 levels, a full snapshot each push); sizes contracts -> coin.
    mark-price        PERP_MARK_PRICE markPx, ts.
    index-tickers     PERP_INDEX_PRICE idxPx for BTC-USDT ... (the venue's spot index).
    funding-rate      FUNDING_RATE fundingRate for the settlement at fundingTime (interval = nextFundingTime -
                      fundingTime, derived from the venue's own times); PREDICTED_FUNDING nextFundingRate for the
                      FOLLOWING period ONLY when OKX publishes it (it is empty for current-period collection).
    open-interest     OPEN_INTEREST oi (contracts), oiCcy (base coin, published by OKX), oiUsd when present.
    liquidation-orders LIQUIDATION details[]: side = the FORCED ORDER side, posSide = the POSITION side (long /
                      short; "net" in one-way mode -> position derived from the order side, flagged). bkPx is the
                      BANKRUPTCY price (not a fill price); sz contracts. The channel is documented as a sample
                      (quality SAMPLED): totals are lower bounds.
REST (GET)  /api/v5/public/instruments?instType=SWAP (INSTRUMENT: ctVal), /api/v5/public/funding-rate-history
            (FUNDING_SETTLED), /api/v5/market/trades (backfill).
"""
import json
import os

from market_data.normalization import epoch_ms, optional_price, price, size
from market_data.types import AggressorSemantics, EventFlag
from perp_data.normalization import (POSITION_CLOSED_BY_FORCED, funding_normalized, funding_rate,
                                     optional_funding_rate, side)
from perp_data.sources.base import PerpAdapter, new_result
from perp_data.types import PerpEventType as T, PerpFlag

WS_URL = os.environ.get("OKX_WS_URL", "wss://ws.okx.com:8443/ws/v5/public")
REST_BASE = os.environ.get("OKX_REST_URL", "https://www.okx.com")


class OkxSwapAdapter(PerpAdapter):
    venue = source = "okx_swap"
    stream = "ws"
    transport = "ws"
    keepalive_text = "ping"
    keepalive_s = 25
    documentation = "OKX v5 public: trades, books5, mark-price, index-tickers, funding-rate, open-interest, liquidation-orders"

    def __init__(self, assets, depth=5):
        super().__init__(assets, depth=max(1, min(depth, 5)))
        self.url = WS_URL
        self.by_index = {self.spec.index_symbols[a]: a for a in self.assets}

    def subscribe_messages(self):
        args = []
        for a in self.assets:
            i = self.spec.symbols[a]
            args += [{"channel": c, "instId": i} for c in ("trades", "books5", "mark-price", "funding-rate", "open-interest")]
            args.append({"channel": "index-tickers", "instId": self.spec.index_symbols[a]})
        args.append({"channel": "liquidation-orders", "instType": "SWAP"})
        return [json.dumps({"op": "subscribe", "args": args})]

    def rest_urls(self):
        out = {"instruments": (f"{REST_BASE}/api/v5/public/instruments", {"instType": "SWAP"}, 3600)}
        for a in self.assets:
            out[f"funding_hist:{a}"] = (f"{REST_BASE}/api/v5/public/funding-rate-history",
                                        {"instId": self.spec.symbols[a], "limit": 3}, 60)
        return out

    def backfill_url(self, asset, limit=100):
        return f"{REST_BASE}/api/v5/market/trades", {"instId": self.spec.symbols[asset], "limit": limit}

    # ---------------- websocket ----------------
    def parse(self, text, ctx):
        res = new_result()
        if isinstance(text, str) and text.strip() == "pong":
            res.control.append("pong")
            return res
        m = self.load(text, ctx, res)
        if m is None:
            return res
        if isinstance(m, dict) and "event" in m:
            res.control.append(m)                   # subscribe / error acknowledgements
            return res
        arg = m.get("arg") if isinstance(m, dict) else None
        if not isinstance(arg, dict) or not isinstance(m.get("data"), list):
            self.fail(res, ctx, "message without arg/data", text)
            return res
        ch = arg.get("channel")
        if ch == "liquidation-orders":
            for item in m["data"]:
                asset = self.by_symbol.get(item.get("instId")) if isinstance(item, dict) else None
                if asset is None:
                    continue                        # another swap: not one of ours (not a failure)
                self.guarded(res, ctx, item, lambda it=item, a=asset: res.events.extend(self._liqs(it, a, ctx)))
            return res
        if ch == "index-tickers":
            asset = self.by_index.get(arg.get("instId"))
        else:
            asset = self.by_symbol.get(arg.get("instId"))
        fn = {"trades": self._trade, "books5": self._book, "mark-price": self._mark, "index-tickers": self._index,
              "funding-rate": self._funding, "open-interest": self._oi}.get(ch)
        if fn is None or asset is None:
            self.fail(res, ctx, f"unexpected channel/instrument {ch!r}/{arg.get('instId')!r}", text)
            return res
        for item in m["data"]:
            self.guarded(res, ctx, item, lambda it=item: res.events.extend(fn(it, asset, ctx)))
        return res

    def _trade(self, x, asset, ctx, backfill=False):
        sz, px = size(x["sz"], "sz"), price(x["px"], "px")
        coin, fl, q = self.to_coin(asset, sz)
        return [self.event(ctx, asset=asset, event_type=T.PERP_TRADE, symbol=x["instId"],
                           event_ts_ms=epoch_ms(int(x["ts"]), "ts"), source_seq=x.get("tradeId"), mode=self.mode(backfill),
                           quality=q, flags=fl,
                           payload={"price": px, "qty_native": sz, "qty_unit": "contracts", "qty_coin": coin,
                                    "notional_quote": px * coin if coin is not None else None,
                                    "aggressor": side(x["side"]),
                                    "aggressor_semantics": AggressorSemantics.TAKER_SIDE_FIELD.value,
                                    "trade_id": str(x["tradeId"]), "quote_ccy": "USDT"})]

    def _book(self, x, asset, ctx):
        def lv(rows, rev):
            out = []
            for r in rows:
                px, sz = price(r[0], "level price"), size(r[1], "level size")
                coin, _fl, _q = self.to_coin(asset, sz)
                out.append([px, coin])
            return sorted(out, key=lambda z: -z[0] if rev else z[0])[:self.depth]
        coin_known = self.to_coin(asset, 1.0)[0] is not None
        return [self.event(ctx, asset=asset, event_type=T.ORDERBOOK_TOP, symbol=self.spec.symbols[asset],
                           event_ts_ms=epoch_ms(int(x["ts"]), "ts"), source_seq=x.get("seqId"),
                           quality="OK" if coin_known else "UNVERIFIED_UNITS",
                           flags=() if coin_known else (PerpFlag.CONTRACT_SIZE_UNVERIFIED.value,),
                           payload={"bids": lv(x["bids"], True), "asks": lv(x["asks"], False), "depth": self.depth,
                                    "update_id": x.get("seqId"), "prev_update_id": None, "quote_ccy": "USDT",
                                    "native": {"sizes_contracts": [[r[0], r[1]] for r in x["bids"][:1] + x["asks"][:1]]}})]

    def _mark(self, x, asset, ctx):
        return [self.event(ctx, asset=asset, event_type=T.PERP_MARK_PRICE, symbol=x["instId"],
                           event_ts_ms=epoch_ms(int(x["ts"]), "ts"),
                           payload={"mark": price(x["markPx"], "markPx"), "quote_ccy": "USDT"})]

    def _index(self, x, asset, ctx):
        return [self.event(ctx, asset=asset, event_type=T.PERP_INDEX_PRICE, symbol=x["instId"],
                           event_ts_ms=epoch_ms(int(x["ts"]), "ts"),
                           payload={"index": price(x["idxPx"], "idxPx"), "quote_ccy": "USDT"})]

    def _funding(self, x, asset, ctx):
        r = funding_rate(x["fundingRate"], "fundingRate")
        ft = int(x["fundingTime"]) if x.get("fundingTime") not in (None, "") else None
        nft = int(x["nextFundingTime"]) if x.get("nextFundingTime") not in (None, "") else None
        ms = (nft - ft) if (ft and nft and nft > ft) else None
        ph, r8, ra = funding_normalized(r, ms)
        ts = epoch_ms(int(x["ts"]), "ts") if x.get("ts") not in (None, "") else None
        out = [self.event(ctx, asset=asset, event_type=T.FUNDING_RATE, symbol=x["instId"], event_ts_ms=ts,
                          quality="ESTIMATE", flags=(PerpFlag.ESTIMATE.value,) + (() if ts else (EventFlag.EVENT_TIME_MISSING.value,)),
                          payload={"rate_native": r, "interval_ms": ms,
                                   "interval_source": "nextFundingTime_minus_fundingTime" if ms else "unknown",
                                   "next_funding_ts_ms": ft, "rate_per_hour": ph, "rate_8h": r8,
                                   "rate_annual_simple": ra, "is_estimate": True,
                                   "native": {"method": x.get("method"), "settFundingRate": x.get("settFundingRate")}})]
        nxt = optional_funding_rate(x.get("nextFundingRate"), "nextFundingRate")
        if nxt is not None:
            p8 = funding_normalized(nxt, ms)
            out.append(self.event(ctx, asset=asset, event_type=T.PREDICTED_FUNDING, symbol=x["instId"], event_ts_ms=ts,
                                  quality="ESTIMATE", flags=(PerpFlag.ESTIMATE.value,),
                                  payload={"rate_native": nxt, "interval_ms": ms, "applies_at_ts_ms": nft,
                                           "rate_per_hour": p8[0], "rate_8h": p8[1]}))
        return out

    def _oi(self, x, asset, ctx):
        oi = size(x["oi"], "oi")
        occ = x.get("oiCcy")
        if occ not in (None, ""):
            coin = size(occ, "oiCcy")                    # published by OKX in base coin: reliable
        else:
            coin = self.to_coin(asset, oi)[0]
        return [self.event(ctx, asset=asset, event_type=T.OPEN_INTEREST, symbol=x["instId"],
                           event_ts_ms=epoch_ms(int(x["ts"]), "ts"),
                           quality="OK" if coin is not None else "UNVERIFIED_UNITS",
                           payload={"oi_native": oi, "oi_unit": "contracts", "oi_coin": coin,
                                    "oi_quote_native": optional_price(x.get("oiUsd"), "oiUsd")})]

    def _liqs(self, item, asset, ctx):
        out = []
        for dx in sorted(item.get("details") or [], key=lambda z: int(z["ts"])):
            forced = side(dx["side"], "side")
            pos = str(dx.get("posSide") or "").lower()
            flags = [PerpFlag.SAMPLED_STREAM.value]
            if pos in ("long", "short"):
                liquidated = pos
                sem = "ORDER_SIDE + POSITION_SIDE (posSide)"
            else:
                liquidated = POSITION_CLOSED_BY_FORCED[forced]
                flags.append(PerpFlag.POSITION_SIDE_DERIVED.value)
                sem = "ORDER_SIDE (position derived; posSide net)"
            sz, px = size(dx["sz"], "sz"), price(dx["bkPx"], "bkPx")
            coin, fl, q = self.to_coin(asset, sz)
            out.append(self.event(ctx, asset=asset, event_type=T.LIQUIDATION, symbol=item["instId"],
                                  event_ts_ms=epoch_ms(int(dx["ts"]), "ts"),
                                  quality="SAMPLED" if q == "OK" else q, flags=tuple(flags) + fl,
                                  payload={"price": px, "qty_native": sz, "qty_unit": "contracts", "qty_coin": coin,
                                           "notional_quote": px * coin if coin is not None else None,
                                           "forced_side": forced, "liquidated_position": liquidated,
                                           "side_semantics": sem, "quote_ccy": "USDT",
                                           "native": {"posSide": dx.get("posSide"), "bkLoss": dx.get("bkLoss"),
                                                      "price_is_bankruptcy_price": True}}))
        return out

    # ---------------- REST ----------------
    def parse_rest(self, stream, body, ctx):
        res = new_result()
        kind, _, asset = stream.partition(":")
        try:
            if not isinstance(body, dict) or str(body.get("code")) != "0" or not isinstance(body.get("data"), list):
                raise ValueError(f"code {body.get('code') if isinstance(body, dict) else '?'}")
            if kind == "instruments":
                rows = {x.get("instId"): x for x in body["data"] if isinstance(x, dict)}
                for a in self.assets:
                    x = rows.get(self.spec.symbols[a])
                    if x is None:
                        continue
                    cv = size(x["ctVal"], "ctVal")
                    ok = x.get("ctValCcy") == a and x.get("ctType") in (None, "", "linear") and cv > 0
                    if ok:
                        self.contract_values[a] = (cv, True)
                    res.events.append(self.event(ctx, asset=a, event_type=T.INSTRUMENT, symbol=self.spec.symbols[a],
                                                 event_ts_ms=None, flags=(EventFlag.EVENT_TIME_MISSING.value,),
                                                 payload={"contract_value": cv, "contract_value_ccy": x.get("ctValCcy"),
                                                          "funding_interval_ms": None, "verified": ok,
                                                          "native": {"ctType": x.get("ctType"), "state": x.get("state")}}))
            elif kind == "funding_hist":
                for x in sorted(body["data"], key=lambda x: int(x["fundingTime"])):
                    r = funding_rate(x.get("realizedRate") or x["fundingRate"])
                    t = int(x["fundingTime"])
                    res.events.append(self.event(ctx, asset=asset, event_type=T.FUNDING_SETTLED, symbol=x["instId"],
                                                 event_ts_ms=epoch_ms(t, "fundingTime"), mode=self.mode(True),
                                                 payload={"rate_native": r, "funding_ts_ms": t, "interval_ms": None,
                                                          "rate_8h": None,
                                                          "native": {"fundingRate": x.get("fundingRate"),
                                                                     "realizedRate": x.get("realizedRate")}}))
            elif kind == "backfill":
                for x in sorted(body["data"], key=lambda x: int(x["ts"])):
                    res.events += self._trade(x, asset, ctx, backfill=True)
            else:
                raise ValueError(f"unknown REST stream {stream!r}")
        except (KeyError, TypeError, ValueError) as e:
            self.fail(res, ctx, f"{stream}: {type(e).__name__}: {e}", json.dumps(body, default=str)[:300])
        return res
