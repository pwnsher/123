"""
Kalshi perpetuals — public REST only, READ-ONLY, as a Step-4 RESEARCH venue.

This is an independent reader of the same two public endpoints the existing perp telemetry uses; it shares
no code or state with perp_telemetry.py and nothing it produces reaches the existing shadow / overlay /
live-veto chain.

    GET {base}/margin/markets                          bid, ask, price (last), settlement_mark_price{price,ts_ms},
                                                       reference_price{price,ts_ms}, contract_size,
                                                       underlying_multiplier, status
    GET {base}/margin/funding_rates/estimate?ticker=   funding_rate (estimate for the in-progress period),
                                                       next_funding_time, computed_time

Prices are PER-CONTRACT dollars (quote_ccy "USD_PER_CONTRACT"); the per-coin scale and the funding units /
interval are NOT verified, so events carry quality UNVERIFIED_UNITS, normalized funding is None, and only
ratios (returns, mark / index premium) are meaningful. No trades with sides, open interest, liquidations or
depth are published. Tickers: KALSHI_PERP_TICKER_<COIN> override, else the unique market naming the coin.
"""
import json
import os

from market_data.normalization import iso_ms, mid, optional_price
from market_data.types import EventFlag
from perp_data.normalization import funding_rate
from perp_data.sources.base import PerpAdapter, new_result
from perp_data.types import PerpEventType as T, PerpFlag

REST_BASE = os.environ.get("KALSHI_PERP_API_BASE", "https://external-api.kalshi.com/trade-api/v2")


def _ts(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        x = int(v)
        return x if x > 10**12 else x * 1000
    return iso_ms(str(v))


class KalshiPerpAdapter(PerpAdapter):
    venue = source = "kalshi_perp"
    stream = "rest"
    transport = "rest"
    documentation = "Kalshi perps public REST: /margin/markets, /margin/funding_rates/estimate (read-only)"

    def __init__(self, assets, depth=0, overrides=None):
        super().__init__(assets, depth=0)
        self.assets = list(assets)
        env = overrides if overrides is not None else {a: os.environ.get(f"KALSHI_PERP_TICKER_{a}") for a in self.assets}
        self.overrides = {a: t for a, t in env.items() if t}
        self.tickers = {}                          # asset -> resolved ticker

    def rest_urls(self):
        out = {"markets": (f"{REST_BASE}/margin/markets", None, 2)}
        for a, t in self.tickers.items():
            out[f"funding:{a}"] = (f"{REST_BASE}/margin/funding_rates/estimate", {"ticker": t}, 60)
        return out

    def resolve(self, markets):
        for a in self.assets:
            if a in self.overrides:
                self.tickers[a] = self.overrides[a]
                continue
            c = [m.get("ticker") for m in markets if isinstance(m, dict) and a in str(m.get("ticker", "")).upper()]
            if len(c) == 1:
                self.tickers[a] = c[0]

    def parse_rest(self, stream, body, ctx):
        res = new_result()
        kind, _, asset = stream.partition(":")
        try:
            if kind == "markets":
                ms = body.get("markets") if isinstance(body, dict) else None
                if not isinstance(ms, list):
                    raise ValueError("markets list missing")
                self.resolve(ms)
                by_t = {m.get("ticker"): m for m in ms if isinstance(m, dict)}
                for a, t in sorted(self.tickers.items()):
                    m = by_t.get(t)
                    if m is None:
                        continue
                    bid, ask = optional_price(m.get("bid"), "bid"), optional_price(m.get("ask"), "ask")
                    common = dict(asset=a, symbol=t, quality="UNVERIFIED_UNITS", flags=(PerpFlag.UNITS_UNVERIFIED.value,))
                    res.events.append(self.event(ctx, event_type=T.PERP_QUOTE, event_ts_ms=None,
                                                 payload={"bid": bid, "bid_qty_coin": None, "ask": ask, "ask_qty_coin": None,
                                                          "mid": mid(bid, ask),
                                                          "quote_ccy": "USD_PER_CONTRACT",
                                                          "native": {"price": m.get("price"), "status": m.get("status"),
                                                                     "contract_size": m.get("contract_size"),
                                                                     "underlying_multiplier": m.get("underlying_multiplier")}},
                                                 **dict(common, flags=common["flags"] + (EventFlag.EVENT_TIME_MISSING.value,))))
                    for key, et, field in (("settlement_mark_price", T.PERP_MARK_PRICE, "mark"),
                                           ("reference_price", T.PERP_INDEX_PRICE, "index")):
                        o = m.get(key)
                        if isinstance(o, dict) and optional_price(o.get("price"), key) is not None:
                            res.events.append(self.event(ctx, event_type=et, event_ts_ms=_ts(o.get("ts_ms")),
                                                         payload={field: optional_price(o["price"], key),
                                                                  "quote_ccy": "USD_PER_CONTRACT"}, **common))
            elif kind == "funding":
                if not isinstance(body, dict):
                    raise ValueError("funding estimate is not an object")
                r = funding_rate(body["funding_rate"], "funding_rate")
                res.events.append(self.event(ctx, asset=asset, event_type=T.FUNDING_RATE, symbol=self.tickers.get(asset, "?"),
                                             event_ts_ms=_ts(body.get("computed_time")), quality="UNVERIFIED_UNITS",
                                             flags=(PerpFlag.ESTIMATE.value, PerpFlag.UNITS_UNVERIFIED.value),
                                             payload={"rate_native": r, "interval_ms": None, "interval_source": "unverified",
                                                      "next_funding_ts_ms": _ts(body.get("next_funding_time")),
                                                      "rate_per_hour": None, "rate_8h": None, "rate_annual_simple": None,
                                                      "is_estimate": True}))
            else:
                raise ValueError(f"unknown REST stream {stream!r}")
        except (KeyError, TypeError, ValueError) as e:
            self.fail(res, ctx, f"{stream}: {type(e).__name__}: {e}", json.dumps(body, default=str)[:300])
        return res

