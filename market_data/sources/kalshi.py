"""
Kalshi 15-minute markets — READ-ONLY public REST polling. No authentication, no order endpoints.

Transport  GET {KALSHI_BASE}/markets?series_ticker=<S>&status=open    (current market per series)
           GET {KALSHI_BASE}/markets/{ticker}/orderbook                 (depth; public)
           GET {KALSHI_BASE}/markets/trades?ticker=<T>&limit=<n>        (recent trades; public)
           GET {KALSHI_BASE}/markets/{ticker}                           (after close: result + expiration_value)
KALSHI_BASE defaults to the production dashboard's public base (env KALSHI_MD_BASE overrides).

Events
    MARKET_STATE  ticker, strike (floor_strike|cap_strike|strike, via settlement.kalshi_markets), close /
                  open time, YES/NO bid/ask in CENTS (from *_dollars fields; a missing ask is derived as
                  100 - opposite bid and flagged DERIVED_ASK), status. The market object carries no
                  update timestamp -> event_ts None + EVENT_TIME_MISSING; receive time is the poll response.
    BOOK          YES terms: bids = YES bids; asks = YES asks derived from NO bids (100 - p), flagged
                  DERIVED_ASK. Accepts {"orderbook": {"yes": [[cents, qty]], "no": [...]}} and the
                  *_dollars variants ([["0.45", qty]]); anything else is a parse failure.
    TRADE         price in YES cents, size = contracts, event time = created_time. `taker_side` is the
                  aggressor: "yes" -> "buy" (bought YES), "no" -> "sell" (in YES terms). Trades returned
                  by the first poll after start/reconnect happened before we were watching -> BACKFILLED.
    RESOLUTION    result + expiration_value of a settled market (a LABEL; never a feature).
"""
import os

from market_data.normalization import NormalizationError, epoch_ms, iso_ms, price, size
from market_data.sources.base import SourceAdapter, new_result
from market_data.types import AggressorSemantics, EventFlag, EventType, IngestMode
from settlement.assets import SERIES_ASSET
from settlement.kalshi_markets import parse_market

KALSHI_BASE = os.environ.get("KALSHI_MD_BASE", "https://external-api.kalshi.com/trade-api/v2")
ASSET_SERIES = {v: k for k, v in SERIES_ASSET.items()}


def _cents_from_dollars(v, name):
    if v is None or v == "":
        return None
    return price(v, name) * 100.0


def _level(entry, dollars):
    if not isinstance(entry, (list, tuple)) or len(entry) < 2:
        raise NormalizationError(f"bad book level {entry!r}")
    p = price(entry[0], "level price") * (100.0 if dollars else 1.0)
    return [round(p, 4), size(entry[1], "level qty")]


class KalshiAdapter(SourceAdapter):
    source = "kalshi"
    stream = "rest"
    transport = "rest"
    critical = True
    documentation = "Kalshi public REST (markets, orderbook, trades); read-only polling"

    def __init__(self, assets):
        self.assets = [a for a in assets if a in ASSET_SERIES]

    # ---------- URLs (GET only) ----------
    def markets_url(self, asset):
        return f"{KALSHI_BASE}/markets", {"series_ticker": ASSET_SERIES[asset], "status": "open", "limit": 100}

    def orderbook_url(self, ticker, depth=10):
        return f"{KALSHI_BASE}/markets/{ticker}/orderbook", {"depth": depth}

    def trades_url(self, ticker, limit=100):
        return f"{KALSHI_BASE}/markets/trades", {"ticker": ticker, "limit": limit}

    def market_url(self, ticker):
        return f"{KALSHI_BASE}/markets/{ticker}", None

    # ---------- parsers ----------
    def parse_markets(self, asset, body, ctx, now_ms=None):
        """Open markets of one series -> MARKET_STATE for the market closing soonest in the future."""
        res = new_result()
        ms = body.get("markets") if isinstance(body, dict) else None
        if not isinstance(ms, list):
            self.fail(res, ctx, "markets response without 'markets' list", body)
            return res, None
        best = None
        for obj in ms:
            m, _r, issues = parse_market(obj)
            if m is None or m.asset != asset:
                continue
            if now_ms is not None and m.close_ts_ms <= now_ms:
                continue
            if best is None or m.close_ts_ms < best[0].close_ts_ms:
                best = (m, obj)
        if best is None:
            return res, None
        self.guarded(res, ctx, best[1], lambda: res.events.append(self._state(best[0], best[1], ctx)))
        return res, best[0]

    def _state(self, m, obj, ctx):
        yb = _cents_from_dollars(obj.get("yes_bid_dollars"), "yes_bid")
        ya = _cents_from_dollars(obj.get("yes_ask_dollars"), "yes_ask")
        nb = _cents_from_dollars(obj.get("no_bid_dollars"), "no_bid")
        na = _cents_from_dollars(obj.get("no_ask_dollars"), "no_ask")
        flags = [EventFlag.EVENT_TIME_MISSING.value]
        if ya is None and nb is not None:
            ya = 100.0 - nb
            flags.append(EventFlag.DERIVED_ASK.value)
        if na is None and yb is not None:
            na = 100.0 - yb
            flags.append(EventFlag.DERIVED_ASK.value)
        return self.event(ctx, asset=m.asset, event_type=EventType.MARKET_STATE, symbol=m.ticker, event_ts_ms=None,
                          flags=tuple(sorted(set(flags))),
                          payload={"ticker": m.ticker, "strike": m.strike, "strike_source": m.strike_source,
                                   "close_ts_ms": m.close_ts_ms, "open_ts_ms": m.open_ts_ms, "yes_bid": yb,
                                   "yes_ask": ya, "no_bid": nb, "no_ask": na, "status": obj.get("status")})

    def parse_orderbook(self, asset, ticker, body, ctx):
        res = new_result()
        ob = body.get("orderbook") if isinstance(body, dict) else None
        if not isinstance(ob, dict):
            self.fail(res, ctx, "orderbook response without 'orderbook'", body)
            return res
        if "yes" in ob or "no" in ob:
            yes, no, dollars = ob.get("yes") or [], ob.get("no") or [], False
        elif "yes_dollars" in ob or "no_dollars" in ob:
            yes, no, dollars = ob.get("yes_dollars") or [], ob.get("no_dollars") or [], True
        else:
            self.fail(res, ctx, f"unrecognised orderbook schema {sorted(ob)}", body)
            return res

        def build():
            bids = sorted((_level(e, dollars) for e in yes), key=lambda x: -x[0])
            asks = sorted(([round(100.0 - p, 4), q] for p, q in (_level(e, dollars) for e in no)), key=lambda x: x[0])
            res.events.append(self.event(ctx, asset=asset, event_type=EventType.BOOK, symbol=ticker, event_ts_ms=None,
                                         flags=(EventFlag.EVENT_TIME_MISSING.value, EventFlag.DERIVED_ASK.value),
                                         payload={"bids": bids, "asks": asks, "depth_levels": max(len(bids), len(asks))}))
        self.guarded(res, ctx, body, build)
        return res

    def parse_trades(self, asset, body, ctx, backfill=False):
        res = new_result()
        ts = body.get("trades") if isinstance(body, dict) else None
        if not isinstance(ts, list):
            self.fail(res, ctx, "trades response without 'trades' list", body)
            return res
        for t in sorted((x for x in ts if isinstance(x, dict)), key=lambda x: str(x.get("created_time"))):
            self.guarded(res, ctx, t, lambda t=t: res.events.append(self._trade(asset, t, ctx, backfill)))
        return res

    def _trade(self, asset, t, ctx, backfill):
        if t.get("yes_price_dollars") not in (None, ""):
            p = price(t["yes_price_dollars"], "yes_price_dollars") * 100.0
        else:
            p = price(t["yes_price"], "yes_price")
        n = size(t["count_fp"] if t.get("count_fp") not in (None, "") else t["count"], "count")
        side = t.get("taker_side")
        aggressor = {"yes": "buy", "no": "sell"}.get(side)
        created = t["created_time"]
        ev_ts = iso_ms(created) if isinstance(created, str) else epoch_ms(created)
        return self.event(ctx, asset=asset, event_type=EventType.TRADE, symbol=t["ticker"], event_ts_ms=ev_ts,
                          mode=IngestMode.BACKFILLED if backfill else IngestMode.LIVE,
                          flags=() if aggressor else (EventFlag.AGGRESSOR_UNAVAILABLE.value,),
                          payload={"price": round(p, 4), "size": n, "aggressor": aggressor,
                                   "aggressor_semantics": (AggressorSemantics.TAKER_SIDE_FIELD.value if aggressor
                                                           else AggressorSemantics.UNAVAILABLE.value),
                                   "trade_id": str(t["trade_id"])})

    def parse_settled(self, body, ctx):
        """GET /markets/{ticker} after close -> RESOLUTION (only when Kalshi reports yes/no)."""
        res = new_result()
        m, r, issues = parse_market(body)
        if m is None:
            self.fail(res, ctx, "; ".join(i.detail for i in issues) or "unparseable market", body)
            return res, None, None
        if r is not None and r.result in ("yes", "no"):
            res.events.append(self.event(ctx, asset=m.asset, event_type=EventType.RESOLUTION, symbol=m.ticker,
                                         event_ts_ms=None, flags=(EventFlag.EVENT_TIME_MISSING.value,),
                                         payload={"ticker": m.ticker, "result": r.result,
                                                  "expiration_value": r.expiration_value}))
        return res, m, r
