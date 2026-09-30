"""
Kalshi 15-minute markets — READ-ONLY public REST polling. No authentication, no order endpoints.

Transport  GET {KALSHI_BASE}/markets?series_ticker=<S>&status=open    (current market per series)
           GET {KALSHI_BASE}/markets/{ticker}/orderbook                 (depth; public)
           GET {KALSHI_BASE}/markets/trades?ticker=<T>&limit=<n>        (recent trades; public)
           GET {KALSHI_BASE}/markets/{ticker}                           (after close: result + expiration_value)
KALSHI_BASE defaults to the production dashboard's public base (env KALSHI_MD_BASE overrides).

Events
    MARKET_STATE  ticker, strike (floor_strike|cap_strike|strike, via settlement.kalshi_markets), close /
                  open time, YES/NO bid/ask in CENTS (from *_dollars fields), status. The market object carries no
                  update timestamp -> event_ts None + EVENT_TIME_MISSING; receive time is the poll response.
                  Step 6.4 (observed in the first real capture): a side quoted "0.0000" with size "0.00" (or no size
                  field) means NO resting order -> None + NO_QUOTE, never a zero price and never a parse failure of
                  the whole observation; the venue's "1.0000" ask that mirrors such a missing opposite bid is
                  likewise None. A missing ask is derived as 100 - opposite bid (DERIVED_ASK) ONLY when that bid is
                  genuinely available. A zero price with a non-zero size is contradictory -> parse failure.
    BOOK          YES terms: bids = YES bids; asks = YES asks derived from NO bids (100 - p), flagged DERIVED_ASK.
                  CURRENT schema (Kalshi REST, observed in the first real capture):
                      {"orderbook_fp": {"yes_dollars": [["0.1500", "100.00"], ...], "no_dollars": [...]}}
                  prices are dollar strings, quantities fixed-point contract counts (fractional counts kept); both
                  are decoded with exact Decimal arithmetic; an empty side is an empty ladder. The earlier
                  {"orderbook": {"yes" | "yes_dollars": ...}} shape is kept only for historical captures and flagged
                  LEGACY_SCHEMA. Anything else is a parse failure (never guessed).
    TRADE         price in YES cents, size = contracts, event time = created_time. `taker_side` is the
                  aggressor: "yes" -> "buy" (bought YES), "no" -> "sell" (in YES terms). Trades returned
                  by the first poll after start/reconnect happened before we were watching -> BACKFILLED.
                  Step 6.4 (the first real capture stored each asset's 1,700 trade rows with only 625-1,319 unique
                  ids): each normalized trade_id is emitted ONCE per market (the earliest observation wins); later
                  polls are incremental (min_ts with a 1-s overlap + cursor pages, see KalshiPoller), and every raw
                  page is still stored for provenance.
    RESOLUTION    result + expiration_value of a settled market (a LABEL; never a feature).
Contract metadata (Step 6.2, KALSHI_METADATA_VERSION 2): MARKET_STATE and RESOLUTION payloads also carry "contract",
the market's ORIGINAL rules_primary / rules_secondary text, its hash, series / event tickers, capture time, the market's
update time when present, schema fingerprint and any fee metadata (fee_type / fee_multiplier / *_override) from the
market object or its event (GET /events/{event_ticker}, fetched once per event). Research provenance only: no feature
reads it.
"""
import os
from decimal import Decimal, InvalidOperation

from market_data.normalization import NormalizationError, epoch_ms, iso_ms, price, size
from market_data.sources.base import SourceAdapter, new_result
from market_data.types import AggressorSemantics, EventFlag, EventType, IngestMode
from settlement.assets import SERIES_ASSET
from settlement.kalshi_markets import parse_market
from settlement.market_rules import contract_snapshot

KALSHI_BASE = os.environ.get("KALSHI_MD_BASE", "https://external-api.kalshi.com/trade-api/v2")
KALSHI_METADATA_VERSION = 2                    # 2 (Step 6.2): contract rule text + fee metadata retained
ASSET_SERIES = {v: k for k, v in SERIES_ASSET.items()}


def _dec(v, name):
    if isinstance(v, bool) or v is None or v == "":
        raise NormalizationError(f"{name} missing")
    try:
        d = Decimal(str(v).strip())
    except InvalidOperation:
        raise NormalizationError(f"{name} not numeric: {str(v)[:30]!r}")
    if not d.is_finite():
        raise NormalizationError(f"{name} not finite")
    return d


def quote_cents(obj, price_key, size_key, name):
    """One market-object quote side -> (cents | None, state). state: PRESENT | ABSENT (field missing) | NO_QUOTE
    ("0.0000" with a zero / missing size: no resting order). A zero price with a non-zero size fails closed."""
    v = obj.get(price_key)
    if v is None or v == "":
        return None, "ABSENT"
    d = _dec(v, name)
    if d == 0:
        sz = obj.get(size_key) if size_key else None
        if sz is None or sz == "" or _dec(sz, f"{name} size") == 0:
            return None, "NO_QUOTE"
        raise NormalizationError(f"{name} is 0 with a non-zero size {sz!r}")
    if d < 0 or d > 1:
        raise NormalizationError(f"{name} outside [0, 1] dollars: {v!r}")
    return float(d * 100), "PRESENT"                                   # exact decimal cents (no float artifacts)


def _fp_level(entry, name):
    """orderbook_fp level ["0.1500", "100.00"] -> (price Decimal dollars, qty Decimal contracts)."""
    if not isinstance(entry, (list, tuple)) or len(entry) < 2:
        raise NormalizationError(f"bad {name} level {entry!r}")
    px, q = _dec(entry[0], f"{name} level price"), _dec(entry[1], f"{name} level qty")
    if not 0 < px <= 1:
        raise NormalizationError(f"{name} level price outside (0, 1] dollars: {entry[0]!r}")
    if q < 0:
        raise NormalizationError(f"{name} level qty negative: {entry[1]!r}")
    return px, q


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
        self.events = {}                       # event_ticker -> event object (fee metadata), captured once
        self.event_of = {}                     # market ticker -> event_ticker
        self.trade_seen = {}                   # market ticker -> {trade_id} already normalized (Step 6.4)
        self.trade_newest_ms = {}              # market ticker -> newest created_time seen (ms)
        self.trade_overlap_skipped = 0         # re-fetched trade rows not normalized again (overlap)

    def forget_trades(self, ticker):
        """Drop the dedup state of a market no longer polled (bounded memory)."""
        self.trade_seen.pop(ticker, None)
        self.trade_newest_ms.pop(ticker, None)

    # ---------- URLs (GET only) ----------
    def markets_url(self, asset):
        return f"{KALSHI_BASE}/markets", {"series_ticker": ASSET_SERIES[asset], "status": "open", "limit": 100}

    def orderbook_url(self, ticker, depth=10):
        return f"{KALSHI_BASE}/markets/{ticker}/orderbook", {"depth": depth}

    def trades_url(self, ticker, limit=100, min_ts=None, cursor=None):
        """GET /markets/trades: ticker, limit (<= 1000), optional min_ts (Unix SECONDS) and cursor (next page)."""
        params = {"ticker": ticker, "limit": limit}
        if min_ts is not None:
            params["min_ts"] = int(min_ts)
        if cursor:
            params["cursor"] = cursor
        return f"{KALSHI_BASE}/markets/trades", params

    def market_url(self, ticker):
        return f"{KALSHI_BASE}/markets/{ticker}", None

    def event_url(self, event_ticker):
        return f"{KALSHI_BASE}/events/{event_ticker}", None

    def parse_event(self, body, ctx):
        """GET /events/{event_ticker} -> retained event object (fee metadata for the contract snapshot). No events."""
        res = new_result()
        ev = body.get("event") if isinstance(body, dict) and isinstance(body.get("event"), dict) else body
        if not isinstance(ev, dict) or not isinstance(ev.get("event_ticker"), str):
            self.fail(res, ctx, "event response without an event object", body)
            return res
        self.events[ev["event_ticker"]] = ev
        return res

    def _contract(self, obj, m, ctx):
        et = obj.get("event_ticker") if isinstance(obj.get("event_ticker"), str) else None
        if et:
            self.event_of[m.ticker] = et
        return contract_snapshot(obj, source="kalshi_market_api", capture_ts_ms=ctx.receive_ts_ms,
                                 schema_fingerprint=m.metadata_schema_fingerprint, event_obj=self.events.get(et))

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
        yb, ybs = quote_cents(obj, "yes_bid_dollars", "yes_bid_size_fp", "yes_bid")
        ya, yas = quote_cents(obj, "yes_ask_dollars", "yes_ask_size_fp", "yes_ask")
        nb, nbs = quote_cents(obj, "no_bid_dollars", "no_bid_size_fp", "no_bid")
        na, nas = quote_cents(obj, "no_ask_dollars", "no_ask_size_fp", "no_ask")
        flags = [EventFlag.EVENT_TIME_MISSING.value]
        # an ask at the full $1.00 payout that mirrors a MISSING opposite bid is not a resting offer either
        if ybs == "NO_QUOTE" and na == 100.0:
            na, nas = None, "NO_QUOTE"
        if nbs == "NO_QUOTE" and ya == 100.0:
            ya, yas = None, "NO_QUOTE"
        if "NO_QUOTE" in (ybs, yas, nbs, nas):
            flags.append(EventFlag.NO_QUOTE.value)
        if ya is None and yas == "ABSENT" and nb is not None:          # derive only from a genuinely available bid
            ya = 100.0 - nb
            flags.append(EventFlag.DERIVED_ASK.value)
        if na is None and nas == "ABSENT" and yb is not None:
            na = 100.0 - yb
            flags.append(EventFlag.DERIVED_ASK.value)
        return self.event(ctx, asset=m.asset, event_type=EventType.MARKET_STATE, symbol=m.ticker, event_ts_ms=None,
                          flags=tuple(sorted(set(flags))),
                          payload={"ticker": m.ticker, "strike": m.strike, "strike_source": m.strike_source,
                                   "close_ts_ms": m.close_ts_ms, "open_ts_ms": m.open_ts_ms, "yes_bid": yb,
                                   "yes_ask": ya, "no_bid": nb, "no_ask": na, "status": obj.get("status"),
                                   "contract": self._contract(obj, m, ctx)})

    def parse_orderbook(self, asset, ticker, body, ctx):
        res = new_result()
        if isinstance(body, dict) and "orderbook_fp" in body:
            return self._orderbook_fp(asset, ticker, body, ctx, res)
        ob = body.get("orderbook") if isinstance(body, dict) else None
        if not isinstance(ob, dict):
            self.fail(res, ctx, "orderbook response without 'orderbook_fp' (or the legacy 'orderbook')", body)
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
                                         flags=(EventFlag.EVENT_TIME_MISSING.value, EventFlag.DERIVED_ASK.value,
                                                EventFlag.LEGACY_SCHEMA.value),
                                         payload={"bids": bids, "asks": asks, "depth_levels": max(len(bids), len(asks))}))
        self.guarded(res, ctx, body, build)
        return res

    def _orderbook_fp(self, asset, ticker, body, ctx, res):
        """CURRENT schema {"orderbook_fp": {"yes_dollars": [[price, count_fp]], "no_dollars": [...]}} (exact)."""
        ob = body["orderbook_fp"]
        if not isinstance(ob, dict) or not ({"yes_dollars", "no_dollars"} & set(ob)):
            self.fail(res, ctx, f"unrecognised orderbook_fp shape {sorted(ob) if isinstance(ob, dict) else type(ob).__name__}",
                      body)
            return res
        yes, no = ob.get("yes_dollars"), ob.get("no_dollars")
        if not isinstance(yes if yes is not None else [], list) or not isinstance(no if no is not None else [], list):
            self.fail(res, ctx, "orderbook_fp sides must be lists", body)
            return res

        def build():
            bids = [[float(px * 100), float(q)] for px, q in (_fp_level(e, "yes_dollars") for e in yes or []) if q > 0]
            asks = [[float((1 - px) * 100), float(q)] for px, q in (_fp_level(e, "no_dollars") for e in no or []) if q > 0]
            bids.sort(key=lambda x: -x[0])
            asks.sort(key=lambda x: x[0])
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
            tid, tk = t.get("trade_id"), t.get("ticker")
            seen = self.trade_seen.setdefault(tk, set()) if tid is not None else None
            if seen is not None and str(tid) in seen:
                self.trade_overlap_skipped += 1          # already normalized: the earliest observation wins
                continue
            n0 = len(res.events)
            self.guarded(res, ctx, t, lambda t=t: res.events.append(self._trade(asset, t, ctx, backfill)))
            if len(res.events) > n0:
                if seen is not None:
                    seen.add(str(tid))
                ev = res.events[-1]
                if ev.event_ts_ms is not None and tk is not None:
                    self.trade_newest_ms[tk] = max(self.trade_newest_ms.get(tk, ev.event_ts_ms), ev.event_ts_ms)
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
                                                  "expiration_value": r.expiration_value,
                                                  "contract": self._contract(body.get("market", body)
                                                                             if isinstance(body, dict) else {}, m, ctx)}))
        return res, m, r
