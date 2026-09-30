"""
Kalshi read-only polling cycle (used by PollRunner). GET requests only; no order endpoint exists here.

Each cycle, per asset: the open-markets list (current market state), the current market's order book,
and (every `trades_every` cycles) its recent trades. After a market closes, its settled result and
expiration_value are fetched once (from close + settle_grace_s, retried up to settle_giveup_s).
Trades returned by the first poll after (re)start are BACKFILLED (they happened before we watched).
Step 6.4: trade polling is INCREMENTAL. The first poll of a market is one page of the newest `first_trades_limit` trades
(BACKFILLED). Later polls (including the BACKFILLED first poll after a reconnect, once a trade was seen) ask for every trade since the newest one seen, with a 1-second overlap at the boundary
(`min_ts` is in whole seconds, so trades sharing that timestamp are always re-fetched and never missed) and page
through `cursor` (up to `trade_page_limit` rows per page, at most `trade_max_pages` pages; a truncated walk is
recorded as a gap). Every raw page is stored; the adapter normalizes each trade_id once (earliest observation wins).
Step 6.2: each market's event object is fetched ONCE (GET /events/{event_ticker}) for fee metadata; its raw text is
stored and later market snapshots carry it.
Step 6.3: an event ticker becomes `events_fetched` only AFTER a successful response whose event object was parsed and
retained for THAT ticker. A transport error, a malformed body or a different event keeps it retryable, with capped
exponential backoff (event_retry_s doubling up to event_retry_max_s). Retries happen only while that event's market
is the current one, so they end naturally when the series rolls to the next market.
"""
import json


class KalshiPoller:
    def __init__(self, adapter, getter, collector, clock, trades_every=2, settle_grace_s=90, settle_giveup_s=1800,
                 settle_retry_s=30, event_retry_s=15, event_retry_max_s=300, first_trades_limit=100,
                 trade_page_limit=1000, trade_max_pages=10, trade_overlap_s=1):
        self.adapter, self.getter, self.collector, self.clock = adapter, getter, collector, clock
        self.trades_every = trades_every
        self.settle_grace_s, self.settle_giveup_s, self.settle_retry_s = settle_grace_s, settle_giveup_s, settle_retry_s
        self.cycle = 0
        self.current = {}                    # asset -> SettlementMarket
        self.pending_settle = {}             # ticker -> (asset, close_ms, last_try_ms)
        self.first_trades = set()
        self.events_fetched = set()          # event tickers whose event object (fee metadata) was captured (6.2)
        self.event_retry_s, self.event_retry_max_s = event_retry_s, event_retry_max_s
        self.event_attempts = {}             # event ticker -> (failed attempts, earliest next attempt ms) (6.3)
        self.first_trades_limit, self.trade_page_limit = first_trades_limit, trade_page_limit
        self.trade_max_pages, self.trade_overlap_s = trade_max_pages, trade_overlap_s
        self.trade_pages = 0

    def _get(self, url, params):
        body = self.getter.get_json(url, params)
        return body, self.clock.wall_ms(), self.clock.mono_ns()   # receive time = when the response arrived

    def _event_due(self, et):
        next_ms = self.event_attempts.get(et, (0, None))[1]
        return next_ms is None or self.clock.wall_ms() >= next_ms

    def _fetch_event(self, et):
        """One attempt. Marks et fetched only when its event object was parsed AND retained; else schedules a retry."""
        url, params = self.adapter.event_url(et)
        counts = (0, 0, 0)
        try:
            body, wall, mono = self._get(url, params)
        except Exception as exc:                             # noqa: BLE001 - metadata only; the market data goes on
            self.collector.on_poll_error("kalshi_event", self.clock.wall_ms(), exc)
        else:
            counts = self.collector.on_rest("kalshi", "event", json.dumps(body),
                                            lambda ctx, b=body: self.adapter.parse_event(b, ctx), wall, mono)
        if isinstance(getattr(self.adapter, "events", {}).get(et), dict):
            self.events_fetched.add(et)
            self.event_attempts.pop(et, None)
        else:
            tries = self.event_attempts.get(et, (0, None))[0] + 1
            delay_s = min(self.event_retry_s * 2 ** (tries - 1), self.event_retry_max_s)
            self.event_attempts[et] = (tries, self.clock.wall_ms() + int(delay_s * 1000))
        return counts

    def _poll_trades(self, asset, ticker):
        """One incremental trade poll of a market (see the module docstring). -> (events, failures, gaps)."""
        backfill = ticker not in self.first_trades
        newest = getattr(self.adapter, "trade_newest_ms", {}).get(ticker)
        if newest is None:                                   # nothing seen yet for this market: one first page
            limit, min_ts = self.first_trades_limit, None
        else:
            limit, min_ts = self.trade_page_limit, max(0, newest // 1000 - self.trade_overlap_s)
        n_ev = n_f = n_g = 0
        cursor, pages = None, 0
        while True:
            url, params = self.adapter.trades_url(ticker, limit, min_ts, cursor) if min_ts is not None or cursor \
                else self.adapter.trades_url(ticker, limit)
            body, wall, mono = self._get(url, params)
            pages += 1
            self.trade_pages += 1
            e, f, g = self.collector.on_rest("kalshi", "trades", json.dumps(body),
                                             lambda ctx, b=body, a=asset, bf=backfill: self.adapter.parse_trades(a, b, ctx, bf),
                                             wall, mono)
            n_ev, n_f, n_g = n_ev + e, n_f + f, n_g + g
            rows = body.get("trades") if isinstance(body, dict) else None
            cursor = body.get("cursor") if isinstance(body, dict) else None
            if min_ts is None or not cursor or not isinstance(rows, list) or len(rows) < limit:
                break
            if pages >= self.trade_max_pages:
                from market_data.gaps import Gap
                self.collector._gap(Gap("kalshi", asset, "trades", "TRADE_PAGES_TRUNCATED", min_ts * 1000, wall,
                                        max(0, wall - min_ts * 1000), None, True,
                                        f"{ticker}: more than {self.trade_max_pages} pages of {limit} trades since "
                                        f"min_ts {min_ts}", known_at_ms=wall))
                n_g += 1
                break
        self.first_trades.add(ticker)
        return n_ev, n_f, n_g

    def poll(self, reconnect=False):
        self.cycle += 1
        if reconnect:
            self.first_trades.clear()
        n_ev = n_f = n_g = 0
        for asset in self.adapter.assets:
            url, params = self.adapter.markets_url(asset)
            body, wall, mono = self._get(url, params)
            holder = {}

            def parse_markets(ctx, body=body, asset=asset, wall=wall):
                res, m = self.adapter.parse_markets(asset, body, ctx, now_ms=wall)
                holder["m"] = m
                return res
            e, f, g = self.collector.on_rest("kalshi", "markets", json.dumps(body), parse_markets, wall, mono)
            n_ev, n_f, n_g = n_ev + e, n_f + f, n_g + g
            m = holder.get("m")
            prev = self.current.get(asset)
            if prev is not None and (m is None or m.ticker != prev.ticker):
                self.pending_settle.setdefault(prev.ticker, (asset, prev.close_ts_ms, None))
                if hasattr(self.adapter, "forget_trades"):
                    self.adapter.forget_trades(prev.ticker)      # no longer polled: bounded dedup state
            if m is None:
                continue
            self.current[asset] = m
            et = getattr(self.adapter, "event_of", {}).get(m.ticker)
            if et and et not in self.events_fetched and self._event_due(et):
                e_, f, g = self._fetch_event(et)             # GET, read-only (fee metadata, 6.2)
                n_ev, n_f, n_g = n_ev + e_, n_f + f, n_g + g
            url, params = self.adapter.orderbook_url(m.ticker)
            body, wall, mono = self._get(url, params)
            e, f, g = self.collector.on_rest("kalshi", "orderbook", json.dumps(body),
                                             lambda ctx, b=body, a=asset, t=m.ticker: self.adapter.parse_orderbook(a, t, b, ctx),
                                             wall, mono)
            n_ev, n_f, n_g = n_ev + e, n_f + f, n_g + g
            if self.cycle % self.trades_every == 1 or self.trades_every == 1:
                e, f, g = self._poll_trades(asset, m.ticker)
                n_ev, n_f, n_g = n_ev + e, n_f + f, n_g + g
        now = self.clock.wall_ms()
        for ticker, (asset, close_ms, last_try) in list(self.pending_settle.items()):
            if now < close_ms + self.settle_grace_s * 1000:
                continue
            if now > close_ms + self.settle_giveup_s * 1000:
                self.pending_settle.pop(ticker)
                continue
            if last_try is not None and now - last_try < self.settle_retry_s * 1000:
                continue
            url, params = self.adapter.market_url(ticker)
            body, wall, mono = self._get(url, params)
            holder = {}

            def parse_settled(ctx, body=body):
                res, _m, r = self.adapter.parse_settled(body, ctx)
                holder["r"] = r
                return res
            e, f, g = self.collector.on_rest("kalshi", "settled", json.dumps(body), parse_settled, wall, mono)
            n_ev, n_f, n_g = n_ev + e, n_f + f, n_g + g
            r = holder.get("r")
            if r is not None and r.result in ("yes", "no"):
                self.pending_settle.pop(ticker)
            else:
                self.pending_settle[ticker] = (asset, close_ms, now)
        self.collector.tick()
        return n_ev, n_f, n_g
