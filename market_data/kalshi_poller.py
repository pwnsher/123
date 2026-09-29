"""
Kalshi read-only polling cycle (used by PollRunner). GET requests only; no order endpoint exists here.

Each cycle, per asset: the open-markets list (current market state), the current market's order book,
and (every `trades_every` cycles) its recent trades. After a market closes, its settled result and
expiration_value are fetched once (from close + settle_grace_s, retried up to settle_giveup_s).
Trades returned by the first poll after (re)start are BACKFILLED (they happened before we watched).
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
                 settle_retry_s=30, event_retry_s=15, event_retry_max_s=300):
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
                url, params = self.adapter.trades_url(m.ticker)
                body, wall, mono = self._get(url, params)
                backfill = m.ticker not in self.first_trades
                self.first_trades.add(m.ticker)
                e, f, g = self.collector.on_rest("kalshi", "trades", json.dumps(body),
                                                 lambda ctx, b=body, a=asset, bf=backfill: self.adapter.parse_trades(a, b, ctx, bf),
                                                 wall, mono)
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
