"""
The FROZEN legacy production probability as a research benchmark, computed by the UNMODIFIED production code.

LegacyEvaluator runs kalshi_dashboard.evaluate() (byte-identical, fingerprinted) inside the Step-1 regression
harness sandbox (regression.harness.legacy_sandbox: no network, temp files, silenced output, everything restored),
with the clock set to the checkpoint T and the data functions fed from CAPTURED data available at T:

    spot(product)       last Coinbase trade price RECEIVED <= T (production reads the ticker's last-trade price)
    candles(product)    1-minute OHLC built from Coinbase trades received <= T, oldest first, the current (partial)
                        minute last - exactly the shape production gets from the candles endpoint, which evaluate()
                        itself trims (it drops the partial minute and uses the newest 61 completed closes)
    current_market(s)   the Kalshi market of the row (strike, close time) with the Kalshi quote as of T

Nothing is re-implemented: the probability is whatever the frozen evaluate() returns as p_up. Known approximation
(documented): Coinbase builds its candles from all its trades; the research candles use the trades captured by the
Step-3 feed (same venue, same trades, subject to capture gaps - a capture gap makes a candle missing, as it would be
if Coinbase had no trade in that minute). If evaluate() does not return a probability (stale data, the pinned
missing-ask defect) the legacy probability is None (LEGACY_UNAVAILABLE), never guessed.
"""
import bisect
import contextlib
import datetime as _dt
import types

from market_data.types import EventType, MarketEvent

CANDLE_LIMIT = 300


class _Clock:
    def __init__(self):
        self.now_ms = 0


def _dt_shim(clock):
    class _DT(_dt.datetime):
        @classmethod
        def now(cls, tz=None):
            base = cls.fromtimestamp(clock.now_ms / 1000.0, _dt.timezone.utc)
            return base if tz is not None else base.replace(tzinfo=None)
    return types.SimpleNamespace(datetime=_DT, timezone=_dt.timezone, timedelta=_dt.timedelta)


def _iso(ms):
    return _dt.datetime.fromtimestamp(ms / 1000.0, _dt.timezone.utc).isoformat().replace("+00:00", "Z")


class CoinbaseTape:
    """Coinbase trades of one asset in RECEIVE order; causal as-of queries at T."""

    def __init__(self):
        self.rx, self.ev, self.px = [], [], []

    def add(self, e):
        self.rx.append(e.receive_ts_ms)
        self.ev.append(e.event_ts_ms if e.event_ts_ms is not None else e.receive_ts_ms)
        self.px.append(float(e.payload["price"]))

    def upto(self, t):
        return bisect.bisect_right(self.rx, t)

    def spot(self, t):
        i = self.upto(t)
        return self.px[i - 1] if i else None

    def candles(self, t):
        """1-min OHLC from trades received <= t (bucketed by the trade's event minute)."""
        n = self.upto(t)
        buckets = {}
        for i in range(n):
            m = min(self.ev[i], t) // 60_000
            b = buckets.get(m)
            p = self.px[i]
            if b is None:
                buckets[m] = [p, p, p, p, self.ev[i]]
            else:
                b[1] = max(b[1], p)
                b[2] = min(b[2], p)
                if self.ev[i] >= b[4]:
                    b[3], b[4] = p, self.ev[i]
        keys = sorted(buckets)[-CANDLE_LIMIT:]
        return {"close": [buckets[k][3] for k in keys], "high": [buckets[k][1] for k in keys],
                "low": [buckets[k][2] for k in keys]}


def tapes_from_events(step3_events, assets):
    tapes = {a: CoinbaseTape() for a in assets}
    for e in sorted((e for e in step3_events if isinstance(e, MarketEvent) and e.source == "coinbase"
                     and e.event_type == EventType.TRADE and e.asset in tapes), key=lambda e: (e.receive_ts_ms, e.ingest_seq)):
        tapes[e.asset].add(e)
    return tapes


class LegacyEvaluator:
    """with LegacyEvaluator() as le: le.p_up(asset, T, ticker, strike, close_ts_ms, quote, tape) -> (p_up | None, info)"""

    def __enter__(self):
        import kalshi_dashboard as k
        from regression import harness
        self.k = k
        self.clock = _Clock()
        clock = self.clock
        time_shim = types.SimpleNamespace(time=lambda: clock.now_ms / 1000.0, sleep=lambda s: None)
        self._stack = contextlib.ExitStack()
        self._stack.enter_context(harness.legacy_sandbox({"dt": _dt_shim(clock), "time": time_shim}))
        self.patched = harness.patched
        self.calls = 0
        return self

    def __exit__(self, *exc):
        self._stack.close()
        return False

    def p_up(self, asset, t_ms, ticker, strike, close_ts_ms, quote, tape):
        k = self.k
        cfg = k.COINS[asset]
        self.clock.now_ms = t_ms
        px = tape.spot(t_ms) if tape is not None else None
        cd = tape.candles(t_ms) if tape is not None else {"close": [], "high": [], "low": []}
        q = quote or {}
        m = {"ticker": ticker, "close_time": _iso(close_ts_ms), "floor_strike": strike}
        for key, cents in (("yes_bid_dollars", q.get("yes_bid")), ("yes_ask_dollars", q.get("yes_ask")),
                           ("no_bid_dollars", q.get("no_bid")), ("no_ask_dollars", q.get("no_ask"))):
            if cents is not None:
                m[key] = f"{cents / 100.0:.4f}"
        self.calls += 1
        with self.patched(k, current_market=lambda series: dict(m), spot=lambda product: px,
                          candles=lambda product: {kk: list(v) for kk, v in cd.items()}):
            try:
                r = k.evaluate(asset, cfg)
            except Exception as e:                          # noqa: BLE001 - the pinned legacy defects fail closed
                return None, {"legacy_status": f"RAISES:{type(e).__name__}"}
        if not isinstance(r, dict) or r.get("status") != "ok" or r.get("p_up") is None:
            return None, {"legacy_status": (r or {}).get("status") if isinstance(r, dict) else "invalid"}
        return float(r["p_up"]) / 100.0, {"legacy_status": "ok", "legacy_reason": r.get("reason"),
                                           "legacy_conf": r.get("conf"), "legacy_fav": r.get("fav")}
