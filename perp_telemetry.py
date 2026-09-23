#!/usr/bin/env python3
"""
perp_telemetry.py — PASSIVE, READ-ONLY Kalshi perpetual-futures telemetry.

Step 1 of the perp research project. Think "flight data recorder":
it watches, it records, it controls NOTHING.

  * It never places, amends, or cancels any order (perp or binary).
  * It never touches margin, leverage, transfers, or account settings.
  * Nothing it computes is read by evaluate(), signals, confidence, edge,
    sizing, stops, calls, Discord posts, or the backtest.
  * Any failure (network, schema, missing data) degrades to a status field
    ('unavailable' / 'error' / 'stale'); it never raises into the watcher.

Data source (verified against docs.kalshi.com, perps_openapi.yaml):
  GET {base}/margin/markets                         public, no auth
      -> bid, ask, price (last), settlement_mark_price{price,ts_ms},
         reference_price{price,ts_ms} (CF Benchmarks index, SCALED PER
         CONTRACT), contract_size, underlying_multiplier, status
  GET {base}/margin/funding_rates/estimate?ticker=  public, no auth
      -> funding_rate (estimate for the in-progress period), mark_price,
         computed_time, next_funding_time

One /margin/markets call covers every coin, so a poll cycle costs ONE request
(plus a per-ticker funding estimate, refreshed only every
funding_refresh_seconds because it is a slowly-moving period average).

Step 2 (schema v2, feature_version step2_v1) — causal alignment:
  A PerpSampler thread polls continuously into a bounded SnapshotStore. Each binary
  row selects the newest snapshot whose available_ts <= the row's spot receive time
  (feature_end_ts_epoch_ms), and only if it is at most max_lag_s old. Every Step 2
  feature is computed from data at or before that time. "Causal" in field names means
  "uses no future data" — it is NOT a claim that perp moves cause or lead spot moves.

Schema v3 (feature_version step2_v2) — volatility-regime RESEARCH telemetry:
  Adds volatility-normalized perp/spot momentum and perp-vs-spot gap, a trailing perp
  spread baseline, a premium stress magnitude, an observational volatility regime and a
  rule-based stability state (see "STEP 2 v2" below). They are telemetry only: nothing in
  the bot reads them. They can influence a decision ONLY through the unchanged
  Step 3 -> Step 4 -> Step 5 -> manual promotion -> LIVE_VETO_ONLY chain.
  Only fields the two documented endpoints above actually return are used. The API
  supplies no open interest, liquidation flow, trade aggressor side or book depth, so
  nothing here is (or may be) inferred about them.

Price units — read before analysing the CSV:
  perp_bid/ask/mid/last/mark and index_price are Kalshi PER-CONTRACT dollars.
  spot_price is Coinbase USD per whole coin. They are NOT directly comparable
  in level. premium_bps therefore compares perp_mid to Kalshi's own
  reference_price (same units). Returns are ratios, so perp and spot returns
  ARE comparable.
"""

from __future__ import annotations

import bisect
import csv
import dataclasses
import math
import os
import queue
import threading
import time
import uuid
import datetime as dt
from collections import deque
from dataclasses import dataclass, asdict
from typing import Callable, Optional

import requests

import http_session

# ─────────────────────── defaults (dashboard passes its own config) ───────────────────────
DEFAULT_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
HORIZONS_S = (30, 60, 180)

STATUS_FRESH = "fresh"
STATUS_STALE = "stale"
STATUS_UNAVAILABLE = "unavailable"
STATUS_ERROR = "error"
STATUS_DISABLED = "disabled"

TELEMETRY_SCHEMA_VERSION = 3
FEATURE_VERSION = "step2_v2"

INDEX_SOURCE = "kalshi_reference_price"   # CF Benchmarks RTI, scaled per perp contract
SPOT_SOURCE = "coinbase_ticker"


# ─────────────────────── small parsing helpers ───────────────────────
def _num(v) -> Optional[float]:
    """Parse a numeric/fixed-point string. Missing/invalid/non-finite -> None.
    Never turns 'missing' into 0.0."""
    if v is None or v == "" or isinstance(v, bool):
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _price(v) -> Optional[float]:
    """A price must be strictly positive; zero/negative is treated as invalid."""
    x = _num(v)
    return x if (x is not None and x > 0) else None


def _ticker_price(obj):
    """Parse a documented TickerPrice object {price, ts_ms} -> (price, ts_ms)."""
    if not isinstance(obj, dict):
        return None, None
    p = _price(obj.get("price"))
    ts = obj.get("ts_ms")
    ts = int(ts) if isinstance(ts, (int, float)) and not isinstance(ts, bool) and ts > 0 else None
    return p, ts


# ─────────────────────── pure feature math ───────────────────────
def premium_bps(perp_mid, index_price) -> Optional[float]:
    """((perp_mid / index_price) - 1) * 10,000. None if either input is missing
    or non-positive. Never substitutes another price for the index."""
    if perp_mid is None or index_price is None or index_price <= 0 or perp_mid <= 0:
        return None
    return (perp_mid / index_price - 1.0) * 10000.0


def simple_return_bps(current, old) -> Optional[float]:
    """(current / old - 1) * 10,000; None if either is missing/non-positive."""
    if current is None or old is None or current <= 0 or old <= 0:
        return None
    return (current / old - 1.0) * 10000.0


def lead_bps(perp_ret, spot_ret) -> Optional[float]:
    if perp_ret is None or spot_ret is None:
        return None
    return perp_ret - spot_ret


# ─────────────────────── snapshot ───────────────────────
@dataclass
class PerpSnapshot:
    ts: float                         # local epoch seconds when the data was RECEIVED
    coin: str
    perp_symbol: Optional[str] = None
    perp_market_status: Optional[str] = None
    perp_bid: Optional[float] = None
    perp_ask: Optional[float] = None
    perp_mid: Optional[float] = None
    perp_last: Optional[float] = None
    perp_mark: Optional[float] = None          # settlement_mark_price (used for funding)
    perp_mark_ts_ms: Optional[int] = None
    index_price: Optional[float] = None        # reference_price (per contract)
    index_ts_ms: Optional[int] = None
    funding_rate: Optional[float] = None       # in-progress-period ESTIMATE
    funding_next_time: Optional[str] = None
    funding_computed_time: Optional[str] = None
    contract_size: Optional[float] = None
    underlying_multiplier: Optional[float] = None
    source_status: str = STATUS_UNAVAILABLE
    source_error: Optional[str] = None
    # Step 2: local time at which the COMPLETE snapshot (prices + funding) was in hand.
    # Causal selection keys on this, because funding is fetched after the markets call.
    available_ts: Optional[float] = None

    @property
    def avail(self) -> float:
        return self.available_ts if self.available_ts is not None else self.ts

    @property
    def scale(self):
        return (self.contract_size, self.underlying_multiplier)


def _mid(bid, ask):
    if bid is None or ask is None or bid <= 0 or ask <= 0 or bid > ask:
        return None          # one-sided or crossed book -> no mid (not faked)
    return (bid + ask) / 2.0


# ─────────────────────── providers ───────────────────────
class PerpProvider:
    """Boundary between telemetry and any data source. Read-only by contract."""

    def snapshots(self, coins) -> dict:
        """Return {coin: PerpSnapshot}. Implementations should not raise, but the
        recorder guards against it anyway."""
        return {c: self.snapshot(c) for c in coins}

    def snapshot(self, coin) -> PerpSnapshot:
        raise NotImplementedError


class DisabledPerpProvider(PerpProvider):
    def __init__(self, clock: Callable[[], float] = time.time):
        self.clock = clock

    def snapshot(self, coin):
        return PerpSnapshot(ts=self.clock(), coin=coin, source_status=STATUS_DISABLED)


class KalshiPerpProvider(PerpProvider):
    """Read-only client for documented, unauthenticated Kalshi perps market data.

    Only GET requests to /margin/markets and /margin/funding_rates/estimate are
    issued. There is deliberately no order/transfer/leverage code here.
    """

    def __init__(self, base_url=DEFAULT_BASE_URL, ticker_overrides=None,
                 sample_seconds=4.0, funding_refresh_seconds=60.0,
                 error_backoff_seconds=15.0, timeout=5.0,
                 http_get=None, clock: Callable[[], float] = time.time):
        self.base_url = base_url.rstrip("/")
        self.ticker_overrides = {k.upper(): v for k, v in (ticker_overrides or {}).items() if v}
        self.sample_seconds = float(sample_seconds)
        self.funding_refresh_seconds = float(funding_refresh_seconds)
        self.error_backoff_seconds = float(error_backoff_seconds)
        self.timeout = float(timeout)
        self._http_get = http_get or http_session.get      # pooled, thread-local; same request semantics
        self.clock = clock
        self._lock = threading.Lock()          # guards caches only; never held during HTTP
        self._markets = None                   # (fetched_ts, [market dicts])
        self._markets_err = None               # (err_ts, message)
        self._funding = {}                     # ticker -> (fetched_ts, dict | None, err | None)
        self.request_count = 0                 # for tests / rate accounting

    # -- HTTP (GET only) --
    def _get_json(self, path, params=None):
        self.request_count += 1
        r = self._http_get(f"{self.base_url}{path}", params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _fetch_markets(self):
        """Returns (fetched_ts, markets, error). Cached for sample_seconds; after a
        failure, backs off for error_backoff_seconds so an outage costs little."""
        now = self.clock()
        with self._lock:
            if self._markets is not None and now - self._markets[0] < self.sample_seconds:
                return self._markets[0], self._markets[1], None
            if self._markets_err is not None and now - self._markets_err[0] < self.error_backoff_seconds:
                return None, None, f"backoff: {self._markets_err[1]}"
        try:
            data = self._get_json("/margin/markets")
            markets = data.get("markets") if isinstance(data, dict) else None
            if not isinstance(markets, list):
                raise ValueError("malformed response: 'markets' list missing")
            got = self.clock()
            with self._lock:
                self._markets = (got, markets); self._markets_err = None
            return got, markets, None
        except Exception as e:                           # timeout, HTTP, JSON, schema
            msg = f"{type(e).__name__}: {e}"[:300]
            with self._lock:
                self._markets_err = (self.clock(), msg)
            return None, None, msg

    def _fetch_funding(self, ticker):
        now = self.clock()
        with self._lock:
            hit = self._funding.get(ticker)
            if hit is not None:
                age = now - hit[0]
                ttl = self.funding_refresh_seconds if hit[2] is None else self.error_backoff_seconds
                if age < ttl:
                    return hit[1], hit[2]
        try:
            data = self._get_json("/margin/funding_rates/estimate", {"ticker": ticker})
            if not isinstance(data, dict):
                raise ValueError("malformed funding response")
            with self._lock:
                self._funding[ticker] = (self.clock(), data, None)
            return data, None
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"[:200]
            with self._lock:
                self._funding[ticker] = (self.clock(), None, msg)
            return None, msg

    # -- ticker discovery (no hardcoded symbols) --
    def resolve_ticker(self, coin, markets):
        """Returns (ticker | None, reason | None). An explicit override wins;
        otherwise pick the unique market whose ticker names the coin."""
        coin = coin.upper()
        if coin in self.ticker_overrides:
            return self.ticker_overrides[coin], None
        cands = []
        for m in markets:
            t = str(m.get("ticker", "")).upper() if isinstance(m, dict) else ""
            if not t or coin not in t:
                continue
            core = t[2:] if t.startswith("KX") else t
            score = (core.startswith(coin), "PERP" in t, m.get("status") == "active")
            cands.append((score, m.get("ticker")))
        if not cands:
            return None, f"no perp market listed for {coin}"
        cands.sort(key=lambda x: x[0], reverse=True)
        top = [c for c in cands if c[0] == cands[0][0]]
        if len(top) > 1:
            return None, (f"ambiguous perp tickers for {coin}: " + ",".join(str(c[1]) for c in top)
                          + " (set PERP_TICKERS / KALSHI_PERP_TICKER_" + coin + ")")
        return top[0][1], None

    def snapshots(self, coins):
        fetched, markets, err = self._fetch_markets()
        out = {}
        for coin in coins:
            try:
                out[coin] = self._snapshot_from(coin, fetched, markets, err)
            except Exception as e:  # pragma: no cover — defensive
                out[coin] = PerpSnapshot(ts=self.clock(), coin=coin, source_status=STATUS_ERROR,
                                         source_error=f"{type(e).__name__}: {e}"[:300])
        return out

    def snapshot(self, coin):
        return self.snapshots([coin])[coin]

    def _snapshot_from(self, coin, fetched, markets, err):
        if err is not None or markets is None:
            return PerpSnapshot(ts=self.clock(), coin=coin, source_status=STATUS_ERROR,
                                source_error=err or "no data")
        ticker, why = self.resolve_ticker(coin, markets)
        if ticker is None:
            return PerpSnapshot(ts=fetched, coin=coin, source_status=STATUS_UNAVAILABLE, source_error=why)
        m = next((x for x in markets if isinstance(x, dict) and x.get("ticker") == ticker), None)
        if m is None:
            return PerpSnapshot(ts=fetched, coin=coin, perp_symbol=ticker, source_status=STATUS_UNAVAILABLE,
                                source_error=f"ticker {ticker} not in /margin/markets response")
        bid, ask = _price(m.get("bid")), _price(m.get("ask"))
        mark, mark_ts = _ticker_price(m.get("settlement_mark_price"))
        idx, idx_ts = _ticker_price(m.get("reference_price"))
        snap = PerpSnapshot(
            ts=fetched, coin=coin, perp_symbol=ticker, perp_market_status=m.get("status"),
            perp_bid=bid, perp_ask=ask, perp_mid=_mid(bid, ask), perp_last=_price(m.get("price")),
            perp_mark=mark, perp_mark_ts_ms=mark_ts, index_price=idx, index_ts_ms=idx_ts,
            contract_size=_num(m.get("contract_size")),
            underlying_multiplier=_num(m.get("underlying_multiplier")),
            source_status=STATUS_FRESH)
        problems = []
        fdata, ferr = self._fetch_funding(ticker)
        if fdata is not None:
            snap.funding_rate = _num(fdata.get("funding_rate"))   # a real 0.0 stays 0.0; missing -> None
            snap.funding_next_time = fdata.get("next_funding_time")
            snap.funding_computed_time = fdata.get("computed_time")
            if snap.funding_rate is None:
                problems.append("funding_rate missing")
        else:
            problems.append(f"funding: {ferr}")
        if m.get("status") not in (None, "active"):
            problems.append(f"market status {m.get('status')}")
        if snap.perp_mid is None:
            problems.append("no two-sided book")
        if snap.index_price is None:
            problems.append("reference_price missing")
        if snap.perp_mid is None and snap.perp_last is None and snap.perp_mark is None:
            snap.source_status = STATUS_UNAVAILABLE
        snap.source_error = "; ".join(problems) or None
        return snap


# ─────────────────────── bounded, timestamped history ───────────────────────
class PriceHistory:
    """Bounded (ts, price) series. Lookups select by REAL timestamps and never
    return an observation newer than the requested target time."""

    def __init__(self, maxlen=600, max_age_s=600.0):
        self._obs = deque(maxlen=int(maxlen))
        self.max_age_s = float(max_age_s)

    def __len__(self):
        return len(self._obs)

    @property
    def maxlen(self):
        return self._obs.maxlen

    def clear(self):
        self._obs.clear()

    def append(self, ts, price):
        if price is None or price <= 0 or ts is None:
            return False
        if self._obs and ts <= self._obs[-1][0]:
            return False                              # duplicate / out-of-order: ignore
        self._obs.append((float(ts), float(price)))
        cutoff = ts - self.max_age_s
        while self._obs and self._obs[0][0] < cutoff:
            self._obs.popleft()
        return True

    def latest(self):
        return self._obs[-1] if self._obs else None

    def at_or_before(self, target_ts, tolerance_s):
        """Newest observation with ts <= target_ts, provided it is no more than
        tolerance_s older than the target. Otherwise None (gap/too little history)."""
        for ts, px in reversed(self._obs):
            if ts <= target_ts:
                return (ts, px) if target_ts - ts <= tolerance_s else None
        return None

    def return_bps(self, horizon_s, tolerance_s, now_ts=None, current_max_age_s=None):
        """Simple return over `horizon_s`, anchored at the latest observation.
        If now_ts/current_max_age_s are given, the latest observation must be
        recent enough to count as 'current'."""
        cur = self.latest()
        if cur is None:
            return None
        if now_ts is not None and current_max_age_s is not None and now_ts - cur[0] > current_max_age_s:
            return None
        past = self.at_or_before(cur[0] - horizon_s, tolerance_s)
        return simple_return_bps(cur[1], past[1]) if past else None


# ═══════════════════════ STEP 2: causal features ═══════════════════════
# Every helper below takes a series ALREADY TRUNCATED at the feature end time and
# anchors on its LAST point. Nothing here can see an observation newer than that.
# Series are lists of (ts, value) sorted by ts ascending.

Z_WINDOWS = {"5m": (300.0, 20, 240.0), "15m": (900.0, 60, 720.0)}    # (window, min_n, min_span)
RV_WINDOWS = {60: (6, 45.0), 300: (25, 225.0), 900: (75, 675.0)}     # window -> (min_n, min_span)
RV_MAX_GAP_S = 20.0          # no single internal gap may exceed this (no bridging outages)
ZERO_VAR_EPS = 1e-12

Q_BINARY_NOT_OK = "BINARY_NOT_OK"
Q_MISSING_SPOT = "MISSING_SPOT"
Q_SPOT_TS_MISSING = "SPOT_TIMESTAMP_MISSING"
Q_NO_PRIOR_PERP = "NO_PRIOR_PERP"
Q_PERP_TOO_OLD = "PERP_TOO_OLD"
Q_PERP_SOURCE_ERROR = "PERP_SOURCE_ERROR"
Q_PERP_STALE = "PERP_STALE"
Q_INDEX_STALE = "INDEX_STALE"
Q_MISSING_INDEX = "MISSING_INDEX"
Q_NO_BOOK = "NO_TWO_SIDED_BOOK"
Q_INSUFF = {30: "INSUFFICIENT_30S_HISTORY", 60: "INSUFFICIENT_60S_HISTORY", 180: "INSUFFICIENT_180S_HISTORY"}
Q_INSUFF_Z = {"5m": "INSUFFICIENT_5M_HISTORY", "15m": "INSUFFICIENT_15M_HISTORY"}
Q_ZERO_VAR = "PREMIUM_ZERO_VARIANCE"
Q_RV_INSUFF = "RV_INSUFFICIENT"
Q_SCALE = "CONTRACT_SCALE_CHANGED"
Q_FUNDING_MISSING = "FUNDING_MISSING"
Q_QUEUE_DROPS = "QUEUE_DROPS_SINCE_PREV"
Q_NORM_FLOOR = "VOL_NORM_DENOMINATOR_TOO_SMALL"   # 300 s RV present but below NORM_MIN_VOL_BPS
Q_SPREAD_BASE_INSUFF = "INSUFFICIENT_SPREAD_BASELINE"
QUALITY_FLAG_DELIM = ";"


def basis_bps(a, b) -> Optional[float]:
    """(a / b - 1) * 10,000 for two Kalshi prices in the SAME per-contract units."""
    return premium_bps(a, b)


def spread_bps(bid, ask) -> Optional[float]:
    """((ask - bid) / mid) * 10,000; None unless a valid, uncrossed two-sided book."""
    mid = _mid(bid, ask)
    return None if mid is None else (ask - bid) / mid * 10000.0


def ratio(num, den) -> Optional[float]:
    if num is None or den is None or den <= 0:
        return None
    return num / den


def point_at_or_before(series, target_ts, tolerance_s):
    """Newest point with ts <= target_ts, if no more than tolerance_s older than it."""
    i = _count_le(series, target_ts) - 1                 # binary search; no timestamp list allocated
    if i < 0:
        return None
    ts, v = series[i]
    return (ts, v) if target_ts - ts <= tolerance_s else None


def _count_le(series, t):
    """Number of points with ts <= t in a ts-sorted series (== bisect_right on the timestamps)."""
    lo, hi = 0, len(series)
    while lo < hi:
        mid = (lo + hi) // 2
        if series[mid][0] <= t:
            lo = mid + 1
        else:
            hi = mid
    return lo


def _count_lt(series, t):
    """Number of points with ts < t in a ts-sorted series (== bisect_left on the timestamps)."""
    lo, hi = 0, len(series)
    while lo < hi:
        mid = (lo + hi) // 2
        if series[mid][0] < t:
            lo = mid + 1
        else:
            hi = mid
    return lo


def causal_return_bps(series, horizon_s, tolerance_s) -> Optional[float]:
    """Simple return from the point <= (anchor - horizon) to the anchor (last point)."""
    if not series:
        return None
    cur_ts, cur = series[-1]
    past = point_at_or_before(series, cur_ts - horizon_s, tolerance_s)
    return simple_return_bps(cur, past[1]) if past else None


def causal_change(series, horizon_s, tolerance_s) -> Optional[float]:
    """Level change (anchor value - value <= horizon ago). Used for premium bps."""
    if not series:
        return None
    cur_ts, cur = series[-1]
    past = point_at_or_before(series, cur_ts - horizon_s, tolerance_s)
    return (cur - past[1]) if past else None


def trailing_zscore(series, window_s, min_n, min_span_s):
    """Z of the anchor value vs. all points in (anchor - window, anchor], anchor included.
    Sample std (n-1). Returns (z | None, n, reason | None)."""
    if not series:
        return None, 0, "empty"
    end = series[-1][0]
    window = series[_count_le(series, end - window_s):]      # same elements, same order, one pass
    pts = [v for ts, v in window]
    tss = [ts for ts, v in window]
    n = len(pts)
    if n < min_n or (tss[-1] - tss[0]) < min_span_s:
        return None, n, "insufficient"
    mean = sum(pts) / n
    sd = math.sqrt(sum((x - mean) ** 2 for x in pts) / (n - 1))
    if not math.isfinite(sd) or sd <= ZERO_VAR_EPS:
        return None, n, "zero_variance"
    return (pts[-1] - mean) / sd, n, None


def realized_vol_bps(series, window_s, min_n, min_span_s, max_gap_s=RV_MAX_GAP_S) -> Optional[float]:
    """Realized volatility over [anchor - window, anchor] on irregular samples:
         r_i = ln(P_i / P_{i-1});  RV = sum r_i^2
         rv_1m = sqrt(RV * 60 / span_seconds) * 10,000     (bps per sqrt-minute)
    None if too few points, too little span, or ANY internal gap > max_gap_s."""
    if not series:
        return None
    end = series[-1][0]
    pts = series[_count_lt(series, end - window_s):]         # same elements, same order
    if len(pts) < min_n:
        return None
    span = pts[-1][0] - pts[0][0]
    if span < min_span_s or span <= 0:
        return None
    rv = 0.0
    for (t0, p0), (t1, p1) in zip(pts, pts[1:]):
        if t1 - t0 > max_gap_s or p0 <= 0 or p1 <= 0:
            return None
        rv += math.log(p1 / p0) ** 2
    return math.sqrt(rv * 60.0 / span) * 10000.0


def _iso_to_epoch(s) -> Optional[float]:
    if not isinstance(s, str) or not s:
        return None
    try:
        d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.timestamp()


# ═══════════════════════ STEP 2 v2: volatility-regime research features ═══════════════════════
# TELEMETRY / RESEARCH INPUTS ONLY. Nothing below is read by evaluate(), signals, confidence,
# edge, sizing, entries, exits, stops, calls or Discord posts. Every threshold is a fixed,
# predeclared constant that follows from the definitions; none is tuned against outcomes.
# The categorical labels are diagnostics for humans; the continuous values are what Step 3
# analyses. Volatility is treated as a RELIABILITY / regime variable, never as bullish or bearish.
#
# (A) Volatility-normalised momentum (perp and spot, 30/60/180 s):
#       sigma_h = rv_300s * sqrt(h / 60)        rv in bps per sqrt-minute (realized_vol_bps)
#       z_h     = return_h / sigma_h
#     The 300 s RV is the normaliser: 60 s RV reacts to the very move being measured and 900 s RV
#     adapts too slowly to a regime change. A 900 s-normalised value would be exactly
#     return / (rv_900s * sqrt(h/60)) from columns that are already logged, so it is not
#     duplicated as a column.
NORM_RV_WINDOW_S = 300
NORM_MIN_VOL_BPS = 0.01        # numerical guard only (one 1-bp price change in 300 s alone gives
                               # ~0.45): a smaller RV -> None, never clipped or replaced
# (B) Normalised perp-vs-spot gap:
#       gap_z_h = (perp_ret_h - spot_ret_h) / sqrt(sigma_perp_h^2 + sigma_spot_h^2)
#     The denominator equals the sd of the difference only if perp and spot returns were
#     independent. They are strongly positively correlated (same underlying), so the true sd of
#     the gap is SMALLER and this metric is conservative. It is a volatility-scaled disagreement
#     measure, NOT a calibrated z-score.
# (C) Spread baseline: median of the valid two-sided spreads of the snapshots strictly BEFORE the
#     selected snapshot and within 300 s of it (same contract scale, fresh at receipt).
#       perp_spread_ratio_5m = perp_spread_bps / perp_spread_median_5m_bps
SPREAD_BASELINE_WINDOW_S = 300.0
SPREAD_BASELINE_MIN_N = 20
SPREAD_BASELINE_MIN_SPAN_S = 240.0
# (D) Premium stress: premium_stress_5m = |premium_z_5m| (distance of the perp premium from its own
#     trailing 5-minute behaviour, sign-free). Existing premium columns are not duplicated.
# (E) Observational volatility regime from the RV shock ratios (60 s RV / 300 s or 900 s RV):
#       s = max of the AVAILABLE ratios {perp 60v300 (required), perp 60v900, spot 60v300, spot 60v900}
#       LOW s < 0.5 | NORMAL 0.5 <= s < 1.5 | HIGH 1.5 <= s < 2.5 | EXTREME s >= 2.5
#       UNKNOWN when perp_vol_shock_60v300 is unavailable.
#     Rationale (sampling theory, not outcomes): ~15 returns per 60 s at 4 s sampling give the 60 s
#     vol estimate a relative sd of ~sqrt(1/30) ~ 0.18 in a CONSTANT regime, so 1.5 is ~2.7 sd above
#     a ratio of 1, 2.5 is ~8 sd above it, and 0.5 is ~2.7 sd below it.
VOL_REGIME_LOW_BELOW = 0.5
VOL_REGIME_HIGH_AT = 1.5
VOL_REGIME_EXTREME_AT = 2.5
VR_LOW, VR_NORMAL, VR_HIGH, VR_EXTREME, VR_UNKNOWN = "LOW", "NORMAL", "HIGH", "EXTREME", "UNKNOWN"
VOL_REGIMES = (VR_LOW, VR_NORMAL, VR_HIGH, VR_EXTREME, VR_UNKNOWN)
# (F) Stability state: a transparent RULE, not a score, and never an input to any probability.
#       component              CAUTION at     UNSTABLE at
#       vol regime             HIGH           EXTREME
#       |momentum_gap_z_60s|   >= 1.0         >= 2.0
#       premium_stress_5m      >= 2.5         >= 4.0
#       perp_spread_ratio_5m   >= 2.0         >= 4.0
#     UNSTABLE if any component is at its UNSTABLE level, or >= 2 components are at CAUTION;
#     CAUTION  if exactly one component is at CAUTION;
#     STABLE   only if all four components are available and none is triggered;
#     UNKNOWN  otherwise (row not analysis_ready, or inputs missing and nothing triggered).
#     With inputs missing, CAUTION/UNSTABLE are lower bounds (a missing input can only hide stress).
#     gap_z 1.0 = the two moves differ by the combined 1-sigma horizon scale (e.g. perp +0.7 sigma
#     while spot -0.7 sigma); premium |z| 2.5 / 4 are far tails of the trailing 5-min distribution;
#     a spread ratio of 2 / 4 means the book is 2x / 4x wider than its own 5-minute median.
GAP_Z_CAUTION, GAP_Z_UNSTABLE = 1.0, 2.0
PREMIUM_STRESS_CAUTION, PREMIUM_STRESS_UNSTABLE = 2.5, 4.0
SPREAD_RATIO_CAUTION, SPREAD_RATIO_UNSTABLE = 2.0, 4.0
STABILITY_MULTI_CAUTION = 2
ST_STABLE, ST_CAUTION, ST_UNSTABLE, ST_UNKNOWN = "STABLE", "CAUTION", "UNSTABLE", "UNKNOWN"
STABILITY_STATES = (ST_STABLE, ST_CAUTION, ST_UNSTABLE, ST_UNKNOWN)
DIRECTION_Z_MIN = 1.0          # dashboard label only: perp_momentum_z_60s >= +1 UP, <= -1 DOWN, else FLAT


def _finite(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def horizon_sigma_bps(vol_bps_per_sqrt_min, horizon_s) -> Optional[float]:
    """Expected 1-sigma move in bps over horizon_s from a volatility in bps per sqrt-minute:
    vol * sqrt(horizon_s / 60). None unless the vol is finite and >= NORM_MIN_VOL_BPS."""
    if not _finite(vol_bps_per_sqrt_min) or vol_bps_per_sqrt_min < NORM_MIN_VOL_BPS or not horizon_s > 0:
        return None
    return vol_bps_per_sqrt_min * math.sqrt(horizon_s / 60.0)


def normalized_return(ret_bps, vol_bps_per_sqrt_min, horizon_s) -> Optional[float]:
    """return / expected horizon sigma (see (A)). None if either input is unusable; never inf/nan."""
    sig = horizon_sigma_bps(vol_bps_per_sqrt_min, horizon_s)
    if sig is None or not _finite(ret_bps):
        return None
    z = ret_bps / sig
    return z if math.isfinite(z) else None


def normalized_gap(gap_bps, perp_vol, spot_vol, horizon_s) -> Optional[float]:
    """Volatility-scaled perp-vs-spot disagreement (see (B)). None if any input is unusable."""
    sp, ss = horizon_sigma_bps(perp_vol, horizon_s), horizon_sigma_bps(spot_vol, horizon_s)
    if sp is None or ss is None or not _finite(gap_bps):
        return None
    z = gap_bps / math.hypot(sp, ss)
    return z if math.isfinite(z) else None


def trailing_median_before(series, anchor_ts, window_s, min_n, min_span_s):
    """Median of the points with anchor_ts - window_s <= ts < anchor_ts. The anchor itself and
    anything later are excluded. Returns (median | None, n); None unless >= min_n points that
    span >= min_span_s."""
    lo, hi = _count_lt(series, anchor_ts - window_s), _count_lt(series, anchor_ts)
    n = hi - lo
    if n <= 0 or n < min_n or series[hi - 1][0] - series[lo][0] < min_span_s:
        return None, max(n, 0)
    v = sorted(x for _, x in series[lo:hi])
    m = n // 2
    return (v[m] if n % 2 else (v[m - 1] + v[m]) / 2.0), n


def spread_ratio(current_bps, baseline_bps) -> Optional[float]:
    """current spread / trailing median spread. None if either is missing or the baseline is ~0."""
    if not _finite(current_bps) or not _finite(baseline_bps) or current_bps < 0 or baseline_bps <= ZERO_VAR_EPS:
        return None
    return current_bps / baseline_bps


def vol_regime(perp_60v300, perp_60v900=None, spot_60v300=None, spot_60v900=None) -> str:
    """Observational regime label (see (E)). Deterministic; UNKNOWN without perp 60v300."""
    if not _finite(perp_60v300) or perp_60v300 < 0:
        return VR_UNKNOWN
    s = max(x for x in (perp_60v300, perp_60v900, spot_60v300, spot_60v900) if _finite(x) and x >= 0)
    if s >= VOL_REGIME_EXTREME_AT:
        return VR_EXTREME
    if s >= VOL_REGIME_HIGH_AT:
        return VR_HIGH
    return VR_LOW if s < VOL_REGIME_LOW_BELOW else VR_NORMAL


def _stress_level(value, caution, unstable):
    """0 normal, 1 caution, 2 unstable, None unavailable (on |value|)."""
    if not _finite(value):
        return None
    v = abs(value)
    return 2 if v >= unstable else (1 if v >= caution else 0)


_VOL_LEVEL = {VR_LOW: 0, VR_NORMAL: 0, VR_HIGH: 1, VR_EXTREME: 2}
# component -> (reason at CAUTION, reason at UNSTABLE, reason when unavailable)
_STABILITY_COMPONENTS = (("vol", "VOL_HIGH", "VOL_EXTREME", "VOL_UNKNOWN"),
                         ("agreement", "SPOT_PERP_DIVERGING", "SPOT_PERP_DISAGREE", "AGREEMENT_UNKNOWN"),
                         ("premium", "PREMIUM_ELEVATED", "PREMIUM_EXTREME", "PREMIUM_UNKNOWN"),
                         ("spread", "SPREAD_WIDENING", "SPREAD_BLOWOUT", "SPREAD_UNKNOWN"))


def stability_levels(regime, gap_z_60s, premium_stress, spread_ratio_5m):
    return {"vol": _VOL_LEVEL.get(regime),
            "agreement": _stress_level(gap_z_60s, GAP_Z_CAUTION, GAP_Z_UNSTABLE),
            "premium": _stress_level(premium_stress, PREMIUM_STRESS_CAUTION, PREMIUM_STRESS_UNSTABLE),
            "spread": _stress_level(spread_ratio_5m, SPREAD_RATIO_CAUTION, SPREAD_RATIO_UNSTABLE)}


def stability_state(analysis_ready, regime, gap_z_60s, premium_stress, spread_ratio_5m):
    """The rule in (F). Returns (state, [reason codes]). Deterministic; no weights, no score."""
    if analysis_ready is not True:
        return ST_UNKNOWN, ["NOT_ANALYSIS_READY"]
    lv = stability_levels(regime, gap_z_60s, premium_stress, spread_ratio_5m)
    reasons, n_caution, n_unstable, n_missing = [], 0, 0, 0
    for name, r_caution, r_unstable, r_missing in _STABILITY_COMPONENTS:
        v = lv[name]
        if v is None:
            n_missing += 1; reasons.append(r_missing)
        elif v == 2:
            n_unstable += 1; reasons.append(r_unstable)
        elif v == 1:
            n_caution += 1; reasons.append(r_caution)
    if n_unstable or n_caution >= STABILITY_MULTI_CAUTION:
        return ST_UNSTABLE, reasons
    if n_caution:
        return ST_CAUTION, reasons
    return (ST_UNKNOWN if n_missing else ST_STABLE), reasons


_LEVEL_LABELS = {"agreement": ("AGREE", "DIVERGING", "DISAGREE"),
                 "premium": ("NORMAL", "ELEVATED", "EXTREME"),
                 "spread": ("NORMAL", "WIDENING", "BLOWOUT")}


def dashboard_view(row):
    """Compact DISPLAY-ONLY summary of one telemetry row (direction + reliability). Pure, never
    raises; unavailable inputs show as None / UNKNOWN. Nothing reads this back."""
    try:
        r = row if isinstance(row, dict) else {}
        g = lambda c: _num(r.get(c))
        z = g("perp_momentum_z_60s")
        regime = r.get("vol_regime") if r.get("vol_regime") in VOL_REGIMES else VR_UNKNOWN
        lv = stability_levels(regime, g("momentum_gap_z_60s"), g("premium_stress_5m"), g("perp_spread_ratio_5m"))
        label = lambda k: "UNKNOWN" if lv[k] is None else _LEVEL_LABELS[k][lv[k]]
        state = r.get("perp_stability_state")
        lag = g("perp_lag_to_spot_ms")
        return {"direction": None if z is None else
                ("UP" if z >= DIRECTION_Z_MIN else ("DOWN" if z <= -DIRECTION_Z_MIN else "FLAT")),
                "momentum_z_60s": z, "vol_regime": regime, "vol_shock_60v300": g("perp_vol_shock_60v300"),
                "agreement": label("agreement"), "gap_z_60s": g("momentum_gap_z_60s"),
                "premium_stress": label("premium"), "premium_stress_5m": g("premium_stress_5m"),
                "spread_stress": label("spread"), "spread_ratio_5m": g("perp_spread_ratio_5m"),
                "stability": state if state in STABILITY_STATES else ST_UNKNOWN,
                "stability_reasons": r.get("perp_stability_reasons") or None,
                "source_status": r.get("source_status"),
                "analysis_ready": r.get("analysis_ready") if isinstance(r.get("analysis_ready"), bool) else None,
                "lag_s": None if lag is None else round(lag / 1000.0, 1)}
    except Exception:                                   # display must never break
        return {"direction": None, "vol_regime": VR_UNKNOWN, "stability": ST_UNKNOWN, "view_error": True}


# ─────────────────────── bounded, thread-safe snapshot store ───────────────────────
class SnapshotStore:
    """Per-coin bounded history of complete PerpSnapshots, ordered by availability
    time. Bounded by BOTH maxlen and max_age_s. A contract re-scale clears the
    coin's history (pre/post-scale per-contract prices are never joined)."""

    def __init__(self, coins, maxlen=600, max_age_s=1800.0):
        self.maxlen = int(maxlen)
        self.max_age_s = float(max_age_s)
        self._d = {c: deque(maxlen=self.maxlen) for c in coins}
        self._scale = {}
        self._scale_changes = {c: deque(maxlen=16) for c in coins}
        self._lock = threading.Lock()

    def add(self, snap: PerpSnapshot) -> bool:
        with self._lock:
            d = self._d.get(snap.coin)
            if d is None:
                return False
            if d and (snap.avail <= d[-1].avail or snap.ts <= d[-1].ts):
                return False                         # duplicate (cache hit) or out-of-time
            if None not in snap.scale:
                prev = self._scale.get(snap.coin)
                if prev is not None and prev != snap.scale:
                    d.clear()
                    self._scale_changes[snap.coin].append(snap.avail)
                self._scale[snap.coin] = snap.scale
            d.append(snap)
            cutoff = snap.avail - self.max_age_s
            while d and d[0].avail < cutoff:
                d.popleft()
            return True

    def latest_at_or_before(self, coin, t) -> Optional[PerpSnapshot]:
        """Newest snapshot AVAILABLE at or before t. Never a later one."""
        with self._lock:
            for s in reversed(self._d.get(coin, ())):
                if s.avail <= t:
                    return s
        return None

    def upto(self, coin, t):
        with self._lock:
            return [s for s in self._d.get(coin, ()) if s.avail <= t]

    def latest(self, coin):
        with self._lock:
            d = self._d.get(coin)
            return d[-1] if d else None

    def scale_changes(self, coin):
        with self._lock:
            return list(self._scale_changes.get(coin, ()))

    def count(self, coin):
        with self._lock:
            return len(self._d.get(coin, ()))


# ─────────────────────── continuous background sampler ───────────────────────
class PerpSampler:
    """Polls the provider on its own thread, independent of the binary watcher.
    Monotonic scheduling; error backoff; never raises out of its loop.
    `clock`, `monotonic` and `wait` are injectable for deterministic tests."""

    def __init__(self, provider: PerpProvider, coins, store: Optional[SnapshotStore] = None,
                 interval_s=4.0, error_backoff_s=15.0, clock: Callable[[], float] = time.time,
                 monotonic: Callable[[], float] = time.monotonic, wait=None):
        self.provider = provider
        self.coins = list(coins)
        self.store = store or SnapshotStore(self.coins)
        self.interval_s = float(interval_s)
        self.error_backoff_s = float(error_backoff_s)
        self.clock = clock
        self.monotonic = monotonic
        self._stop = threading.Event()
        self._wait = wait or self._stop.wait
        self._thread = None
        self.samples_total = 0
        self.sample_errors = 0
        self.last_sample_ts = None
        self.last_error = None

    def sample_once(self):
        """One provider call (no lock held during it), then a short store update."""
        try:
            snaps = self.provider.snapshots(self.coins)
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"[:300]
            now = self.clock()
            snaps = {c: PerpSnapshot(ts=now, coin=c, source_status=STATUS_ERROR, source_error=msg)
                     for c in self.coins}
        got = self.clock()
        all_err = True
        for c in self.coins:
            s = snaps.get(c) if isinstance(snaps, dict) else None
            if not isinstance(s, PerpSnapshot):
                s = PerpSnapshot(ts=got, coin=c, source_status=STATUS_ERROR, source_error="no snapshot")
            s = dataclasses.replace(s, available_ts=max(got, s.ts))
            if s.source_status != STATUS_ERROR:
                all_err = False
            self.store.add(s)
        self.samples_total += 1
        self.last_sample_ts = got
        if all_err:
            self.sample_errors += 1
        return all_err

    def run(self, max_iterations=None):
        n = 0
        next_run = self.monotonic()
        while not self._stop.is_set():
            try:
                all_err = self.sample_once()
            except Exception as e:                   # never die
                self.last_error = f"{type(e).__name__}: {e}"
                all_err = True
            n += 1
            if max_iterations is not None and n >= max_iterations:
                return
            next_run += self.error_backoff_s if all_err else self.interval_s
            now = self.monotonic()
            if next_run < now:                       # a slow request overran the slot:
                next_run = now + (self.error_backoff_s if all_err else self.interval_s)   # full gap, no burst
            if self._wait(max(0.0, next_run - now)):
                return

    def start(self):
        if self._thread is None or not self._thread.is_alive():
            self._stop.clear()
            self._thread = threading.Thread(target=self.run, name="perp-sampler", daemon=True)
            self._thread.start()

    def stop(self):
        self._stop.set()

    @property
    def alive(self):
        return self._thread is not None and self._thread.is_alive()


# ─────────────────────── CSV logging ───────────────────────
STEP1_COLUMNS = [
    "ts_utc", "ts_epoch_ms", "coin",
    "binary_status", "binary_ticker", "binary_close_time", "minutes_left",
    "spot_source", "spot_price", "spot_ts_epoch_ms", "strike",
    "base_p_up", "base_conf", "fav", "side_ask", "raw_edge", "net_edge",
    "binary_signal", "binary_reason",
    "perp_symbol", "perp_market_status", "perp_bid", "perp_ask", "perp_mid", "perp_last",
    "perp_mark", "perp_mark_ts_ms",
    "index_source", "index_price", "index_ts_ms", "premium_bps",
    "funding_rate", "funding_next_time", "funding_computed_time",
    "perp_contract_size", "perp_underlying_multiplier",
    "perp_ret_30s_bps", "perp_ret_60s_bps", "perp_ret_180s_bps",
    "spot_ret_30s_bps", "spot_ret_60s_bps", "spot_ret_180s_bps",
    "lead_30s_bps", "lead_60s_bps", "lead_180s_bps",
    "perp_fetch_ts_epoch_ms", "perp_data_age_ms", "index_age_ms",
    "source_status", "source_error",
]

# Schema v3 (feature_version step2_v2): volatility-regime research telemetry (see STEP 2 v2 above).
VOLATILITY_COLUMNS = [
    "perp_momentum_z_30s", "perp_momentum_z_60s", "perp_momentum_z_180s",
    "spot_momentum_z_30s", "spot_momentum_z_60s", "spot_momentum_z_180s",
    "momentum_gap_z_30s", "momentum_gap_z_60s", "momentum_gap_z_180s",
    "perp_spread_median_5m_bps", "perp_spread_baseline_n", "perp_spread_ratio_5m",
    "premium_stress_5m",
    "vol_regime", "perp_stability_state", "perp_stability_reasons",
]

# Step 2 columns. Canonical analysis fields; Step 1 columns above are kept unchanged
# for backward compatibility / diagnostics (lead_* is just perp_ret - spot_ret).
STEP2_COLUMNS = [
    "spot_observed_ts_epoch_ms", "spot_request_latency_ms",
    "perp_snapshot_ts_epoch_ms", "perp_lag_to_spot_ms", "feature_end_ts_epoch_ms",
    "causal_pair_ok", "analysis_ready", "quality_flags",
    "perp_spread_bps", "causal_premium_bps",
    "mark_index_premium_bps", "last_index_premium_bps", "mid_mark_basis_bps",
    "premium_change_30s_bps", "premium_change_60s_bps", "premium_change_180s_bps",
    "premium_z_5m", "premium_z_5m_n", "premium_z_15m", "premium_z_15m_n",
    "causal_perp_ret_30s_bps", "causal_perp_ret_60s_bps", "causal_perp_ret_180s_bps",
    "causal_spot_ret_30s_bps", "causal_spot_ret_60s_bps", "causal_spot_ret_180s_bps",
    "momentum_gap_30s_bps", "momentum_gap_60s_bps", "momentum_gap_180s_bps",
    "perp_rv_60s_bps", "perp_rv_300s_bps", "perp_rv_900s_bps",
    "spot_rv_60s_bps", "spot_rv_300s_bps", "spot_rv_900s_bps",
    "perp_vol_shock_60v300", "perp_vol_shock_60v900",
    "spot_vol_shock_60v300", "spot_vol_shock_60v900",
] + VOLATILITY_COLUMNS + [
    "funding_available", "funding_age_ms",
    "submitted_cycles_total", "processed_cycles_total", "dropped_cycles_total",
    "drops_since_previous_row",
]

VERSION_COLUMNS = ["telemetry_schema_version", "feature_version", "telemetry_session_id", "cycle_id"]
CSV_COLUMNS = VERSION_COLUMNS + STEP1_COLUMNS + STEP2_COLUMNS


class TelemetryCSVLogger:
    """Append-only CSV with a single header. If an existing file has a different
    header (schema change), it is renamed aside rather than appended to, so a
    file never mixes schemas or contains duplicate headers."""

    def __init__(self, path, columns=CSV_COLUMNS):
        self.path = path
        self.columns = list(columns)
        self._lock = threading.Lock()
        self._validated_id = None          # (st_dev, st_ino) of the file whose header we verified

    def _header_ok(self):
        try:
            with open(self.path, newline="") as f:
                first = next(csv.reader(f), None)
            return first == self.columns
        except (FileNotFoundError, StopIteration):
            return None

    def write_rows(self, rows):
        if not rows:
            return
        with self._lock:
            try:
                st = os.stat(self.path)
                file_id, size = (st.st_dev, st.st_ino), st.st_size
            except FileNotFoundError:
                file_id, size = None, 0
            if file_id is not None and size > 0 and file_id == self._validated_id:
                state = True                        # same file we already validated: plain append
            else:
                state = self._header_ok() if size > 0 else None
                if state is False:
                    os.replace(self.path, f"{self.path}.schema-{int(time.time())}.bak")
                    state = None
            with open(self.path, "a", newline="") as f:
                w = csv.DictWriter(f, fieldnames=self.columns, extrasaction="ignore")
                if state is None:
                    w.writeheader()
                for r in rows:
                    w.writerow({c: ("" if r.get(c) is None else r.get(c)) for c in self.columns})
            try:
                st = os.stat(self.path)
                self._validated_id = (st.st_dev, st.st_ino)
            except OSError:
                self._validated_id = None


# ─────────────────────── the recorder ───────────────────────
def _r(x, n=4):
    return None if x is None else round(x, n)


class PerpTelemetry:
    """Joins one poll cycle's binary results with perp data, computes raw features,
    logs CSV rows. It only READS the binary result dicts it is given (copies).

    Two modes:
      * sampler attached (production, Step 2): perp data comes ONLY from the
        continuous sampler's SnapshotStore; record_cycle never makes a request.
      * no sampler (Step 1 compatibility / direct calls): record_cycle fetches on
        demand or uses the snapshots passed in, as in Step 1. Such snapshots are
        received AFTER the spot observation, so causal selection rejects them
        (NO_PRIOR_PERP) — the legacy columns still get Step 1 semantics.

    FEATURE END TIME (feature_end_ts_epoch_ms) = the binary row's spot receive time
    (spot_observed_ts, captured inside evaluate() right after the spot response).
    Every causal_* / premium_* / momentum_gap_* / *_rv_* / *_vol_shock_* feature, and every
    schema-v3 volatility-regime column (*_momentum_z_*, momentum_gap_z_*, perp_spread_*_5m,
    premium_stress_5m, vol_regime, perp_stability_*), uses only perp snapshots with
    available_ts <= feature_end and spot observations with ts <= feature_end.
    """

    def __init__(self, provider: PerpProvider, coins, log_path=None,
                 stale_seconds=15.0, lookback_tolerance_s=10.0, history_maxlen=600,
                 on_rows: Optional[Callable[[dict], None]] = None,
                 clock: Callable[[], float] = time.time,
                 sampler: Optional["PerpSampler"] = None, max_lag_s=8.0,
                 snapshot_maxlen=600, snapshot_max_age_s=1800.0,
                 session_id: Optional[str] = None):
        self.provider = provider
        self.coins = list(coins)
        self.stale_seconds = float(stale_seconds)
        self.tolerance_s = float(lookback_tolerance_s)
        max_age = max(HORIZONS_S) + self.tolerance_s + 60.0
        # Step 1 (legacy diagnostic) histories — unchanged
        self.perp_hist = {c: PriceHistory(history_maxlen, max_age) for c in self.coins}
        self.spot_hist = {c: PriceHistory(history_maxlen, max_age) for c in self.coins}
        self._scale = {}
        # Step 2 (causal) state
        self.sampler = sampler
        self.store = sampler.store if sampler is not None else SnapshotStore(
            self.coins, snapshot_maxlen, snapshot_max_age_s)
        self.spot_long = {c: PriceHistory(snapshot_maxlen, snapshot_max_age_s) for c in self.coins}
        self.max_lag_s = float(max_lag_s)
        self.session_id = session_id or (
            dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8])
        self.submitted_cycles_total = 0
        self.processed_cycles_total = 0
        self.dropped_cycles_total = 0
        self._drops_at_last_row = 0
        self._cycle_seq = 0
        self._counter_lock = threading.Lock()
        self.latest_rows = {}
        # shared
        self.logger = TelemetryCSVLogger(log_path) if log_path else None
        self.on_rows = on_rows
        self.clock = clock
        self._lock = threading.Lock()
        self._q = queue.Queue(maxsize=1)
        self._thread = None
        self.last_error = None

    def start_sampler(self):
        if self.sampler is not None:
            self.sampler.start()

    # -- stale classification (Step 1, unchanged) --
    def classify(self, snap: PerpSnapshot, now=None):
        """Returns (status, perp_data_age_ms, index_age_ms)."""
        now = self.clock() if now is None else now
        age_ms = int(round((now - snap.ts) * 1000)) if snap.ts else None
        idx_age = int(round(now * 1000 - snap.index_ts_ms)) if snap.index_ts_ms else None
        status = snap.source_status
        if status == STATUS_FRESH:
            if (age_ms is not None and age_ms > self.stale_seconds * 1000) or \
               (idx_age is not None and idx_age > self.stale_seconds * 1000):
                status = STATUS_STALE
        return status, age_ms, idx_age

    def _fresh_at_receipt(self, s: PerpSnapshot):
        """Was this snapshot fresh when it arrived? (for building premium/price series)"""
        if s.source_status != STATUS_FRESH:
            return False
        if s.index_ts_ms is not None and s.avail * 1000 - s.index_ts_ms > self.stale_seconds * 1000:
            return False
        return True

    # -- one cycle (synchronous; used by the worker and by tests) --
    def record_cycle(self, binary_results, spot_ts=None, snapshots=None, now=None, cycle_id=None):
        """binary_results: {coin: evaluate() result}. spot_ts: {coin: spot observed epoch s}.
        Returns {coin: row}. Never raises."""
        try:
            results = {c: dict(r or {}) for c, r in (binary_results or {}).items()}
            spot_ts = dict(spot_ts or {})
            if snapshots is None and self.sampler is None:        # Step 1 compatibility mode
                try:
                    snapshots = self.provider.snapshots(self.coins)
                except Exception as e:
                    msg = f"{type(e).__name__}: {e}"[:300]
                    snapshots = {c: PerpSnapshot(ts=self.clock(), coin=c, source_status=STATUS_ERROR,
                                                 source_error=msg) for c in self.coins}
            if snapshots is not None:
                for c in self.coins:
                    if isinstance(snapshots.get(c), PerpSnapshot):
                        self.store.add(snapshots[c])
            now = self.clock() if now is None else now
            with self._counter_lock:
                if cycle_id is None:
                    self._cycle_seq += 1
                    cycle_id = self._cycle_seq
                self.processed_cycles_total += 1
                drops_since = self.dropped_cycles_total - self._drops_at_last_row
                self._drops_at_last_row = self.dropped_cycles_total
                counters = (self.submitted_cycles_total, self.processed_cycles_total,
                            self.dropped_cycles_total)
            rows = {}
            with self._lock:
                for coin in self.coins:
                    r = results.get(coin, {})
                    s_ts = spot_ts.get(coin)
                    if s_ts is None:
                        s_ts = _num(r.get("spot_observed_ts"))
                    feats, sel = self._causal(coin, r, s_ts)
                    if snapshots is not None:
                        legacy_snap = snapshots.get(coin) or PerpSnapshot(ts=now, coin=coin,
                                                                          source_error="no snapshot")
                    else:
                        legacy_snap = sel or PerpSnapshot(
                            ts=s_ts or now, coin=coin, source_status=STATUS_UNAVAILABLE,
                            source_error="no perp snapshot available at or before observation")
                    row = self._row(coin, r, legacy_snap, s_ts, now)
                    row.update(feats)
                    row.update({
                        "telemetry_schema_version": TELEMETRY_SCHEMA_VERSION,
                        "feature_version": FEATURE_VERSION,
                        "telemetry_session_id": self.session_id, "cycle_id": cycle_id,
                        "submitted_cycles_total": counters[0], "processed_cycles_total": counters[1],
                        "dropped_cycles_total": counters[2], "drops_since_previous_row": drops_since,
                    })
                    if drops_since:
                        row["quality_flags"] = QUALITY_FLAG_DELIM.join(
                            [f for f in (row["quality_flags"] or "").split(QUALITY_FLAG_DELIM) if f]
                            + [Q_QUEUE_DROPS])
                    rows[coin] = row
                self.latest_rows = rows
            if self.logger:
                try:
                    self.logger.write_rows([rows[c] for c in self.coins])
                except OSError as e:
                    self.last_error = f"csv: {e}"
            if self.on_rows:
                try:
                    self.on_rows(rows)
                except Exception as e:
                    self.last_error = f"on_rows: {e}"
            return rows
        except Exception as e:                        # absolute backstop
            self.last_error = f"{type(e).__name__}: {e}"
            return {}

    # -- Step 2: causal features --
    def _causal(self, coin, r, s_ts, mutate=True):
        """Step 2 causal features. With mutate=False this is a pure READ-ONLY preview:
        the current spot observation is used but never appended, so nothing in the recorder
        changes (same code path, same numbers)."""
        flags = []
        f = {c: None for c in STEP2_COLUMNS}
        end = s_ts                                     # FEATURE END TIME
        binary_ok = r.get("status") == "ok"
        spot = _price(r.get("spot_raw", r.get("spot")))
        lat = _num(r.get("spot_request_latency_ms"))
        if not binary_ok:
            flags.append(Q_BINARY_NOT_OK)
        if spot is None:
            flags.append(Q_MISSING_SPOT)
        if end is None:
            flags.append(Q_SPOT_TS_MISSING)
        f["spot_request_latency_ms"] = lat
        if end is not None:
            f["spot_observed_ts_epoch_ms"] = int(round(end * 1000))
            f["feature_end_ts_epoch_ms"] = int(round(end * 1000))
        if spot is not None and end is not None and mutate:
            self.spot_long[coin].append(end, spot)

        sel = self.store.latest_at_or_before(coin, end) if end is not None else None
        lag = None
        if end is not None and sel is None:
            flags.append(Q_NO_PRIOR_PERP)
        if sel is not None:
            lag = end - sel.avail                      # >= 0 by construction of the selection
            f["perp_snapshot_ts_epoch_ms"] = int(round(sel.avail * 1000))
            f["perp_lag_to_spot_ms"] = int(round(lag * 1000))
        causal_pair_ok = sel is not None and lag is not None and 0.0 <= lag <= self.max_lag_s
        if sel is not None and not causal_pair_ok:
            flags.append(Q_PERP_TOO_OLD)
        f["causal_pair_ok"] = causal_pair_ok

        perp_ok = False
        if causal_pair_ok:
            status, _age, idx_age = self.classify(sel, now=end)
            if sel.source_status in (STATUS_ERROR, STATUS_UNAVAILABLE, STATUS_DISABLED):
                flags.append(Q_PERP_SOURCE_ERROR)
            elif status == STATUS_STALE:
                flags.append(Q_PERP_STALE)
                if idx_age is not None and idx_age > self.stale_seconds * 1000:
                    flags.append(Q_INDEX_STALE)
            perp_ok = status == STATUS_FRESH
            if sel.source_status not in (STATUS_ERROR, STATUS_UNAVAILABLE, STATUS_DISABLED):
                if sel.perp_mid is None:
                    flags.append(Q_NO_BOOK)
                if sel.index_price is None:
                    flags.append(Q_MISSING_INDEX)

        # scale change inside the longest feature window -> older levels were discarded
        if end is not None and any(end - 1080.0 <= t <= end for t in self.store.scale_changes(coin)):
            flags.append(Q_SCALE)

        # --- perp-side features (anchored on the selected snapshot; data <= end only) ---
        perp_series, prem_series, spread_series = [], [], []
        if perp_ok:
            spread_from = sel.avail - SPREAD_BASELINE_WINDOW_S
            for s in self.store.upto(coin, end):
                if s.scale != sel.scale and None not in s.scale and None not in sel.scale:
                    continue                          # defense in depth: never bridge a re-scale
                if not self._fresh_at_receipt(s) and s is not sel:
                    continue
                a = s.avail
                if s is not sel and a >= spread_from:                # spread baseline: strictly before sel
                    sp = spread_bps(s.perp_bid, s.perp_ask)          # None if one-sided or crossed
                    if sp is not None:
                        spread_series.append((a, sp))
                if s.perp_mid is not None:
                    perp_series.append((a, s.perp_mid))
                    p = premium_bps(s.perp_mid, s.index_price)
                    if p is not None:
                        prem_series.append((a, p))
            # the anchor must be the selected snapshot itself
            if not perp_series or perp_series[-1][0] != sel.avail:
                perp_series = []
            if not prem_series or prem_series[-1][0] != sel.avail:
                prem_series = []
            f["perp_spread_bps"] = _r(spread_bps(sel.perp_bid, sel.perp_ask))
            med, f["perp_spread_baseline_n"] = trailing_median_before(
                spread_series, sel.avail, SPREAD_BASELINE_WINDOW_S, SPREAD_BASELINE_MIN_N, SPREAD_BASELINE_MIN_SPAN_S)
            f["perp_spread_median_5m_bps"] = _r(med)
            f["perp_spread_ratio_5m"] = _r(spread_ratio(f["perp_spread_bps"], f["perp_spread_median_5m_bps"]))
            if f["perp_spread_bps"] is not None and med is None:
                flags.append(Q_SPREAD_BASE_INSUFF)
            f["causal_premium_bps"] = _r(premium_bps(sel.perp_mid, sel.index_price))
            f["mark_index_premium_bps"] = _r(basis_bps(sel.perp_mark, sel.index_price))
            f["last_index_premium_bps"] = _r(basis_bps(sel.perp_last, sel.index_price))
            f["mid_mark_basis_bps"] = _r(basis_bps(sel.perp_mid, sel.perp_mark))
            for h in HORIZONS_S:
                f[f"premium_change_{h}s_bps"] = _r(causal_change(prem_series, h, self.tolerance_s))
            for name, (w, mn, span) in Z_WINDOWS.items():
                z, n, why = trailing_zscore(prem_series, w, mn, span)
                f[f"premium_z_{name}"], f[f"premium_z_{name}_n"] = _r(z), n
                if prem_series and why == "insufficient":
                    flags.append(Q_INSUFF_Z[name])
                elif why == "zero_variance":
                    flags.append(Q_ZERO_VAR)
            if f["premium_z_5m"] is not None:
                f["premium_stress_5m"] = abs(f["premium_z_5m"])
            for h in HORIZONS_S:
                f[f"causal_perp_ret_{h}s_bps"] = _r(causal_return_bps(perp_series, h, self.tolerance_s))
            for w, (mn, span) in RV_WINDOWS.items():
                f[f"perp_rv_{w}s_bps"] = _r(realized_vol_bps(perp_series, w, mn, span))
            f["funding_available"] = sel.funding_rate is not None
            fc = _iso_to_epoch(sel.funding_computed_time)
            f["funding_age_ms"] = int(round((end - fc) * 1000)) if fc is not None else None
            if sel.funding_rate is None:
                flags.append(Q_FUNDING_MISSING)

        # --- spot-side features (anchored on THIS row's spot observation) ---
        spot_series = []
        if spot is not None and end is not None:
            spot_series = [p for p in self.spot_long[coin]._obs if p[0] <= end]
            if not mutate and (not spot_series or spot_series[-1][0] < end):
                spot_series = spot_series + [(end, spot)]      # virtual: identical to what record would append
            if not spot_series or abs(spot_series[-1][0] - end) > 1e-3:
                spot_series = []
        for h in HORIZONS_S:
            f[f"causal_spot_ret_{h}s_bps"] = _r(causal_return_bps(spot_series, h, self.tolerance_s))
        for w, (mn, span) in RV_WINDOWS.items():
            f[f"spot_rv_{w}s_bps"] = _r(realized_vol_bps(spot_series, w, mn, span))

        # --- derived ---
        for h in HORIZONS_S:
            pr, sr = f[f"causal_perp_ret_{h}s_bps"], f[f"causal_spot_ret_{h}s_bps"]
            f[f"momentum_gap_{h}s_bps"] = _r(lead_bps(pr, sr))
            if (perp_ok and pr is None) or (spot_series and sr is None):
                flags.append(Q_INSUFF[h])
        for side in ("perp", "spot"):
            s60, s300, s900 = (f[f"{side}_rv_{w}s_bps"] for w in (60, 300, 900))
            f[f"{side}_vol_shock_60v300"] = _r(ratio(s60, s300))
            f[f"{side}_vol_shock_60v900"] = _r(ratio(s60, s900))
        if (perp_ok and None in (f["perp_rv_60s_bps"], f["perp_rv_300s_bps"], f["perp_rv_900s_bps"])) or \
           (spot_series and None in (f["spot_rv_60s_bps"], f["spot_rv_300s_bps"], f["spot_rv_900s_bps"])):
            flags.append(Q_RV_INSUFF)

        # --- STEP 2 v2 volatility-regime research features (derived from the logged values above) ---
        prv, srv = f[f"perp_rv_{NORM_RV_WINDOW_S}s_bps"], f[f"spot_rv_{NORM_RV_WINDOW_S}s_bps"]
        for h in HORIZONS_S:
            f[f"perp_momentum_z_{h}s"] = _r(normalized_return(f[f"causal_perp_ret_{h}s_bps"], prv, h))
            f[f"spot_momentum_z_{h}s"] = _r(normalized_return(f[f"causal_spot_ret_{h}s_bps"], srv, h))
            f[f"momentum_gap_z_{h}s"] = _r(normalized_gap(f[f"momentum_gap_{h}s_bps"], prv, srv, h))
        if any(v is not None and v < NORM_MIN_VOL_BPS for v in (prv, srv)):
            flags.append(Q_NORM_FLOOR)
        f["vol_regime"] = vol_regime(f["perp_vol_shock_60v300"], f["perp_vol_shock_60v900"],
                                     f["spot_vol_shock_60v300"], f["spot_vol_shock_60v900"])

        # analysis_ready: base row usable; depends on NO outcome and NO feature warm-up
        f["analysis_ready"] = bool(
            binary_ok and spot is not None and end is not None and causal_pair_ok and perp_ok
            and sel.perp_mid is not None and sel.index_price is not None)
        state, why = stability_state(f["analysis_ready"], f["vol_regime"], f["momentum_gap_z_60s"],
                                     f["premium_stress_5m"], f["perp_spread_ratio_5m"])
        f["perp_stability_state"], f["perp_stability_reasons"] = state, QUALITY_FLAG_DELIM.join(why)
        f["quality_flags"] = QUALITY_FLAG_DELIM.join(dict.fromkeys(flags))
        return f, sel

    def preview_row(self, coin, binary_result, spot_ts=None):
        """READ-ONLY causal feature preview for the live gate (spec 11).

        Uses only the background SnapshotStore (snapshots available at/before spot_ts) and the
        existing spot history plus the current observation. It makes NO request, writes no CSV,
        touches no counters/queue/session state, calls no callback, and appends nothing to any
        history or the store. Returns the same feature values the eventual telemetry row will
        contain, or None if it cannot be built."""
        try:
            r = dict(binary_result or {})
            s_ts = spot_ts if spot_ts is not None else _num(r.get("spot_observed_ts"))
            with self._lock:
                feats, sel = self._causal(coin, r, s_ts, mutate=False)
            row = dict(feats)
            row.update({
                "coin": coin, "telemetry_session_id": self.session_id, "cycle_id": None,
                "binary_status": r.get("status"), "binary_ticker": r.get("ticker"),
                "binary_close_time": r.get("close"), "minutes_left": r.get("remain"),
                "binary_signal": bool(r.get("signal")), "fav": r.get("fav"),
                "base_p_up": r.get("p_up"), "base_conf": r.get("conf"), "side_ask": r.get("side_ask"),
                "raw_edge": r.get("raw_edge"), "net_edge": r.get("net_edge"),
                "spot_price": _price(r.get("spot_raw", r.get("spot"))),
                "perp_symbol": sel.perp_symbol if sel is not None else None,
                "preview": True})
            return row
        except Exception as e:
            self.last_error = f"preview: {e}"[:200]
            return None

    # -- Step 1 legacy row (unchanged math; now fed spot_observed_ts) --
    def _row(self, coin, r, snap, s_ts, now):
        status, age_ms, idx_age = self.classify(snap, now)

        # contract re-scaling would break the per-contract price series -> reset it
        scale = (snap.contract_size, snap.underlying_multiplier)
        if None not in scale:
            if coin in self._scale and self._scale[coin] != scale:
                self.perp_hist[coin].clear()
            self._scale[coin] = scale

        if status == STATUS_FRESH and snap.perp_mid is not None:
            self.perp_hist[coin].append(snap.ts, snap.perp_mid)
        spot = _price(r.get("spot_raw", r.get("spot")))
        if spot is not None and s_ts is not None:
            self.spot_hist[coin].append(s_ts, spot)

        cur_ok = self.stale_seconds
        perp_rets = {h: (self.perp_hist[coin].return_bps(h, self.tolerance_s, now, cur_ok)
                         if status == STATUS_FRESH else None) for h in HORIZONS_S}
        spot_rets = {h: (self.spot_hist[coin].return_bps(h, self.tolerance_s, now, cur_ok)
                         if spot is not None else None) for h in HORIZONS_S}
        prem = premium_bps(snap.perp_mid, snap.index_price) if status == STATUS_FRESH else None

        row = {
            "ts_utc": dt.datetime.fromtimestamp(now, dt.timezone.utc).isoformat(),
            "ts_epoch_ms": int(round(now * 1000)), "coin": coin,
            "binary_status": r.get("status"), "binary_ticker": r.get("ticker"),
            "binary_close_time": r.get("close"), "minutes_left": r.get("remain"),
            "spot_source": SPOT_SOURCE, "spot_price": spot,
            "spot_ts_epoch_ms": int(round(s_ts * 1000)) if s_ts else None,
            "strike": r.get("strike"),
            "base_p_up": r.get("p_up"), "base_conf": r.get("conf"), "fav": r.get("fav"),
            "side_ask": r.get("side_ask"), "raw_edge": r.get("raw_edge"), "net_edge": r.get("net_edge"),
            "binary_signal": r.get("signal"), "binary_reason": r.get("reason"),
            "index_source": INDEX_SOURCE, "premium_bps": _r(prem),
            "perp_contract_size": snap.contract_size,
            "perp_underlying_multiplier": snap.underlying_multiplier,
            "perp_fetch_ts_epoch_ms": int(round(snap.ts * 1000)) if snap.ts else None,
            "perp_data_age_ms": age_ms, "index_age_ms": idx_age,
            "source_status": status, "source_error": snap.source_error,
        }
        for k in ("perp_symbol", "perp_market_status", "perp_bid", "perp_ask", "perp_mid", "perp_last",
                  "perp_mark", "perp_mark_ts_ms", "index_price", "index_ts_ms", "funding_rate",
                  "funding_next_time", "funding_computed_time"):
            row[k] = getattr(snap, k)
        for h in HORIZONS_S:
            row[f"perp_ret_{h}s_bps"] = _r(perp_rets[h])
            row[f"spot_ret_{h}s_bps"] = _r(spot_rets[h])
            row[f"lead_{h}s_bps"] = _r(lead_bps(perp_rets[h], spot_rets[h]))
        return row

    # -- health (for STATE["perp_health"]) --
    def health(self):
        now = self.clock()
        smp = self.sampler
        coins = {}
        for c in self.coins:
            last = self.store.latest(c)
            row = self.latest_rows.get(c) or {}
            coins[c] = {"latest_snapshot_age_s": round(now - last.avail, 1) if last else None,
                        "snapshots_held": self.store.count(c),
                        "analysis_ready": row.get("analysis_ready"),
                        "perp_lag_to_spot_ms": row.get("perp_lag_to_spot_ms"),
                        "quality_flags": row.get("quality_flags"),
                        "vol_regime": row.get("vol_regime"),
                        "perp_stability_state": row.get("perp_stability_state")}
        return {"telemetry_schema_version": TELEMETRY_SCHEMA_VERSION, "feature_version": FEATURE_VERSION,
                "session_id": self.session_id,
                "sampler_alive": bool(smp and smp.alive),
                "sampler_samples_total": smp.samples_total if smp else 0,
                "sampler_error_samples": smp.sample_errors if smp else 0,
                "sampler_last_sample_age_s": round(now - smp.last_sample_ts, 1) if smp and smp.last_sample_ts else None,
                "submitted_cycles_total": self.submitted_cycles_total,
                "processed_cycles_total": self.processed_cycles_total,
                "dropped_cycles_total": self.dropped_cycles_total,
                "worker_alive": bool(self._thread and self._thread.is_alive()),
                "last_error": self.last_error, "coins": coins}

    # -- background worker: the poller never waits on perp work --
    def submit(self, binary_results, spot_ts):
        """Non-blocking hand-off from the poller. Keeps only the newest cycle and
        COUNTS every cycle it has to discard."""
        with self._counter_lock:
            self.submitted_cycles_total += 1
            self._cycle_seq += 1
            cid = self._cycle_seq
        item = (cid, {c: dict(r or {}) for c, r in binary_results.items()}, dict(spot_ts or {}))
        try:
            self._q.put_nowait(item)
        except queue.Full:
            try:
                self._q.get_nowait()
                with self._counter_lock:
                    self.dropped_cycles_total += 1
            except queue.Empty:
                pass
            try:
                self._q.put_nowait(item)
            except queue.Full:
                with self._counter_lock:
                    self.dropped_cycles_total += 1     # lost the race: this cycle is dropped
        self._ensure_worker()

    def _ensure_worker(self):
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(target=self._worker, name="perp-telemetry", daemon=True)
            self._thread.start()

    def _worker(self):
        while True:
            try:
                cid, results, spot_ts = self._q.get()
                self.record_cycle(results, spot_ts, cycle_id=cid)
            except Exception as e:                    # never die
                self.last_error = f"worker: {e}"
                time.sleep(1)


def snapshot_dict(s: PerpSnapshot):
    return asdict(s)
