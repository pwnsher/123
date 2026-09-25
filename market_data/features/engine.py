"""
Causal, incremental research FEATURE ENGINE (one per asset). RESEARCH_ONLY.

    eng = FeatureEngine("BTC")
    for ev in events_in_availability_order:      # sorted by (receive_ts, ingest_seq)
        eng.ingest(ev)
    row = eng.features_at(T, market)             # every ingested event has receive_ts <= T (guarded)

Causality is structural:
  * ingest() requires non-decreasing receive times (availability order) and features_at(T) refuses a T
    earlier than any ingested receive time (CausalityError) -> nothing received after T can be inside.
  * RESOLUTION events (labels) are never ingested: the engine has no field for them.
  * compute_at(events, T) is the batch path: it filters with alignment.available() and replays into a
    fresh engine. The dataset builder's single pass is tested equal to it at every checkpoint.

Per event the work is O(log n) (buffer insert); features_at() does bounded work per feature (as-of
lookups on a 1-s grid, bisect ranges for trade windows). History is pruned to `retention_ms`.
"""
import math
from collections import deque
from dataclasses import dataclass, field

from market_data.alignment import available
from market_data.features import calc
from market_data.features.buffers import StreamBuffer
from market_data.features.definitions import (CFSPOT_VEL_HORIZONS, FEATURE_NAMES, HORIZONS_MS, KALSHI_SPREAD_HORIZONS,
                                              KALSHI_VEL_HORIZONS, LEADLAG_MAX_LAG_S, PRICE_SOURCES, RV_RATIOS,
                                              RV_SOURCES, RV_WINDOWS, SLOPE_WINDOWS, STRIKE_SOURCES, TRADE_SOURCES,
                                              VOLUME_BASELINE_MS, VOLUME_WINDOWS, XEX_RET_HORIZONS, FeatureConfig,
                                              Status)
from market_data.types import EventType
from settlement.reconstruction import reconstruct
from settlement.types import SettlementObservation

SPOT = ("coinbase", "kraken")
SOURCE_OF = {"cf_via_kalshi": "cf", "cf_direct": "cf", "coinbase": "coinbase", "kraken": "kraken", "kalshi": "kalshi"}


class CausalityError(ValueError):
    pass


@dataclass
class FeatureRow:
    t_ms: int
    asset: str
    ticker: str
    values: dict = field(default_factory=dict)
    status: dict = field(default_factory=dict)

    def set(self, name, value, status=Status.READY):
        if status in (Status.READY, Status.PARTIAL):
            if value is None or (isinstance(value, float) and not math.isfinite(value)):
                value, status = None, Status.UNDEFINED
        else:
            value = None
        self.values[name] = value
        self.status[name] = status

    def ready(self, name):
        return self.status.get(name) == Status.READY


class FeatureEngine:
    def __init__(self, asset, config=None, sources_enabled=("cf", "coinbase", "kraken", "kalshi")):
        self.asset = asset
        self.cfg = config or FeatureConfig()
        self.enabled = set(sources_enabled)
        self.px = {s: StreamBuffer() for s in ("cf",) + SPOT}          # cf value / venue mids
        self.last = {s: StreamBuffer() for s in SPOT}                   # last trade price
        self.trades = {s: StreamBuffer() for s in SPOT}                 # (size, aggressor)
        self.kalshi_trades, self.kalshi_state, self.kalshi_book = {}, {}, {}
        self.cf_obs = deque()                                           # SettlementObservation, arrival order
        self.alive = {}                                                 # source -> last receive (any message)
        self.first_seen = {}                                            # source -> first receive
        self.gaps = {}                                                  # source -> [Gap]
        self._seen = set()
        self._seen_order = deque()
        self.duplicates = 0
        self.max_receive = None
        self.ingested = 0

    # ---------------- ingestion ----------------
    def ingest(self, ev):
        if self.max_receive is not None and ev.receive_ts_ms < self.max_receive:
            raise CausalityError("events must be ingested in availability order (receive_ts non-decreasing)")
        self.max_receive = ev.receive_ts_ms
        src = SOURCE_OF.get(ev.source)
        if src is None or (ev.asset != self.asset and ev.asset != "*"):
            return False
        self.alive[src] = ev.receive_ts_ms
        self.first_seen.setdefault(src, ev.receive_ts_ms)
        if ev.asset == "*" or ev.event_type in (EventType.HEARTBEAT, EventType.FEED_STATUS, EventType.RESOLUTION,
                                                EventType.PUBLISHED_AVERAGE):
            return False                                # liveness only; RESOLUTION is a label and never stored
        key = ev.dedup_key()
        if key is not None:
            if key in self._seen:
                self.duplicates += 1                    # first (earliest) arrival wins
                return False
            self._seen.add(key)
            self._seen_order.append(key)
            if len(self._seen_order) > 500_000:
                self._seen.discard(self._seen_order.popleft())
        p, s, r, q = ev.payload, ev.series_ts_ms, ev.receive_ts_ms, ev.ingest_seq
        et = ev.event_type
        if et == EventType.INDEX_VALUE:
            self.px["cf"].add(s, r, q, p["value"])
            self.cf_obs.append(SettlementObservation.from_dict(p["observation"]))
        elif et == EventType.QUOTE and src in SPOT:
            if p.get("mid") is not None:
                self.px[src].add(s, r, q, p["mid"])
        elif et == EventType.TRADE and src in SPOT:
            self.last[src].add(s, r, q, p["price"])
            self.trades[src].add(s, r, q, (p["size"], p["aggressor"]))
        elif et == EventType.TRADE and src == "kalshi":
            self.kalshi_trades.setdefault(ev.symbol, StreamBuffer()).add(s, r, q, (p["size"], p["aggressor"]))
        elif et == EventType.MARKET_STATE:
            self.kalshi_state.setdefault(ev.symbol, StreamBuffer()).add(s, r, q, p)
        elif et == EventType.BOOK and src == "kalshi":
            self.kalshi_book.setdefault(ev.symbol, StreamBuffer()).add(s, r, q, p)
        self.ingested += 1
        if self.ingested % 5000 == 0:
            self.prune(self.max_receive - self.cfg.retention_ms)
        return True

    def ingest_gap(self, gap):
        src = SOURCE_OF.get(gap.source)
        if src is not None and gap.asset in (self.asset, "*"):
            self.gaps.setdefault(src, []).append(gap)

    def prune(self, before_ms):
        for b in list(self.px.values()) + list(self.last.values()) + list(self.trades.values()) + \
                list(self.kalshi_trades.values()) + list(self.kalshi_state.values()) + list(self.kalshi_book.values()):
            b.prune(before_ms)
        while self.cf_obs and self.cf_obs[0].event_ts_ms < before_ms:
            self.cf_obs.popleft()

    # ---------------- helpers ----------------
    def _gap_in(self, src, lo, hi, t):
        return any(g.overlaps(lo, hi) and (g.known_at_ms or g.end_ts_ms) <= t for g in self.gaps.get(src, ()))

    def _first(self, src):
        if src == "ref":
            fs = [self.px[s].first_series for s in SPOT if self.px[s].first_series is not None]
            return min(fs) if fs else None
        return self.px[src].first_series

    def price_at(self, src, t):
        if src == "ref":
            vals = [v for v in (self.px[s].asof(t, self.cfg.asof_max_age_ms) for s in SPOT) if v is not None]
            return calc.median([v[1] for v in vals]) if vals else None
        v = self.px[src].asof(t, self.cfg.asof_max_age_ms)
        return v[1] if v is not None else None

    def _src_enabled(self, src):
        if src == "ref":
            return bool(self.enabled & set(SPOT))
        return src in self.enabled

    def _lookback_status(self, src, t_back):
        first = self._first(src)
        if first is None:
            return Status.MISSING
        return Status.NOT_READY if t_back < first else Status.MISSING

    def _grid(self, src, t_end, w_ms):
        return calc.grid(lambda g: self.price_at(src, g), t_end, w_ms)

    def _grid_status(self, src, samples, t_start):
        if all(v is not None for v in samples):
            return Status.READY
        if self._first(src) is None or t_start < self._first(src):
            return Status.NOT_READY
        cov = sum(v is not None for v in samples) / len(samples)
        return Status.PARTIAL if (self.cfg.partial_windows and cov >= self.cfg.partial_min_coverage) else Status.MISSING

    # ---------------- arbitrary windows ----------------
    def window_features(self, t, w_ms, sources=("cf", "coinbase", "kraken", "ref")):
        """Research helper for ANY window length (the registry uses the standard windows 1s..5m):
        per price source logret / rv / absvol over the 1-s grid of (t - w, t], and per trade source
        volume / count / signed volume over (t - w, t]. Same causal rules and statuses as features_at()."""
        if self.max_receive is not None and t < self.max_receive:
            raise CausalityError(f"features at {t} requested after ingesting data received at {self.max_receive}")
        if w_ms < 1000 or w_ms % 1000:
            raise ValueError("window must be a positive whole number of seconds")
        row = FeatureRow(t, self.asset, "")
        tag = f"{w_ms // 1000}s"
        for s in sources:
            if not self._src_enabled(s):
                continue
            smp = self._grid(s, t, w_ms)
            st = self._grid_status(s, smp, t - w_ms)
            if st in (Status.READY, Status.PARTIAL):
                pts = [v for v in smp if v is not None]
                row.set(f"price.{s}.logret.{tag}", math.log(pts[-1] / pts[0]) if len(pts) > 1 else None, st)
                rv, _rvar, absv = calc.realized(smp)
                row.set(f"vol.{s}.rv.{tag}", rv, st)
                row.set(f"vol.{s}.absvol.{tag}", absv, st)
            else:
                for n in (f"price.{s}.logret.{tag}", f"vol.{s}.rv.{tag}", f"vol.{s}.absvol.{tag}"):
                    row.set(n, None, st)
        for s in SPOT:
            if s not in self.enabled:
                continue
            obs = self._observed(s, t - w_ms, t)
            tr = self.trades[s].range(t - w_ms, t) if obs == Status.READY else []
            row.set(f"volume.{s}.vol.{tag}", math.fsum(x[0] for x in tr) if obs == Status.READY else None, obs)
            row.set(f"volume.{s}.count.{tag}", float(len(tr)) if obs == Status.READY else None, obs)
            if obs == Status.READY and any(x[1] not in ("buy", "sell") for x in tr):
                row.set(f"flow.{s}.signed_vol.{tag}", None, Status.UNAVAILABLE)
            else:
                row.set(f"flow.{s}.signed_vol.{tag}",
                        math.fsum(x[0] if x[1] == "buy" else -x[0] for x in tr) if obs == Status.READY else None, obs)
        return row

    # ---------------- features ----------------
    def features_at(self, t, market=None):
        if self.max_receive is not None and t < self.max_receive:
            raise CausalityError(f"features at {t} requested after ingesting data received at {self.max_receive}")
        row = FeatureRow(t, self.asset, market.ticker if market is not None else "")
        rv5 = self._price_and_vol(row, t)
        self._trades(row, t, market)
        self._cross(row, t)
        self._strike(row, t, market, rv5)
        self._settlement(row, t, market)
        self._kalshi(row, t, market)
        for n in FEATURE_NAMES:                                  # every registered name is present, in order
            if n not in row.status:
                row.set(n, None, Status.UNAVAILABLE)
        row.values = {n: row.values[n] for n in FEATURE_NAMES}
        row.status = {n: row.status[n] for n in FEATURE_NAMES}
        return row

    def _price_and_vol(self, row, t):
        rv5 = {}
        for s in PRICE_SOURCES:
            if not self._src_enabled(s):
                continue
            p_now = self.price_at(s, t)
            if s == "ref":
                row.set("price.ref.last", p_now, Status.READY if p_now is not None else self._lookback_status(s, t))
            else:
                if s == "cf":
                    row.set("price.cf.last", p_now, Status.READY if p_now is not None else self._lookback_status(s, t))
                else:
                    lt = self.last[s].asof(t, self.cfg.trade_asof_max_age_ms)
                    row.set(f"price.{s}.last", lt[1] if lt else None, Status.READY if lt else Status.MISSING)
                    row.set(f"price.{s}.mid", p_now, Status.READY if p_now is not None else self._lookback_status(s, t))
            for h, hm in HORIZONS_MS.items():
                p_back, p_back2 = self.price_at(s, t - hm), self.price_at(s, t - 2 * hm)
                if p_now is not None and p_back is not None:
                    lr = math.log(p_now / p_back)
                    row.set(f"price.{s}.logret.{h}", lr)
                    row.set(f"price.{s}.ret.{h}", p_now / p_back - 1.0)
                    if p_back2 is not None:
                        row.set(f"price.{s}.accel.{h}", lr - math.log(p_back / p_back2))
                    else:
                        row.set(f"price.{s}.accel.{h}", None, self._lookback_status(s, t - 2 * hm))
                else:
                    st = self._lookback_status(s, t - hm) if p_now is not None else self._lookback_status(s, t)
                    for k in ("logret", "ret", "accel"):
                        row.set(f"price.{s}.{k}.{h}", None, st)
            for w in SLOPE_WINDOWS:
                wm = HORIZONS_MS[w]
                smp = self._grid(s, t, wm)
                st = self._grid_status(s, smp, t - wm)
                if st in (Status.READY, Status.PARTIAL):
                    slope, path, pers = calc.slope_path_persistence(smp)
                    row.set(f"price.{s}.slope.{w}", slope, st)
                    row.set(f"price.{s}.path.{w}", path, st)
                    row.set(f"price.{s}.persistence.{w}", pers, st if pers is not None else Status.UNDEFINED)
                else:
                    for k in ("slope", "path", "persistence"):
                        row.set(f"price.{s}.{k}.{w}", None, st)
        for s in RV_SOURCES:
            if not self._src_enabled(s):
                continue
            rvs = {}
            for w in RV_WINDOWS:
                wm = HORIZONS_MS[w]
                smp = self._grid(s, t, wm)
                st = self._grid_status(s, smp, t - wm)
                if st in (Status.READY, Status.PARTIAL):
                    rv, rvar, absv = calc.realized(smp)
                    rvs[w] = (rv, st)
                    row.set(f"vol.{s}.rv.{w}", rv, st)
                    row.set(f"vol.{s}.rvar.{w}", rvar, st)
                    row.set(f"vol.{s}.absvol.{w}", absv, st)
                    prev = self._grid(s, t - wm, wm)
                    pst = self._grid_status(s, prev, t - 2 * wm)
                    if pst in (Status.READY, Status.PARTIAL) and rv is not None:
                        prv = calc.realized(prev)[0]
                        row.set(f"vol.{s}.accel.{w}", None if prv is None else rv - prv,
                                Status.PARTIAL if Status.PARTIAL in (st, pst) else Status.READY)
                    else:
                        row.set(f"vol.{s}.accel.{w}", None, pst)
                else:
                    for k in ("rv", "rvar", "absvol", "accel"):
                        row.set(f"vol.{s}.{k}.{w}", None, st)
            for a, b in RV_RATIOS:
                name = f"vol.{s}.ratio.{a}_{b}"
                if a in rvs and b in rvs:
                    ra, rb = rvs[a][0] / math.sqrt(HORIZONS_MS[a] / 1000), rvs[b][0] / math.sqrt(HORIZONS_MS[b] / 1000)
                    row.set(name, ra / rb if rb > 0 else None, Status.READY if rb > 0 else Status.UNDEFINED)
                else:
                    row.set(name, None, row.status.get(f"vol.{s}.rv.{b}") if b not in rvs else row.status.get(f"vol.{s}.rv.{a}"))
            if "5m" in rvs:
                rv5[s] = rvs["5m"]
        return rv5

    def _trade_buf(self, src, market):
        if src == "kalshi":
            return self.kalshi_trades.get(market.ticker) if market is not None else None
        return self.trades[src]

    def _observed(self, src, lo, t):
        """READY if the source was observed throughout (lo, t]; else the reason it was not."""
        first = self.first_seen.get(src)
        if first is None:
            return Status.MISSING
        if lo < first:
            return Status.NOT_READY
        if t - self.alive.get(src, -10**18) > self.cfg.source_alive_ms or self._gap_in(src, lo, t, t):
            return Status.MISSING
        return Status.READY

    def _trades(self, row, t, market):
        for src in TRADE_SOURCES:
            if src not in self.enabled:
                continue
            buf = self._trade_buf(src, market)
            if src == "kalshi" and market is None:
                continue
            for w in VOLUME_WINDOWS:
                wm = HORIZONS_MS[w]
                obs = self._observed(src, t - wm, t)
                names = [f"volume.{src}.{k}.{w}" for k in ("vol", "count", "avg_size", "max_size", "rel")] + \
                        [f"flow.{src}.{k}.{w}" for k in ("buy_vol", "sell_vol", "signed_vol", "imbalance", "count_imbalance")]
                if obs != Status.READY:
                    for n in names:
                        row.set(n, None, obs)
                    continue
                tr = buf.range(t - wm, t) if buf is not None else []
                vol = math.fsum(x[0] for x in tr)
                n = len(tr)
                row.set(f"volume.{src}.vol.{w}", vol)
                row.set(f"volume.{src}.count.{w}", float(n))
                row.set(f"volume.{src}.avg_size.{w}", vol / n if n else None, Status.READY if n else Status.UNDEFINED)
                row.set(f"volume.{src}.max_size.{w}", max(x[0] for x in tr) if n else None,
                        Status.READY if n else Status.UNDEFINED)
                bobs = self._observed(src, t - VOLUME_BASELINE_MS, t - wm)
                if bobs == Status.READY:
                    base = buf.range(t - VOLUME_BASELINE_MS, t - wm) if buf is not None else []
                    bvol = math.fsum(x[0] for x in base)
                    rate_b = bvol / ((VOLUME_BASELINE_MS - wm) / 1000)
                    row.set(f"volume.{src}.rel.{w}", (vol / (wm / 1000)) / rate_b if rate_b > 0 else None,
                            Status.READY if rate_b > 0 else Status.UNDEFINED)
                else:
                    row.set(f"volume.{src}.rel.{w}", None, bobs)
                if any(x[1] not in ("buy", "sell") for x in tr):
                    for k in ("buy_vol", "sell_vol", "signed_vol", "imbalance", "count_imbalance"):
                        row.set(f"flow.{src}.{k}.{w}", None, Status.UNAVAILABLE)
                    continue
                buy = math.fsum(x[0] for x in tr if x[1] == "buy")
                sell = math.fsum(x[0] for x in tr if x[1] == "sell")
                nb = sum(1 for x in tr if x[1] == "buy")
                row.set(f"flow.{src}.buy_vol.{w}", buy)
                row.set(f"flow.{src}.sell_vol.{w}", sell)
                row.set(f"flow.{src}.signed_vol.{w}", buy - sell)
                row.set(f"flow.{src}.imbalance.{w}", (buy - sell) / (buy + sell) if buy + sell > 0 else None,
                        Status.READY if buy + sell > 0 else Status.UNDEFINED)
                row.set(f"flow.{src}.count_imbalance.{w}", (2 * nb - n) / n if n else None,
                        Status.READY if n else Status.UNDEFINED)
            name = f"flow.{src}.cvd_since_open"
            if market is None or market.open_ts_ms is None:
                row.set(name, None, Status.UNAVAILABLE)
            else:
                obs = self._observed(src, market.open_ts_ms, t)
                tr = buf.range(market.open_ts_ms, t) if (buf is not None and obs == Status.READY) else []
                if obs != Status.READY:
                    row.set(name, None, obs)
                elif any(x[1] not in ("buy", "sell") for x in tr):
                    row.set(name, None, Status.UNAVAILABLE)
                else:
                    row.set(name, math.fsum(x[0] if x[1] == "buy" else -x[0] for x in tr))

    def _cross(self, row, t):
        if not (self.enabled & set(SPOT)):
            return
        age = self.cfg.asof_max_age_ms
        mids = {s: self.px[s].asof(t, age) for s in SPOT}
        lasts = {s: self.last[s].asof(t, self.cfg.trade_asof_max_age_ms) for s in SPOT}
        avail = [v[1] for v in mids.values() if v is not None]
        row.set("xex.n_sources", float(len(avail)))
        both = mids["coinbase"] is not None and mids["kraken"] is not None
        st_pair = Status.READY if both else self._lookback_status("ref", t)
        if both:
            cb, kr = mids["coinbase"][1], mids["kraken"][1]
            row.set("xex.cb_minus_kr.mid", cb - kr)
            row.set("xex.cb_minus_kr.mid_bps", (cb - kr) / kr * 1e4)
            row.set("xex.dispersion_bps", (max(avail) - min(avail)) / calc.median(avail) * 1e4)
        else:
            for n in ("xex.cb_minus_kr.mid", "xex.cb_minus_kr.mid_bps", "xex.dispersion_bps"):
                row.set(n, None, st_pair)
        if lasts["coinbase"] is not None and lasts["kraken"] is not None:
            row.set("xex.cb_minus_kr.last", lasts["coinbase"][1] - lasts["kraken"][1])
        else:
            row.set("xex.cb_minus_kr.last", None, Status.MISSING)
        for h in XEX_RET_HORIZONS:
            a, b = row.values.get(f"price.coinbase.logret.{h}"), row.values.get(f"price.kraken.logret.{h}")
            if row.ready(f"price.coinbase.logret.{h}") and row.ready(f"price.kraken.logret.{h}"):
                row.set(f"xex.retdiv.{h}", a - b)
            else:
                row.set(f"xex.retdiv.{h}", None, calc.worst_status(row.status.get(f"price.coinbase.logret.{h}"),
                                                                   row.status.get(f"price.kraken.logret.{h}")))
        g_cb, g_kr = self._grid("coinbase", t, 60_000), self._grid("kraken", t, 60_000)
        st = calc.worst_status(self._grid_status("coinbase", g_cb, t - 60_000), self._grid_status("kraken", g_kr, t - 60_000))
        if st == Status.READY:
            lag, corr = calc.lead_lag(g_cb, g_kr, LEADLAG_MAX_LAG_S)
            row.set("xex.leadlag.lag_s", None if lag is None else float(lag), Status.READY if lag is not None else Status.UNDEFINED)
            row.set("xex.leadlag.corr", corr, Status.READY if corr is not None else Status.UNDEFINED)
        else:
            row.set("xex.leadlag.lag_s", None, Status.MISSING if st == Status.PARTIAL else st)
            row.set("xex.leadlag.corr", None, Status.MISSING if st == Status.PARTIAL else st)
        if "cf" not in self.enabled:
            return
        cf = self.price_at("cf", t)
        for s in ("coinbase", "kraken", "ref"):
            m = self.price_at(s, t)
            if cf is not None and m is not None:
                row.set(f"cfspot.cf_minus_{s}", cf - m)
                row.set(f"cfspot.cf_minus_{s}_bps", (cf - m) / m * 1e4)
            else:
                stt = calc.worst_status(self._lookback_status("cf", t) if cf is None else Status.READY,
                                        self._lookback_status(s, t) if m is None else Status.READY)
                row.set(f"cfspot.cf_minus_{s}", None, stt)
                row.set(f"cfspot.cf_minus_{s}_bps", None, stt)
        for h in XEX_RET_HORIZONS:
            if row.ready(f"price.cf.logret.{h}") and row.ready(f"price.ref.logret.{h}"):
                row.set(f"cfspot.retdiv.{h}", row.values[f"price.cf.logret.{h}"] - row.values[f"price.ref.logret.{h}"])
            else:
                row.set(f"cfspot.retdiv.{h}", None, calc.worst_status(row.status.get(f"price.cf.logret.{h}"),
                                                                      row.status.get(f"price.ref.logret.{h}")))
        b_now = calc.basis_bps(cf, self.price_at("ref", t))
        for h in CFSPOT_VEL_HORIZONS:
            hm = HORIZONS_MS[h]
            b_back = calc.basis_bps(self.price_at("cf", t - hm), self.price_at("ref", t - hm))
            if b_now is not None and b_back is not None:
                row.set(f"cfspot.div_velocity.{h}", (b_now - b_back) / (hm / 1000))
            else:
                row.set(f"cfspot.div_velocity.{h}", None,
                        calc.worst_status(self._lookback_status("cf", t - hm), self._lookback_status("ref", t - hm)))

    def _strike(self, row, t, market, rv5):
        if market is None or market.strike is None:
            return
        sec = (market.close_ts_ms - t) / 1000.0
        row.set("strike.seconds_remaining", sec)
        k = market.strike
        for s in STRIKE_SOURCES:
            if not self._src_enabled(s):
                continue
            p = self.price_at(s, t)
            if p is None:
                st = self._lookback_status(s, t)
                for n in ("dist", "dist_pct", "dist_volnorm"):
                    row.set(f"strike.{s}.{n}", None, st)
                continue
            row.set(f"strike.{s}.dist", p - k)
            row.set(f"strike.{s}.dist_pct", (p - k) / k)
            rv = rv5.get(s)
            if rv is None:
                row.set(f"strike.{s}.dist_volnorm", None, row.status.get(f"vol.{s}.rv.5m", Status.MISSING))
            else:
                sigma = rv[0] / math.sqrt(300.0) * math.sqrt(max(sec, 1.0))
                row.set(f"strike.{s}.dist_volnorm", math.log(p / k) / sigma if sigma > 0 else None,
                        (rv[1] if sigma > 0 else Status.UNDEFINED))

    def _settlement(self, row, t, market):
        if market is None or "cf" not in self.enabled:
            return
        # every retained observation of the index (Step-2 quality, e.g. staleness before the window, needs them);
        # all of them were received by t (ingest order) and reconstruct() filters again as of t
        obs = [o for o in self.cf_obs if o.index_id == market.index_id]
        st = reconstruct(market, obs, as_of_ms=t).state
        # None before the window opens is "not yet" (NOT_READY); inside/after the window it is a data problem
        empty = Status.NOT_READY if st.phase.value == "PRE_WINDOW" else Status.MISSING
        num = lambda v: (v, Status.READY) if v is not None else (None, empty)  # noqa: E731
        cur = self.price_at("cf", t)
        row.set("settle.current_index", cur, Status.READY if cur is not None else self._lookback_status("cf", t))
        row.set("settle.accumulated_mean", *num(st.accumulated_mean))
        row.set("settle.observations_seen", float(st.observations_seen))
        row.set("settle.samples_expected", float(st.samples_expected))
        row.set("settle.samples_filled", float(st.samples_filled))
        row.set("settle.coverage_elapsed", *num(st.coverage_elapsed))
        age = (t - self.px["cf"].asof(t, 10**12)[0]) / 1000.0 if self.px["cf"].asof(t, 10**12) else None
        row.set("settle.last_observation_age_s", age, Status.READY if age is not None else Status.MISSING)
        row.set("settle.seconds_remaining", st.seconds_remaining)
        row.set("settle.accum_minus_strike", *(num(st.accumulated_mean - market.strike)
                                                if (st.accumulated_mean is not None and market.strike is not None)
                                                else num(None)))
        row.set("settle.quality", st.quality.value)
        row.set("settle.phase", st.phase.value)

    def _kalshi(self, row, t, market):
        if market is None or "kalshi" not in self.enabled:
            return
        age = self.cfg.kalshi_max_age_ms
        sb, bb = self.kalshi_state.get(market.ticker), self.kalshi_book.get(market.ticker)
        cur = sb.asof(t, age) if sb is not None else None
        names = ("yes_bid", "yes_ask", "no_bid", "no_ask", "yes_mid", "yes_spread", "no_mid", "no_spread",
                 "exec_yes_ask", "exec_no_ask", "implied_prob", "state_age_s")
        row.set("kalshi.model_vs_market", None, Status.UNAVAILABLE)
        if cur is None:
            for n in names:                                  # no fresh state for this market
                row.set(f"kalshi.{n}", None, Status.MISSING)
        else:
            q = calc.kalshi_quote(cur[1])
            for n in names[:-1]:
                v = q[n]
                row.set(f"kalshi.{n}", v, Status.READY if v is not None else Status.MISSING)
            row.set("kalshi.state_age_s", (t - cur[0]) / 1000.0)
        for h in KALSHI_VEL_HORIZONS:
            hm = HORIZONS_MS[h]
            back = sb.asof(t - hm, age) if sb is not None else None
            now_mid = calc.kalshi_quote(cur[1])["yes_mid"] if cur else None
            back_mid = calc.kalshi_quote(back[1])["yes_mid"] if back else None
            if now_mid is not None and back_mid is not None:
                row.set(f"kalshi.yes_mid_velocity.{h}", (now_mid - back_mid) / (hm / 1000))
            else:
                row.set(f"kalshi.yes_mid_velocity.{h}", None,
                        Status.NOT_READY if (sb is not None and sb.first_series is not None and t - hm < sb.first_series)
                        else Status.MISSING)
        for h in KALSHI_SPREAD_HORIZONS:
            hm = HORIZONS_MS[h]
            back = sb.asof(t - hm, age) if sb is not None else None
            a = calc.kalshi_quote(cur[1])["yes_spread"] if cur else None
            b = calc.kalshi_quote(back[1])["yes_spread"] if back else None
            if a is not None and b is not None:
                row.set(f"kalshi.spread_change.{h}", a - b)
            else:
                row.set(f"kalshi.spread_change.{h}", None,
                        Status.NOT_READY if (sb is not None and sb.first_series is not None and t - hm < sb.first_series)
                        else Status.MISSING)
        bk = bb.asof(t, age) if bb is not None else None
        dnames = ("yes_bid_qty", "yes_ask_qty", "imbalance", "weighted_mid", "book_age_s")
        if bk is None:
            for n in dnames:
                row.set(f"kalshi.depth.{n}", None, Status.MISSING if bb is not None else Status.UNAVAILABLE)
            return
        d = calc.book_top(bk[1])
        for n in dnames[:-1]:
            v = d[n]
            row.set(f"kalshi.depth.{n}", v, Status.READY if v is not None else
                    (Status.UNDEFINED if n in ("imbalance", "weighted_mid") and d["yes_bid_qty"] is not None else Status.MISSING))
        row.set("kalshi.depth.book_age_s", (t - bk[0]) / 1000.0)


def compute_at(asset, events, t, market=None, gaps=(), config=None, sources_enabled=None):
    """BATCH path: only events available at t (receive_ts <= t), replayed in availability order."""
    eng = FeatureEngine(asset, config, sources_enabled or ("cf", "coinbase", "kraken", "kalshi"))
    for g in gaps:
        if (g.known_at_ms if g.known_at_ms is not None else g.end_ts_ms) <= t:
            eng.ingest_gap(g)
    for e in sorted((e for e in events if available(e, t)), key=lambda e: (e.receive_ts_ms, e.ingest_seq)):
        eng.ingest(e)
    return eng.features_at(t, market)
