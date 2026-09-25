"""
Causal, incremental PERP FEATURE ENGINE (one per asset). RESEARCH ONLY.

    eng = PerpFeatureEngine("BTC")
    for ev in events_in_availability_order:       # PerpEvents AND Step-3 MarketEvents (spot / CF), merged by
        eng.ingest(ev)                            # (receive_ts, ingest_seq)
    row = eng.features_at(T)                      # every ingested event has receive_ts <= T (guarded)

The Step-3 guards apply unchanged: availability-order ingestion (non-decreasing receive_ts, else
CausalityError), no features before the last ingested receive time, labels never ingested, gaps visible
from the time they were detected. Per-venue features, then transparent cross-venue aggregates (medians,
dispersions, totals that require EVERY size venue to be observed) and perp-vs-spot / perp-vs-CF divergence.
Missing never becomes 0: a window with no liquidations is 0 only when the venue was observed throughout it.
"""
import math
import statistics

from market_data.features import calc
from market_data.features.buffers import StreamBuffer
from market_data.features.engine import CausalityError, FeatureRow
from market_data.alignment import available
from market_data.types import EventType, MarketEvent
from perp_data.features.definitions import (BASIS_ACC_H, BASIS_CHG_H, BOOK_CHG_H, CVD_W, DEPTHS, DIV_FLOW_W, DIV_H,
                                            DIV_RV, FEATURE_NAMES, FLOW_W, FUND_CHG_H, HORIZONS_MS, LIQ_W, MOVE_RV_H,
                                            OI_ACC_H, OI_H, RET_H, X_OI_H, X_RET_H, PerpFeatureConfig, Status)
from perp_data.types import PerpEvent, PerpEventType as T
from perp_data.venues import PERP_VENUES, SIZE_VENUES, VENUES

SPOT_SOURCES = ("coinbase", "kraken")
CF_SOURCES = ("cf_via_kalshi", "cf_direct")
S = Status


def _med(vals):
    return statistics.median(vals) if vals else None


def _ols_slope(ys, step_s=1.0):
    n = len(ys)
    if n < 2:
        return None
    xs = [i * step_s for i in range(n)]
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    sxx = math.fsum((x - mx) ** 2 for x in xs)
    return math.fsum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx if sxx > 0 else None


class PerpFeatureEngine:
    def __init__(self, asset, config=None, venues=PERP_VENUES, spot=True):
        self.asset = asset
        self.cfg = config or PerpFeatureConfig()
        self.venues = tuple(v for v in PERP_VENUES if v in venues)
        self.size_venues = tuple(v for v in self.venues if v in SIZE_VENUES)
        self.spot_enabled = spot
        B = StreamBuffer
        self.mid, self.quote, self.book, self.last = ({v: B() for v in self.venues} for _ in range(4))
        self.mark, self.index, self.fund, self.pred, self.oi = ({v: B() for v in self.venues} for _ in range(5))
        self.trades, self.liq = ({v: B() for v in self.venues} for _ in range(2))
        self.spot_mid = {s: B() for s in SPOT_SOURCES}
        self.spot_trades = B()
        self.cf = B()
        self.usdt = B()
        self.alive, self.first_seen, self.gaps = {}, {}, {}
        self._seen = set()
        self.duplicates = 0
        self.max_receive = None
        self.ingested = 0

    # ================= ingestion =================
    def ingest(self, ev):
        if self.max_receive is not None and ev.receive_ts_ms < self.max_receive:
            raise CausalityError("events must be ingested in availability order (receive_ts non-decreasing)")
        self.max_receive = ev.receive_ts_ms
        if isinstance(ev, MarketEvent):
            return self._ingest_spot(ev)
        if not isinstance(ev, PerpEvent):
            return False
        src = ev.source
        if ev.event_type == T.STABLECOIN_RATE:
            self._mark_alive(src, ev)
            if ev.payload.get("mid") is not None:
                self.usdt.add(ev.series_ts_ms, ev.receive_ts_ms, ev.ingest_seq, ev.payload["mid"])
                self.ingested += 1
            return True
        if src not in self.venues or ev.asset not in (self.asset, "*"):
            return False
        self._mark_alive(src, ev)
        if ev.asset == "*" or ev.event_type in (T.HEARTBEAT, T.INSTRUMENT, T.FUNDING_SETTLED, T.ORDERBOOK_UPDATE):
            return False
        key = ev.dedup_key()
        if key is not None:
            if key in self._seen:
                self.duplicates += 1                          # first (earliest) arrival wins
                return False
            self._seen.add(key)
        p, s, r, q = ev.payload, ev.series_ts_ms, ev.receive_ts_ms, ev.ingest_seq
        et = ev.event_type
        if et == T.PERP_TRADE:
            self.last[src].add(s, r, q, p["price"])
            self.trades[src].add(s, r, q, (p["qty_coin"], p["notional_quote"], p["aggressor"]))
        elif et == T.PERP_QUOTE:
            self.quote[src].add(s, r, q, (p["bid"], p["bid_qty_coin"], p["ask"], p["ask_qty_coin"]))
            if p["mid"] is not None:
                self.mid[src].add(s, r, q, p["mid"])
        elif et == T.ORDERBOOK_TOP:
            self.book[src].add(s, r, q, (p["bids"], p["asks"]))
            if p["bids"] and p["asks"]:
                (b, bq), (a, aq) = p["bids"][0], p["asks"][0]
                self.quote[src].add(s, r, q, (b, bq, a, aq))
                if a >= b:
                    self.mid[src].add(s, r, q, (a + b) / 2)
        elif et == T.PERP_MARK_PRICE:
            self.mark[src].add(s, r, q, p["mark"])
        elif et == T.PERP_INDEX_PRICE:
            self.index[src].add(s, r, q, p["index"])
        elif et == T.FUNDING_RATE:
            self.fund[src].add(s, r, q, p)
        elif et == T.PREDICTED_FUNDING:
            self.pred[src].add(s, r, q, p)
        elif et == T.OPEN_INTEREST:
            self.oi[src].add(s, r, q, p["oi_coin"])
        elif et == T.LIQUIDATION:
            self.liq[src].add(s, r, q, (p["notional_quote"], p["forced_side"], p["liquidated_position"]))
        self.ingested += 1
        if self.ingested % 5000 == 0:
            self.prune(self.max_receive - self.cfg.retention_ms)
        return True

    def _mark_alive(self, src, ev):
        # liveness of a websocket venue counts only websocket traffic: REST polls (e.g. Binance OI) keep arriving
        # during a websocket outage and must not make trade / liquidation windows look observed
        if getattr(ev, "channel", "ws") == "rest" and VENUES.get(src) is not None and src != "kalshi_perp":
            return
        self.alive[src] = ev.receive_ts_ms
        self.first_seen.setdefault(src, ev.receive_ts_ms)

    def _ingest_spot(self, ev):
        if ev.asset != self.asset or not self.spot_enabled:
            return False
        src = ev.source
        if src in SPOT_SOURCES or src in CF_SOURCES:
            self._mark_alive(src if src in SPOT_SOURCES else "cf", ev)
        p = ev.payload
        if ev.event_type == EventType.QUOTE and src in SPOT_SOURCES and p.get("mid") is not None:
            self.spot_mid[src].add(ev.series_ts_ms, ev.receive_ts_ms, ev.ingest_seq, p["mid"])
        elif ev.event_type == EventType.TRADE and src == "coinbase":
            key = ev.dedup_key()
            if key in self._seen:
                return False
            self._seen.add(key)
            self.spot_trades.add(ev.series_ts_ms, ev.receive_ts_ms, ev.ingest_seq, (p["price"] * p["size"], p["aggressor"]))
        elif ev.event_type == EventType.INDEX_VALUE and src in CF_SOURCES:
            self.cf.add(ev.series_ts_ms, ev.receive_ts_ms, ev.ingest_seq, p["value"])
        else:
            return False
        return True

    def ingest_gap(self, gap):
        src = "cf" if gap.source in CF_SOURCES else gap.source
        if gap.asset in (self.asset, "*", "USDT"):
            self.gaps.setdefault(src, []).append(gap)

    def prune(self, before_ms):
        for group in (self.mid, self.quote, self.book, self.last, self.mark, self.index, self.fund, self.pred,
                      self.oi, self.trades, self.liq, self.spot_mid):
            for b in group.values():
                b.prune(before_ms)
        for b in (self.spot_trades, self.cf, self.usdt):
            b.prune(before_ms)

    # ================= helpers =================
    def _asof(self, buf, t, age=None):
        v = buf.asof(t, self.cfg.asof_max_age_ms if age is None else age)
        return v[1] if v is not None else None

    def _lookback(self, buf, t_back):
        if buf.first_series is None:
            return S.MISSING
        return S.NOT_READY if t_back < buf.first_series else S.MISSING

    def _observed(self, src, lo, t):
        first = self.first_seen.get(src)
        if first is None:
            return S.MISSING
        if lo < first:
            return S.NOT_READY
        if t - self.alive.get(src, -10**18) > self.cfg.source_alive_ms:
            return S.MISSING
        known = [g for g in self.gaps.get(src, ()) if (g.known_at_ms if g.known_at_ms is not None else g.end_ts_ms) <= t]
        for g in known:
            if g.kind == "DISCONNECT_OPEN":
                closes = [c.end_ts_ms for c in known if c.kind == "DISCONNECT" and c.start_ts_ms == g.start_ts_ms]
                end = min(closes) if closes else float("inf")          # still open as far as anyone knew at t
                if g.start_ts_ms < t and end > lo:
                    return S.MISSING
            elif g.overlaps(lo, t):
                return S.MISSING
        return S.READY

    def _grid(self, buf, t_end, w_ms, age=None):
        return calc.grid(lambda g: self._asof(buf, g, age), t_end, w_ms)

    def _grid_status(self, buf, samples, t_start):
        if all(v is not None for v in samples):
            return S.READY
        if buf.first_series is None or t_start < buf.first_series:
            return S.NOT_READY if buf.first_series is not None else S.MISSING
        cov = sum(v is not None for v in samples) / len(samples)
        return S.PARTIAL if (self.cfg.partial_windows and cov >= self.cfg.partial_min_coverage) else S.MISSING

    def usdt_rate(self, t):
        return self._asof(self.usdt, t, self.cfg.usdt_max_age_ms)

    def to_usd(self, v, px, t):
        """USD value of a venue price at t; None when a USDT price cannot be converted (no fresh rate)."""
        q = VENUES[v].quote_ccy
        if px is None:
            return None
        if q == "USD":
            return px
        if q == "USDT":
            r = self.usdt_rate(t)
            return px * r if r is not None else None
        return None

    def ref_at(self, t):
        vals = [x for x in (self._asof(self.spot_mid[s], t) for s in SPOT_SOURCES) if x is not None]
        return _med(vals)

    def _ref_first(self):
        fs = [self.spot_mid[s].first_series for s in SPOT_SOURCES if self.spot_mid[s].first_series is not None]
        return min(fs) if fs else None

    def _ref_lookback(self, t_back):
        f = self._ref_first()
        if f is None:
            return S.MISSING
        return S.NOT_READY if t_back < f else S.MISSING

    def basis_ref_bps(self, v, t):
        m, ref = self.to_usd(v, self._asof(self.mark[v], t), t), self.ref_at(t)
        if m is None or ref is None:
            return None
        return (m - ref) / ref * 1e4

    def _basis_status(self, v, t):
        if VENUES[v].quote_ccy not in ("USD", "USDT"):
            return S.UNAVAILABLE
        return calc.worst_status(S.READY if self._asof(self.mark[v], t) is not None else self._lookback(self.mark[v], t),
                                 S.READY if self.ref_at(t) is not None else self._ref_lookback(t),
                                 S.READY if (VENUES[v].quote_ccy == "USD" or self.usdt_rate(t) is not None) else
                                 (S.MISSING if self.usdt.first_series is not None and t >= self.usdt.first_series else
                                  (S.NOT_READY if self.usdt.first_series is not None else S.MISSING)))

    def _window_vals(self, buf, src, lo, hi):
        """(status, values) of an event window (lo, hi]: READY only if the source was observed throughout."""
        st = self._observed(src, lo, hi)
        return (st, buf.range(lo, hi)) if st == S.READY else (st, None)

    # ================= features =================
    def features_at(self, t, market=None):
        if self.max_receive is not None and t < self.max_receive:
            raise CausalityError(f"features at {t} requested after ingesting data received at {self.max_receive}")
        row = FeatureRow(t, self.asset, market.ticker if market is not None else "")
        for v in PERP_VENUES:
            if v not in self.venues:
                continue
            self._price(row, v, t)
            self._basis(row, v, t)
            self._funding(row, v, t)
            if v in self.size_venues:
                self._oi(row, v, t)
                self._flow_cvd(row, v, t)
                self._liquidations(row, v, t)
            self._book(row, v, t)
        self._cross(row, t)
        self._divergence(row, t)
        for n in FEATURE_NAMES:
            if n not in row.status:
                row.set(n, None, S.UNAVAILABLE)
        row.values = {n: row.values[n] for n in FEATURE_NAMES}
        row.status = {n: row.status[n] for n in FEATURE_NAMES}
        return row

    # ---------- PRICE ----------
    def _price(self, row, v, t):
        p = f"perp.{v}"
        if v in self.size_venues:
            lt = self.last[v].asof(t, 60_000)
            row.set(f"{p}.last", lt[1] if lt else None, S.READY if lt else self._lookback(self.last[v], t) if self.last[v].first_series else S.MISSING)
        for name, buf in (("mid", self.mid[v]), ("mark", self.mark[v]), ("index", self.index[v])):
            x = self._asof(buf, t)
            row.set(f"{p}.{name}", x, S.READY if x is not None else self._lookback(buf, t))
        for name, buf in (("mid", self.mid[v]), ("mark", self.mark[v])):
            now = self._asof(buf, t)
            for h in RET_H:
                back = self._asof(buf, t - HORIZONS_MS[h])
                if now is not None and back is not None:
                    row.set(f"{p}.{name}_logret.{h}", math.log(now / back))
                else:
                    row.set(f"{p}.{name}_logret.{h}", None,
                            self._lookback(buf, t) if now is None else self._lookback(buf, t - HORIZONS_MS[h]))
        rv = {}
        for w in ("60s", "5m"):
            smp = self._grid(self.mid[v], t, HORIZONS_MS[w])
            st = self._grid_status(self.mid[v], smp, t - HORIZONS_MS[w])
            val = calc.realized(smp)[0] if st in (S.READY, S.PARTIAL) else None
            rv[w] = (val, st)
            row.set(f"{p}.mid_rv.{w}", val, st)
        for h in MOVE_RV_H:
            r5, st5 = rv["5m"]
            lr = row.values.get(f"{p}.mid_logret.{h}")
            if lr is not None and r5 is not None and r5 > 0:
                row.set(f"{p}.move_over_rv.{h}", lr / (r5 * math.sqrt(HORIZONS_MS[h] / 300_000)),
                        calc.worst_status(row.status[f"{p}.mid_logret.{h}"], st5))
            else:
                row.set(f"{p}.move_over_rv.{h}", None,
                        S.UNDEFINED if (lr is not None and r5 == 0) else calc.worst_status(row.status[f"{p}.mid_logret.{h}"], st5))

    # ---------- BASIS ----------
    def _basis(self, row, v, t):
        p = f"perp.{v}"
        mark, idx, md = self._asof(self.mark[v], t), self._asof(self.index[v], t), self._asof(self.mid[v], t)
        for name, a, abuf in (("mark_minus_index_bps", mark, self.mark[v]), ("mid_minus_index_bps", md, self.mid[v])):
            if a is not None and idx is not None:
                row.set(f"{p}.{name}", (a - idx) / idx * 1e4)
            else:
                row.set(f"{p}.{name}", None, calc.worst_status(S.READY if a is not None else self._lookback(abuf, t),
                                                                S.READY if idx is not None else self._lookback(self.index[v], t)))
        mi = calc.grid(lambda g: self._mi_bps(v, g), t, 300_000)
        st = calc.worst_status(self._grid_status(self.mark[v], mi, t - 300_000), self._grid_status(self.index[v], mi, t - 300_000))
        if st in (S.READY, S.PARTIAL):
            xs = [x for x in mi if x is not None]
            sd = statistics.pstdev(xs) if len(xs) > 1 else 0.0
            row.set(f"{p}.basis_z_5m", (xs[-1] - statistics.fmean(xs)) / sd if sd > 0 and mi[-1] is not None else None,
                    st if sd > 0 and mi[-1] is not None else S.UNDEFINED)
        else:
            row.set(f"{p}.basis_z_5m", None, st)
        if VENUES[v].quote_ccy not in ("USD", "USDT"):
            return                                                  # Kalshi perps: per-contract units -> UNAVAILABLE
        b_now = self.basis_ref_bps(v, t)
        st_now = S.READY if b_now is not None else self._basis_status(v, t)
        m_usd, ref = self.to_usd(v, mark, t), self.ref_at(t)
        row.set(f"{p}.mark_minus_ref_usd", (m_usd - ref) if (m_usd is not None and ref is not None) else None, st_now)
        row.set(f"{p}.mark_minus_ref_bps", b_now, st_now)
        row.set(f"{p}.mark_minus_ref_raw_bps", (mark - ref) / ref * 1e4 if (mark is not None and ref is not None) else None,
                calc.worst_status(S.READY if mark is not None else self._lookback(self.mark[v], t),
                                  S.READY if ref is not None else self._ref_lookback(t)))
        md_usd = self.to_usd(v, md, t)
        for name, sm, sbuf in (("mid_minus_coinbase_bps", self._asof(self.spot_mid["coinbase"], t), self.spot_mid["coinbase"]),
                               ("mid_minus_kraken_bps", self._asof(self.spot_mid["kraken"], t), self.spot_mid["kraken"]),
                               ("mark_minus_cf_bps", self._asof(self.cf, t), self.cf)):
            a = m_usd if name.startswith("mark") else md_usd
            if a is not None and sm is not None:
                row.set(f"{p}.{name}", (a - sm) / sm * 1e4)
            else:
                abuf = self.mark[v] if name.startswith("mark") else self.mid[v]
                row.set(f"{p}.{name}", None, calc.worst_status(
                    S.READY if a is not None else calc.worst_status(self._lookback(abuf, t) if (self._asof(abuf, t) is None) else S.READY,
                                                                    self._basis_status(v, t) if self.usdt_rate(t) is None else S.READY),
                    S.READY if sm is not None else self._lookback(sbuf, t)))
        for h in BASIS_CHG_H:
            b_back = self.basis_ref_bps(v, t - HORIZONS_MS[h])
            if b_now is not None and b_back is not None:
                row.set(f"{p}.basis_change_bps.{h}", b_now - b_back)
            else:
                row.set(f"{p}.basis_change_bps.{h}", None, st_now if b_now is None else self._basis_status(v, t - HORIZONS_MS[h]))
        for h in BASIS_ACC_H:
            hm = HORIZONS_MS[h]
            b1, b2 = self.basis_ref_bps(v, t - hm), self.basis_ref_bps(v, t - 2 * hm)
            if b_now is not None and b1 is not None and b2 is not None:
                row.set(f"{p}.basis_accel_bps.{h}", (b_now - b1) - (b1 - b2))
            else:
                row.set(f"{p}.basis_accel_bps.{h}", None,
                        st_now if b_now is None else self._basis_status(v, t - hm) if b1 is None else self._basis_status(v, t - 2 * hm))
        rv5 = row.values.get(f"{p}.mid_rv.5m")
        if b_now is not None and rv5 is not None and rv5 > 0:
            row.set(f"{p}.basis_over_vol", b_now / (rv5 * 1e4), calc.worst_status(st_now, row.status[f"{p}.mid_rv.5m"]))
        else:
            row.set(f"{p}.basis_over_vol", None,
                    S.UNDEFINED if (b_now is not None and rv5 == 0) else calc.worst_status(st_now, row.status.get(f"{p}.mid_rv.5m")))

    def _mi_bps(self, v, g):
        m, i = self._asof(self.mark[v], g), self._asof(self.index[v], g)
        return (m - i) / i * 1e4 if (m is not None and i is not None) else None

    # ---------- FUNDING ----------
    def _funding(self, row, v, t):
        p = f"perp.{v}"
        age = self.cfg.funding_max_age_ms
        cur = self._asof(self.fund[v], t, age)
        st_cur = S.READY if cur is not None else self._lookback(self.fund[v], t)
        row.set(f"{p}.funding_rate_native", cur["rate_native"] if cur else None, st_cur)
        norm_ok = cur is not None and cur.get("rate_8h") is not None
        st_norm = S.READY if norm_ok else (S.UNAVAILABLE if cur is not None else st_cur)
        row.set(f"{p}.funding_rate_8h", cur["rate_8h"] if norm_ok else None, st_norm)
        row.set(f"{p}.funding_rate_annual", cur["rate_annual_simple"] if norm_ok else None, st_norm)
        row.set(f"{p}.funding_interval_h", cur["interval_ms"] / 3_600_000 if (cur and cur.get("interval_ms")) else None,
                S.READY if (cur and cur.get("interval_ms")) else (S.UNAVAILABLE if cur is not None else st_cur))
        nt = cur.get("next_funding_ts_ms") if cur else None
        row.set(f"{p}.funding_secs_to_next", (nt - t) / 1000.0 if nt is not None else None,
                S.READY if nt is not None else (S.UNAVAILABLE if cur is not None else st_cur))
        if VENUES[v].publishes_predicted_funding:
            pr = self._asof(self.pred[v], t, age)
            row.set(f"{p}.funding_predicted_8h", pr["rate_8h"] if pr else None,
                    S.READY if (pr and pr.get("rate_8h") is not None) else S.UNAVAILABLE)
        r8 = lambda tt: (lambda x: x["rate_8h"] if x else None)(self._asof(self.fund[v], tt, age))  # noqa: E731
        now = cur["rate_8h"] if norm_ok else None
        for h in FUND_CHG_H:
            back = r8(t - HORIZONS_MS[h])
            if now is not None and back is not None:
                row.set(f"{p}.funding_change_8h.{h}", now - back)
            else:
                row.set(f"{p}.funding_change_8h.{h}", None, st_norm if now is None else self._lookback(self.fund[v], t - HORIZONS_MS[h]))
        b1, b2 = r8(t - 300_000), r8(t - 600_000)
        if now is not None and b1 is not None and b2 is not None:
            row.set(f"{p}.funding_accel_8h.5m", (now - b1) - (b1 - b2))
        else:
            row.set(f"{p}.funding_accel_8h.5m", None, st_norm if now is None else self._lookback(self.fund[v], t - 600_000))
        if now is not None:
            hist = [x["rate_8h"] for x in self.fund[v].range(t - self.cfg.retention_ms, t) if x.get("rate_8h") is not None]
            span = t - (self.fund[v].first_series or t)
            if len(hist) >= self.cfg.funding_pct_min_n and span >= self.cfg.funding_pct_min_span_ms:
                row.set(f"{p}.funding_percentile", sum(1 for x in hist if x <= now) / len(hist))
            else:
                row.set(f"{p}.funding_percentile", None, S.NOT_READY)
        else:
            row.set(f"{p}.funding_percentile", None, st_norm)

    # ---------- OPEN INTEREST ----------
    def _oi_at(self, v, tt):
        return self._asof(self.oi[v], tt, self.cfg.oi_max_age_ms)

    def _oi(self, row, v, t):
        p = f"perp.{v}"
        buf = self.oi[v]
        raw = buf.asof(t, self.cfg.oi_max_age_ms)
        now = raw[1] if raw else None
        st_now = S.READY if now is not None else (S.UNAVAILABLE if (raw is not None and raw[1] is None) else self._lookback(buf, t))
        mark = self._asof(self.mark[v], t)
        row.set(f"{p}.oi_coin", now, st_now)
        row.set(f"{p}.oi_notional", now * mark if (now is not None and mark is not None) else None,
                calc.worst_status(st_now, S.READY if mark is not None else self._lookback(self.mark[v], t)))
        latest = buf.asof(t, 10**12)
        row.set(f"{p}.oi_age_s", (t - latest[0]) / 1000.0 if latest else None, S.READY if latest else S.MISSING)
        for h in OI_H:
            hm = HORIZONS_MS[h]
            back, mk_back = self._oi_at(v, t - hm), self._asof(self.mark[v], t - hm)
            if now is not None and back is not None:
                row.set(f"{p}.oi_change_coin.{h}", now - back)
                row.set(f"{p}.oi_pct_change.{h}", (now - back) / back if back > 0 else None, S.READY if back > 0 else S.UNDEFINED)
                if mark is not None and mk_back is not None:
                    row.set(f"{p}.oi_notional_change.{h}", now * mark - back * mk_back)
                else:
                    row.set(f"{p}.oi_notional_change.{h}", None, self._lookback(self.mark[v], t - hm if mark is not None else t))
            else:
                st = st_now if now is None else self._lookback(buf, t - hm)
                for k in ("oi_change_coin", "oi_pct_change", "oi_notional_change"):
                    row.set(f"{p}.{k}.{h}", None, st)
        for h in OI_ACC_H:
            hm = HORIZONS_MS[h]
            b1, b2 = self._oi_at(v, t - hm), self._oi_at(v, t - 2 * hm)
            if now is not None and b1 is not None and b2 is not None:
                row.set(f"{p}.oi_accel_coin.{h}", (now - b1) - (b1 - b2))
            else:
                row.set(f"{p}.oi_accel_coin.{h}", None, st_now if now is None else self._lookback(buf, t - 2 * hm))

    # ---------- FLOW + CVD ----------
    def _flow_cvd(self, row, v, t):
        p = f"perp.{v}"
        for w in FLOW_W:
            wm = HORIZONS_MS[w]
            st, tr = self._window_vals(self.trades[v], v, t - wm, t)
            names = [f"{p}.flow_{k}.{w}" for k in ("buy_notional", "sell_notional", "signed_notional", "imbalance",
                                                   "count_imbalance", "count", "avg_size_coin", "max_size_coin")]
            if st != S.READY:
                for n in names:
                    row.set(n, None, st)
                continue
            if any(x[0] is None or x[1] is None for x in tr):
                for n in names:
                    row.set(n, None, S.UNAVAILABLE)           # sizes in unverified contract units
                continue
            if any(x[2] not in ("buy", "sell") for x in tr):
                for n in names:
                    row.set(n, None, S.UNAVAILABLE)           # aggressor side not documented
                continue
            buy = math.fsum(x[1] for x in tr if x[2] == "buy")
            sell = math.fsum(x[1] for x in tr if x[2] == "sell")
            n, nb = len(tr), sum(1 for x in tr if x[2] == "buy")
            row.set(f"{p}.flow_buy_notional.{w}", buy)
            row.set(f"{p}.flow_sell_notional.{w}", sell)
            row.set(f"{p}.flow_signed_notional.{w}", buy - sell)
            row.set(f"{p}.flow_imbalance.{w}", (buy - sell) / (buy + sell) if buy + sell > 0 else None,
                    S.READY if buy + sell > 0 else S.UNDEFINED)
            row.set(f"{p}.flow_count_imbalance.{w}", (2 * nb - n) / n if n else None, S.READY if n else S.UNDEFINED)
            row.set(f"{p}.flow_count.{w}", float(n))
            row.set(f"{p}.flow_avg_size_coin.{w}", math.fsum(x[0] for x in tr) / n if n else None, S.READY if n else S.UNDEFINED)
            row.set(f"{p}.flow_max_size_coin.{w}", max(x[0] for x in tr) if n else None, S.READY if n else S.UNDEFINED)
        for w in CVD_W:
            wm = HORIZONS_MS[w]
            names = [f"{p}.cvd_notional.{w}", f"{p}.cvd_slope.{w}", f"{p}.cvd_accel.{w}"]
            st, tr = self._window_vals(self.trades[v], v, t - wm, t)
            if st != S.READY or any(x[1] is None or x[2] not in ("buy", "sell") for x in tr):
                for n in names:
                    row.set(n, None, st if st != S.READY else S.UNAVAILABLE)
                continue
            signed = lambda lo, hi: math.fsum(x[1] if x[2] == "buy" else -x[1] for x in self.trades[v].range(lo, hi))  # noqa: E731
            cvd = signed(t - wm, t)
            row.set(names[0], cvd)
            path = [signed(t - wm, t - wm + k * 1000) for k in range(0, wm // 1000 + 1)]
            row.set(names[1], _ols_slope(path))
            st2 = self._observed(v, t - 2 * wm, t)
            row.set(names[2], cvd - signed(t - 2 * wm, t - wm) if st2 == S.READY else None, st2)

    # ---------- LIQUIDATIONS ----------
    def _liq_window(self, v, lo, hi):
        st, ls = self._window_vals(self.liq[v], v, lo, hi)
        if st != S.READY:
            return st, None
        if any(x[0] is None for x in ls):
            return S.UNAVAILABLE, None
        return S.READY, ls

    def _liquidations(self, row, v, t):
        p = f"perp.{v}"
        for w in LIQ_W:
            wm = HORIZONS_MS[w]
            st, ls = self._liq_window(v, t - wm, t)
            names = [f"{p}.liq_{k}.{w}" for k in ("forced_sell_notional", "forced_buy_notional", "long_liq_notional",
                                                 "short_liq_notional", "total_notional", "imbalance", "count",
                                                 "max_notional", "accel", "over_trade_notional")]
            if st != S.READY:
                for n in names:
                    row.set(n, None, st)                       # NOT observed -> never 0
                continue
            fs = math.fsum(x[0] for x in ls if x[1] == "sell")
            fb = math.fsum(x[0] for x in ls if x[1] == "buy")
            lg = math.fsum(x[0] for x in ls if x[2] == "long")
            sh = math.fsum(x[0] for x in ls if x[2] == "short")
            tot = math.fsum(x[0] for x in ls)
            row.set(names[0], fs)
            row.set(names[1], fb)
            row.set(names[2], lg)
            row.set(names[3], sh)
            row.set(names[4], tot)
            row.set(names[5], (fs - fb) / tot if tot > 0 else None, S.READY if tot > 0 else S.UNDEFINED)
            row.set(names[6], float(len(ls)))
            row.set(names[7], max(x[0] for x in ls) if ls else None, S.READY if ls else S.UNDEFINED)
            st_prev, prev = self._liq_window(v, t - 2 * wm, t - wm)
            row.set(names[8], tot - math.fsum(x[0] for x in prev) if st_prev == S.READY else None, st_prev)
            tst, tr = self._window_vals(self.trades[v], v, t - wm, t)
            if tst == S.READY and all(x[1] is not None for x in tr):
                tn = math.fsum(x[1] for x in tr)
                row.set(names[9], tot / tn if tn > 0 else None, S.READY if tn > 0 else S.UNDEFINED)
            else:
                row.set(names[9], None, tst if tst != S.READY else S.UNAVAILABLE)

    # ---------- ORDER BOOK ----------
    def _book_at(self, v, tt):
        return self._asof(self.book[v], tt)

    def _spread_bps(self, v, tt):
        q = self._asof(self.quote[v], tt)
        if not q or q[0] is None or q[2] is None or q[2] < q[0]:
            return None
        m = (q[0] + q[2]) / 2
        return (q[2] - q[0]) / m * 1e4

    def _depth5(self, v, tt):
        bk = self._book_at(v, tt)
        if not bk or len(bk[0]) < 5 or len(bk[1]) < 5 or any(x[1] is None for x in bk[0][:5] + bk[1][:5]):
            return None
        return math.fsum(px * q for px, q in bk[0][:5] + bk[1][:5])

    def _top_imb(self, v, tt):
        q = self._asof(self.quote[v], tt)
        if not q or q[1] is None or q[3] is None or q[1] + q[3] <= 0:
            return None
        return (q[1] - q[3]) / (q[1] + q[3])

    def _book(self, row, v, t):
        p = f"perp.{v}"
        q = self._asof(self.quote[v], t)
        st_q = S.READY if q else self._lookback(self.quote[v], t)
        has_sizes = v in self.size_venues
        bid, bq, ask, aq = q if q else (None, None, None, None)
        row.set(f"{p}.book_bid", bid, st_q if bid is not None or not q else S.MISSING)
        row.set(f"{p}.book_ask", ask, st_q if ask is not None or not q else S.MISSING)
        sp = self._spread_bps(v, t)
        row.set(f"{p}.book_spread_bps", sp, S.READY if sp is not None else (S.MISSING if q else st_q))
        if has_sizes:
            for k, x in (("bid_qty_coin", bq), ("ask_qty_coin", aq)):
                row.set(f"{p}.book_{k}", x, S.READY if x is not None else (S.UNAVAILABLE if q else st_q))
            ti = self._top_imb(v, t)
            row.set(f"{p}.book_top_imbalance", ti, S.READY if ti is not None else (S.UNAVAILABLE if q else st_q))
            if q and None not in (bid, bq, ask, aq) and bq + aq > 0:
                row.set(f"{p}.book_weighted_mid", (bid * aq + ask * bq) / (bq + aq))
            else:
                row.set(f"{p}.book_weighted_mid", None, S.UNAVAILABLE if q else st_q)
            bk = self._book_at(v, t)
            st_b = S.READY if bk else self._lookback(self.book[v], t)
            for n in DEPTHS:
                name = f"{p}.book_depth_imbalance_{n}"
                if n > VENUES[v].book_depth_max:
                    row.set(name, None, S.UNAVAILABLE)
                elif not bk:
                    row.set(name, None, st_b)
                elif len(bk[0]) < n or len(bk[1]) < n or any(x[1] is None for x in bk[0][:n] + bk[1][:n]):
                    row.set(name, None, S.UNAVAILABLE)          # depth not captured at this level / units unverified
                else:
                    sb, sa = math.fsum(x[1] for x in bk[0][:n]), math.fsum(x[1] for x in bk[1][:n])
                    row.set(name, (sb - sa) / (sb + sa) if sb + sa > 0 else None, S.READY if sb + sa > 0 else S.UNDEFINED)
            d5 = self._depth5(v, t)
            row.set(f"{p}.book_depth_notional_5", d5, S.READY if d5 is not None else (S.UNAVAILABLE if bk else st_b))
        smp = calc.grid(lambda g: self._spread_bps(v, g), t, 300_000)
        st = self._grid_status(self.quote[v], smp, t - 300_000)
        med = _med([x for x in smp if x is not None]) if st in (S.READY, S.PARTIAL) else None
        if sp is not None and med is not None:
            row.set(f"{p}.book_spread_ratio_5m", sp / med if med > 0 else None, st if med > 0 else S.UNDEFINED)
        else:
            row.set(f"{p}.book_spread_ratio_5m", None, st if st not in (S.READY, S.PARTIAL) else S.MISSING)
        for h in BOOK_CHG_H:
            hm = HORIZONS_MS[h]
            sb = self._spread_bps(v, t - hm)
            row.set(f"{p}.book_spread_change_bps.{h}", sp - sb if (sp is not None and sb is not None) else None,
                    S.READY if (sp is not None and sb is not None) else self._lookback(self.quote[v], t - hm if sp is not None else t))
            if not has_sizes:
                continue
            d_now, d_back = self._depth5(v, t), self._depth5(v, t - hm)
            if d_now is not None and d_back is not None:
                row.set(f"{p}.book_depth_change_pct.{h}", (d_now - d_back) / d_back if d_back > 0 else None,
                        S.READY if d_back > 0 else S.UNDEFINED)
            else:
                row.set(f"{p}.book_depth_change_pct.{h}", None,
                        self._lookback(self.book[v], t - hm) if d_now is not None else (S.UNAVAILABLE if self._book_at(v, t) else self._lookback(self.book[v], t)))
            i_now, i_back = self._top_imb(v, t), self._top_imb(v, t - hm)
            row.set(f"{p}.book_pressure_change.{h}", i_now - i_back if (i_now is not None and i_back is not None) else None,
                    S.READY if (i_now is not None and i_back is not None) else
                    (self._lookback(self.quote[v], t - hm) if i_now is not None else (S.UNAVAILABLE if q else st_q)))

    # ---------- CROSS-EXCHANGE AGGREGATES ----------
    def _collect(self, row, fmt, venues):
        vals, sts = {}, []
        for v in venues:
            n = fmt.format(v=v)
            if row.status.get(n) in (S.READY, S.PARTIAL):
                vals[v] = row.values[n]
            else:
                sts.append(row.status.get(n, S.UNAVAILABLE))
        return vals, sts

    def _set_median(self, row, name, vals, sts):
        if vals:
            row.set(name, _med(list(vals.values())))
        else:
            row.set(name, None, calc.worst_status(*sts) if sts else S.UNAVAILABLE)

    def _set_dispersion(self, row, name, vals, sts, scale=1.0):
        if len(vals) >= 2:
            row.set(name, (max(vals.values()) - min(vals.values())) * scale)
        elif len(vals) == 1:
            row.set(name, None, S.UNDEFINED)
        else:
            row.set(name, None, calc.worst_status(*sts) if sts else S.UNAVAILABLE)

    def _set_total(self, row, name, fmt):
        """Sum over ALL size venues: any venue not READY -> that status (a missing venue is never a 0)."""
        vals, sts = self._collect(row, fmt, self.size_venues)
        if len(vals) == len(self.size_venues) and vals:
            row.set(name, math.fsum(vals.values()))
        else:
            row.set(name, None, calc.worst_status(*sts) if sts else S.UNAVAILABLE)
        return vals

    def _cross(self, row, t):
        fresh = [v for v in self.venues if self._asof(self.mid[v], t) is not None]
        row.set("x.n_venues_fresh", float(len(fresh)))
        for h in X_RET_H:
            vals, sts = self._collect(row, "perp.{v}.mid_logret." + h, self.venues)
            self._set_median(row, f"x.median_mid_logret.{h}", vals, sts)
            self._set_dispersion(row, f"x.disagreement_bps.{h}", vals, sts, 1e4)
            if len(vals) >= 2:
                m = _med(list(vals.values()))
                row.set(f"x.max_venue_dev_bps.{h}", max(abs(x - m) for x in vals.values()) * 1e4)
                row.set(f"x.sign_agreement.{h}", (sum(x > 0 for x in vals.values()) - sum(x < 0 for x in vals.values())) / len(vals))
            else:
                st = S.UNDEFINED if len(vals) == 1 else (calc.worst_status(*sts) if sts else S.UNAVAILABLE)
                row.set(f"x.max_venue_dev_bps.{h}", None, st)
                row.set(f"x.sign_agreement.{h}", None, st)
            w_vals, w_sts = self._collect(row, "perp.{v}.oi_notional", self.size_venues)
            pairs = [(vals[v], w_vals[v]) for v in self.size_venues if v in vals and v in w_vals and w_vals[v] > 0]
            if pairs:
                row.set(f"x.oi_weighted_logret.{h}", math.fsum(r * w for r, w in pairs) / math.fsum(w for _, w in pairs))
            else:
                row.set(f"x.oi_weighted_logret.{h}", None, calc.worst_status(*(sts + w_sts)) if (sts or w_sts) else S.UNAVAILABLE)
        self._set_total(row, "x.oi_total_notional", "perp.{v}.oi_notional")
        for h in X_OI_H:
            self._set_total(row, f"x.oi_total_change_notional.{h}", "perp.{v}.oi_notional_change." + h)
        vals, sts = self._collect(row, "perp.{v}.funding_rate_8h", self.venues)
        self._set_median(row, "x.funding_median_8h", vals, sts)
        self._set_dispersion(row, "x.funding_dispersion_8h", vals, sts)
        vals, sts = self._collect(row, "perp.{v}.mark_minus_ref_bps", self.venues)
        self._set_median(row, "x.basis_median_bps", vals, sts)
        self._set_dispersion(row, "x.basis_dispersion_bps", vals, sts)
        vals, sts = self._collect(row, "perp.{v}.book_spread_bps", self.venues)
        self._set_median(row, "x.spread_median_bps", vals, sts)
        for w in LIQ_W:
            for k in ("total_notional", "forced_sell_notional", "forced_buy_notional"):
                self._set_total(row, f"x.liq_{k}.{w}", "perp.{v}.liq_" + k + "." + w)
        for w in CVD_W:
            self._set_total(row, f"x.cvd_total_notional.{w}", "perp.{v}.cvd_notional." + w)
        for w in DIV_FLOW_W:
            s_vals, s_sts = self._collect(row, "perp.{v}.flow_signed_notional." + w, self.size_venues)
            b_vals, _ = self._collect(row, "perp.{v}.flow_buy_notional." + w, self.size_venues)
            l_vals, _ = self._collect(row, "perp.{v}.flow_sell_notional." + w, self.size_venues)
            if len(s_vals) == len(self.size_venues) and s_vals:
                tot = math.fsum(b_vals.values()) + math.fsum(l_vals.values())
                row.set(f"x.flow_imbalance.{w}", math.fsum(s_vals.values()) / tot if tot > 0 else None,
                        S.READY if tot > 0 else S.UNDEFINED)
            else:
                row.set(f"x.flow_imbalance.{w}", None, calc.worst_status(*s_sts) if s_sts else S.UNAVAILABLE)

    # ---------- PERP vs SPOT / CF ----------
    def _divergence(self, row, t):
        if not self.spot_enabled:
            return
        for h in DIV_H:
            hm = HORIZONS_MS[h]
            pm = row.values.get(f"x.median_mid_logret.{h}")
            pst = row.status.get(f"x.median_mid_logret.{h}")
            r_now, r_back = self.ref_at(t), self.ref_at(t - hm)
            if pm is not None and r_now is not None and r_back is not None:
                row.set(f"div.perp_minus_spot_ret.{h}", pm - math.log(r_now / r_back), pst)
            else:
                row.set(f"div.perp_minus_spot_ret.{h}", None, calc.worst_status(
                    pst, S.READY if r_now is not None else self._ref_lookback(t),
                    S.READY if r_back is not None else self._ref_lookback(t - hm)))
            c_now, c_back = self._asof(self.cf, t), self._asof(self.cf, t - hm)
            if pm is not None and c_now is not None and c_back is not None:
                row.set(f"div.perp_minus_cf_ret.{h}", pm - math.log(c_now / c_back), pst)
            else:
                row.set(f"div.perp_minus_cf_ret.{h}", None, calc.worst_status(
                    pst, S.READY if c_now is not None else self._lookback(self.cf, t),
                    S.READY if c_back is not None else self._lookback(self.cf, t - hm)))
        for w in DIV_FLOW_W:
            wm = HORIZONS_MS[w]
            pi, pst = row.values.get(f"x.flow_imbalance.{w}"), row.status.get(f"x.flow_imbalance.{w}")
            sst, st = self._window_vals(self.spot_trades, "coinbase", t - wm, t)
            if sst == S.READY and any(x[1] not in ("buy", "sell") for x in st):
                sst = S.UNAVAILABLE
            if sst == S.READY:
                b = math.fsum(x[0] for x in st if x[1] == "buy")
                s_ = math.fsum(x[0] for x in st if x[1] == "sell")
                si = (b - s_) / (b + s_) if b + s_ > 0 else None
                sst = S.READY if si is not None else S.UNDEFINED
            else:
                si = None
            if pi is not None and si is not None:
                row.set(f"div.perp_minus_spot_flow_imb.{w}", pi - si, calc.worst_status(pst, sst))
            else:
                row.set(f"div.perp_minus_spot_flow_imb.{w}", None, calc.worst_status(pst, sst))
        for w in DIV_RV:
            vals, sts = self._collect(row, "perp.{v}.mid_rv." + w, self.venues)
            smp = calc.grid(lambda g: self.ref_at(g), t, HORIZONS_MS[w])
            f = self._ref_first()
            sst = S.READY if all(x is not None for x in smp) else (S.NOT_READY if (f is None or t - HORIZONS_MS[w] < f) else S.MISSING)
            srv = calc.realized(smp)[0] if sst == S.READY else None
            if vals and srv is not None and srv > 0:
                row.set(f"div.perp_over_spot_rv.{w}", _med(list(vals.values())) / srv)
            else:
                row.set(f"div.perp_over_spot_rv.{w}", None,
                        S.UNDEFINED if (vals and srv == 0) else calc.worst_status(*(sts + [sst])) if (sts or sst != S.READY) else S.MISSING)
        vals, sts = self._collect(row, "perp.{v}.basis_accel_bps.60s", self.venues)
        self._set_median(row, "div.basis_accel_median_bps.60s", vals, sts)


def event_key(e):
    """Availability order across Step-3 and Step-4 events: (receive_ts, ingest_seq, family)."""
    return (e.receive_ts_ms, e.ingest_seq, 1 if isinstance(e, PerpEvent) else 0)


def compute_at(asset, events, t, market=None, gaps=(), config=None, venues=PERP_VENUES, spot=True):
    """BATCH path: only events available at t (receive_ts <= t), replayed in availability order."""
    eng = PerpFeatureEngine(asset, config, venues, spot)
    for g in gaps:
        if (g.known_at_ms if g.known_at_ms is not None else g.end_ts_ms) <= t:
            eng.ingest_gap(g)
    for e in sorted((e for e in events if available(e, t)), key=event_key):
        eng.ingest(e)
    return eng.features_at(t, market)
