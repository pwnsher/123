"""
MicroFeatureEngine: causal microstructure features for ONE asset at checkpoint T (research only).

Streaming contract (identical to Steps 3 / 4): ingest(ev) in availability order (receive_ts non-decreasing; ties by
ingest_seq, merged across the Step-3 / Step-4 / Step-5 families with event_key), then features_at(T) with
T >= the last receive time ingested (else CausalityError). compute_at() is the batch path used to prove the
streaming path equal: it replays only events with receive_ts <= T.

Inputs
    MicroEvent   book snapshots / deltas / resets (all venues) and Kalshi websocket trades
    MarketEvent  Step-3 spot TRADES (Coinbase / Kraken) for the spot books' trade features (+ liveness)
    PerpEvent    Step-4 perp TRADES (+ liveness) and OKX INSTRUMENT contract values
Books are rebuilt by microstructure.reconstruction.BookReconstructor (the live collector's code). The book at T is
the book after every book event received <= T; every window is (T - w, T] in RECEIVE time.

Statuses: level features READY only when the book is READY at T (NOT_READY while warming up / awaiting a snapshot,
MISSING when stale / invalid / needing a resnapshot); windowed book features also require the book to have been
continuously valid over the whole window (a resnapshot starts a new interval - nothing is repaired retroactively).
A missing book is never turned into 0 (imbalance / OFI stay None with a non-READY status).
"""
import bisect
import math
from dataclasses import dataclass, field
from typing import Optional

from market_data.features.definitions import Status
from market_data.features.engine import CausalityError, FeatureRow
from market_data.types import EventType as ET, MarketEvent
from microstructure.features.definitions import (ACCEL_H, CANCEL_W, CHANGE_H, DEPTH_LEVELS, FEATURE_NAMES, IMB_W, K_CHG_W,
                                                 K_DEPTH, K_H, K_LIQ_C, K_OFI_W, LIQ_BPS, NET_H, SHORT, TRADE_COUNT_W,
                                                 WINDOW_MS, WINDOWS, MicroFeatureConfig)
from microstructure.reconstruction import BookReconstructor, BookStatus as BS
from microstructure.types import MicroEvent, MicroEventType as MT
from microstructure.venues import CONTRACT_VALUE_SOURCE, EXCHANGE_BOOKS, PERP_BOOKS, SPOT_BOOKS, TRADE_SOURCE, VENUES
from perp_data.types import PerpEvent, PerpEventType as PT

S = Status
H_MS = dict(WINDOW_MS)
FAMILY_RANK = {"market": 0, "perp": 1, "micro": 2}


def _ms(h):
    return H_MS[h]


def _median(xs):
    v = sorted(xs)
    n = len(v)
    if not n:
        return None
    return v[n // 2] if n % 2 else (v[n // 2 - 1] + v[n // 2]) / 2


def _imb(b, a):
    return (b - a) / (b + a) if (b + a) > 0 else None


def _microprice(bid, bq, ask, aq):
    if bid is None or ask is None or not bq or not aq:
        return None
    return (bid * aq + ask * bq) / (bq + aq)


def _level_ofi(bb, ba, ab, aa):
    """OFI contribution of one level: bb/ab = (price, qty) of the bid before/after, ba/aa the ask before/after."""
    e = 0.0
    if bb is not None and ab is not None:
        e += (ab[1] if ab[0] >= bb[0] else 0.0) - (bb[1] if ab[0] <= bb[0] else 0.0)
    if ba is not None and aa is not None:
        e += -(aa[1] if aa[0] <= ba[0] else 0.0) + (ba[1] if aa[0] >= ba[0] else 0.0)
    return e


@dataclass
class BookSeries:
    """Per-book history derived from applied updates (receive-time axis)."""
    key: tuple
    t: list = field(default_factory=list)          # receive ts of each applied update (snapshot or delta)
    state: list = field(default_factory=list)      # (bid, bq, ask, aq, imb5, imb10, spread, mp, bid_d10, ask_d10)
    c_ofi: list = field(default_factory=list)      # cumulative sums aligned with t
    c_mlofi: list = field(default_factory=list)
    c_badd: list = field(default_factory=list)
    c_brem: list = field(default_factory=list)
    c_aadd: list = field(default_factory=list)
    c_arem: list = field(default_factory=list)
    lat_t: list = field(default_factory=list)
    lat: list = field(default_factory=list)
    sweeps: list = field(default_factory=list)     # (t, direction, levels, notional, impact_bps)
    replen: list = field(default_factory=list)     # (t, side, price, run)
    depletion: dict = field(default_factory=dict)  # side -> (price, q0, t)
    last_replen: dict = field(default_factory=dict)  # side -> (price, run)
    first_t: Optional[int] = None

    def cum(self, arr, t):
        i = bisect.bisect_right(self.t, t) - 1
        return arr[i] if i >= 0 else 0.0

    def window(self, arr, lo, hi):
        return self.cum(arr, hi) - self.cum(arr, lo)

    def asof(self, t):
        i = bisect.bisect_right(self.t, t) - 1
        return self.state[i] if i >= 0 else None

    def count(self, lo, hi):
        return bisect.bisect_right(self.t, hi) - bisect.bisect_right(self.t, lo)

    def prune(self, before):
        i = bisect.bisect_left(self.t, before)
        if i > 1:
            i -= 1                                 # keep one state as-of `before`
            for a in (self.t, self.state, self.c_ofi, self.c_mlofi, self.c_badd, self.c_brem, self.c_aadd, self.c_arem):
                del a[:i]
        j = bisect.bisect_left(self.lat_t, before)
        del self.lat_t[:j], self.lat[:j]
        self.sweeps = [s for s in self.sweeps if s[0] >= before]
        self.replen = [r for r in self.replen if r[0] >= before]


@dataclass
class TradeSeries:
    t: list = field(default_factory=list)
    px: list = field(default_factory=list)
    qty: list = field(default_factory=list)        # coin (Kalshi: contracts); None when unconverted
    sign: list = field(default_factory=list)       # +1 buy aggressor, -1 sell, 0 unknown
    # VPIN-style state
    first_t: Optional[int] = None
    calib_vol: float = 0.0
    bucket_v: Optional[float] = None
    cur_buy: float = 0.0
    cur_sell: float = 0.0
    buckets: list = field(default_factory=list)    # (completed at t, |buy - sell|)

    def idx(self, lo, hi):
        return bisect.bisect_right(self.t, lo), bisect.bisect_right(self.t, hi)

    def prune(self, before):
        i = bisect.bisect_left(self.t, before)
        if i:
            del self.t[:i], self.px[:i], self.qty[:i], self.sign[:i]
        j = 0
        while j < len(self.buckets) - 200 and self.buckets[j][0] < before:
            j += 1
        del self.buckets[:j]


class MicroFeatureEngine:
    def __init__(self, asset, config=None, venues=EXCHANGE_BOOKS + ("kalshi_ws",)):
        self.asset = asset
        self.cfg = config or MicroFeatureConfig()
        self.venues = tuple(v for v in EXCHANGE_BOOKS + ("kalshi_ws",) if v in venues)
        self.recon = BookReconstructor(warmup_ms=self.cfg.warmup_ms)
        self.series = {}                     # book key -> BookSeries
        self.trades = {}                     # trade key: venue source (exchange) or ("kalshi_ws", ticker) -> TradeSeries
        self.alive, self.first_seen = {}, {}
        self.contract_value = {}             # book venue -> verified contract value (base coin per contract)
        self._seen = set()
        self.duplicates = 0
        self.max_receive = None
        self.ingested = 0
        self.gaps = []

    # ================= ingestion =================
    def ingest(self, ev):
        if self.max_receive is not None and ev.receive_ts_ms < self.max_receive:
            raise CausalityError("events must be ingested in availability order (receive_ts non-decreasing)")
        self.max_receive = ev.receive_ts_ms
        if isinstance(ev, MicroEvent):
            return self._ingest_micro(ev)
        if isinstance(ev, MarketEvent):
            if ev.source not in ("coinbase", "kraken") or ev.asset != self.asset:
                return False
            self._alive(ev.source, ev.receive_ts_ms)
            if ev.event_type == ET.TRADE:
                p = ev.payload
                return self._trade(ev.source, ev, p["price"], p["size"], p.get("aggressor"))
            return False
        if isinstance(ev, PerpEvent):
            if ev.asset not in (self.asset, "*"):
                return False
            if getattr(ev, "channel", "ws") == "ws":
                self._alive(ev.source, ev.receive_ts_ms)
            if ev.event_type == PT.INSTRUMENT:
                for bv, src in CONTRACT_VALUE_SOURCE.items():
                    if src == ev.source and ev.symbol == VENUES[bv].symbols.get(self.asset):
                        p = ev.payload
                        if p.get("verified") and p.get("contract_value"):
                            self.contract_value[bv] = float(p["contract_value"])
                return False
            if ev.event_type == PT.PERP_TRADE and ev.asset == self.asset:
                p = ev.payload
                return self._trade(ev.source, ev, p["price"], p.get("qty_coin"), p.get("aggressor"))
        return False

    def _alive(self, src, t):
        self.alive[src] = t
        self.first_seen.setdefault(src, t)

    def _trade(self, key, ev, px, qty, aggr):
        dk = ev.dedup_key()
        if dk is not None:
            if dk in self._seen:
                self.duplicates += 1
                return False
            self._seen.add(dk)
        ts = self.trades.setdefault(key, TradeSeries())
        r = ev.receive_ts_ms
        sgn = 1 if aggr == "buy" else -1 if aggr == "sell" else 0
        ts.t.append(r); ts.px.append(px); ts.qty.append(qty); ts.sign.append(sgn)
        self._vpin(ts, r, qty, sgn)
        self.ingested += 1
        self._maybe_prune()
        return True

    def _vpin(self, ts, r, qty, sgn):
        cfg = self.cfg
        if qty is None or sgn == 0:
            return
        if ts.first_t is None:
            ts.first_t = r
        if ts.bucket_v is None:
            if r - ts.first_t < cfg.vpin_calibration_ms:
                ts.calib_vol += qty
                return
            if ts.calib_vol <= 0:
                return
            ts.bucket_v = ts.calib_vol / cfg.vpin_bucket_div
        rem = qty
        while rem > 1e-15:
            room = ts.bucket_v - (ts.cur_buy + ts.cur_sell)
            take = min(room, rem)
            if sgn > 0:
                ts.cur_buy += take
            else:
                ts.cur_sell += take
            rem -= take
            if ts.cur_buy + ts.cur_sell >= ts.bucket_v - 1e-12:
                ts.buckets.append((r, abs(ts.cur_buy - ts.cur_sell)))
                ts.cur_buy = ts.cur_sell = 0.0

    def _ingest_micro(self, ev):
        if ev.source not in self.venues or (ev.asset not in (self.asset, "*")):
            return False
        self._alive(ev.source, ev.receive_ts_ms)
        et = ev.event_type
        if et == MT.TRADE:
            p = ev.payload
            return self._trade((ev.source, ev.symbol), ev, p["price"], p["qty"], p.get("aggressor"))
        key = (ev.source, ev.payload.get("book"))
        tr = self.recon.tracks.get(key)
        before = tr.book.copy_top(20) if (tr is not None and et == MT.BOOK_DELTA) else None
        ups = self.recon.apply(ev)
        if et in (MT.BOOK_RESET, MT.INSTRUMENT):
            return True
        u = ups[0] if isinstance(ups, list) else ups
        if u is None or u.kind not in ("snapshot", "delta"):
            return True
        tr = self.recon.tracks[key]
        if tr.base != BS.READY:
            return True
        s = self.series.get(key)
        if s is None:
            s = self.series[key] = BookSeries(key, first_t=ev.receive_ts_ms)
        r = ev.receive_ts_ms
        if ev.event_ts_ms is not None:
            s.lat_t.append(r); s.lat.append(r - ev.event_ts_ms)
        b = tr.book
        bb, ba = b.best_bid(), b.best_ask()
        bids10, asks10 = b.top("bid", 10), b.top("ask", 10)
        bd10, ad10 = sum(q for _, q in bids10), sum(q for _, q in asks10)
        bd5, ad5 = sum(q for _, q in bids10[:5]), sum(q for _, q in asks10[:5])
        mid = (bb[0] + ba[0]) / 2 if bb and ba else None
        state = (bb[0] if bb else None, bb[1] if bb else None, ba[0] if ba else None, ba[1] if ba else None,
                 _imb(bd5, ad5), _imb(bd10, ad10), ((ba[0] - bb[0]) / mid * 1e4) if mid else None,
                 _microprice(bb[0] if bb else None, bb[1] if bb else None, ba[0] if ba else None, ba[1] if ba else None),
                 bd10, ad10)
        vals = [0.0] * 6
        if u.kind == "delta" and before is not None and s.t:
            pb, pa = before
            ab, aa = b.top("bid", 5), b.top("ask", 5)
            vals[0] = _level_ofi(pb[0] if pb else None, pa[0] if pa else None, ab[0] if ab else None, aa[0] if aa else None)
            vals[1] = sum(_level_ofi(pb[m] if m < len(pb) else None, pa[m] if m < len(pa) else None,
                                     ab[m] if m < len(ab) else None, aa[m] if m < len(aa) else None) for m in range(5))
            for side, _px, old, new, rank in u.changes:
                if rank < 10:
                    d = new - old
                    if side == "bid":
                        vals[2 if d > 0 else 3] += abs(d)
                    else:
                        vals[4 if d > 0 else 5] += abs(d)
            self._sweep_replen(s, ev, before, b, state, mid)
        s.t.append(r)
        s.state.append(state)
        for arr, v in zip((s.c_ofi, s.c_mlofi, s.c_badd, s.c_brem, s.c_aadd, s.c_arem), vals):
            arr.append((arr[-1] if arr else 0.0) + v)
        self.ingested += 1
        self._maybe_prune()
        return True

    def _trade_key(self, bkey):
        return bkey if bkey[0] == "kalshi_ws" else TRADE_SOURCE[bkey[0]]

    def _sweep_replen(self, s, ev, before, book, state, mid_after):
        cfg = self.cfg
        r = ev.receive_ts_ms
        pb, pa = before
        tsr = self.trades.get(self._trade_key(s.key))
        mid_before = (pb[0][0] + pa[0][0]) / 2 if pb and pa else None

        def recent(sign, price=None):
            if tsr is None:
                return []
            i, j = tsr.idx(r - cfg.sweep_trade_window_ms, r)
            return [k for k in range(i, j) if tsr.sign[k] == sign and (price is None or abs(tsr.px[k] - price) <= 1e-9 * max(1.0, price))]
        # ---- sweep: >= 2 levels better than the new best cleared in ONE update, AND same-side trades that reach
        #      into the cleared levels and account for >= sweep_min_trade_share of the cleared size (evidence, not a
        #      guess from the book alone: a repricing without trades is not a sweep) ----
        mult = 1.0 if s.key[0] == "kalshi_ws" else self._unit_mult(s.key[0])[0]
        nb, na = book.best_bid(), book.best_ask()
        for side, levels_before, new_best, sign in (("ask", pa, na, 1), ("bid", pb, nb, -1)):
            if not levels_before or mult is None:
                continue
            if side == "ask":
                cleared = [(p, q) for p, q in levels_before if new_best is None or p < new_best[0]]
            else:
                cleared = [(p, q) for p, q in levels_before if new_best is None or p > new_best[0]]
            if len(cleared) < cfg.sweep_min_levels:
                continue
            ks = [k for k in recent(sign) if tsr.qty[k] is not None and
                  (tsr.px[k] >= cleared[1][0] if sign > 0 else tsr.px[k] <= cleared[1][0])]
            traded = sum(tsr.qty[k] for k in ks)
            if ks and traded >= cfg.sweep_min_trade_share * sum(q for _, q in cleared) * mult:
                impact = ((mid_after - mid_before) / mid_before * 1e4) if (mid_after and mid_before) else None
                s.sweeps.append((r, sign, len(cleared), sum(p * q for p, q in cleared), impact, tsr.t[ks[-1]] - tsr.t[ks[0]]))
        # ---- replenishment pattern at the best level ----
        for side, lv_before, now, sign in (("bid", pb, nb, -1), ("ask", pa, na, 1)):
            if not lv_before or now is None:
                s.depletion.pop(side, None)
                continue
            p0, q0 = lv_before[0]
            dep = s.depletion.get(side)
            if dep is not None and (now[0] != dep[0] or r - dep[2] > cfg.replenish_window_ms):
                s.depletion.pop(side, None)
                dep = None
            if dep is not None and now[0] == dep[0] and now[1] > q0 and now[1] >= cfg.replenish_fraction * dep[1]:
                last = s.last_replen.get(side)
                run = last[1] + 1 if last is not None and last[0] == now[0] else 1
                s.last_replen[side] = (now[0], run)
                s.replen.append((r, side, now[0], run))
                s.depletion.pop(side, None)
                continue
            # a trade hitting this side: sell aggressors hit bids (sign -1), buy aggressors lift asks (+1)
            if now[0] == p0 and now[1] < q0 and recent(sign, p0):
                prev = s.depletion.get(side)
                s.depletion[side] = (p0, max(q0, prev[1]) if prev and prev[0] == p0 else q0, r if prev is None else prev[2])

    def ingest_gap(self, gap):
        self.gaps.append(gap)

    def _maybe_prune(self):
        if self.ingested % 5000 == 0 and self.max_receive is not None:
            before = self.max_receive - self.cfg.retention_ms
            for s in self.series.values():
                s.prune(before)
            for t in self.trades.values():
                t.prune(before)

    # ================= helpers =================
    def _key(self, v):
        if v == "kalshi_ws":
            return None
        sym = VENUES[v].symbols.get(self.asset)
        return (v, sym) if sym else None

    def _level_status(self, key, t):
        if key is None or key not in self.recon.tracks:
            return S.MISSING
        st = self.recon.status(key, t)
        if st == BS.READY:
            return S.READY
        if st in (BS.NO_BOOK, BS.AWAITING_SNAPSHOT, BS.WARMING_UP):
            return S.NOT_READY
        return S.MISSING

    def _window_status(self, key, t, w_ms):
        st = self._level_status(key, t)
        if st != S.READY:
            return st
        return S.READY if self.recon.valid_throughout(key, t - w_ms, t) else S.NOT_READY

    def _trade_status(self, src, t, w_ms):
        a = self.alive.get(src)
        if a is None or t - a > self.cfg.source_alive_ms:
            return S.MISSING
        return S.READY if self.first_seen[src] <= t - w_ms else S.NOT_READY

    def _unit_mult(self, v):
        """(multiplier to base coin, status) for size features."""
        if VENUES[v].qty_unit == "coin":
            return 1.0, S.READY
        cv = self.contract_value.get(v)
        return (cv, S.READY) if cv else (None, S.UNAVAILABLE)

    # ================= features =================
    def features_at(self, t, market=None):
        if self.max_receive is not None and t < self.max_receive:
            raise CausalityError(f"features at {t} requested after ingesting data received at {self.max_receive}")
        row = FeatureRow(t, self.asset, market.ticker if market is not None else "")
        for v in EXCHANGE_BOOKS:
            if v in self.venues:
                self._exchange(row, v, t)
        if "kalshi_ws" in self.venues and market is not None:
            self._kalshi(row, market.ticker, t)
        self._cross(row, t)
        for n in FEATURE_NAMES:
            if n not in row.status:
                row.set(n, None, S.UNAVAILABLE)
        row.values = {n: row.values[n] for n in FEATURE_NAMES}
        row.status = {n: row.status[n] for n in FEATURE_NAMES}
        return row

    def _exchange(self, row, v, t):
        p = f"micro.{SHORT[v]}"
        key = self._key(v)
        s = self.series.get(key)
        tr = self.recon.tracks.get(key) if key else None
        lvl = self._level_status(key, t)
        mult, ust = self._unit_mult(v)
        size_st = lvl if lvl != S.READY else ust

        def sz(x):
            return None if (x is None or mult is None) else x * mult
        row.set(f"{p}.book_ready", 1.0 if lvl == S.READY else 0.0, S.READY if tr is not None else S.MISSING)
        book = tr.book if tr is not None else None
        bb = book.best_bid() if (book and lvl == S.READY) else None
        ba = book.best_ask() if (book and lvl == S.READY) else None
        mid = (bb[0] + ba[0]) / 2 if bb and ba else None
        two = lvl == S.READY and bb is not None and ba is not None
        lst = lvl if lvl != S.READY else (S.READY if two else S.UNDEFINED)
        mp = _microprice(bb[0], bb[1], ba[0], ba[1]) if two else None
        row.set(f"{p}.best_bid", bb[0] if bb else None, lvl if lvl != S.READY else (S.READY if bb else S.UNDEFINED))
        row.set(f"{p}.best_ask", ba[0] if ba else None, lvl if lvl != S.READY else (S.READY if ba else S.UNDEFINED))
        row.set(f"{p}.mid", mid, lst)
        row.set(f"{p}.spread", (ba[0] - bb[0]) if two else None, lst)
        row.set(f"{p}.spread_bps", ((ba[0] - bb[0]) / mid * 1e4) if two else None, lst)
        row.set(f"{p}.microprice", mp, lst)
        row.set(f"{p}.microprice_minus_mid_bps", ((mp - mid) / mid * 1e4) if two else None, lst)
        bids = book.top("bid", 20) if (book and lvl == S.READY) else []
        asks = book.top("ask", 20) if (book and lvl == S.READY) else []
        for lv in DEPTH_LEVELS:
            B, A = sum(q for _, q in bids[:lv]), sum(q for _, q in asks[:lv])
            row.set(f"{p}.bid_depth_{lv}", sz(B) if lvl == S.READY else None, size_st)
            row.set(f"{p}.ask_depth_{lv}", sz(A) if lvl == S.READY else None, size_st)
            row.set(f"{p}.imbalance_{lv}", _imb(B, A) if lvl == S.READY else None, lvl)
        B10, A10 = sum(q for _, q in bids[:10]), sum(q for _, q in asks[:10])
        row.set(f"{p}.total_depth_10", sz(B10 + A10) if lvl == S.READY else None, size_st)
        row.set(f"{p}.next_level_gap_bid_bps", ((bids[0][0] - bids[1][0]) / mid * 1e4) if (two and len(bids) > 1) else None, lst)
        row.set(f"{p}.next_level_gap_ask_bps", ((asks[1][0] - asks[0][0]) / mid * 1e4) if (two and len(asks) > 1) else None, lst)
        row.set(f"{p}.depth_concentration_bid", (bids[0][1] / B10) if (bids and B10 > 0) else None, lvl)
        row.set(f"{p}.depth_concentration_ask", (asks[0][1] / A10) if (asks and A10 > 0) else None, lvl)
        for side, lv_ in (("bid", bids[:10]), ("ask", asks[:10])):
            val = None
            if two and lv_:
                cum, num_, den = 0.0, 0.0, 0.0
                for px, q in lv_:
                    cum += q
                    dist = abs(px - mid) / mid * 1e4
                    num_ += cum * dist
                    den += dist * dist
                val = sz(num_ / den) if den > 0 else None
            row.set(f"{p}.depth_slope_{side}", val, size_st if lvl != S.READY else (ust if ust != S.READY else lst))
        # ---- changes over horizons ----
        cur = s.asof(t) if s else None
        for h in CHANGE_H:
            st = self._window_status(key, t, _ms(h))
            old = s.asof(t - _ms(h)) if (s and st == S.READY) else None
            ok = st == S.READY and old is not None and cur is not None
            row.set(f"{p}.imbalance_10_change.{h}", (cur[5] - old[5]) if ok and cur[5] is not None and old[5] is not None else None,
                    st if not ok else S.READY)
            row.set(f"{p}.spread_change_bps.{h}", (cur[6] - old[6]) if ok and cur[6] is not None and old[6] is not None else None,
                    st if not ok else S.READY)
            row.set(f"{p}.microprice_change_bps.{h}", ((cur[7] / old[7] - 1) * 1e4) if ok and cur[7] and old[7] else None,
                    st if not ok else S.READY)
        for h in ACCEL_H:
            st = self._window_status(key, t, 2 * _ms(h))
            a1 = s.asof(t - _ms(h)) if (s and st == S.READY) else None
            a2 = s.asof(t - 2 * _ms(h)) if (s and st == S.READY) else None
            ok = st == S.READY and cur and a1 and a2 and cur[7] and a1[7] and a2[7]
            row.set(f"{p}.microprice_accel_bps.{h}", (((cur[7] - a1[7]) - (a1[7] - a2[7])) / a1[7] * 1e4) if ok else None,
                    st if st != S.READY else (S.READY if ok else S.UNDEFINED))
        # ---- telemetry ----
        seen = self.first_seen.get(v)
        tel_st = S.MISSING if seen is None else (S.READY if seen <= t - 60_000 else S.NOT_READY)
        lats = []
        if s:
            i = bisect.bisect_right(s.lat_t, t - 60_000)
            lats = s.lat[i:]
        row.set(f"{p}.latency_median_ms.60s", _median(lats), tel_st if (tel_st != S.READY or lats) else S.UNDEFINED)
        row.set(f"{p}.book_updates.60s", float(s.count(t - 60_000, t)) if s else 0.0, tel_st)
        row.set(f"{p}.book_age_ms", float(t - s.t[-1]) if s and s.t else None, S.READY if (s and s.t) else S.MISSING)
        # ---- LIQUIDITY ----
        for bp in LIQ_BPS:
            for side, lv_ in (("bid", bids), ("ask", asks)):
                val = None
                if two:
                    lim = mid * bp / 1e4
                    full = book.top(side, 10_000)
                    val = sum(px * q for px, q in full if abs(px - mid) <= lim + 1e-12)
                    val = sz(val)
                row.set(f"{p}.{side}_liquidity_{bp}bps", val, size_st if lvl != S.READY else (ust if ust != S.READY else lst))
        for w in WINDOWS:
            st = self._window_status(key, t, _ms(w))
            fst = st if st != S.READY else ust
            for side, add_a, rem_a in (("bid", "c_badd", "c_brem"), ("ask", "c_aadd", "c_arem")):
                ok = st == S.READY and s is not None
                row.set(f"{p}.{side}_depth_added.{w}", sz(s.window(getattr(s, add_a), t - _ms(w), t)) if ok else None, fst)
                row.set(f"{p}.{side}_depth_removed.{w}", sz(s.window(getattr(s, rem_a), t - _ms(w), t)) if ok else None, fst)
        for w in NET_H:
            st = self._window_status(key, t, _ms(w))
            old = s.asof(t - _ms(w)) if (s and st == S.READY) else None
            ok = st == S.READY and old is not None and cur is not None
            fst = (st if st != S.READY else ust) if not ok or ust != S.READY else S.READY
            row.set(f"{p}.net_bid_depth_change_10.{w}", sz(cur[8] - old[8]) if ok else None, fst if ok else (st if st != S.READY else S.UNDEFINED))
            row.set(f"{p}.net_ask_depth_change_10.{w}", sz(cur[9] - old[9]) if ok else None, fst if ok else (st if st != S.READY else S.UNDEFINED))
        self._baseline(row, p, key, s, t, lvl, two, B10 + A10, ((ba[0] - bb[0]) / mid * 1e4) if two else None)
        # ---- ORDER_FLOW ----
        for w in WINDOWS:
            st = self._window_status(key, t, _ms(w))
            fst = st if st != S.READY else ust
            ok = st == S.READY and s is not None
            row.set(f"{p}.ofi_l1.{w}", sz(s.window(s.c_ofi, t - _ms(w), t)) if ok else None, fst)
            row.set(f"{p}.mlofi_5.{w}", sz(s.window(s.c_mlofi, t - _ms(w), t)) if ok else None, fst)
        tsrc = TRADE_SOURCE[v]
        tsr = self.trades.get(tsrc)
        for w in CANCEL_W:
            st = self._window_status(key, t, _ms(w))
            tst = self._trade_status(tsrc, t, _ms(w))
            fst = st if st != S.READY else (tst if tst != S.READY else ust)
            ok = fst == S.READY and s is not None
            if ok:
                sell_v, buy_v, unconv = self._side_volumes(tsr, t - _ms(w), t)
                if unconv:
                    ok, fst = False, S.UNAVAILABLE
            row.set(f"{p}.est_cancel_bid.{w}", max(0.0, sz(s.window(s.c_brem, t - _ms(w), t)) - sell_v) if ok else None, fst)
            row.set(f"{p}.est_cancel_ask.{w}", max(0.0, sz(s.window(s.c_arem, t - _ms(w), t)) - buy_v) if ok else None, fst)
        st = self._window_status(key, t, 60_000)
        tst = self._trade_status(tsrc, t, 60_000)
        fst = st if st != S.READY else (tst if tst != S.READY else ust)
        val = None
        if fst == S.READY and s is not None:
            sell_v, buy_v, unconv = self._side_volumes(tsr, t - 60_000, t)
            removed = sz(s.window(s.c_brem, t - 60_000, t) + s.window(s.c_arem, t - 60_000, t))
            if unconv:
                fst = S.UNAVAILABLE
            elif removed > 0:
                val = (sell_v + buy_v) / removed
            else:
                fst = S.UNDEFINED
        row.set(f"{p}.est_exec_share_of_removed.60s", val, fst)
        self._trade_feats(row, p, tsrc, t, v)
        self._sweeps(row, p, s, key, t, lvl, sz)
        self._replen(row, p, s, key, t, ("bid", "ask"), ("bid", "ask"))
        self._toxicity(row, p, tsrc, tsrc, s, key, t, bps=True)

    def _side_volumes(self, tsr, lo, hi):
        if tsr is None:
            return 0.0, 0.0, False
        i, j = tsr.idx(lo, hi)
        sell = buy = 0.0
        for k in range(i, j):
            q = tsr.qty[k]
            if q is None:
                return 0.0, 0.0, True
            if tsr.sign[k] < 0:
                sell += q
            elif tsr.sign[k] > 0:
                buy += q
        return sell, buy, False

    def _baseline(self, row, p, key, s, t, lvl, two, depth_now, spread_now):
        cfg = self.cfg
        depths, spreads = [], []
        if s is not None and lvl == S.READY:
            n = cfg.baseline_ms // 1000
            for k in range(n):
                x = t - k * 1000
                if self.recon.valid_since(key, x) is None:
                    continue
                st = s.asof(x)
                if st is None:
                    continue
                depths.append(st[8] + st[9])
                if st[6] is not None:
                    spreads.append(st[6])
            enough = len(depths) >= 0.8 * n
        else:
            enough = False
        st = lvl if lvl != S.READY else (S.READY if enough else S.NOT_READY)
        md, ms_ = _median(depths), _median(spreads)
        dr = (depth_now / md) if (st == S.READY and md) else None
        sr = (spread_now / ms_) if (st == S.READY and ms_ and spread_now is not None) else None
        row.set(f"{p}.depth10_vs_5m_median", dr, st if st != S.READY else (S.READY if dr is not None else S.UNDEFINED))
        row.set(f"{p}.spread_vs_5m_median", sr, st if st != S.READY else (S.READY if sr is not None else S.UNDEFINED))
        vac = None
        if dr is not None and sr is not None:
            vac = 1.0 if (dr < cfg.vacuum_depth_ratio and sr > cfg.vacuum_spread_ratio) else 0.0
        row.set(f"{p}.liquidity_vacuum", vac, st if st != S.READY else (S.READY if vac is not None else S.UNDEFINED))

    def _trade_feats(self, row, p, tsrc, t, v):
        tsr = self.trades.get(tsrc)
        for w in TRADE_COUNT_W:
            st = self._trade_status(tsrc, t, _ms(w))
            n = 0
            if tsr is not None:
                i, j = tsr.idx(t - _ms(w), t)
                n = j - i
            row.set(f"{p}.trade_count.{w}", float(n) if st == S.READY else None, st)
        st5, st60 = self._trade_status(tsrc, t, 5000), self._trade_status(tsrc, t, 60_000)
        n5 = n60 = 0
        if tsr is not None:
            i, j = tsr.idx(t - 5000, t); n5 = j - i
            i, j = tsr.idx(t - 60_000, t); n60 = j - i
        rst = st60 if st60 != S.READY else (S.READY if n60 > 0 else S.UNDEFINED)
        row.set(f"{p}.trade_rate_ratio_5s_60s", ((n5 / 5.0) / (n60 / 60.0)) if (rst == S.READY and st5 == S.READY) else None, rst)
        for w in IMB_W:
            st = self._trade_status(tsrc, t, _ms(w))
            val = None
            if st == S.READY:
                sell, buy, unconv = self._side_volumes(tsr, t - _ms(w), t)
                if unconv:
                    st = S.UNAVAILABLE
                elif buy + sell > 0:
                    val = (buy - sell) / (buy + sell)
                else:
                    st = S.UNDEFINED
            row.set(f"{p}.buy_sell_imbalance.{w}", val, st)
        st = st60
        val = None
        if st == S.READY and tsr is not None:
            i, j = tsr.idx(t - 60_000, t)
            gaps = [tsr.t[k] - tsr.t[k - 1] for k in range(max(i, 1), j) if k - 1 >= i]
            val = _median(gaps)
            if val is None:
                st = S.UNDEFINED
        row.set(f"{p}.median_interarrival_ms.60s", float(val) if val is not None else None, st)
        thr, tst = self._large_threshold(tsr, tsrc, t)
        cnt = share = None
        if tst == S.READY:
            i, j = tsr.idx(t - 60_000, t)
            qs = [tsr.qty[k] for k in range(i, j) if tsr.qty[k] is not None]
            big = [q for q in qs if q > thr]
            cnt = float(len(big))
            share = (sum(big) / sum(qs)) if sum(qs) > 0 else None
        row.set(f"{p}.large_trade_count.60s", cnt, tst)
        row.set(f"{p}.large_trade_volume_share.60s", share, tst if tst != S.READY else (S.READY if share is not None else S.UNDEFINED))

    def _large_threshold(self, tsr, tsrc, t):
        cfg = self.cfg
        st = self._trade_status(tsrc, t, 60_000)
        if st != S.READY:
            return None, st
        if tsr is None:
            return None, S.NOT_READY
        i, j = tsr.idx(t - cfg.large_trade_lookback_ms, t - 60_000)
        qs = sorted(tsr.qty[k] for k in range(i, j) if tsr.qty[k] is not None)
        if len(qs) < cfg.large_trade_min_n or self.first_seen.get(tsrc, t) > t - cfg.large_trade_lookback_ms // 3:
            return None, S.NOT_READY
        return qs[min(len(qs) - 1, int(math.floor(cfg.large_trade_pct * len(qs))))], S.READY

    def _sweeps(self, row, p, s, key, t, lvl, sz):
        st = self._window_status(key, t, 60_000)
        n = sum(1 for x in s.sweeps if t - 60_000 < x[0] <= t) if s else 0
        row.set(f"{p}.sweep_count.60s", float(n) if st == S.READY else None, st)
        if p == "micro.kalshi":
            return
        last = None
        if s:
            for x in reversed(s.sweeps):
                if x[0] <= t:
                    if t - x[0] <= 300_000:
                        last = x
                    break
        base = lvl if lvl != S.READY else (S.READY if last else S.UNDEFINED)
        row.set(f"{p}.last_sweep_direction", float(last[1]) if last else None, base)
        row.set(f"{p}.last_sweep_levels", float(last[2]) if last else None, base)
        mult, ust = self._unit_mult(key[0]) if key else (None, S.UNAVAILABLE)
        row.set(f"{p}.last_sweep_notional", (last[3] * mult) if (last and mult) else None,
                base if base != S.READY else ust)
        row.set(f"{p}.last_sweep_age_ms", float(t - last[0]) if last else None, base)
        row.set(f"{p}.last_sweep_duration_ms", float(last[5]) if last else None, base)
        row.set(f"{p}.last_sweep_impact_bps", last[4] if last else None, base if (not last or last[4] is not None) else S.UNDEFINED)

    def _replen(self, row, p, s, key, t, sides, names):
        st = self._window_status(key, t, 60_000)
        evs = [x for x in s.replen if t - 60_000 < x[0] <= t] if s else []
        for side, nm in zip(sides, names):
            row.set(f"{p}.replenishment_{nm}_count.60s", float(sum(1 for x in evs if x[1] == side)) if st == S.READY else None, st)
        if p != "micro.kalshi":
            row.set(f"{p}.replenishment_max_run.60s", float(max((x[3] for x in evs), default=0)) if st == S.READY else None, st)

    def _toxicity(self, row, p, tkey, tsrc, s, key, t, bps):
        """tkey: trade-series key; tsrc: the source whose liveness gates the windows."""
        cfg = self.cfg
        tsr = self.trades.get(tkey)
        # VPIN-style (exchange books only)
        if bps:
            st = self._trade_status(tsrc, t, 0)
            val = None
            if st == S.READY:
                done = [b for b in (tsr.buckets if tsr else []) if b[0] <= t]
                if tsr is None or tsr.bucket_v is None or len(done) < cfg.vpin_buckets:
                    st = S.NOT_READY
                else:
                    val = sum(b[1] for b in done[-cfg.vpin_buckets:]) / (cfg.vpin_buckets * tsr.bucket_v)
            row.set(f"{p}.vpin_style_50", val, st)
        # trade-sign autocorrelation
        st = self._trade_status(tsrc, t, 60_000)
        val = None
        if st == S.READY:
            sg = []
            if tsr is not None:
                i, j = tsr.idx(t - 60_000, t)
                sg = [tsr.sign[k] for k in range(i, j) if tsr.sign[k] != 0]
            if len(sg) < cfg.autocorr_min_n:
                st = S.NOT_READY
            else:
                x, y = sg[:-1], sg[1:]
                mx, my = sum(x) / len(x), sum(y) / len(y)
                vx = sum((a - mx) ** 2 for a in x)
                vy = sum((b - my) ** 2 for b in y)
                if vx <= 0 or vy <= 0:
                    st = S.UNDEFINED
                else:
                    val = sum((a - mx) * (b - my) for a, b in zip(x, y)) / math.sqrt(vx * vy)
        row.set(f"{p}.trade_sign_autocorr.60s", val, st)
        # adverse move: trades received in [T-65 s, T-5 s], mid 5 s later (<= T)
        name = f"{p}.adverse_move_bps" if bps else f"{p}.adverse_move_cents"
        st = self._trade_status(tsrc, t, cfg.adverse_lookback_ms)
        if st == S.READY:
            st = self._level_status(key, t) if key else S.MISSING
        val = None
        if st == S.READY:
            moves = []
            if tsr is not None and s is not None:
                i, j = tsr.idx(t - cfg.adverse_lookback_ms - 1, t - cfg.adverse_horizon_ms)
                for k in range(i, j):
                    if tsr.sign[k] == 0:
                        continue
                    t0, t1 = tsr.t[k], tsr.t[k] + cfg.adverse_horizon_ms
                    if t1 > t or not self.recon.valid_throughout(key, t0, t1):
                        continue
                    a, b = s.asof(t0), s.asof(t1)
                    if not a or not b or a[0] is None or a[2] is None or b[0] is None or b[2] is None:
                        continue
                    m0, m1 = (a[0] + a[2]) / 2, (b[0] + b[2]) / 2
                    moves.append(tsr.sign[k] * ((m1 - m0) / m0 * 1e4 if bps else (m1 - m0)))
            if len(moves) < cfg.adverse_min_n:
                st = S.NOT_READY
            else:
                val = sum(moves) / len(moves)
        row.set(name, val, st)

    # ================= Kalshi =================
    def _kalshi(self, row, ticker, t):
        p = "micro.kalshi"
        key = ("kalshi_ws", ticker)
        s = self.series.get(key)
        tr = self.recon.tracks.get(key)
        lvl = self._level_status(key, t)
        row.set(f"{p}.book_ready", 1.0 if lvl == S.READY else 0.0, S.READY if tr is not None else S.MISSING)
        book = tr.book if tr is not None else None
        yb = book.top("bid", 10) if (book and lvl == S.READY) else []
        ya = book.top("ask", 10) if (book and lvl == S.READY) else []
        from microstructure.kalshi import executable_state
        ex = executable_state(yb, ya) if lvl == S.READY else {}
        for n in ("yes_bid", "yes_ask", "no_bid", "no_ask"):
            v = ex.get(f"{n}_cents")
            row.set(f"{p}.{n}", v, lvl if lvl != S.READY else (S.READY if v is not None else S.UNDEFINED))
            sz = ex.get(f"{n}_size")
            row.set(f"{p}.{n}_size", sz, lvl if lvl != S.READY else (S.READY if sz is not None else S.UNDEFINED))
        two = lvl == S.READY and yb and ya
        lst = lvl if lvl != S.READY else (S.READY if two else S.UNDEFINED)
        mid = (yb[0][0] + ya[0][0]) / 2 if two else None
        mp = _microprice(yb[0][0], yb[0][1], ya[0][0], ya[0][1]) if two else None
        row.set(f"{p}.spread_cents", (ya[0][0] - yb[0][0]) if two else None, lst)
        row.set(f"{p}.mid_cents", mid, lst)
        row.set(f"{p}.microprice_cents", mp, lst)
        row.set(f"{p}.microprice_minus_mid_cents", (mp - mid) if two else None, lst)
        for lv in K_DEPTH:
            B, A = sum(q for _, q in yb[:lv]), sum(q for _, q in ya[:lv])
            row.set(f"{p}.yes_depth_{lv}", B if lvl == S.READY else None, lvl)
            row.set(f"{p}.no_depth_{lv}", A if lvl == S.READY else None, lvl)
            row.set(f"{p}.imbalance_{lv}", _imb(B, A) if lvl == S.READY else None, lvl)
        cur = s.asof(t) if s else None
        for h in K_H:
            st = self._window_status(key, t, _ms(h))
            old = s.asof(t - _ms(h)) if (s and st == S.READY) else None
            ok = st == S.READY and old is not None and cur is not None
            sp_c = (cur[2] - cur[0]) if ok and cur[0] is not None and cur[2] is not None else None
            sp_o = (old[2] - old[0]) if ok and old[0] is not None and old[2] is not None else None
            row.set(f"{p}.imbalance_5_change.{h}", (cur[4] - old[4]) if ok and cur[4] is not None and old[4] is not None else None,
                    st if not ok else S.READY)
            row.set(f"{p}.spread_change_cents.{h}", (sp_c - sp_o) if (sp_c is not None and sp_o is not None) else None,
                    st if not ok else S.READY)
            row.set(f"{p}.microprice_change_cents.{h}", (cur[7] - old[7]) if ok and cur[7] is not None and old[7] is not None else None,
                    st if not ok else S.READY)
        seen = self.first_seen.get("kalshi_ws")
        tel_st = S.MISSING if seen is None else (S.READY if seen <= t - 60_000 else S.NOT_READY)
        lats = s.lat[bisect.bisect_right(s.lat_t, t - 60_000):] if s else []
        row.set(f"{p}.latency_median_ms.60s", _median(lats), tel_st if (tel_st != S.READY or lats) else S.UNDEFINED)
        row.set(f"{p}.book_updates.60s", float(s.count(t - 60_000, t)) if s else 0.0, tel_st)
        row.set(f"{p}.book_age_ms", float(t - s.t[-1]) if s and s.t else None, S.READY if (s and s.t) else S.MISSING)
        for c in K_LIQ_C:
            row.set(f"{p}.yes_liquidity_{c}c", sum(q for px, q in book.top("bid", 200) if mid - px <= c + 1e-9) if two else None, lst)
            row.set(f"{p}.no_liquidity_{c}c", sum(q for px, q in book.top("ask", 200) if px - mid <= c + 1e-9) if two else None, lst)
        for w in K_CHG_W:
            st = self._window_status(key, t, _ms(w))
            ok = st == S.READY and s is not None
            row.set(f"{p}.yes_depth_added.{w}", s.window(s.c_badd, t - _ms(w), t) if ok else None, st)
            row.set(f"{p}.yes_depth_removed.{w}", s.window(s.c_brem, t - _ms(w), t) if ok else None, st)
            row.set(f"{p}.no_depth_added.{w}", s.window(s.c_aadd, t - _ms(w), t) if ok else None, st)
            row.set(f"{p}.no_depth_removed.{w}", s.window(s.c_arem, t - _ms(w), t) if ok else None, st)
        for w in K_OFI_W:
            st = self._window_status(key, t, _ms(w))
            row.set(f"{p}.ofi_l1.{w}", s.window(s.c_ofi, t - _ms(w), t) if (st == S.READY and s) else None, st)
        tsr = self.trades.get(key)
        for w in K_H:
            st = self._trade_status("kalshi_ws", t, _ms(w))
            n = 0
            if tsr is not None:
                i, j = tsr.idx(t - _ms(w), t)
                n = j - i
            row.set(f"{p}.trade_count.{w}", float(n) if st == S.READY else None, st)
        st = self._trade_status("kalshi_ws", t, 60_000)
        imb = vwap = None
        if st == S.READY and tsr is not None:
            i, j = tsr.idx(t - 60_000, t)
            buy = sum(tsr.qty[k] for k in range(i, j) if tsr.sign[k] > 0)
            sell = sum(tsr.qty[k] for k in range(i, j) if tsr.sign[k] < 0)
            vol = sum(tsr.qty[k] for k in range(i, j))
            imb = (buy - sell) / (buy + sell) if buy + sell > 0 else None
            vwap = sum(tsr.px[k] * tsr.qty[k] for k in range(i, j)) / vol if vol > 0 else None
        row.set(f"{p}.buy_sell_imbalance.60s", imb, st if st != S.READY else (S.READY if imb is not None else S.UNDEFINED))
        row.set(f"{p}.vwap_cents.60s", vwap, st if st != S.READY else (S.READY if vwap is not None else S.UNDEFINED))
        last_t = None
        if tsr is not None:
            i = bisect.bisect_right(tsr.t, t) - 1
            last_t = tsr.t[i] if i >= 0 else None
        row.set(f"{p}.last_trade_age_ms", float(t - last_t) if last_t is not None else None,
                S.READY if last_t is not None else (S.MISSING if seen is None else S.UNDEFINED))
        thr, tst = self._large_threshold(tsr, "kalshi_ws", t)
        cnt = None
        if tst == S.READY:
            i, j = tsr.idx(t - 60_000, t)
            cnt = float(sum(1 for k in range(i, j) if tsr.qty[k] > thr))
        row.set(f"{p}.large_trade_count.60s", cnt, tst)
        self._sweeps(row, p, s, key, t, lvl, None)
        self._replen(row, p, s, key, t, ("bid", "ask"), ("yes", "no"))
        self._toxicity(row, p, key, "kalshi_ws", s, key, t, bps=False)

    # ================= cross venue =================
    def _cross(self, row, t):
        X = "micro.x"
        V = row.values

        def ready(n):
            return row.status.get(n) == S.READY
        spot = [v for v in SPOT_BOOKS if v in self.venues]
        perp = [v for v in PERP_BOOKS if v in self.venues]
        cb, kr = "micro.coinbase.mid", "micro.kraken.mid"
        if ready(cb) and ready(kr):
            row.set(f"{X}.spot_mid_dispersion_bps", abs(V[cb] - V[kr]) / ((V[cb] + V[kr]) / 2) * 1e4)
        else:
            row.set(f"{X}.spot_mid_dispersion_bps", None, S.MISSING if len(spot) == 2 else S.UNAVAILABLE)
        pm = [V[f"micro.{SHORT[v]}.mid"] for v in perp if ready(f"micro.{SHORT[v]}.mid")]
        if len(pm) >= 2:
            row.set(f"{X}.perp_mid_dispersion_bps", (max(pm) - min(pm)) / _median(pm) * 1e4)
        else:
            row.set(f"{X}.perp_mid_dispersion_bps", None, S.MISSING if len(perp) >= 2 else S.UNAVAILABLE)
        for grp, vs in (("spot", spot), ("perp", perp)):
            xs = [V[f"micro.{SHORT[v]}.imbalance_10"] for v in vs if ready(f"micro.{SHORT[v]}.imbalance_10")]
            row.set(f"{X}.imbalance_10_median_{grp}", _median(xs), S.READY if xs else (S.MISSING if vs else S.UNAVAILABLE))
        allx = [V[f"micro.{SHORT[v]}.imbalance_10"] for v in spot + perp if ready(f"micro.{SHORT[v]}.imbalance_10")]
        if len(allx) >= 2:
            md = _median(allx)
            sg = (md > 0) - (md < 0)
            row.set(f"{X}.imbalance_10_sign_agreement", sum(1 for x in allx if ((x > 0) - (x < 0)) == sg) / len(allx))
        else:
            row.set(f"{X}.imbalance_10_sign_agreement", None, S.MISSING if len(spot + perp) >= 2 else S.UNAVAILABLE)
        for w in NET_H:
            xs = [V[f"micro.{SHORT[v]}.ofi_l1.{w}"] for v in spot + perp if ready(f"micro.{SHORT[v]}.ofi_l1.{w}")]
            row.set(f"{X}.ofi_l1_total.{w}", sum(xs) if xs else None, S.READY if xs else (S.MISSING if spot + perp else S.UNAVAILABLE))
        row.set(f"{X}.books_ready", float(sum(1 for v in spot + perp if V.get(f"micro.{SHORT[v]}.book_ready") == 1.0)))


def event_key(e):
    """Availability order across Step-3 / Step-4 / Step-5 events: (receive_ts, ingest_seq, family)."""
    fam = getattr(e, "family", None) or "market"
    return (e.receive_ts_ms, e.ingest_seq, FAMILY_RANK.get(fam, 0))


def available(e, t):
    return e.receive_ts_ms <= t


def compute_at(asset, events, t, market=None, config=None, venues=EXCHANGE_BOOKS + ("kalshi_ws",)):
    """BATCH path: only events available at t (receive_ts <= t), replayed in availability order."""
    eng = MicroFeatureEngine(asset, config, venues)
    for e in sorted((e for e in events if available(e, t)), key=event_key):
        eng.ingest(e)
    return eng.features_at(t, market)
