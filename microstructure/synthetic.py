"""
SYNTHETIC book feeds for offline tests and benchmarks - NOT market data.

SynthBook       a 'true' price-level book per venue / asset that follows the Step-3 World (spot) or the Step-4 PerpWorld
                (perps, USDT) on a tick grid: every step the best levels move with the price, a few sizes change,
                levels crossing the new mid are removed. Kalshi books follow a synthetic probability in YES cents.
VenueFeed       renders that book in each venue's NATIVE websocket format, with the venue's own sequence fields, per
                CONNECTION (connect() -> subscription acks + snapshots; step(t) -> deltas; trades for Kalshi).
FakeBookRest    GET-only fake of Binance /fapi/v1/depth (lastUpdateId = the current update id) and Kalshi /markets.
run_research_session(...)   one deterministic SYNTHETIC session with Step 3 + Step 4 + Step 5 collectors sharing one
                arrival counter, with optional FAULTS:
                    ("drop", venue, t_ms)          the first message of `venue` at or after t is lost (sequence gap)
                    ("crossed", venue, t_ms)       one delta crosses the book (bid above the best ask)
                    ("disconnect", venue, a, b)    messages in [a, b) are lost; disconnect / reconnect reported
                When the live reconstructor demands a resnapshot the loop reconnects the venue (as the runner would).
"""
import json
import math

from market_data.clock import FakeClock
from market_data.synthetic import SERIES, World
from microstructure.book import LocalBook, kraken_checksum
from microstructure.collector import MicroCollector, ResnapshotRequired
from microstructure.poller import SnapshotPoller
from microstructure.sources.binance_depth import BinanceDepthAdapter
from microstructure.sources.bybit_book import BybitBookAdapter
from microstructure.sources.coinbase_l2 import CoinbaseL2Adapter
from microstructure.sources.kalshi_ws import KalshiWsAdapter
from microstructure.sources.kraken_book import KrakenBookAdapter
from microstructure.sources.okx_books import OkxBooksAdapter
from microstructure.venues import VENUES
from perp_data.venues import VENUES as PERP_SPECS
from settlement.synthetic import eastern_ticker, lcg

MICRO_VENUES = ("coinbase_l2", "kraken_book", "binance_usdm_book", "bybit_linear_book", "okx_swap_book", "kalshi_ws")
CADENCE_MS = {"coinbase_l2": 200, "kraken_book": 250, "binance_usdm_book": 100, "bybit_linear_book": 100,
              "okx_swap_book": 100, "kalshi_ws": 500}
OFFSET_MS = {"coinbase_l2": 11, "kraken_book": 23, "binance_usdm_book": 37, "bybit_linear_book": 53,
             "okx_swap_book": 71, "kalshi_ws": 89}
PERP_OF = {"binance_usdm_book": "binance_usdm", "bybit_linear_book": "bybit_linear", "okx_swap_book": "okx_swap"}
ADAPTERS = {"coinbase_l2": CoinbaseL2Adapter, "kraken_book": KrakenBookAdapter, "binance_usdm_book": BinanceDepthAdapter,
            "bybit_linear_book": BybitBookAdapter, "okx_swap_book": OkxBooksAdapter, "kalshi_ws": KalshiWsAdapter}
LEVELS = 40


def tick_of(p):
    e = math.floor(math.log10(p)) - 5
    return 10.0 ** e, max(0, -e)


def _iso(ms):
    from market_data.synthetic import iso
    return iso(ms)


class SynthBook:
    def __init__(self, price_fn, tick, decimals, seed, qty_scale=1.0):
        self.price_fn, self.tick, self.dec = price_fn, tick, decimals
        self.g = lcg(seed)
        self.qty_scale = qty_scale
        self.bids, self.asks = {}, {}

    def _q(self):
        return round(self.qty_scale * math.exp(2.0 * next(self.g)) * 0.2, 6)

    def _px(self, k):
        return round(k * self.tick, self.dec)

    def evolve(self, t, force_cross=False):
        """Advance to time t; return the absolute level changes [(side, px, new_qty)] (0 = removed)."""
        old_b, old_a = dict(self.bids), dict(self.asks)
        m = self.price_fn(t)
        spread_ticks = 1 + int(next(self.g) * 3)
        kb = math.floor(m / self.tick - spread_ticks / 2)
        ka = kb + spread_ticks
        nb = {p: q for p, q in self.bids.items() if p <= self._px(kb) and p >= self._px(kb - LEVELS + 1)}
        na = {p: q for p, q in self.asks.items() if p >= self._px(ka) and p <= self._px(ka + LEVELS - 1)}
        for i in range(LEVELS):
            nb.setdefault(self._px(kb - i), self._q())
            na.setdefault(self._px(ka + i), self._q())
        for _ in range(3):
            i = int(next(self.g) * 10)
            nb[self._px(kb - i)] = self._q()
            j = int(next(self.g) * 10)
            na[self._px(ka + j)] = self._q()
        if force_cross:
            nb[self._px(ka + 1)] = self._q()
        self.bids, self.asks = nb, na
        ch = []
        for side, o, n in (("bid", old_b, nb), ("ask", old_a, na)):
            for p in sorted(set(o) | set(n)):
                if o.get(p) != n.get(p):
                    ch.append((side, p, n.get(p, 0.0)))
        return ch

    def top(self, side, n):
        d = self.bids if side == "bid" else self.asks
        return sorted(d.items(), key=lambda x: -x[0] if side == "bid" else x[0])[:n]


class VenueFeed:
    """Native-format messages of one venue for several assets (one connection at a time)."""

    def __init__(self, venue, assets, world, pworld, seed):
        self.venue, self.assets = venue, list(assets)
        self.spec = VENUES[venue]
        self.books = {}
        self.conn_seq = 0
        self.u = {}                  # asset -> update id (Binance / Bybit / OKX)
        self.sid_seq = {}
        self.kalshi_market = {}
        self.g = lcg(seed)
        self.tid = 0
        for k, a in enumerate(self.assets):
            if venue in PERP_OF:
                fn = (lambda t, a=a: pworld.perp_usdt(PERP_OF[venue], a, t))  # noqa: E731
                p0 = fn(world.start_ms)
            elif venue == "kalshi_ws":
                fn, p0 = None, None
            else:
                fn = (lambda t, a=a, s=(1 + 0.3e-4 * (k + 1) * (1 if venue == "kraken_book" else -1)): world.spot(a, t) * s)  # noqa: E731
                p0 = fn(world.start_ms)
            if fn is not None:
                tick, dec = tick_of(p0)
                self.books[a] = SynthBook(fn, tick, dec, seed * 31 + k)
            self.u[a] = 1000 + 7 * k
        self.world = world

    # ---------- formatting ----------
    def _p(self, a, px):
        return f"{px:.{self.books[a].dec}f}"

    def _okx_ct(self, a, q):
        cv = PERP_SPECS["okx_swap"].contract_values[a]
        return max(1, int(round(q / cv)))

    # ---------- Kalshi ----------
    def _kalshi_prob(self, a, t, close):
        k = self.world.cf(a, close - 900_000)
        z = math.log(self.world.cf(a, t) / k) / max(1e-4 * math.sqrt(max((close - t) / 1000, 1)), 1e-9)
        return min(max(0.5 * (1 + math.erf(z / math.sqrt(2))), 0.03), 0.97)

    def _kalshi_levels(self, a, t, close):
        p = self._kalshi_prob(a, t, close)
        yb = int(math.floor(p * 100 - 1))
        nb = 100 - yb - 2
        yes = {c: 50 + ((c * 37 + t // 500) % 200) for c in range(max(1, yb - 9), yb + 1)}
        no = {c: 40 + ((c * 53 + t // 700) % 180) for c in range(max(1, nb - 9), nb + 1)}
        return yes, no

    # ---------- connections ----------
    def connect(self, t, conn):
        """Messages right after a (re)connect: acks + snapshots for every asset."""
        out = []
        self.conn_seq = 0
        v = self.venue
        if v == "coinbase_l2":
            out.append(self._cb({"channel": "subscriptions", "events": [{"subscriptions": {"level2": [VENUES[v].symbols[a] for a in self.assets]}}]}, t))
            for a in self.assets:
                b = self.books[a]
                if not b.bids:
                    b.evolve(t)
                ups = [{"side": "bid", "event_time": _iso(t), "price_level": self._p(a, p), "new_quantity": f"{q:.6f}"} for p, q in b.top("bid", LEVELS)] + \
                      [{"side": "offer", "event_time": _iso(t), "price_level": self._p(a, p), "new_quantity": f"{q:.6f}"} for p, q in b.top("ask", LEVELS)]
                out.append(self._cb({"channel": "l2_data", "events": [{"type": "snapshot", "product_id": VENUES[v].symbols[a], "updates": ups}]}, t))
        elif v == "kraken_book":
            pairs = [{"symbol": VENUES[v].symbols[a], "price_precision": self.books[a].dec, "qty_precision": 8,
                      "price_increment": self.books[a].tick, "qty_increment": 1e-8} for a in self.assets]
            out.append(json.dumps({"channel": "instrument", "type": "snapshot", "data": {"assets": [], "pairs": pairs}}))
            for a in self.assets:
                b = self.books[a]
                if not b.bids:
                    b.evolve(t)
                out.append(json.dumps({"channel": "book", "type": "snapshot", "data": [self._kraken_view(a, snapshot=True)]}))
        elif v == "bybit_linear_book":
            for a in self.assets:
                b = self.books[a]
                if not b.bids:
                    b.evolve(t)
                self.u[a] += 1
                out.append(json.dumps({"topic": f"orderbook.200.{VENUES[v].symbols[a]}", "type": "snapshot", "ts": t, "cts": t - 3,
                                       "data": {"s": VENUES[v].symbols[a], "b": [[self._p(a, p), f"{q:.6f}"] for p, q in b.top("bid", LEVELS)],
                                                "a": [[self._p(a, p), f"{q:.6f}"] for p, q in b.top("ask", LEVELS)], "u": self.u[a], "seq": self.u[a] * 3}}))
        elif v == "okx_swap_book":
            for a in self.assets:
                b = self.books[a]
                if not b.bids:
                    b.evolve(t)
                self.u[a] += 1
                out.append(json.dumps({"arg": {"channel": "books", "instId": VENUES[v].symbols[a]}, "action": "snapshot", "data": [{
                    "bids": [[self._p(a, p), str(self._okx_ct(a, q)), "0", "3"] for p, q in b.top("bid", LEVELS)],
                    "asks": [[self._p(a, p), str(self._okx_ct(a, q)), "0", "2"] for p, q in b.top("ask", LEVELS)],
                    "ts": str(t - 2), "checksum": 0, "prevSeqId": -1, "seqId": self.u[a]}]}))
        elif v == "binance_usdm_book":
            for a in self.assets:
                if not self.books[a].bids:
                    self.books[a].evolve(t)
        elif v == "kalshi_ws":
            self.sid_seq = {}
            self.kalshi_market = {}
            out += self._kalshi_subscribe(t)
        return out

    def _kalshi_subscribe(self, t):
        out = []
        close = (t // 900_000 + 1) * 900_000
        for k, a in enumerate(self.assets):
            tk = eastern_ticker(SERIES[a], close)
            if self.kalshi_market.get(a, (None,))[0] == tk:
                continue
            sid = 1 + len(self.sid_seq)
            self.sid_seq[sid] = 0
            yes, no = self._kalshi_levels(a, t, close)
            self.kalshi_market[a] = (tk, close, sid, yes, no)
            out.append(json.dumps({"type": "subscribed", "id": sid, "msg": {"channel": "orderbook_delta", "sid": sid}}))
            out.append(self._ks(sid, {"type": "orderbook_snapshot", "msg": {
                "market_ticker": tk, "market_id": f"id-{tk}",
                "yes_dollars_fp": [[f"{c / 100:.4f}", f"{q:.2f}"] for c, q in sorted(yes.items())],
                "no_dollars_fp": [[f"{c / 100:.4f}", f"{q:.2f}"] for c, q in sorted(no.items())]}}))
        return out

    def _ks(self, sid, m):
        self.sid_seq[sid] += 1
        m["sid"], m["seq"] = sid, self.sid_seq[sid]
        return json.dumps(m)

    def _cb(self, m, t):
        m.update({"client_id": "", "timestamp": _iso(t), "sequence_num": self.conn_seq})
        self.conn_seq += 1
        return json.dumps(m)

    def _kraken_view(self, a, snapshot=False, changes=None):
        b = self.books[a]
        view_b, view_a = b.top("bid", self.spec.default_depth), b.top("ask", self.spec.default_depth)
        lb = LocalBook()
        lb.load(view_b, view_a)
        cs = kraken_checksum(lb, b.dec, 8)
        if snapshot:
            return {"symbol": VENUES[self.venue].symbols[a], "bids": [{"price": p, "qty": q} for p, q in view_b],
                    "asks": [{"price": p, "qty": q} for p, q in view_a], "checksum": cs}
        return cs

    def heartbeat(self, t):
        return self._cb({"channel": "heartbeats", "events": [{"current_time": _iso(t), "heartbeat_counter": t // 1000}]}, t) \
            if self.venue == "coinbase_l2" else None

    def step(self, t, force_cross=False):
        out = []
        v = self.venue
        if v == "kalshi_ws":
            out += self._kalshi_subscribe(t)
            for a in self.assets:
                tk, close, sid, yes, no = self.kalshi_market[a]
                if t >= close:
                    continue
                ny, nn = self._kalshi_levels(a, t, close)
                if force_cross:
                    nn = dict(nn)
                    nn[100 - max(yes)] = 10          # a NO bid at 100 - best YES bid: YES ask == YES bid (locked)
                # removals before additions (a real exchange never shows the intermediate crossed state)
                ds = [(side, c, n.get(c, 0) - o.get(c, 0)) for side, o, n in (("yes", yes, ny), ("no", no, nn))
                      for c in sorted(set(o) | set(n)) if n.get(c, 0) != o.get(c, 0)]
                for side, c, d in sorted(ds, key=lambda x: (x[2] > 0, x[0], x[1])):
                    out.append(self._ks(sid, {"type": "orderbook_delta", "msg": {
                        "market_ticker": tk, "price_dollars": f"{c / 100:.4f}", "delta_fp": f"{d:.2f}", "side": side,
                        "ts": t // 1000, "ts_ms": t - 5}}))
                self.kalshi_market[a] = (tk, close, sid, ny, nn)
                if next(self.g) < 0.5:
                    self.tid += 1
                    taker = "yes" if next(self.g) < 0.5 else "no"
                    yp = max(ny) + (2 if taker == "yes" else 0)
                    out.append(json.dumps({"type": "trade", "sid": 99, "msg": {
                        "trade_id": f"k{self.tid}", "market_ticker": tk, "yes_price_dollars": f"{yp / 100:.4f}",
                        "no_price_dollars": f"{(100 - yp) / 100:.4f}", "count_fp": f"{1 + int(next(self.g) * 40)}.00",
                        "taker_side": taker, "ts": t // 1000, "ts_ms": t - 5}}))
            return out
        views = {a: (dict(self.books[a].top("bid", self.spec.default_depth or LEVELS)),
                     dict(self.books[a].top("ask", self.spec.default_depth or LEVELS))) for a in self.assets} \
            if v == "kraken_book" else {}
        for a in self.assets:
            b = self.books[a]
            ch = b.evolve(t, force_cross=force_cross)
            sym = VENUES[v].symbols[a]
            if v == "coinbase_l2":
                ups = [{"side": "bid" if s == "bid" else "offer", "event_time": _iso(t), "price_level": self._p(a, p),
                        "new_quantity": f"{q:.6f}"} for s, p, q in ch]
                out.append(self._cb({"channel": "l2_data", "events": [{"type": "update", "product_id": sym, "updates": ups}]}, t))
            elif v == "kraken_book":
                depth = self.spec.default_depth
                ob, oa = views[a]
                nb, na = dict(b.top("bid", depth)), dict(b.top("ask", depth))
                # levels new to / changed in the subscribed view, and deletions of levels that left the book entirely;
                # levels merely pushed out of range are not sent (the client truncates) - Kraken's documented rule
                bids = [{"price": p, "qty": q} for p, q in sorted(nb.items()) if ob.get(p) != q] + \
                       [{"price": p, "qty": 0.0} for p in sorted(ob) if p not in nb and p not in b.bids]
                asks = [{"price": p, "qty": q} for p, q in sorted(na.items()) if oa.get(p) != q] + \
                       [{"price": p, "qty": 0.0} for p in sorted(oa) if p not in na and p not in b.asks]
                out.append(json.dumps({"channel": "book", "type": "update", "data": [{
                    "symbol": sym, "bids": bids, "asks": asks, "checksum": self._kraken_view(a), "timestamp": _iso(t - 4)}]}))
            elif v == "binance_usdm_book":
                U = self.u[a] + 1
                u = U + int(next(self.g) * 4)
                out.append(json.dumps({"stream": f"{sym.lower()}@depth@100ms", "data": {
                    "e": "depthUpdate", "E": t - 2, "T": t - 4, "s": sym, "U": U, "u": u, "pu": self.u[a],
                    "b": [[self._p(a, p), f"{q:.6f}"] for s, p, q in ch if s == "bid"],
                    "a": [[self._p(a, p), f"{q:.6f}"] for s, p, q in ch if s == "ask"]}}))
                self.u[a] = u
            elif v == "bybit_linear_book":
                self.u[a] += 1
                out.append(json.dumps({"topic": f"orderbook.200.{sym}", "type": "delta", "ts": t, "cts": t - 3, "data": {
                    "s": sym, "b": [[self._p(a, p), f"{q:.6f}"] for s, p, q in ch if s == "bid"],
                    "a": [[self._p(a, p), f"{q:.6f}"] for s, p, q in ch if s == "ask"], "u": self.u[a], "seq": self.u[a] * 3}}))
            elif v == "okx_swap_book":
                prev = self.u[a]
                self.u[a] += 1
                out.append(json.dumps({"arg": {"channel": "books", "instId": sym}, "action": "update", "data": [{
                    "bids": [[self._p(a, p), str(self._okx_ct(a, q) if q else 0), "0", "1"] for s, p, q in ch if s == "bid"],
                    "asks": [[self._p(a, p), str(self._okx_ct(a, q) if q else 0), "0", "1"] for s, p, q in ch if s == "ask"],
                    "ts": str(t - 2), "checksum": 0, "prevSeqId": prev, "seqId": self.u[a]}]}))
        return out

    def binance_snapshot(self, a):
        b = self.books[a]
        return {"lastUpdateId": self.u[a], "E": 0, "T": 0,
                "bids": [[self._p(a, p), f"{q:.6f}"] for p, q in b.top("bid", LEVELS)],
                "asks": [[self._p(a, p), f"{q:.6f}"] for p, q in b.top("ask", LEVELS)]}


class FakeBookRest:
    """GET-only fake: Binance depth snapshots from the live synthetic feed."""

    def __init__(self, feeds, clock):
        self.feeds, self.clock = feeds, clock
        self.requests = []

    def get_json(self, url, params=None):
        self.requests.append(("GET", url, dict(params or {})))
        if url.endswith("/fapi/v1/depth"):
            f = self.feeds["binance_usdm_book"]
            a = {v: k for k, v in VENUES["binance_usdm_book"].symbols.items()}[params["symbol"]]
            return f.binance_snapshot(a)
        raise ConnectionError("HTTP 404")


def micro_adapters(assets, venues=MICRO_VENUES):
    return {v: ADAPTERS[v](assets) for v in venues}


def run_research_session(root, assets=("BTC",), start_ms=1_790_000_000_000, duration_s=300, seed=7, step3=True, perps=True,
                         micro=True, micro_venues=MICRO_VENUES, faults=(), keep_raw=True, fsync=False, kalshi=True,
                         snapshot_delay_ms=150):
    """Write one SYNTHETIC research session (Steps 3 + 4 + 5). Returns (step3 collector, perp collector, micro collector)."""
    from market_data.collector import Collector
    from market_data.kalshi_poller import KalshiPoller
    from market_data.sources.cf import CfViaKalshiAdapter
    from market_data.sources.coinbase import CoinbaseAdapter
    from market_data.sources.kalshi import KalshiAdapter
    from market_data.sources.kraken import KrakenAdapter
    from market_data.synthetic import FakeKalshi, ws_messages
    from perp_data.collector import PerpCollector
    from perp_data.poller import PerpPoller
    from perp_data.sources.binance import BinanceUsdmAdapter
    from perp_data.sources.bybit import BybitLinearAdapter
    from perp_data.sources.okx import OkxSwapAdapter
    from perp_data.sources.stablecoin import CoinbaseUsdtAdapter
    from perp_data.synthetic import FakePerpRest, PerpWorld, perp_ws_messages

    end_ms = start_ms + int(duration_s * 1000)
    world = World(assets, start_ms, end_ms, seed)
    pw = PerpWorld(world)
    clock = FakeClock(wall_ms=start_ms - 1)
    sid = f"SYNTHETIC-{start_ms}-{seed}"
    col3 = pcol = mcol = None
    events = []
    if step3:
        ad3 = {"coinbase": CoinbaseAdapter(assets), "kraken": KrakenAdapter(assets), "cf_via_kalshi": CfViaKalshiAdapter(assets)}
        meta = {n: {"enabled": True, "critical": a.critical, "transport": a.transport} for n, a in ad3.items()}
        meta["_synthetic"] = {"enabled": True, "note": "SYNTHETIC session - not market data"}
        col3 = Collector(root, assets, meta, clock, keep_raw=keep_raw, live_features=False, fsync=fsync, synthetic=True, session_id=sid)
        fk = FakeKalshi(world, clock)
        kpoll = KalshiPoller(KalshiAdapter(assets), fk, col3, clock, trades_every=2)
        events += [(rx, 0, "ws3", src, text) for rx, src, text in ws_messages(world, assets, start_ms, end_ms, seed)]
        if kalshi:
            events += [(t, 1, "poll3", None, None) for t in range(start_ms + 1000, end_ms, 1000)]
    if perps:
        ad4 = {"binance_usdm": BinanceUsdmAdapter(assets), "bybit_linear": BybitLinearAdapter(assets),
               "okx_swap": OkxSwapAdapter(assets, depth=5), "coinbase_usdt": CoinbaseUsdtAdapter()}
        pcol = PerpCollector(root, assets, ad4, clock, session_id=sid, seq=col3.seq if col3 else None, keep_raw=keep_raw,
                             fsync=fsync, synthetic=True, step3_session_dir=col3.dir if col3 else None)
        pmsgs = perp_ws_messages(pw, assets, start_ms, end_ms, seed + 1, ("binance_usdm", "bybit_linear", "okx_swap"), True, 10)
        prest = FakePerpRest(pw, clock, assets, {})
        ppoll = {v: PerpPoller(a, prest, pcol, clock) for v, a in ad4.items() if a.rest_urls()}
        events += [(rx, 2, "ws4", src, text) for rx, src, text in pmsgs]
        events += [(t, 3, "poll4", None, None) for t in range(start_ms + 500, end_ms, 1000)]
    feeds, ad5 = {}, {}
    if micro:
        ad5 = micro_adapters(assets, micro_venues)
        seq = col3.seq if col3 else (pcol.seq if pcol else None)
        mcol = MicroCollector(root, assets, ad5, clock, session_id=sid, seq=seq, keep_raw=keep_raw, fsync=fsync, synthetic=True,
                              step3_session_dir=col3.dir if col3 else None)
        mcol.manifest.notes.append("SYNTHETIC data generated by microstructure.synthetic - not market data")
        for k, v in enumerate(micro_venues):
            feeds[v] = VenueFeed(v, ad5[v].assets, world, pw, seed * 7 + k)
        mrest = FakeBookRest(feeds, clock)
        snap = SnapshotPoller(ad5["binance_usdm_book"], mrest, mcol, clock, min_interval_s=0.5) if "binance_usdm_book" in ad5 else None
        for v in micro_venues:
            events.append((start_ms + OFFSET_MS[v], 4, "connect5", v, None))
            events += [(t, 5, "step5", v, None) for t in range(start_ms + OFFSET_MS[v] + CADENCE_MS[v], end_ms, CADENCE_MS[v])]
            if v == "coinbase_l2":
                events += [(t, 5, "hb5", v, None) for t in range(start_ms + 500, end_ms, 1000)]
        if snap is not None:
            events += [(t, 6, "snap5", None, None) for t in range(start_ms + snapshot_delay_ms, end_ms, 250)]
        if "kalshi_ws" in ad5:
            for a in ad5["kalshi_ws"].assets:
                ad5["kalshi_ws"].markets[eastern_ticker(SERIES[a], (start_ms // 900_000 + 1) * 900_000)] = (start_ms // 900_000 + 1) * 900_000
    events.sort(key=lambda e: (e[0], e[1]))

    def to(t):
        if t > clock.wall_ms():
            clock.advance(t - clock.wall_ms())
    down = {}
    pending = {}
    faults = list(faults)
    mcol.fault_log = [] if mcol else None

    def deliver(v, msgs):
        ad = ad5[v]
        for text in msgs:
            try:
                mcol.on_message(ad, text, clock.wall_ms(), clock.mono_ns())
            except ResnapshotRequired as e:
                mcol.on_disconnect(ad, clock.wall_ms(), f"{v}: {e}")
                pending[v] = clock.wall_ms() + 300              # the runner's backoff, then a fresh connection
                mcol.fault_log.append(("resnapshot", v, clock.wall_ms(), str(e)[:120]))
                return

    for rx, _pri, kind, src, text in events:
        to(rx)
        if kind == "ws3":
            col3.on_message(ad3[src], text, clock.wall_ms(), clock.mono_ns())
        elif kind == "poll3":
            kpoll.poll()
        elif kind == "ws4":
            pcol.on_message(ad4[src], text, clock.wall_ms(), clock.mono_ns())
        elif kind == "poll4":
            for p in ppoll.values():
                try:
                    p.poll()
                except ConnectionError:
                    pass
        elif kind == "snap5":
            try:
                snap.poll()
            except ConnectionError:
                pass
        elif kind in ("connect5", "step5", "hb5"):
            v = src
            f = feeds[v]
            dis = next((x for x in faults if x[0] == "disconnect" and x[1] == v and x[2] <= rx < x[3]), None)
            if dis is not None:
                if kind == "step5":
                    f.step(rx)                                  # the venue keeps moving while we are away
                if not down.get(v):
                    mcol.on_disconnect(ad5[v], clock.wall_ms(), "synthetic outage")
                    down[v] = dis[3]
                continue
            if down.get(v) or (v in pending and rx >= pending[v]):
                down.pop(v, None)
                pending.pop(v, None)
                mcol.on_connect(ad5[v], clock.wall_ms(), reconnect=True)
                deliver(v, f.connect(rx, mcol.conn[v]))
                if kind == "connect5":
                    continue
            elif v in pending:
                if kind == "step5":
                    f.step(rx)
                continue
            if kind == "connect5":
                mcol.on_connect(ad5[v], clock.wall_ms(), reconnect=False)
                deliver(v, f.connect(rx, mcol.conn[v]))
                continue
            if kind == "hb5":
                deliver(v, [f.heartbeat(rx)])
                continue
            cross = next((x for x in faults if x[0] == "crossed" and x[1] == v and x[2] <= rx), None)
            if cross is not None:
                faults.remove(cross)
            msgs = f.step(rx, force_cross=cross is not None)
            drop = next((x for x in faults if x[0] == "drop" and x[1] == v and x[2] <= rx), None)
            if drop is not None and msgs:
                faults.remove(drop)
                mcol.fault_log.append(("dropped", v, rx, msgs[0][:80]))
                msgs = msgs[1:]
            deliver(v, msgs)
    for c in (col3, pcol, mcol):
        if c is not None:
            c.close(status="COMPLETED")
    if mcol is not None:
        mcol.world, mcol.perp_world, mcol.feeds = world, pw, feeds
    return col3, pcol, mcol
