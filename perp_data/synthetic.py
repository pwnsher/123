"""
Deterministic SYNTHETIC derivatives sessions (Step 4) — for tests, benchmarks and clearly-labelled demos.

EVERYTHING HERE IS FAKE. Messages are shaped like each venue's documented payloads (Binance combined-stream
aggTrade / bookTicker / markPriceUpdate / forceOrder / depthUpdate and REST openInterest / fundingInfo /
fundingRate / aggTrades; Bybit publicTrade / tickers snapshot+delta / orderbook snapshot+delta /
allLiquidation and REST funding history / recent trades; OKX trades / books5 / mark-price / index-tickers /
funding-rate / open-interest / liquidation-orders and REST instruments / funding history / trades; Kalshi
perps /margin/markets + funding estimate; Coinbase USDT-USD ticker) and go through the REAL adapters,
collectors, store and replay. Sessions are marked synthetic (session id SYNTHETIC-..., manifests).

run_joint_session() writes a Step-3 session (spot / CF / Kalshi binary, via market_data.synthetic) and a
Step-4 session (<session>/perp/) in ONE arrival order with ONE shared arrival counter - exactly like the
combined collector - so the joint dataset can be tested. Optional impairments: a venue websocket outage
(with Binance REST backfill by aggregate-trade id on reconnect) and a liquidation cascade window.
"""
import json
import math

from market_data.clock import FakeClock
from market_data.collector import Collector
from market_data.kalshi_poller import KalshiPoller
from market_data.sources.cf import CfViaKalshiAdapter
from market_data.sources.coinbase import CoinbaseAdapter
from market_data.sources.kalshi import KalshiAdapter
from market_data.sources.kraken import KrakenAdapter
from market_data.synthetic import FakeKalshi, World, iso, ws_messages
from perp_data.collector import PerpCollector
from perp_data.poller import PerpPoller, backfill
from perp_data.sources.binance import BinanceUsdmAdapter
from perp_data.sources.bybit import BybitLinearAdapter
from perp_data.sources.kalshi_perp import KalshiPerpAdapter
from perp_data.sources.okx import OkxSwapAdapter
from perp_data.sources.stablecoin import CoinbaseUsdtAdapter
from settlement.synthetic import lcg

SCALE = {"BTC": 1.0, "ETH": 10.0, "SOL": 100.0, "XRP": 10_000.0}          # typical trade size in coin
OI_BASE = {"BTC": 80_000.0, "ETH": 900_000.0, "SOL": 9_000_000.0, "XRP": 500_000_000.0}
CT_VAL = {"BTC": 0.01, "ETH": 0.1, "SOL": 1.0, "XRP": 100.0}
VENUE_OFF = {"binance_usdm": 0.0, "bybit_linear": 1.0, "okx_swap": -1.0, "kalshi_perp": 0.5}
EIGHT_H = 8 * 3_600_000


class PerpWorld:
    """Synthetic perp prices around the Step-3 synthetic spot world."""

    def __init__(self, world, cascade=None):
        self.w = world
        self.cascade = cascade                     # (a_ms, b_ms): liquidation burst + OI drop + basis dislocation

    def usdt(self, t):
        return 1.0002 + 0.0001 * math.sin(t / 600_000)

    def in_cascade(self, t):
        return self.cascade is not None and self.cascade[0] <= t < self.cascade[1]

    def basis(self, v, t):
        b = (2.0 + VENUE_OFF[v] + 3.0 * math.sin(2 * math.pi * t / 300_000 + VENUE_OFF[v])) * 1e-4
        return b - (8e-4 if self.in_cascade(t) else 0.0)

    def perp_usd(self, v, a, t):
        return self.w.spot(a, t) * (1 + self.basis(v, t))

    def perp_usdt(self, v, a, t):
        return self.perp_usd(v, a, t) / self.usdt(t)

    def mark_usdt(self, v, a, t):
        return self.perp_usdt(v, a, t) * (1 + 0.3e-4)

    def index_usdt(self, a, t):
        return self.w.spot(a, t) / self.usdt(t)

    def funding(self, v, t):
        return 1e-4 + 0.5e-4 * math.sin(t / 900_000 + VENUE_OFF[v])

    def oi_coin(self, v, a, t):
        base = OI_BASE[a] * (1.0 + 0.1 * VENUE_OFF[v])
        drift = 1.0 + 0.002 * math.sin(t / 120_000 + VENUE_OFF[v])
        if self.cascade and t >= self.cascade[0]:
            drop = min(t, self.cascade[1]) - self.cascade[0]
            drift *= 1.0 - 0.03 * drop / max(self.cascade[1] - self.cascade[0], 1)
        return base * drift

    def next_funding(self, t):
        return (t // EIGHT_H + 1) * EIGHT_H


def _f(x, d=6):
    return f"{x:.{d}f}"


def perp_ws_messages(pw, assets, start_ms, end_ms, seed=11, venues=("binance_usdm", "bybit_linear", "okx_swap"),
                     usdt=True, book_levels=10):
    """[(receive_ms, source, text)] in receive order; FIFO per source (one connection per venue)."""
    g = lcg(seed)
    out = []
    lat = lambda lo, hi: lo + int(next(g) * (hi - lo))  # noqa: E731
    agg_id = {a: 5_000_000 for a in assets}
    bb_trade = {a: 0 for a in assets}
    u_prev = {a: 900_000 for a in assets}
    bb_u = {a: 0 for a in assets}
    bb_book = {}
    ok_trade = {a: 700_000 for a in assets}
    for t in range(start_ms, end_ms, 100):
        casc = pw.in_cascade(t)
        for a in assets:
            sym = f"{a}USDT"
            s = sym.lower()
            if "binance_usdm" in venues:
                v = "binance_usdm"
                px = pw.perp_usdt(v, a, t)
                if t % 400 == 0 or (casc and t % 200 == 0):
                    agg_id[a] += 1
                    out.append((t + lat(10, 40), v, json.dumps({"stream": f"{s}@aggTrade", "data": {
                        "e": "aggTrade", "E": t + 5, "s": sym, "a": agg_id[a], "p": _f(px, 4),
                        "q": _f(SCALE[a] * (0.01 + next(g)), 4), "f": agg_id[a] * 3, "l": agg_id[a] * 3 + 1, "T": t,
                        "m": (next(g) < (0.7 if casc else 0.5))}})))
                if t % 250 == 0:
                    sp = px * 1e-5
                    out.append((t + lat(10, 40), v, json.dumps({"stream": f"{s}@bookTicker", "data": {
                        "e": "bookTicker", "u": t, "E": t + 3, "T": t, "s": sym, "b": _f(px - sp, 4),
                        "B": _f(SCALE[a] * 2, 3), "a": _f(px + sp, 4), "A": _f(SCALE[a] * 1.5, 3)}})))
                if t % 1000 == 0:
                    out.append((t + lat(10, 40), v, json.dumps({"stream": f"{s}@markPrice@1s", "data": {
                        "e": "markPriceUpdate", "E": t, "s": sym, "p": _f(pw.mark_usdt(v, a, t), 4),
                        "i": _f(pw.index_usdt(a, t), 4), "P": _f(pw.mark_usdt(v, a, t), 4),
                        "r": f"{pw.funding(v, t):.8f}", "T": pw.next_funding(t)}})))
                if t % 500 == 0:
                    lv = lambda sgn: [[_f(px * (1 + sgn * (k + 1) * 1e-5), 4), _f(SCALE[a] * (1 + k), 3)]  # noqa: E731
                                      for k in range(book_levels)]
                    u = u_prev[a] + 7
                    out.append((t + lat(10, 40), v, json.dumps({"stream": f"{s}@depth{book_levels}@100ms", "data": {
                        "e": "depthUpdate", "E": t + 2, "T": t, "s": sym, "U": u_prev[a] + 1, "u": u, "pu": u_prev[a],
                        "b": lv(-1), "a": lv(1)}})))
                    u_prev[a] = u
                if casc and t % 1000 == 0:          # the stream pushes at most the largest one per second
                    q = SCALE[a] * (1 + 3 * next(g))
                    out.append((t + lat(10, 40), v, json.dumps({"stream": f"{s}@forceOrder", "data": {
                        "e": "forceOrder", "E": t + 1, "o": {"s": sym, "S": "SELL" if next(g) < 0.8 else "BUY",
                                                             "o": "LIMIT", "f": "IOC", "q": _f(q, 3), "p": _f(px * 0.999, 4),
                                                             "ap": _f(px * 0.9995, 4), "X": "FILLED", "l": _f(q, 3),
                                                             "z": _f(q, 3), "T": t}}})))
            if "bybit_linear" in venues:
                v = "bybit_linear"
                px = pw.perp_usdt(v, a, t)
                if t % 500 == 0 or (casc and t % 250 == 0):
                    bb_trade[a] += 1
                    out.append((t + lat(20, 60), v, json.dumps({"topic": f"publicTrade.{sym}", "type": "snapshot",
                                                                "ts": t + 3, "data": [{
                                                                    "T": t, "s": sym, "S": "Sell" if next(g) < (0.7 if casc else 0.5) else "Buy",
                                                                    "v": _f(SCALE[a] * (0.01 + next(g)), 3), "p": _f(px, 2),
                                                                    "L": "PlusTick", "i": f"bb-{a}-{bb_trade[a]}", "BT": False}]})))
                if t == start_ms or t % 500 == 0:
                    first = t == start_ms
                    d = {"symbol": sym, "markPrice": _f(pw.mark_usdt(v, a, t), 2), "indexPrice": _f(pw.index_usdt(a, t), 2),
                         "bid1Price": _f(px * (1 - 1e-5), 2), "bid1Size": _f(SCALE[a] * 2, 3),
                         "ask1Price": _f(px * (1 + 1e-5), 2), "ask1Size": _f(SCALE[a], 3)}
                    if first or t % 2000 == 0:
                        oi = pw.oi_coin(v, a, t)
                        d.update({"openInterest": _f(oi, 3), "openInterestValue": _f(oi * px, 2)})
                    if first or t % 10_000 == 0:
                        d.update({"fundingRate": f"{pw.funding(v, t):.6f}", "nextFundingTime": str(pw.next_funding(t)),
                                  "fundingIntervalHour": "8"})
                    out.append((t + lat(20, 60), v, json.dumps({"topic": f"tickers.{sym}", "type": "snapshot" if first else "delta",
                                                                "data": d, "cs": t, "ts": t})))
                if t == start_ms or t % 500 == 0:
                    first = t == start_ms
                    bb_u[a] = 1 if first else bb_u[a] + 1
                    lv = lambda sgn: [[_f(px * (1 + sgn * (k + 1) * 1e-5), 2), _f(SCALE[a] * (1 + k + next(g)), 3)]  # noqa: E731
                                      for k in range(12)]
                    nb, na = lv(-1), lv(1)
                    if first:
                        db, da = nb, na
                    else:                          # delta: delete levels that moved away, then set the new ones
                        pb, pa = bb_book.get(a, ([], []))
                        keep_b, keep_a = {x[0] for x in nb}, {x[0] for x in na}
                        db = [[x[0], "0"] for x in pb if x[0] not in keep_b] + nb
                        da = [[x[0], "0"] for x in pa if x[0] not in keep_a] + na
                    bb_book[a] = (nb, na)
                    out.append((t + lat(20, 60), v, json.dumps({"topic": f"orderbook.50.{sym}",
                                                                "type": "snapshot" if first else "delta", "ts": t + 2,
                                                                "cts": t, "data": {"s": sym, "b": db, "a": da,
                                                                                   "u": bb_u[a], "seq": t}})))
                if casc and t % 300 == 0:
                    out.append((t + lat(20, 60), v, json.dumps({"topic": f"allLiquidation.{sym}", "type": "snapshot", "ts": t + 5,
                                                                "data": [{"T": t, "s": sym, "S": "Buy" if next(g) < 0.8 else "Sell",
                                                                          "v": _f(SCALE[a] * (0.5 + next(g)), 3),
                                                                          "p": _f(px * 0.999, 2)}]})))
            if "okx_swap" in venues:
                v = "okx_swap"
                inst = f"{a}-USDT-SWAP"
                px = pw.perp_usdt(v, a, t)
                if t % 600 == 0:
                    ok_trade[a] += 1
                    out.append((t + lat(30, 80), v, json.dumps({"arg": {"channel": "trades", "instId": inst}, "data": [{
                        "instId": inst, "tradeId": str(ok_trade[a]), "px": _f(px, 2),
                        "sz": str(max(1, int(SCALE[a] * (0.01 + next(g)) / CT_VAL[a]))),
                        "side": "sell" if next(g) < (0.7 if casc else 0.5) else "buy", "ts": str(t)}]})))
                if t % 500 == 0:
                    lv = lambda sgn: [[_f(px * (1 + sgn * (k + 1) * 1e-5), 2), str(int(SCALE[a] * (1 + k) / CT_VAL[a])), "0", "3"]  # noqa: E731
                                      for k in range(5)]
                    out.append((t + lat(30, 80), v, json.dumps({"arg": {"channel": "books5", "instId": inst}, "data": [{
                        "asks": lv(1), "bids": lv(-1), "instId": inst, "ts": str(t), "seqId": t}]})))
                if t % 1000 == 0:
                    out.append((t + lat(30, 80), v, json.dumps({"arg": {"channel": "mark-price", "instId": inst}, "data": [{
                        "instId": inst, "instType": "SWAP", "markPx": _f(pw.mark_usdt(v, a, t), 2), "ts": str(t)}]})))
                    out.append((t + lat(30, 80), v, json.dumps({"arg": {"channel": "index-tickers", "instId": f"{a}-USDT"},
                                                                "data": [{"instId": f"{a}-USDT", "idxPx": _f(pw.index_usdt(a, t), 2),
                                                                          "ts": str(t)}]})))
                if t % 3000 == 0:
                    oi = pw.oi_coin(v, a, t)
                    out.append((t + lat(30, 80), v, json.dumps({"arg": {"channel": "open-interest", "instId": inst}, "data": [{
                        "instId": inst, "instType": "SWAP", "oi": _f(oi / CT_VAL[a], 0), "oiCcy": _f(oi, 2),
                        "oiUsd": _f(oi * px, 2), "ts": str(t)}]})))
                if t % 10_000 == 0:
                    ft = pw.next_funding(t)
                    out.append((t + lat(30, 80), v, json.dumps({"arg": {"channel": "funding-rate", "instId": inst}, "data": [{
                        "instId": inst, "instType": "SWAP", "method": "next_period", "fundingRate": f"{pw.funding(v, t):.8f}",
                        "nextFundingRate": f"{pw.funding(v, t + EIGHT_H):.8f}", "fundingTime": str(ft),
                        "nextFundingTime": str(ft + EIGHT_H), "ts": str(t)}]})))
                if casc and t % 1000 == 500:
                    forced = "sell" if next(g) < 0.8 else "buy"
                    out.append((t + lat(30, 80), v, json.dumps({"arg": {"channel": "liquidation-orders", "instType": "SWAP"},
                                                                "data": [{"instId": inst, "instFamily": f"{a}-USDT", "instType": "SWAP",
                                                                          "uly": f"{a}-USDT", "details": [{
                                                                              "side": forced, "posSide": "long" if forced == "sell" else "short",
                                                                              "bkPx": _f(px * 0.998, 2),
                                                                              "sz": str(max(1, int(SCALE[a] / CT_VAL[a]))),
                                                                              "bkLoss": "0", "ccy": "", "ts": str(t)}]}]})))
        if usdt and t % 1000 == 0:
            r = pw.usdt(t)
            out.append((t + lat(20, 60), "coinbase_usdt", json.dumps({
                "type": "ticker", "product_id": "USDT-USD", "time": iso(t), "best_bid": _f(r - 0.00005, 5),
                "best_ask": _f(r + 0.00005, 5), "price": _f(r, 5)})))
    last = {}
    ordered = []
    for rx, src, text in out:                       # FIFO per connection
        rx = max(rx, last.get(src, rx))
        last[src] = rx
        ordered.append((rx, src, text))
    ordered.sort(key=lambda x: x[0])
    return ordered


class FakePerpRest:
    """GET-only fake of the perp venues' public REST endpoints, answering from the synthetic world at clock time."""

    def __init__(self, pw, clock, assets, ws_log=None):
        self.pw, self.clock, self.assets = pw, clock, assets
        self.requests = []
        self.ws_log = ws_log if ws_log is not None else {}     # backfill source: trades the fake exchange "has"
        self.fail = set()                                       # URL substrings that answer with an error

    def get_json(self, url, params=None):
        self.requests.append(("GET", url, dict(params or {})))
        if any(f in url for f in self.fail):
            raise ConnectionError("HTTP 503")
        t = self.clock.wall_ms()
        p = params or {}
        pw = self.pw
        if url.endswith("/fapi/v1/openInterest"):
            a = p["symbol"][:-4]
            return {"openInterest": _f(pw.oi_coin("binance_usdm", a, t - 50), 3), "symbol": p["symbol"], "time": t - 50}
        if url.endswith("/fapi/v1/fundingInfo"):
            return [{"symbol": "DOGEUSDT", "adjustedFundingRateCap": "0.02", "adjustedFundingRateFloor": "-0.02",
                     "fundingIntervalHours": 4, "disclaimer": False}]
        if url.endswith("/fapi/v1/fundingRate"):
            ft = (t // EIGHT_H) * EIGHT_H
            return [{"symbol": p["symbol"], "fundingTime": ft - k * EIGHT_H,
                     "fundingRate": f"{pw.funding('binance_usdm', ft - k * EIGHT_H):.8f}", "markPrice": "1"} for k in (1, 0)]
        if url.endswith("/fapi/v1/aggTrades"):
            a = p["symbol"][:-4]
            rows = [r for r in self.ws_log.get(("binance_usdm", a), []) if r["T"] < t]
            if "fromId" in p:
                rows = [r for r in rows if r["a"] >= p["fromId"]]
            return rows[: p.get("limit", 500)]
        if url.endswith("/v5/market/funding/history"):
            ft = (t // EIGHT_H) * EIGHT_H
            return {"retCode": 0, "retMsg": "OK", "result": {"category": "linear", "list": [
                {"symbol": p["symbol"], "fundingRate": f"{pw.funding('bybit_linear', ft - k * EIGHT_H):.6f}",
                 "fundingRateTimestamp": str(ft - k * EIGHT_H)} for k in (0, 1)]}}
        if url.endswith("/v5/market/recent-trade"):
            return {"retCode": 0, "result": {"list": []}}
        if url.endswith("/api/v5/public/instruments"):
            return {"code": "0", "msg": "", "data": [{"instId": f"{a}-USDT-SWAP", "ctVal": str(CT_VAL[a]), "ctValCcy": a,
                                                      "ctType": "linear", "state": "live"} for a in ("BTC", "ETH", "SOL", "XRP")]}
        if url.endswith("/api/v5/public/funding-rate-history"):
            ft = (t // EIGHT_H) * EIGHT_H
            return {"code": "0", "msg": "", "data": [{"instId": p["instId"], "fundingTime": str(ft - k * EIGHT_H),
                                                      "fundingRate": f"{pw.funding('okx_swap', ft - k * EIGHT_H):.8f}",
                                                      "realizedRate": f"{pw.funding('okx_swap', ft - k * EIGHT_H):.8f}"}
                                                     for k in (0, 1)]}
        if url.endswith("/api/v5/market/trades"):
            return {"code": "0", "data": []}
        if url.endswith("/margin/markets"):
            k = 0.001
            ms = []
            for a in self.assets:
                spot = pw.w.cf(a, t)
                px = pw.perp_usd("kalshi_perp", a, t) * k
                ms.append({"ticker": f"KX{a}PERP", "status": "active", "bid": _f(px * (1 - 2e-4), 4),
                           "ask": _f(px * (1 + 2e-4), 4), "price": _f(px, 4),
                           "settlement_mark_price": {"price": _f(px * (1 + 1e-4), 4), "ts_ms": t - 500},
                           "reference_price": {"price": _f(spot * k, 4), "ts_ms": t - 800},
                           "contract_size": "1", "underlying_multiplier": str(k)})
            return {"markets": ms}
        if url.endswith("/margin/funding_rates/estimate"):
            return {"funding_rate": f"{pw.funding('kalshi_perp', t):.8f}", "next_funding_time": pw.next_funding(t),
                    "computed_time": t - 1000, "mark_price": "1"}
        raise ConnectionError("HTTP 404")


def run_joint_session(root, assets=("BTC",), start_ms=1_790_000_000_000, duration_s=600, seed=7, step3=True, perps=True,
                      venues=("binance_usdm", "bybit_linear", "okx_swap", "kalshi_perp"), usdt=True, cascade=None,
                      perp_disconnect=None, keep_raw=True, fsync=False, kalshi=True, book_levels=10):
    """Write one SYNTHETIC joint session. Returns (step3_collector | None, perp_collector | None).

    perp_disconnect=(venue, a_ms, b_ms): that venue's websocket messages received in [a, b) are lost; a disconnect
    and reconnect are reported and (Binance) the missed aggregate trades are REST-backfilled by id at b.
    """
    end_ms = start_ms + int(duration_s * 1000)
    world = World(assets, start_ms, end_ms, seed)
    pw = PerpWorld(world, cascade)
    clock = FakeClock(wall_ms=start_ms - 1)
    sid = f"SYNTHETIC-{start_ms}-{seed}"
    col3 = pcol = None
    events = []
    if step3:
        ad3 = {"coinbase": CoinbaseAdapter(assets), "kraken": KrakenAdapter(assets), "cf_via_kalshi": CfViaKalshiAdapter(assets)}
        meta = {n: {"enabled": True, "critical": a.critical, "transport": a.transport} for n, a in ad3.items()}
        meta["_synthetic"] = {"enabled": True, "note": "SYNTHETIC session - not market data"}
        col3 = Collector(root, assets, meta, clock, keep_raw=keep_raw, live_features=False, fsync=fsync, synthetic=True,
                         session_id=sid)
        col3.manifest.notes.append("SYNTHETIC data generated by perp_data.synthetic - not market data")
        fk = FakeKalshi(world, clock)
        kpoll = KalshiPoller(KalshiAdapter(assets), fk, col3, clock, trades_every=2)
        events += [(rx, 0, "ws3", src, text) for rx, src, text in ws_messages(world, assets, start_ms, end_ms, seed)]
        if kalshi:
            events += [(t, 1, "poll3", None, None) for t in range(start_ms + 1000, end_ms, 1000)]
    ad4 = {}
    if perps:
        ws_venues = tuple(v for v in venues if v != "kalshi_perp")
        mk = {"binance_usdm": BinanceUsdmAdapter, "bybit_linear": BybitLinearAdapter, "okx_swap": OkxSwapAdapter}
        for v in ws_venues:
            ad4[v] = mk[v](assets, depth=book_levels if v != "okx_swap" else 5)
        if "kalshi_perp" in venues:
            ad4["kalshi_perp"] = KalshiPerpAdapter(assets, overrides={})
        if usdt:
            ad4["coinbase_usdt"] = CoinbaseUsdtAdapter()
        pcol = PerpCollector(root, assets, ad4, clock, session_id=sid, seq=col3.seq if col3 else None, keep_raw=keep_raw,
                             fsync=fsync, synthetic=True, step3_session_dir=col3.dir if col3 else None)
        pcol.manifest.notes.append("SYNTHETIC data generated by perp_data.synthetic - not market data")
        pmsgs = perp_ws_messages(pw, assets, start_ms, end_ms, seed + 1, ws_venues, usdt, book_levels)
        ws_log = {}
        for rx, src, text in pmsgs:
            if src == "binance_usdm" and "@aggTrade" in text:
                d = json.loads(text)["data"]
                ws_log.setdefault((src, d["s"][:-4]), []).append({k: d[k] for k in ("a", "p", "q", "f", "l", "T", "m")})
        rest = FakePerpRest(pw, clock, assets, ws_log)
        pollers = {v: PerpPoller(a, rest, pcol, clock) for v, a in ad4.items() if a.rest_urls()}
        events += [(rx, 2, "ws4", src, text) for rx, src, text in pmsgs]
        events += [(t, 3, "poll4", None, None) for t in range(start_ms + 500, end_ms, 1000)]
    events.sort(key=lambda e: (e[0], e[1]))

    def to(t):
        if t > clock.wall_ms():
            clock.advance(t - clock.wall_ms())
    down = False
    for rx, _pri, kind, src, text in events:
        if perp_disconnect and kind == "ws4" and src == perp_disconnect[0]:
            v, a_ms, b_ms = perp_disconnect
            if a_ms <= rx < b_ms:
                if not down:
                    to(a_ms)
                    pcol.on_disconnect(ad4[v], clock.wall_ms(), "synthetic outage")
                    down = True
                continue
            if rx >= b_ms and down:
                to(b_ms)
                pcol.on_connect(ad4[v], clock.wall_ms(), reconnect=True)
                backfill(ad4[v], rest, pcol, clock)
                down = False
        to(rx)
        if kind == "ws3":
            col3.on_message(ad3[src], text, clock.wall_ms(), clock.mono_ns())
        elif kind == "poll3":
            kpoll.poll()
        elif kind == "ws4":
            pcol.on_message(ad4[src], text, clock.wall_ms(), clock.mono_ns())
        else:
            for p in pollers.values():
                try:
                    p.poll()
                except ConnectionError:
                    pass
    if col3:
        col3.close(status="COMPLETED")
    if pcol:
        pcol.close(status="COMPLETED")
        pcol.world, pcol.perp_world, pcol.rest = world, pw, rest
    return col3, pcol
