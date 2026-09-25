"""
Deterministic SYNTHETIC market-data sessions for tests, benchmarks and clearly-labelled demos.

EVERYTHING HERE IS FAKE. The messages are shaped like the real payloads (Coinbase ticker / match /
heartbeat, Kraken v2 trade / ticker / heartbeat, Kalshi cfbenchmarks_value, Kalshi REST markets /
orderbook / trades / settled market), so a synthetic session exercises the whole path
    raw text -> adapter -> normalized event -> event store -> replay -> features
exactly as a live capture would. Sessions written by run_session() are marked synthetic in the manifest
(status note + sources["_synthetic"]); no report may present them as real data.

Receive times are event time + a deterministic latency; optional impairments: a Coinbase disconnect
with REST backfill on reconnect, a delayed Coinbase trade, a CF amendment, a duplicate trade.
"""
import datetime as dt
import json
import math

from market_data.clock import FakeClock
from market_data.collector import Collector
from market_data.kalshi_poller import KalshiPoller
from market_data.sources.cf import CfViaKalshiAdapter
from market_data.sources.coinbase import SYMBOLS as CB_SYMBOLS, CoinbaseAdapter
from market_data.sources.kalshi import KalshiAdapter
from market_data.sources.kraken import SYMBOLS as KR_SYMBOLS, KrakenAdapter
from settlement.assets import ASSET_INDEX
from settlement.synthetic import cf_frame, eastern_ticker, kalshi_message, lcg

BASE = {"BTC": 100_000.0, "ETH": 3_500.0, "SOL": 180.0, "XRP": 2.5}
STEP_MS = 250
SERIES = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M", "XRP": "KXXRP15M"}


def iso(ms):
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


class World:
    """The synthetic 'true' prices: a random walk per asset on a 250-ms grid."""

    def __init__(self, assets, start_ms, end_ms, seed=7, vol_bp=1.5):
        self.assets, self.start_ms, self.end_ms = list(assets), start_ms, end_ms
        self.px = {}
        for k, a in enumerate(self.assets):
            g, v, path = lcg(seed * 101 + k), BASE[a], []
            for _t in range(start_ms - 1_200_000, end_ms + 1_200_000, STEP_MS):
                path.append(v)
                v *= math.exp((next(g) - 0.5) * math.sqrt(12.0) * vol_bp * 1e-4)
            self.px[a] = path

    def spot(self, a, t):
        i = (t - (self.start_ms - 1_200_000)) // STEP_MS
        return self.px[a][max(0, min(int(i), len(self.px[a]) - 1))]

    def cf(self, a, t):
        return round(self.spot(a, t) * (1 + 0.5e-4), 6)             # small constant basis vs spot

    def settlement_mean(self, a, close_ms):
        return math.fsum(self.cf(a, close_ms - k * 1000) for k in range(1, 61)) / 60


def _dec(v, digits=2):
    return f"{v:.{digits}f}"


def ws_messages(world, assets, start_ms, end_ms, seed=7):
    """[(receive_ms, source, text)] for Coinbase, Kraken and CF-via-Kalshi, in receive order."""
    g = lcg(seed)
    out = []
    cb_id = {a: 1000 for a in assets}
    kr_id = {a: 5000 for a in assets}
    lat = lambda lo, hi: lo + int(next(g) * (hi - lo))  # noqa: E731
    for t in range(start_ms, end_ms, 100):
        for a in assets:
            p = world.spot(a, t)
            sp = p * 1e-5
            if t % 500 == 0:
                out.append((t + lat(20, 60), "coinbase", json.dumps({
                    "type": "ticker", "product_id": CB_SYMBOLS[a], "time": iso(t), "sequence": t,
                    "price": _dec(p, 6), "best_bid": _dec(p - sp, 6), "best_bid_size": "1.5",
                    "best_ask": _dec(p + sp, 6), "best_ask_size": "2.0"})))
            if t % 700 == 0:
                cb_id[a] += 1
                out.append((t + lat(20, 60), "coinbase", json.dumps({
                    "type": "match", "product_id": CB_SYMBOLS[a], "time": iso(t), "trade_id": cb_id[a],
                    "sequence": t, "price": _dec(p, 6), "size": _dec(0.01 + next(g), 6),
                    "side": "sell" if next(g) < 0.5 else "buy"})))
            if t % 1000 == 0:
                out.append((t + lat(20, 60), "coinbase", json.dumps({
                    "type": "heartbeat", "product_id": CB_SYMBOLS[a], "time": iso(t), "sequence": t,
                    "last_trade_id": cb_id[a]})))
            if t % 600 == 0:
                out.append((t + lat(40, 120), "kraken", json.dumps({
                    "channel": "ticker", "type": "update", "data": [{
                        "symbol": KR_SYMBOLS[a], "bid": round(p * (1 - 2e-5), 6), "bid_qty": 0.8,
                        "ask": round(p * (1 + 2e-5), 6), "ask_qty": 1.1, "last": p}]})))
            if t % 900 == 0:
                kr_id[a] += 1
                out.append((t + lat(40, 120), "kraken", json.dumps({
                    "channel": "trade", "type": "update", "data": [{
                        "symbol": KR_SYMBOLS[a], "side": "buy" if next(g) < 0.5 else "sell",
                        "price": round(p, 6), "qty": round(0.02 + next(g), 6), "ord_type": "market",
                        "trade_id": kr_id[a], "timestamp": iso(t)}]})))
            if t % 1000 == 0:
                out.append((t + lat(120, 300), "cf_via_kalshi", json.dumps(
                    kalshi_message(cf_frame(ASSET_INDEX[a], t, world.cf(a, t)), seq=t // 1000,
                                   avg60=math.fsum(world.cf(a, t - k * 1000) for k in range(60)) / 60))))
        if t % 1000 == 0:
            out.append((t + lat(40, 120), "kraken", json.dumps({"channel": "heartbeat"})))
    # one connection per source delivers in SEND order (TCP): walk the messages in generation (send) order
    # and never let a later message of the same source arrive before an earlier one
    last = {}
    ordered = []
    for rx, src, text in out:
        rx = max(rx, last.get(src, rx))
        last[src] = rx
        ordered.append((rx, src, text))
    ordered.sort(key=lambda x: x[0])                   # stable: same-source order is preserved
    return ordered


class FakeKalshi:
    """GET-only fake of the Kalshi public REST endpoints, answering from the synthetic World."""

    def __init__(self, world, clock, seed=11):
        self.world, self.clock, self.g = world, clock, lcg(seed)
        self.requests = []
        self._tid = 0

    def _close(self, now):
        return (now // 900_000 + 1) * 900_000

    def _market(self, a, close, settled=False):
        k = round(self.world.cf(a, close - 900_000), 2)
        m = {"ticker": eastern_ticker(SERIES[a], close), "close_time": iso(close), "open_time": iso(close - 900_000),
             "floor_strike": k, "strike_type": "greater", "status": "active"}
        now = self.clock.wall_ms()
        z = (math.log(self.world.cf(a, now) / k)) / max(1e-4 * math.sqrt(max((close - now) / 1000, 1)), 1e-9)
        p = min(max(0.5 * (1 + math.erf(z / math.sqrt(2))), 0.02), 0.98)
        yb = math.floor(p * 100 - 1) / 100
        m.update({"yes_bid_dollars": f"{yb:.4f}", "yes_ask_dollars": f"{yb + 0.02:.4f}",
                  "no_bid_dollars": f"{1 - yb - 0.02:.4f}", "no_ask_dollars": f"{1 - yb:.4f}"})
        if settled:
            ev = self.world.settlement_mean(a, close)
            m.update({"status": "settled", "result": "yes" if ev > k else "no", "expiration_value": f"{ev:.6f}"})
        return m

    def get_json(self, url, params=None):
        self.requests.append(("GET", url, dict(params or {})))
        now = self.clock.wall_ms()
        if url.endswith("/markets") and params and "series_ticker" in params:
            a = {v: k for k, v in SERIES.items()}[params["series_ticker"]]
            return {"markets": [self._market(a, self._close(now))], "cursor": ""}
        if url.endswith("/orderbook"):
            return {"orderbook": {"yes_dollars": [["0.4500", 120], ["0.4400", 300]],
                                  "no_dollars": [["0.5300", 90], ["0.5200", 200]]}}
        if url.endswith("/markets/trades"):
            self._tid += 1
            return {"trades": [{"trade_id": f"t{self._tid}", "ticker": params["ticker"], "yes_price_dollars": "0.4600",
                                "count_fp": "5.00", "taker_side": "yes" if self._tid % 2 else "no",
                                "created_time": iso(now - 300)}]}
        ticker = url.rsplit("/", 1)[-1]
        for a, s in SERIES.items():
            if ticker.startswith(s):
                close = None
                for c in range(self._close(now) - 900_000 * 8, self._close(now) + 1, 900_000):
                    if eastern_ticker(s, c) == ticker:
                        close = c
                if close is None:
                    raise ConnectionError("HTTP 404")
                return {"market": self._market(a, close, settled=now >= close + 60_000)}
        raise ConnectionError("HTTP 404")


def run_session(root, assets=("BTC",), start_ms=1_790_000_000_000, duration_s=600, seed=7, kalshi=True,
                cb_disconnect=None, settlement_store_path=None, keep_raw=True, live_features=False, fsync=False):
    """Write one SYNTHETIC session through the real Collector. Returns the Collector (closed).

    cb_disconnect=(a_ms, b_ms): Coinbase messages received in [a, b) are lost; a disconnect / reconnect
    is reported and the lost trades are REST-backfilled at b (BACKFILLED, receive_ts = b).
    """
    end_ms = start_ms + int(duration_s * 1000)
    world = World(assets, start_ms, end_ms, seed)
    clock = FakeClock(wall_ms=start_ms - 1)
    adapters = {"coinbase": CoinbaseAdapter(assets), "kraken": KrakenAdapter(assets),
                "cf_via_kalshi": CfViaKalshiAdapter(assets)}
    ka = KalshiAdapter(assets)
    meta = {n: {"enabled": True, "critical": ad.critical, "transport": ad.transport} for n, ad in adapters.items()}
    meta["kalshi"] = {"enabled": kalshi, "critical": True, "transport": "rest"}
    meta["_synthetic"] = {"enabled": True, "note": "SYNTHETIC session - not market data"}
    col = Collector(root, assets, meta, clock, settlement_store_path=settlement_store_path, keep_raw=keep_raw,
                    live_features=live_features, fsync=fsync, synthetic=True, session_id=f"SYNTHETIC-{start_ms}-{seed}")
    col.manifest.notes.append("SYNTHETIC data generated by market_data.synthetic - not market data")
    fake = FakeKalshi(world, clock)
    poller = KalshiPoller(ka, fake, col, clock, trades_every=2)
    msgs = ws_messages(world, assets, start_ms, end_ms, seed)
    polls = list(range(start_ms + 1000, end_ms, 1000)) if kalshi else []
    lost = []
    disc_done = rec_done = False
    pi = 0

    def to(t):
        if t > clock.wall_ms():
            clock.advance(t - clock.wall_ms())

    for rx, src, text in msgs:
        while pi < len(polls) and polls[pi] <= rx:
            to(polls[pi])
            poller.poll()
            pi += 1
        if cb_disconnect and src == "coinbase":
            a_ms, b_ms = cb_disconnect
            if a_ms <= rx < b_ms:
                if not disc_done:
                    to(a_ms)
                    col.on_disconnect(adapters["coinbase"], clock.wall_ms(), "synthetic outage")
                    disc_done = True
                lost.append(text)
                continue
            if rx >= b_ms and not rec_done:
                to(b_ms)
                col.on_connect(adapters["coinbase"], clock.wall_ms(), reconnect=True)
                by_asset = {}
                for tx in lost:
                    m = json.loads(tx)
                    if m["type"] == "match":
                        by_asset.setdefault(m["product_id"], []).append(
                            {"time": m["time"], "trade_id": m["trade_id"], "price": m["price"], "size": m["size"],
                             "side": m["side"]})
                for a in assets:
                    body = list(reversed(by_asset.get(CB_SYMBOLS[a], [])))      # REST: newest first
                    col.on_rest("coinbase", "rest_backfill", json.dumps(body),
                                lambda ctx, b=body, s=a: adapters["coinbase"].parse_backfill(s, b, ctx),
                                clock.wall_ms(), clock.mono_ns())
                rec_done = True
        to(rx)
        col.on_message(adapters[src], text, clock.wall_ms(), clock.mono_ns())
    while pi < len(polls):
        to(polls[pi])
        poller.poll()
        pi += 1
    col.close(status="COMPLETED")
    col.world = world
    col.fake_kalshi = fake
    return col
