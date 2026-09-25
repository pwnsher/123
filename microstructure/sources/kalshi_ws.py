"""
Kalshi websocket — orderbook_delta (snapshot + deltas) and public trade channels, READ ONLY.

Transport  wss://api.elections.kalshi.com/trade-api/ws/v2 (env KALSHI_WS_URL overrides). The connection is
           authenticated with the read-only API key headers already used by Step 3's CF-via-Kalshi feed
           (market_data.transport.kalshi_auth.ws_headers, GET signature); no order command exists here.
Commands   {"id": n, "cmd": "subscribe", "params": {"channels": ["orderbook_delta", "trade"], "market_tickers": [...]}}
           {"id": n, "cmd": "unsubscribe", "params": {"sids": [...]}}   (markets closed for > 2 minutes)
           The markets to follow come from GET /markets?series_ticker=<S>&status=open (MarketsPoller); new
           markets are subscribed on the open connection (a new subscription id each time).

Messages (both the dollar / fixed-point and the legacy cent / integer field names are accepted)
    orderbook_snapshot  {sid, seq, msg: {market_ticker, yes_dollars_fp | yes_dollars | yes: [[price, qty]], no...}}
                        -> BOOK_SNAPSHOT in YES terms: bids = YES bids; asks = 100 - NO bids (cents). The native
                        NO bids are kept under payload["native_no_bids"].
    orderbook_delta     {sid, seq, msg: {market_ticker, price_dollars | price, delta_fp | delta, side yes|no, ts_ms}}
                        delta is a RELATIVE size change at a price -> BOOK_DELTA [side, px, dq, "rel"]; a NO-side
                        change at p cents becomes an ASK change at 100 - p (YES terms).
    trade               {msg: {trade_id, market_ticker, yes_price_dollars | yes_price, count_fp | count, taker_side,
                        ts_ms | ts}} -> TRADE (price in YES cents; aggressor = "buy" when the taker bought YES,
                        "sell" when the taker bought NO, i.e. sold YES in YES terms).
    subscribed / ok / error   control
Sequence: seq increases by exactly 1 per message on a SUBSCRIPTION (sid); chain id "kalshi_ws:c<conn>:sid<sid>".
"""
import json
import os

from market_data.normalization import epoch_ms
from microstructure import kalshi as K
from microstructure.sources.base import MicroAdapter, new_result, num
from microstructure.types import MicroEventType as MT
from settlement.assets import SERIES_ASSET

WS_URL = os.environ.get("KALSHI_WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2")
WS_PATH = "/trade-api/ws/v2"
UNSUBSCRIBE_AFTER_CLOSE_MS = 120_000


def asset_of(ticker):
    series = str(ticker).split("-", 1)[0]
    return SERIES_ASSET.get(series)


def _price_cents(msg, dollars_key, cents_key):
    if msg.get(dollars_key) is not None:
        return K.dollars_to_cents(msg[dollars_key], dollars_key)
    if msg.get(cents_key) is not None:
        return float(K._cents(msg[cents_key], cents_key))
    raise KeyError(f"{dollars_key} / {cents_key} missing")


def _levels(msg, side):
    for key, dollars in ((f"{side}_dollars_fp", True), (f"{side}_dollars", True), (side, False)):
        if key in msg:
            out = []
            for e in msg.get(key) or []:
                p = K.dollars_to_cents(e[0], key) if dollars else float(K._cents(e[0], key))
                out.append([p, num(e[1], "qty")])
            return out
    return []


class KalshiWsAdapter(MicroAdapter):
    venue = source = "kalshi_ws"
    stream = "ws"
    transport = "ws"
    auth_required = True
    documentation = "Kalshi websocket orderbook_delta + trade (read-only; authenticated connection)"

    def __init__(self, assets, depth=None):
        super().__init__(assets, depth)
        self.assets = [a for a in assets if a in SERIES_ASSET.values()]
        self.url = WS_URL
        self.conn = 0
        self.last_seq = {}                 # sid -> last seq
        self.broken_from = {}
        self.sid_markets = {}              # sid -> set(tickers)   (from snapshots / deltas on that sid)
        # live subscription management (not part of parsing)
        self.markets = {}                  # ticker -> close_ts_ms (open markets we follow)
        self.subscribed = set()
        self.unsubscribed_sids = set()
        self._cmd_id = 0

    # ---------------- subscriptions (live only) ----------------
    def _cmd(self, cmd, params):
        self._cmd_id += 1
        return json.dumps({"id": self._cmd_id, "cmd": cmd, "params": params})

    def subscribe_messages(self):
        self.subscribed = set()
        self.unsubscribed_sids = set()
        return self.pending_messages(None)

    def pending_messages(self, wall_ms):
        out = []
        new = sorted(t for t in self.markets if t not in self.subscribed)
        if new:
            self.subscribed.update(new)
            out.append(self._cmd("subscribe", {"channels": ["orderbook_delta", "trade"], "market_tickers": new}))
        if wall_ms is not None:
            old = [sid for sid, ts in self.sid_markets.items() if sid not in self.unsubscribed_sids and ts and
                   all(self.markets.get(t, 0) and wall_ms - self.markets[t] > UNSUBSCRIBE_AFTER_CLOSE_MS for t in ts)]
            if old:
                self.unsubscribed_sids.update(old)
                out.append(self._cmd("unsubscribe", {"sids": sorted(old)}))
        return out

    def markets_url(self, asset):
        from market_data.sources.kalshi import KALSHI_BASE
        series = {v: k for k, v in SERIES_ASSET.items()}[asset]
        return f"{KALSHI_BASE}/markets", {"series_ticker": series, "status": "open", "limit": 100}

    def rest_urls(self):
        return {f"markets:{a}": (*self.markets_url(a), 30.0) for a in self.assets}

    def parse_rest(self, stream, body, ctx):
        """Open markets of one series: remembered for subscription (control only - Step 3 records MARKET_STATE)."""
        from settlement.kalshi_markets import parse_market
        res = new_result()
        ms = body.get("markets") if isinstance(body, dict) else None
        if not isinstance(ms, list):
            self.fail(res, ctx, "markets response without 'markets' list", body)
            return res
        for obj in ms:
            m, _r, _issues = parse_market(obj)
            if m is not None and m.asset in self.assets and m.close_ts_ms > ctx.receive_ts_ms:
                self.markets[m.ticker] = m.close_ts_ms
        res.control.append({"open_markets": sorted(t for t in self.markets if self.markets[t] > ctx.receive_ts_ms)})
        return res

    # ---------------- parsing ----------------
    def on_new_connection(self, conn):
        self.conn = conn
        self.last_seq, self.broken_from, self.sid_markets = {}, {}, {}

    def _track(self, sid, seq):
        if seq is None:
            return None
        last = self.last_seq.get(sid)
        if last is not None and seq != last + 1 and sid not in self.broken_from:
            self.broken_from[sid] = last
        prev = self.broken_from.pop(sid, last)
        self.last_seq[sid] = seq
        return prev

    def parse(self, text, ctx):
        res = new_result()
        m = self.load(text, ctx, res)
        if m is None:
            return res
        if not isinstance(m, dict):
            self.fail(res, ctx, "message is not an object", text)
            return res
        if getattr(ctx, "conn", 0) != self.conn:
            self.on_new_connection(getattr(ctx, "conn", 0))
        typ, msg, sid = m.get("type"), m.get("msg") or {}, m.get("sid")
        seq = m.get("seq")
        seq = int(seq) if isinstance(seq, (int, float)) and not isinstance(seq, bool) else None
        if typ in ("orderbook_snapshot", "orderbook_delta"):
            prev = self._track(sid, seq)
            chain = f"{self.source}:c{self.conn}:sid{sid}"

            def build():
                tk = msg["market_ticker"]
                asset = asset_of(tk)
                if asset is None:
                    raise KeyError(f"unknown series for {tk}")
                self.sid_markets.setdefault(sid, set()).add(tk)
                ts = epoch_ms(msg["ts_ms"], "ts_ms") if msg.get("ts_ms") is not None else None
                if typ == "orderbook_snapshot":
                    yes, no = _levels(msg, "yes"), _levels(msg, "no")
                    asks = [[K.yes_ask_from_no_bid(p), q] for p, q in no]
                    res.events.append(self.event(ctx, asset=asset, event_type=MT.BOOK_SNAPSHOT, symbol=tk, event_ts_ms=ts,
                                                 source_seq=seq, flags=("YES_TERMS",),
                                                 payload=dict(book=tk, bids=yes, asks=asks, update_id=seq, prev_update_id=prev,
                                                              depth=None, chain=chain, native_no_bids=no, **self.units())))
                else:
                    p = _price_cents(msg, "price_dollars", "price")
                    dq = float(msg["delta_fp"] if msg.get("delta_fp") is not None else msg["delta"])
                    side, px = K.native_to_yes_terms(msg["side"], p)
                    res.events.append(self.event(ctx, asset=asset, event_type=MT.BOOK_DELTA, symbol=tk, event_ts_ms=ts,
                                                 source_seq=seq, flags=("YES_TERMS",),
                                                 payload=dict(book=tk, changes=[[side, px, dq, "rel"]], update_id=seq,
                                                              prev_update_id=prev, first_update_id=seq, checksum=None, chain=chain,
                                                              native={"side": msg["side"], "price_cents": p}, **self.units())))
            self.guarded(res, ctx, m, build)
            return res
        if typ == "trade":
            def trade():
                tk = msg["market_ticker"]
                asset = asset_of(tk)
                if asset is None:
                    raise KeyError(f"unknown series for {tk}")
                px = _price_cents(msg, "yes_price_dollars", "yes_price")
                qty = num(msg["count_fp"] if msg.get("count_fp") is not None else msg["count"], "count", True)
                taker = str(msg.get("taker_side") or "").lower()
                aggr = {"yes": "buy", "no": "sell"}.get(taker)
                t = msg.get("ts_ms") if msg.get("ts_ms") is not None else (msg["ts"] * 1000 if msg.get("ts") is not None else None)
                res.events.append(self.event(ctx, asset=asset, event_type=MT.TRADE, symbol=tk,
                                             event_ts_ms=epoch_ms(t, "ts") if t is not None else None, source_seq=seq,
                                             flags=("YES_TERMS",) + (() if aggr else ("AGGRESSOR_UNAVAILABLE",)),
                                             payload={"price": px, "qty": qty, "price_unit": "YES_CENTS", "qty_unit": "contracts",
                                                      "aggressor": aggr, "aggressor_semantics": "TAKER_SIDE_FIELD (YES terms)",
                                                      "trade_id": msg.get("trade_id"), "taker_side_native": taker or None}))
            self.guarded(res, ctx, m, trade)
            return res
        if typ == "error":
            res.control.append({"error": str(msg)[:200]})
        else:
            res.control.append({"type": typ, "sid": sid, "id": m.get("id")})
        return res
