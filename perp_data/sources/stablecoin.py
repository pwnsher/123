"""
USDT/USD rate from Coinbase's public ticker (USDT-USD) — needed because the Binance / Bybit / OKX contracts are
quoted in USDT while the Step-3 spot reference and the CF RTI are in USD. A USDT/USD deviation of a few basis
points is the same size as a perp basis, so USD-denominated basis features convert USDT prices with the rate
that was AVAILABLE at T (receive_ts <= T, at most 60 s old); without it they are MISSING (never assumed 1.0).

Websocket  wss://ws-feed.exchange.coinbase.com, channel "ticker", product USDT-USD (best_bid / best_ask / time).
"""
import json
import os

from market_data.normalization import iso_ms, mid, price
from perp_data.sources.base import PerpAdapter, new_result
from perp_data.types import PerpEventType as T

WS_URL = os.environ.get("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
PAIR = "USDT-USD"


class CoinbaseUsdtAdapter(PerpAdapter):
    venue = source = "coinbase_usdt"
    stream = "ws"
    transport = "ws"
    documentation = "Coinbase Exchange public ticker for USDT-USD (stablecoin conversion)"

    def __init__(self, assets=(), depth=0):
        self.spec = None
        self.assets, self.depth, self.by_symbol, self.contract_values = ["USDT"], 0, {}, {}
        self.url = WS_URL

    def subscribe_messages(self):
        return [json.dumps({"type": "subscribe", "product_ids": [PAIR], "channels": ["ticker", "heartbeat"]})]

    def parse(self, text, ctx):
        res = new_result()
        m = self.load(text, ctx, res)
        if m is None:
            return res
        t = m.get("type") if isinstance(m, dict) else None
        if t in ("subscriptions", "error", "heartbeat"):
            res.control.append(m if t != "heartbeat" else "heartbeat")
            return res
        if t != "ticker" or m.get("product_id") != PAIR:
            self.fail(res, ctx, f"unexpected message {t!r}/{m.get('product_id') if isinstance(m, dict) else None!r}", text)
            return res

        def build():
            bid, ask = price(m["best_bid"], "best_bid"), price(m["best_ask"], "best_ask")
            res.events.append(self.event(ctx, asset="USDT", event_type=T.STABLECOIN_RATE, symbol=PAIR,
                                         event_ts_ms=iso_ms(m["time"]),
                                         payload={"pair": PAIR, "bid": bid, "ask": ask, "mid": mid(bid, ask)}))
        self.guarded(res, ctx, text, build)
        return res
