"""
CF Benchmarks Real-Time Index (RTI) values — the settlement index. Highest-priority source.

This adapter REUSES the Step-2 parsers (settlement.cf_live) and types (SettlementObservation); it
does not define a competing representation. Each value becomes an INDEX_VALUE MarketEvent whose
payload carries the SettlementObservation dict (event time = CF "time", receive time = local receipt,
amendTime, repeatOfPreviousValue, capture sequence), and Kalshi-published averages become
PUBLISHED_AVERAGE events (validation only).

Two transports, both AUTHENTICATED and read-only (the collector never sends anything but a subscribe):
    cf_via_kalshi  Kalshi websocket channel `cfbenchmarks_value` (index_ids documented by Kalshi:
                   BRTI, ETHUSD_RTI, SOLUSD_RTI, XRPUSD_RTI). URL wss://api.elections.kalshi.com/trade-api/ws/v2
                   (env KALSHI_WS_URL). Auth: Kalshi API key id + RSA-PSS signature headers
                   (transport/kalshi_auth.py), credentials from KALSHI_API_KEY_ID / KALSHI_PRIVATE_KEY_PATH.
    cf_direct      CF Benchmarks websocket (env CFB_WS_URL, default wss://www.cfbenchmarks.com/ws/v4), HTTP
                   basic auth from CFB_API_ID / CFB_API_SECRET, {"type":"subscribe","id":<index>,"stream":"value"}.
Without credentials the source is DISABLED and reported as such. It is never replaced by exchange spot.
The subscribe formats above follow the providers' public documentation descriptions and are marked
UNVERIFIED until the first real capture (the parser rejects unknown shapes as parse failures).
"""
import json
import os

from market_data.sources.base import SourceAdapter, new_result
from market_data.types import EventType, IngestMode
from settlement.assets import ASSET_INDEX, INDEX_ASSET
from settlement.cf_live import parse_cfb_frame, parse_kalshi_cfb_message

KALSHI_WS_URL = os.environ.get("KALSHI_WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2")
KALSHI_WS_PATH = "/trade-api/ws/v2"
CFB_WS_URL = os.environ.get("CFB_WS_URL", "wss://www.cfbenchmarks.com/ws/v4")
_CONTROL_TYPES = {"subscribed", "unsubscribed", "ok", "error", "list_subscriptions", "indexlist", "index_list"}


def _index_events(adapter, obs, avgs, ctx):
    out = []
    if obs is not None:
        out.append(adapter.event(ctx, asset=INDEX_ASSET.get(obs.index_id, "?"), event_type=EventType.INDEX_VALUE,
                                 symbol=obs.index_id, event_ts_ms=obs.event_ts_ms, source_seq=obs.seq,
                                 mode=IngestMode.LIVE,
                                 payload={"index_id": obs.index_id, "value": obs.value, "amend_ts_ms": obs.amend_ts_ms,
                                          "repeat_of_previous": obs.repeat_of_previous, "observation": obs.to_dict()}))
    for a in avgs or ():
        out.append(adapter.event(ctx, asset=INDEX_ASSET.get(a.index_id, "?"), event_type=EventType.PUBLISHED_AVERAGE,
                                 symbol=a.index_id, event_ts_ms=a.event_ts_ms,
                                 payload={"index_id": a.index_id, "kind": a.kind, "value": a.value}))
    return out


class CfViaKalshiAdapter(SourceAdapter):
    source = "cf_via_kalshi"
    stream = "ws"
    transport = "ws"
    critical = True
    auth_required = True
    documentation = "Kalshi websocket cfbenchmarks_value (authenticated, read-only)"

    def __init__(self, assets):
        self.assets = [a for a in assets if a in ASSET_INDEX]
        self.url = KALSHI_WS_URL

    def subscribe_messages(self):
        return [json.dumps({"id": 1, "cmd": "subscribe",
                            "params": {"channels": ["cfbenchmarks_value"],
                                       "index_ids": [ASSET_INDEX[a] for a in self.assets]}})]

    def parse(self, text, ctx):
        res = new_result()
        m = self.load(text, ctx, res)
        if m is None:
            return res
        if isinstance(m, dict) and m.get("type") in _CONTROL_TYPES:
            res.control.append(m)
            return res
        seq = m.get("seq") if isinstance(m, dict) else None
        obs, avgs, issues = parse_kalshi_cfb_message(m, receive_ts_ms=ctx.receive_ts_ms, seq=seq)
        for i in issues:
            if i.kind != "SCHEMA_EXTRA_FIELDS":
                self.fail(res, ctx, f"{i.kind}: {i.detail}", text)
        res.events += _index_events(self, obs, avgs, ctx)
        return res


class CfDirectAdapter(SourceAdapter):
    source = "cf_direct"
    stream = "ws"
    transport = "ws"
    critical = True
    auth_required = True
    documentation = "CF Benchmarks websocket value stream (authenticated)"

    def __init__(self, assets):
        self.assets = [a for a in assets if a in ASSET_INDEX]
        self.url = CFB_WS_URL

    def subscribe_messages(self):
        return [json.dumps({"type": "subscribe", "id": ASSET_INDEX[a], "stream": "value"}) for a in self.assets]

    def parse(self, text, ctx):
        res = new_result()
        m = self.load(text, ctx, res)
        if m is None:
            return res
        if isinstance(m, dict) and m.get("type") != "value":
            res.control.append(m)
            return res
        obs, issues = parse_cfb_frame(m if isinstance(m, dict) else {}, receive_ts_ms=ctx.receive_ts_ms,
                                      seq=None, source="cfb_ws")
        for i in issues:
            if i.kind != "SCHEMA_EXTRA_FIELDS":
                self.fail(res, ctx, f"{i.kind}: {i.detail}", text)
        res.events += _index_events(self, obs, [], ctx)
        return res
