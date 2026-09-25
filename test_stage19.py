#!/usr/bin/env python3
"""Stage 19 — STEP 3 HIGH-RESOLUTION MARKET DATA + CAUSAL RESEARCH FEATURES (research only).

Run:  py test_stage19.py                  (or py run_all_tests.py for stages 1-19)
      py test_stage19.py --only leakage,alignment     (a subset; used by scripts/mutation_test_market_data.py)

Everything is offline and deterministic: synthetic payloads shaped like the real Coinbase / Kraken /
Kalshi / CF messages, fake clocks, fake transports and a local loopback websocket server. Covers the
index-ID mapping, normalization, every source adapter, the causal alignment rule, clocks and feed
states, websocket framing, reconnect / backoff, optional-source isolation, storage (corruption,
truncation, append-only), the manifest (no secrets), gaps, backfill semantics, replay determinism,
single-pass == batch features, arbitrary windows, feature values, missing -> None (never 0), warm-up,
partial mode, LEAKAGE perturbations (delayed trade, amended CF value, late Kalshi book, future price,
post-close data, labels) with canaries, settlement evidence capture, the dataset writer + provenance,
the research status page, the collector dry run, the market-data fingerprint, and that the Step-1
strategy, the Step-2 settlement baseline, the perp code and the execution boundary are untouched.
"""
import ast
import hashlib
import http.client
import json
import math
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from market_data import FEATURE_SET_VERSION, fingerprint as mdfp                               # noqa: E402
from market_data.alignment import Timeline, available, latest_available                        # noqa: E402
from market_data.clock import ClockMonitor, FakeClock                                          # noqa: E402
from market_data.collector import Collector                                                    # noqa: E402
from market_data.feed import Backoff, FeedHealth, FeedMonitor, FeedState                       # noqa: E402
from market_data.features.buffers import StreamBuffer                                          # noqa: E402
from market_data.features.dataset import (KEY_COLS, build_dataset, discover_markets, rows_digest,  # noqa: E402
                                          write_dataset)
from market_data.features.definitions import FEATURE_NAMES, FEATURES, FeatureConfig, Status   # noqa: E402
from market_data.features.engine import CausalityError, FeatureEngine, FeatureRow, compute_at  # noqa: E402
from market_data.gaps import CadenceGapDetector, Gap, IdGapDetector                            # noqa: E402
from market_data.kalshi_poller import KalshiPoller                                             # noqa: E402
from market_data.manifest import assert_no_secrets, load_manifest                              # noqa: E402
from market_data.normalization import NormalizationError, epoch_ms, iso_ms, mid, price, size   # noqa: E402
from market_data.replay import Replayer, load_sessions                                         # noqa: E402
from market_data.runner import PollRunner, WsFeedRunner                                        # noqa: E402
from market_data.sources.base import Ctx, Sequencer                                            # noqa: E402
from market_data.sources.cf import CfDirectAdapter, CfViaKalshiAdapter                         # noqa: E402
from market_data.sources.coinbase import CoinbaseAdapter                                       # noqa: E402
from market_data.sources.kalshi import KalshiAdapter                                           # noqa: E402
from market_data.sources.kraken import KrakenAdapter                                           # noqa: E402
from market_data.storage import EventStoreWriter, read_session, segment_files                  # noqa: E402
from market_data.synthetic import FakeKalshi, World, iso, run_session                          # noqa: E402
from market_data.transport import kalshi_auth                                                  # noqa: E402
from market_data.transport.http import HttpGetter                                              # noqa: E402
from market_data.transport.ws import WebSocket, accept_key, encode_frame, read_frame           # noqa: E402
from market_data.types import EventFlag, EventType, IngestMode, MarketEvent                    # noqa: E402
from settlement.assets import ASSET_INDEX, INDEX_ASSET, INDEX_ID_PROVENANCE, SERIES_ASSET      # noqa: E402
from settlement.checkpoints import LABEL_FIELDS                                                # noqa: E402
from settlement.policy import window_policy                                                    # noqa: E402
from settlement.reconstruction import reconstruct                                              # noqa: E402
from settlement.synthetic import cf_frame, kalshi_message                                      # noqa: E402
from settlement.types import SettlementObservation                           # noqa: E402

STEP1_ARTIFACTS = {   # Step-1 baseline artifacts: Step 3 must leave them byte-identical
    "config/strategy_baseline.json": "8f052394a830be88f4bd4c506b5d8069222e3c9b1c06c0bc63e3817463185d04",
    "step5_baseline_manifest.json": "6a2994ff8bdc2d36b41608515c77a13d71fd371030487b5e62b2b064cd096db8",
    "regression/strategy_cases.json": "e942ba3716e42a55fbf8e6086283eb0c8f124564b1cb44a3301b6c12c47abf9c",
    "kalshi_dashboard.py": "aa66c7ae1d6cf8b913b746b05bfd6cf64197454d3b7fd00a2ba174749150c595",
}
UNTOUCHED = {         # production / perp code: Step 3 must not change a byte
    "perp_live.py": "95b0ce6f1f6ce70b62cae1176757dbb27574501817a0d6c868fec4bbd7ba48a2",
    "perp_probability.py": "344114534aef2a90ba41b07a52a454bd1e5ebc3b06b4b2facebde5c90eb76546",
    "perp_shadow.py": "ba3b3444e83fe384f5556050abc7d543d9c06fbc147a51319a314dddf12794c3",
    "perp_telemetry.py": "5e1de6d6553032bd2902d0b9766086b826876c632bb41cfcfe52234c90b4d99e",
    "kalshi_backtest.py": "19f4bc281701db5234c29cc922b3806b21ac333883df174dcd936385fe9f117f",
    "kalshi_bot.py": "8278bdc4d2adaa117e2fdcce101924181d4bc64d3987b584a69741eeb474cd5d",
    "run_local.py": "0749c2a83b3c219ed63dc5d994253883983f9b6b4ebc81a2b72f46b067e53044",
}

C = 1_790_001_000_000                     # a 15-minute boundary (UTC): the close of the test market
START = C - 420_000                       # the shared synthetic session starts 7 minutes before it
DUR_S = 520                               # ... and runs to close + 100 s (settled result polled at close + 90 s)
_CACHE = {}


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


def tmpdir():
    return tempfile.mkdtemp(prefix="stage19-")


def shared_session():
    """One synthetic BTC session (Coinbase outage + REST backfill included), loaded once."""
    if "s" not in _CACHE:
        d = tmpdir()
        col = run_session(d, assets=("BTC",), start_ms=START, duration_s=DUR_S, seed=3,
                          cb_disconnect=(START + 200_000, START + 215_000),
                          settlement_store_path=os.path.join(d, "settle.jsonl"))
        loaded = load_sessions([col.dir])
        mk = discover_markets(loaded.events, ["BTC"])
        market = [m for m, _k in mk.values() if m.close_ts_ms == C][0]
        _CACHE["s"] = (d, col, loaded, market)
    return _CACHE["s"]


_SEQ = [10_000_000]
_DEFAULTS = {EventType.TRADE: {"aggressor_semantics": "TAKER_SIDE_FIELD", "trade_id": None},
             EventType.QUOTE: {"bid_size": None, "ask_size": None},
             EventType.BOOK: {"depth_levels": 1}}


def E(source, et, ev_ts, rx, asset="BTC", symbol="BTC-USD", mode=IngestMode.LIVE, flags=(), **p):
    """A hand-made normalized event with a unique arrival number."""
    _SEQ[0] += 1
    payload = dict(_DEFAULTS.get(et, {}))
    payload.update(p)
    return MarketEvent(source, asset, et, symbol, ev_ts, rx, _SEQ[0], payload, mode=mode, flags=tuple(flags))


def cf_ev(t, value, rx, amend=None, asset="BTC"):
    idx = ASSET_INDEX[asset]
    o = SettlementObservation(asset, idx, "cfb_ws_via_kalshi", value, t, receive_ts_ms=rx, amend_ts_ms=amend)
    return E("cf_via_kalshi", EventType.INDEX_VALUE, t, rx, asset, idx, index_id=idx, value=value, amend_ts_ms=amend,
             repeat_of_previous=False, observation=o.to_dict())


def ctx(rx=1_790_000_000_000):
    return Ctx("S", Sequencer(), rx, 0)


def status_map(row):
    return {k: v.value for k, v in row.status.items()}


# ═══════════════════ 1-3 baselines, index ids, boundaries ═══════════════════
def test_baselines_untouched():
    for rel, h in {**STEP1_ARTIFACTS, **UNTOUCHED}.items():
        assert hashlib.sha256(open(os.path.join(HERE, rel), "rb").read()).hexdigest() == h, rel
    from kalshi_core import baseline
    ok, problems = baseline.verify()
    assert ok, problems
    p = subprocess.run([sys.executable, "-m", "regression.generate", "--check"], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "MATCH (47 cases)" in p.stdout, p.stdout + p.stderr
    import strategy_fingerprint as sf
    assert sf.current_fingerprint(os.path.join(HERE, "kalshi_dashboard.py"))[0] == \
        "8d94f241e8fc8edadc76058e1f12f430b6e4f499f4c0a30fba1cb5cf07dad82a"
    from settlement import fingerprint as sfp
    assert sfp.verify()[0], sfp.verify()[1]
    assert mdfp.verify()[0], mdfp.verify()[1]


def test_index_ids():
    assert ASSET_INDEX == {"BTC": "BRTI", "ETH": "ETHUSD_RTI", "SOL": "SOLUSD_RTI", "XRP": "XRPUSD_RTI"}
    assert INDEX_ASSET == {v: k for k, v in ASSET_INDEX.items()}
    assert SERIES_ASSET == {"KXBTC15M": "BTC", "KXETH15M": "ETH", "KXSOL15M": "SOL", "KXXRP15M": "XRP"}
    pv = INDEX_ID_PROVENANCE
    assert pv["status"] == "DOCUMENTED" and pv["previous_status"].startswith("ASSUMED") and pv["sources"]
    assert "NOT the settlement window convention" in pv["scope"]
    # the mapping is documented; the settlement WINDOW convention is still NOT verified (never auto-verified)
    assert window_policy().verified is False
    sub = json.loads(CfViaKalshiAdapter(["BTC", "ETH", "SOL", "XRP"]).subscribe_messages()[0])
    assert sub["params"] == {"channels": ["cfbenchmarks_value"], "index_ids": ["BRTI", "ETHUSD_RTI", "SOLUSD_RTI", "XRPUSD_RTI"]}
    assert [json.loads(m)["id"] for m in CfDirectAdapter(["SOL", "XRP"]).subscribe_messages()] == ["SOLUSD_RTI", "XRPUSD_RTI"]
    for a, idx in ASSET_INDEX.items():                                   # frames route by index id to the asset
        r = CfViaKalshiAdapter([a]).parse(json.dumps(kalshi_message(cf_frame(idx, 1_790_000_000_000, 10.0))), ctx())
        assert [(e.asset, e.symbol) for e in r.events] == [(a, idx)], r


def _imports(path):
    tree = ast.parse(open(path, encoding="utf-8").read())
    mods = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    return mods | {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}


def _py_files(d):
    out = []
    for root, dirs, files in os.walk(d):
        dirs[:] = [x for x in dirs if x != "__pycache__"]
        out += [os.path.join(root, f) for f in files if f.endswith(".py")]
    return out


def test_import_boundaries_and_read_only():
    prod = ["kalshi_dashboard.py", "kalshi_backtest.py", "perp_live.py", "perp_shadow.py", "perp_telemetry.py",
            "perp_probability.py", "kalshi_bot.py", "run_local.py", "strategy_fingerprint.py"] + \
           [os.path.join("kalshi_core", x) for x in os.listdir(os.path.join(HERE, "kalshi_core")) if x.endswith(".py")]
    for f in prod + _py_files(os.path.join(HERE, "settlement")):     # nothing production (nor settlement) imports it
        bad = [m for m in _imports(os.path.join(HERE, f)) if m == "market_data" or m.startswith("market_data.")]
        assert not bad, (f, bad)
    forbidden = {"kalshi_dashboard", "kalshi_bot", "discord", "kalshi_api_learn", "kalshi_core.execution", "kalshi_core.signal",
                 "perp_live", "perp_shadow", "perp_probability", "perp_telemetry", "kalshi_backtest"}
    md_files = _py_files(os.path.join(HERE, "market_data")) + [os.path.join(HERE, "collect_market_data.py")] + \
        [os.path.join(HERE, "scripts", f) for f in ("replay_market_data.py", "build_market_features.py",
                                                    "bench_market_data.py", "mutation_test_market_data.py")]
    for f in md_files:
        bad = {m for m in _imports(f) if m in forbidden or m.split(".")[0] in {"discord", "kalshi_dashboard"}}
        assert not bad, (f, bad)
        src = open(f, encoding="utf-8").read()
        for s in ("/portfolio/orders", "create_order", "place_order", ".post(", ".put(", ".delete(", "requests.post",
                  "perp"):
            assert s not in src.lower() or (s == "perp" and "perp" not in _code_only(src)), (f, s)
    assert not [n for n in FEATURE_NAMES if "perp" in n]                              # no perp features
    assert not hasattr(HttpGetter, "post") and not hasattr(HttpGetter, "put")
    for method, path in (("POST", "/trade-api/ws/v2"), ("GET", "/trade-api/v2/portfolio/orders"), ("DELETE", "/trade-api/ws/v2")):
        try:
            kalshi_auth.check_request(method, path); raise AssertionError("signed a non-read request")
        except PermissionError:
            pass
    kalshi_auth.check_request("GET", "/trade-api/ws/v2")
    from kalshi_core import execution as ex
    try:
        ex.get_execution_engine("LIVE"); raise AssertionError("LIVE engine returned")
    except ex.LiveExecutionUnavailable:
        pass


def _code_only(src):
    """Source without comments / strings (so a docstring may say 'no perp features')."""
    tree = ast.parse(src)
    return " ".join(n.id for n in ast.walk(tree) if isinstance(n, ast.Name)).lower() + " " + \
        " ".join(n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)).lower()


# ═══════════════════ 4 types + normalization ═══════════════════
def test_types_and_normalization():
    e = E("coinbase", EventType.TRADE, 1000, 1200, price=1.0, size=2.0, aggressor="buy", trade_id=1)
    assert MarketEvent.from_dict(json.loads(json.dumps(e.to_dict()))) == e
    try:
        MarketEvent("coinbase", "BTC", EventType.TRADE, "X", 1, 2, 3, {"price": 1.0}); raise AssertionError
    except ValueError as err:
        assert "payload missing" in str(err)
    for bad_rx in (1.5, True, "1"):
        try:
            E("coinbase", EventType.HEARTBEAT, None, bad_rx, last_trade_id=None); raise AssertionError(bad_rx)
        except ValueError:
            pass
    skew = E("kraken", EventType.QUOTE, 5000, 4000, bid=1.0, ask=2.0, mid=1.5)
    assert skew.series_ts_ms == 4000                                    # a venue clock ahead of ours is clamped
    assert E("kraken", EventType.QUOTE, None, 4000, bid=1.0, ask=2.0, mid=1.5).series_ts_ms == 4000
    for bad in ("0", "-1", "nan", "inf", "", None, "abc", True):
        try:
            price(bad); raise AssertionError(bad)
        except NormalizationError:
            pass
    assert size("0") == 0.0
    for bad in ("-1", "nan", None, ""):
        try:
            size(bad); raise AssertionError(bad)
        except NormalizationError:
            pass
    base = 1_790_000_000_123
    assert iso_ms("2026-09-21T14:13:20.123Z") == iso_ms("2026-09-21T10:13:20.123-04:00") == \
        iso_ms("2026-09-21T14:13:20.123456789Z") == iso_ms("2026-09-21 14:13:20.1234+0000") == base
    assert iso_ms("2026-09-21T14:13:20Z") == base - 123 and iso_ms("2026-09-21T14:13:20.9999999Z") == base - 123 + 999
    for bad in ("2026-09-21T14:13:20", "2026-09-21", "yesterday", 1790000000):
        try:
            iso_ms(bad); raise AssertionError(bad)
        except NormalizationError:
            pass
    assert epoch_ms(1_790_000_000_000) == 1_790_000_000_000
    try:
        epoch_ms(1_790_000_000); raise AssertionError("seconds accepted as ms")
    except NormalizationError:
        pass
    assert mid(1.0, 3.0) == 2.0 and mid(None, 3.0) is None and mid(3.0, None) is None and mid(3.0, 2.0) is None


# ═══════════════════ 5-8 source adapters ═══════════════════
def test_coinbase_adapter():
    a = CoinbaseAdapter(["BTC"])
    m = {"type": "match", "product_id": "BTC-USD", "time": "2026-09-21T14:13:20.123456Z", "trade_id": 7, "sequence": 9,
         "price": "100000.5", "size": "0.25", "side": "sell"}
    r = a.parse(json.dumps(m), ctx(1_790_000_000_500))
    (t,) = r.events
    assert t.event_type == EventType.TRADE and t.payload["aggressor"] == "buy"         # maker sold -> taker bought
    assert t.payload["aggressor_semantics"] == "INVERTED_MAKER_SIDE" and t.mode == IngestMode.LIVE
    assert t.event_ts_ms == 1_790_000_000_123 and t.receive_ts_ms == 1_790_000_000_500 and t.payload["trade_id"] == 7
    lm = a.parse(json.dumps(dict(m, type="last_match", side="buy")), ctx()).events[0]
    assert lm.mode == IngestMode.BACKFILLED and EventFlag.SNAPSHOT.value in lm.flags and lm.payload["aggressor"] == "sell"
    nos = a.parse(json.dumps(dict(m, side=None)), ctx()).events[0]
    assert nos.payload["aggressor"] is None and EventFlag.AGGRESSOR_UNAVAILABLE.value in nos.flags
    q = a.parse(json.dumps({"type": "ticker", "product_id": "BTC-USD", "time": "2026-09-21T14:13:20Z", "best_bid": "10",
                            "best_ask": "12", "best_bid_size": "1", "best_ask_size": "2"}), ctx()).events[0]
    assert q.event_type == EventType.QUOTE and q.payload["mid"] == 11.0
    q1 = a.parse(json.dumps({"type": "ticker", "product_id": "BTC-USD", "time": "2026-09-21T14:13:20Z", "best_bid": "10",
                             "best_ask": ""}), ctx()).events[0]
    assert q1.payload["mid"] is None and EventFlag.ONE_SIDED_BOOK.value in q1.flags
    hb = a.parse(json.dumps({"type": "heartbeat", "product_id": "BTC-USD", "time": "2026-09-21T14:13:20Z",
                             "last_trade_id": 7}), ctx()).events[0]
    assert hb.event_type == EventType.HEARTBEAT and hb.payload["last_trade_id"] == 7
    for bad in (dict(m, price="0"), dict(m, price="x"), dict(m, time="2026-09-21T14:13:20"), dict(m, product_id="DOGE-USD"),
                {"no": "type"}):
        r = a.parse(json.dumps(bad), ctx())
        assert not r.events and len(r.failures) == 1, (bad, r)                      # a failure, never a guessed value
    assert not a.parse("{not json", ctx()).events and a.parse("{not json", ctx()).failures
    assert a.parse(json.dumps({"type": "subscriptions", "channels": []}), ctx()).control
    body = [{"time": "2026-09-21T14:13:22Z", "trade_id": 9, "price": "1", "size": "1", "side": "buy"},
            {"time": "2026-09-21T14:13:21Z", "trade_id": 8, "price": "1", "size": "1", "side": "sell"}]
    bf = a.parse_backfill("BTC", body, ctx(1_790_000_099_000)).events
    assert [e.payload["trade_id"] for e in bf] == [8, 9]                           # REST is newest-first -> oldest first
    assert all(e.mode == IngestMode.BACKFILLED and e.receive_ts_ms == 1_790_000_099_000 for e in bf)
    assert bf[0].event_ts_ms == iso_ms("2026-09-21T14:13:21Z")                     # event time preserved


def test_kraken_adapter():
    a = KrakenAdapter(["BTC", "ETH"])
    tr = {"channel": "trade", "type": "update", "data": [
        {"symbol": "BTC/USD", "side": "sell", "price": 100.5, "qty": 0.5, "ord_type": "market", "trade_id": 11,
         "timestamp": "2026-09-21T14:13:20.123456Z"},
        {"symbol": "ETH/USD", "side": "buy", "price": 3.5, "qty": 2, "ord_type": "limit", "trade_id": 5,
         "timestamp": "2026-09-21T14:13:20.2Z"}]}
    ev = a.parse(json.dumps(tr), ctx()).events
    assert [(e.asset, e.payload["aggressor"], e.payload["aggressor_semantics"]) for e in ev] == \
        [("BTC", "sell", "TAKER_SIDE_FIELD"), ("ETH", "buy", "TAKER_SIDE_FIELD")]
    snap = a.parse(json.dumps(dict(tr, type="snapshot")), ctx()).events
    assert all(e.mode == IngestMode.BACKFILLED and EventFlag.SNAPSHOT.value in e.flags for e in snap)
    q = a.parse(json.dumps({"channel": "ticker", "type": "update", "data": [
        {"symbol": "BTC/USD", "bid": 10, "bid_qty": 1, "ask": 12, "ask_qty": 1}]}), ctx(1_790_000_000_777)).events[0]
    assert q.event_ts_ms is None and EventFlag.EVENT_TIME_MISSING.value in q.flags and q.series_ts_ms == 1_790_000_000_777
    assert q.payload["mid"] == 11.0
    hb = a.parse(json.dumps({"channel": "heartbeat"}), ctx()).events[0]
    assert hb.asset == "*" and hb.event_type == EventType.HEARTBEAT
    assert a.parse(json.dumps({"method": "subscribe", "success": True}), ctx()).control
    assert a.parse(json.dumps({"channel": "status", "data": []}), ctx()).control
    r = a.parse(json.dumps({"channel": "trade", "type": "update", "data": [dict(tr["data"][0], symbol="DOGE/USD"),
                                                                             dict(tr["data"][0], price=-1)]}), ctx())
    assert not r.events and len(r.failures) == 2


def test_kalshi_adapter():
    k = KalshiAdapter(["BTC"])
    w = World(["BTC"], C - 900_000, C, seed=1)
    clock = FakeClock(C - 60_000)
    fk = FakeKalshi(w, clock)
    body = fk.get_json(*k.markets_url("BTC"))
    body["markets"].append(dict(body["markets"][0], ticker="KXBTC15M-26SEP211500", close_time=iso(C + 900_000)))
    res, m = k.parse_markets("BTC", body, ctx(clock.wall_ms()), now_ms=clock.wall_ms())
    assert m.close_ts_ms == C and len(res.events) == 1                         # the market closing soonest in the future
    st = res.events[0]
    assert st.event_type == EventType.MARKET_STATE and st.event_ts_ms is None and st.payload["strike"] == m.strike
    assert 0 < st.payload["yes_bid"] < st.payload["yes_ask"] < 100
    o = dict(body["markets"][0])
    o.pop("yes_ask_dollars")
    der = k.parse_markets("BTC", {"markets": [o]}, ctx(), now_ms=clock.wall_ms())[0].events[0]
    assert EventFlag.DERIVED_ASK.value in der.flags and der.payload["yes_ask"] == round(100 - der.payload["no_bid"], 6)
    b1 = k.parse_orderbook("BTC", m.ticker, {"orderbook": {"yes": [[45, 100], [44, 5]], "no": [[53, 7]]}}, ctx()).events[0]
    b2 = k.parse_orderbook("BTC", m.ticker, {"orderbook": {"yes_dollars": [["0.45", 100], ["0.44", 5]],
                                                           "no_dollars": [["0.53", 7]]}}, ctx()).events[0]
    assert b1.payload["bids"] == b2.payload["bids"] == [[45.0, 100.0], [44.0, 5.0]]
    assert b1.payload["asks"] == b2.payload["asks"] == [[47.0, 7.0]]              # YES ask = 100 - NO bid
    assert k.parse_orderbook("BTC", m.ticker, {"orderbook": {"weird": []}}, ctx()).failures
    assert k.parse_orderbook("BTC", m.ticker, {"orderbook": {"yes": [[45]]}}, ctx()).failures
    tr = k.parse_trades("BTC", {"trades": [
        {"trade_id": "b", "ticker": m.ticker, "yes_price_dollars": "0.4600", "count_fp": "5.00", "taker_side": "no",
         "created_time": "2026-09-21T14:13:21Z"},
        {"trade_id": "a", "ticker": m.ticker, "yes_price": 45, "count": 3, "taker_side": "yes",
         "created_time": "2026-09-21T14:13:20Z"},
        {"trade_id": "c", "ticker": m.ticker, "yes_price": 45, "count": 1, "created_time": "2026-09-21T14:13:22Z"}]},
        ctx(), backfill=True).events
    assert [(e.payload["trade_id"], e.payload["price"], e.payload["size"], e.payload["aggressor"]) for e in tr] == \
        [("a", 45.0, 3.0, "buy"), ("b", 46.0, 5.0, "sell"), ("c", 45.0, 1.0, None)]
    assert all(e.mode == IngestMode.BACKFILLED for e in tr) and EventFlag.AGGRESSOR_UNAVAILABLE.value in tr[2].flags
    clock.advance(200_000)
    res, _m, r = k.parse_settled(fk.get_json(*k.market_url(m.ticker)), ctx())
    assert r.result in ("yes", "no") and res.events[0].event_type == EventType.RESOLUTION
    res2, _m2, r2 = k.parse_settled({"market": dict(body["markets"][0])}, ctx())
    assert not res2.events                                                    # not settled -> no RESOLUTION event
    assert all(req[0] == "GET" for req in fk.requests)


def test_cf_adapters():
    a = CfViaKalshiAdapter(["BTC"])
    msg = kalshi_message(cf_frame("BRTI", 1_790_000_000_000, 100000.25), seq=4, avg60=100001.0, last60=100002.0)
    r = a.parse(json.dumps(msg), ctx(1_790_000_000_180))
    kinds = [e.event_type for e in r.events]
    assert kinds == [EventType.INDEX_VALUE, EventType.PUBLISHED_AVERAGE, EventType.PUBLISHED_AVERAGE], kinds
    iv = r.events[0]
    assert iv.source == "cf_via_kalshi" and iv.payload["value"] == 100000.25 and iv.event_ts_ms == 1_790_000_000_000
    obs = SettlementObservation.from_dict(iv.payload["observation"])
    assert obs.source == "cfb_ws_via_kalshi" and obs.receive_ts_ms == 1_790_000_000_180 and obs.index_id == "BRTI"
    assert a.parse(json.dumps({"type": "subscribed", "msg": {}}), ctx()).control
    bad = a.parse(json.dumps({"type": "cfbenchmarks_value", "msg": {"data": "{nope"}}), ctx())
    assert not bad.events and bad.failures
    d = CfDirectAdapter(["BTC"]).parse(json.dumps(cf_frame("BRTI", 1_790_000_000_000, 5.5)), ctx())
    assert d.events[0].source == "cf_direct" and SettlementObservation.from_dict(d.events[0].payload["observation"]).source == "cfb_ws"
    # CF is never substituted: only CF adapters feed the engine's CF stream; Coinbase data never becomes CF
    eng = FeatureEngine("BTC")
    eng.ingest(E("coinbase", EventType.QUOTE, 1000, 1000, bid=1.0, ask=3.0, mid=2.0))
    assert len(eng.px["cf"]) == 0 and eng.price_at("cf", 1000) is None and eng.features_at(1000).values["price.cf.last"] is None


# ═══════════════════ 9 alignment ═══════════════════
def test_alignment_rule():
    T = 10_000
    late = E("coinbase", EventType.TRADE, T - 500, T + 1, price=1.0, size=1.0, aggressor="buy", trade_id=1)
    on = E("coinbase", EventType.TRADE, T - 500, T, price=1.0, size=1.0, aggressor="buy", trade_id=2)
    assert not available(late, T) and available(late, T + 1) and available(on, T)
    tl = Timeline([late, on])
    assert tl.visible(T) == [on] and tl.visible(T + 1) == [on, late] and tl.visible(T - 1) == []
    q_old = E("kraken", EventType.QUOTE, T - 900, T - 800, bid=1.0, ask=3.0, mid=2.0)
    q_new_late = E("kraken", EventType.QUOTE, T - 100, T + 50, bid=5.0, ask=7.0, mid=6.0)
    assert latest_available([q_old, q_new_late], T) is q_old                      # newer by event time, but not yet received
    assert latest_available([q_old, q_new_late], T + 50) is q_new_late
    assert latest_available([q_old], T, max_age_ms=500) is None
    # the engine refuses to look before what it has ingested, and refuses non-availability-order ingestion
    eng = FeatureEngine("BTC")
    eng.ingest(on)
    for bad in (lambda: eng.features_at(T - 1), lambda: eng.ingest(E("coinbase", EventType.QUOTE, 1, T - 1, bid=1.0,
                                                                          ask=2.0, mid=1.5))):
        try:
            bad(); raise AssertionError("causality guard missing")
        except CausalityError:
            pass


# ═══════════════════ 10 clocks + feed states ═══════════════════
def test_clock_and_feed_states():
    cm = ClockMonitor(threshold_ms=1000)
    assert cm.check(10_000, 0) is None and cm.check(11_000, 1_000_000_000) is None
    assert cm.check(10_500, 2_000_000_000).kind == "WALL_BACKWARDS"
    assert cm.check(20_000, 3_000_000_000).kind == "WALL_JUMP"
    h = FeedHealth("x")
    m = FeedMonitor(h, warmup_ms=5000, stale_after_ms=10_000, degraded_window_ms=30_000)
    ms = lambda s: int(s * 1e9)  # noqa: E731
    seen = [h.state]
    m.on_connect(ms(0)); seen.append(h.state)
    m.on_message(ms(1), data_events=1); seen.append(h.state)
    m.on_message(ms(6), data_events=1); seen.append(h.state)
    m.on_message(ms(7), data_events=1, failures=1); seen.append(h.state)
    m.evaluate(ms(40)); seen.append(h.state)                                   # no message for 33 s -> STALE
    m.on_message(ms(41), data_events=1); seen.append(h.state)
    m.on_disconnect(ms(42), "reset"); seen.append(h.state)
    m.on_connect(ms(45)); m.on_stop(ms(46)); seen.append(h.state)
    assert seen == [FeedState.DISCONNECTED, FeedState.CONNECTED, FeedState.WARMING_UP, FeedState.HEALTHY,
                    FeedState.DEGRADED, FeedState.STALE, FeedState.DEGRADED, FeedState.RECONNECTING,
                    FeedState.DISCONNECTED], seen
    assert {s.value for s in FeedState} == {"CONNECTED", "DEGRADED", "RECONNECTING", "WARMING_UP", "HEALTHY", "STALE",
                                            "DISCONNECTED"}
    assert h.last_reconnect_duration_ms == 3000.0 and h.disconnects == 1
    b = Backoff(initial_s=1, factor=2, max_s=10, jitter=0)
    assert [b.next_delay() for _ in range(6)] == [1, 2, 4, 8, 10, 10]
    b.reset()
    assert b.next_delay() == 1
    bj = Backoff(initial_s=1, factor=2, max_s=60, jitter=0.1, seed=1)
    ds = [bj.next_delay() for _ in range(5)]
    assert all(0.9 * 2 ** i <= d <= 1.1 * 2 ** i for i, d in enumerate(ds)) and ds == \
        [Backoff(jitter=0.1, seed=1).next_delay() for _ in range(1)] + ds[1:]


# ═══════════════════ 11 websocket transport (loopback) ═══════════════════
def _ws_server(frames_after_handshake, got=None):
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]

    def serve():
        c, _ = srv.accept()
        req = b""
        while b"\r\n\r\n" not in req:
            req += c.recv(4096)
        key = [ln.split(b":", 1)[1].strip().decode() for ln in req.split(b"\r\n") if ln.lower().startswith(b"sec-websocket-key")][0]
        if got is not None:
            got["request"] = req.decode()
        c.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                   f"Sec-WebSocket-Accept: {accept_key(key)}\r\n\r\n").encode())
        for f in frames_after_handshake:
            if isinstance(f, tuple) and f[0] == "sleep":
                time.sleep(f[1])
                continue
            c.sendall(f)
        if got is not None:                                   # read what the client sent back (pong, text, close)
            c.settimeout(2)
            data = b""
            try:
                while True:
                    chunk = c.recv(65536)
                    if not chunk:
                        break
                    data += chunk
            except OSError:
                pass
            got["from_client"] = data
        c.close()
        srv.close()
    threading.Thread(target=serve, daemon=True).start()
    return port


class _BufReader:
    def __init__(self, b):
        self.buf = bytearray(b)

    def fill(self, n):
        if len(self.buf) < n:
            raise AssertionError("short")

    def take(self, n):
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out


def test_websocket_transport():
    assert accept_key("dGhlIHNhbXBsZSBub25jZQ==") == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="           # RFC 6455 example
    for n in (0, 125, 126, 65535, 65536, 70000):
        payload = bytes(i % 251 for i in range(n))
        for mask in (True, False):
            fin, op, data = read_frame(_BufReader(encode_frame(0x2, payload, mask=mask)))
            assert fin and op == 0x2 and data == payload, (n, mask)
    big = "x" * 70000
    got = {}
    frames = [encode_frame(0x9, b"hi", mask=False),                                   # ping -> auto pong
              encode_frame(0x1, "hel", mask=False, fin=False),                         # fragmented text
              ("sleep", 0.3),                                                         # client times out between frames
              encode_frame(0x0, "lo", mask=False),
              encode_frame(0x1, big, mask=False)[:50], ("sleep", 0.3),               # timeout MID-frame
              encode_frame(0x1, big, mask=False)[50:],
              encode_frame(0x8, b"\x03\xe8", mask=False)]                             # close
    port = _ws_server(frames, got)
    ws = WebSocket.connect(f"ws://127.0.0.1:{port}/stream?x=1", headers={"X-Test": "1"}, timeout=5)
    ws.settimeout(0.1)
    out, timeouts = [], 0
    while True:
        try:
            t = ws.recv_text()
        except TimeoutError:
            timeouts += 1
            assert timeouts < 50
            continue
        if t is None:
            break
        out.append(t)
    assert out == ["hello", big] and timeouts >= 2, (len(out), timeouts)            # no desync after mid-frame timeouts
    ws.send_text("after")
    ws.close()
    time.sleep(0.3)
    assert "GET /stream?x=1 HTTP/1.1" in got["request"] and "X-Test: 1" in got["request"]
    r = _BufReader(got["from_client"])
    fin, op, data = read_frame(r)
    assert op == 0xA and data == b"hi"                                                 # pong echoed the ping
    ops = []
    while r.buf:
        ops.append(read_frame(r)[1])
    assert 0x8 in ops                                                                  # close echoed / sent


# ═══════════════════ 12-13 runners: reconnect, backoff, isolation, polling ═══════════════════
class FakeWS:
    def __init__(self, msgs, stop=None):
        self.msgs, self.stop, self.sent, self.closed = list(msgs), stop, [], False

    def send_text(self, t):
        self.sent.append(t)

    def settimeout(self, s):
        pass

    def recv_text(self):
        if self.msgs:
            m = self.msgs.pop(0)
            if isinstance(m, Exception):
                raise m
            return m
        if self.stop is not None:
            self.stop.set()
            raise TimeoutError()
        return None                                                              # server closed

    def close(self):
        self.closed = True


def _cb(t, tid):
    return json.dumps({"type": "match", "product_id": "BTC-USD", "time": iso(t), "trade_id": tid, "price": "100",
                       "size": "1", "side": "buy"})


def test_reconnect_backoff_and_isolation():
    d = tmpdir()
    clock = FakeClock(1_790_000_000_000)
    col = Collector(d, ["BTC"], {"coinbase": {}}, clock, fsync=False, live_features=False)
    stop = threading.Event()
    attempts = []
    t0 = 1_790_000_000_000

    def connect(url, headers=None):
        attempts.append(url)
        clock.advance(1000)
        n = len(attempts)
        if n in (1, 2):
            raise ConnectionError("refused")
        if n == 3:
            return FakeWS([_cb(t0, 1), _cb(t0 + 1, 2)])                                # then the server closes
        return FakeWS([_cb(t0 + 5000, 3)], stop)
    backfills = []
    r = WsFeedRunner(CoinbaseAdapter(["BTC"]), col, clock, connect_fn=connect, stop_event=stop,
                     backoff=Backoff(initial_s=0.001, jitter=0), backfill_fn=lambda: backfills.append(1))
    r.run()
    assert len(attempts) == 4 and r.backoff_delays == [0.001, 0.002, 0.001], r.backoff_delays
    assert backfills == [1] and col.manifest.reconnects == {"coinbase": 1} and col.manifest.disconnects["coinbase"] == 3
    kinds = [s for _m, _a, s, _r in r.health.transitions]
    for st in ("RECONNECTING", "CONNECTED", "WARMING_UP", "DISCONNECTED"):
        assert st in kinds, kinds
    assert [g["kind"] for g in col.manifest.gaps] == ["DISCONNECT"] and col.counts["events"] == 3
    # AuthUnavailable -> DISCONNECTED with the reason; no retry storm
    col2 = Collector(tmpdir(), ["BTC"], {}, clock, fsync=False, live_features=False)
    r2 = WsFeedRunner(CfViaKalshiAdapter(["BTC"]), col2, clock, stop_event=threading.Event(),
                      headers_fn=lambda: kalshi_auth.ws_headers("/trade-api/ws/v2", 1, environ={}),
                      connect_fn=lambda *a, **k: (_ for _ in ()).throw(AssertionError("connected without auth")))
    r2.run()
    assert r2.health.state == FeedState.DISCONNECTED and r2.attempts == 0 and "disabled" in col2.manifest.notes[0]
    # an optional source that keeps failing never blocks another source
    col3 = Collector(tmpdir(), ["BTC"], {}, clock, fsync=False, live_features=False)
    stop3 = threading.Event()
    bad = WsFeedRunner(KrakenAdapter(["BTC"]), col3, clock, stop_event=threading.Event(), max_attempts=5,
                       backoff=Backoff(initial_s=0.01, jitter=0),
                       connect_fn=lambda *a, **k: (_ for _ in ()).throw(ConnectionError("down")))
    good = WsFeedRunner(CoinbaseAdapter(["BTC"]), col3, clock, stop_event=stop3,
                        connect_fn=lambda *a, **k: FakeWS([_cb(t0 + i, 100 + i) for i in range(50)], stop3))
    bad.start(); good.start()
    good.join(5); bad.join(5)
    assert col3.counts["events"] == 50 and bad.health.state == FeedState.DISCONNECTED and bad.attempts == 5
    # poll runner: errors back off and are reported; recovery resumes
    calls = []

    def poll(reconnect=False):
        calls.append(reconnect)
        if len(calls) <= 2:
            raise ConnectionError("HTTP 503")
        if len(calls) == 4:
            stop4.set()
        return (1, 0, 0)
    stop4 = threading.Event()
    col4 = Collector(tmpdir(), ["BTC"], {}, clock, fsync=False, live_features=False)
    pr = PollRunner("kalshi", poll, col4, clock, interval_s=0.001, stop_event=stop4, backoff=Backoff(initial_s=0.001, jitter=0))
    pr.run()
    assert len(calls) == 4 and pr.backoff_delays == [0.001, 0.002] and col4.manifest.disconnects == {"kalshi": 2}


def test_kalshi_poller():
    d = tmpdir()
    w = World(["BTC"], C - 900_000, C + 900_000, seed=2)
    clock = FakeClock(C - 5_000)
    col = Collector(d, ["BTC"], {}, clock, fsync=False, live_features=False)
    fk = FakeKalshi(w, clock)
    p = KalshiPoller(KalshiAdapter(["BTC"]), fk, col, clock, trades_every=1)
    for _ in range(100):                                     # C-5 s .. C+95 s: market rolls over, then settles
        p.poll()
        clock.advance(1000)
    col.close()
    ev = load_sessions([col.dir]).events
    states = [e for e in ev if e.event_type == EventType.MARKET_STATE]
    assert len({e.payload["ticker"] for e in states}) == 2
    res = [e for e in ev if e.event_type == EventType.RESOLUTION]
    assert len(res) == 1 and res[0].receive_ts_ms >= C + 90_000 and not p.pending_settle
    trades = [e for e in ev if e.event_type == EventType.TRADE]
    first = {}
    for t in trades:
        first.setdefault(t.symbol, t)
    assert all(t.mode == IngestMode.BACKFILLED for t in first.values())                # first poll per market = history
    assert any(t.mode == IngestMode.LIVE for t in trades)
    assert fk.requests and all(r[0] == "GET" for r in fk.requests)
    assert all(r[1].startswith("https://") or r[1].startswith("http") for r in fk.requests)


# ═══════════════════ 14-15 storage + manifest ═══════════════════
def test_storage_integrity():
    d = tmpdir()
    w = EventStoreWriter(d, flush_lines=3, fsync=False)
    for i in range(10):
        w.write("event", {"i": i})
    w.close()
    r = read_session(d)
    assert [x[1]["i"] for x in r.records] == list(range(10)) and not r.corrupt
    w2 = EventStoreWriter(d, flush_lines=100, fsync=False)                   # a new run never appends to an old segment
    w2.write("note", {"x": 1}); w2.close()
    assert len(segment_files(d)) == 2 and len(read_session(d).records) == 11
    import gzip
    seg = segment_files(d)[0]
    blob = open(seg, "rb").read()
    open(seg, "wb").write(blob[:-7])                                         # torn write: truncated last member
    r = read_session(d)
    assert len(r.records) == 1 + 9 and any("truncated" in c[2] or "corrupt" in c[2] for c in r.corrupt), r.corrupt
    # CRC mismatch inside a valid gzip member, and an unknown kind
    d2 = tmpdir()
    w = EventStoreWriter(d2, flush_lines=1000, fsync=False)
    w.write("event", {"i": 1}); w.write("event", {"i": 2}); w.close()
    seg = segment_files(d2)[0]
    text = gzip.decompress(open(seg, "rb").read()).decode()
    lines = text.splitlines()
    lines[0] = lines[0].replace('"i":1', '"i":7')
    rec = json.loads(lines[1]); rec["k"] = "mystery"; lines[1] = json.dumps(rec)
    open(seg, "wb").write(gzip.compress(("\n".join(lines) + "\n").encode()))
    r = read_session(d2)
    assert not r.records and sorted(c[2] for c in r.corrupt) == ["CRC mismatch", "unknown kind 'mystery'"], r.corrupt
    try:
        w.write("secretstuff", {}); raise AssertionError("unknown kind written")
    except (ValueError, KeyError):
        pass


def test_manifest_has_no_secrets():
    fake = {"KALSHI_API_KEY_ID": "KEYID-5f1e2d3c-not-real", "CFB_API_ID": "cfb-id-not-real",
            "CFB_API_SECRET": "cfb-SECRET-value-not-real", "KALSHI_PRIVATE_KEY_PATH": os.path.join(tmpdir(), "k.pem")}
    open(fake["KALSHI_PRIVATE_KEY_PATH"], "w").write("-----BEGIN PRIVATE KEY-----\nNOTAREALKEYMATERIAL\n-----END PRIVATE KEY-----\n")
    out = tmpdir()
    code = ("import os, sys, json, socket\n"
            "def boom(*a, **k): raise RuntimeError('NETWORK USED')\n"
            "socket.socket.connect = boom; socket.create_connection = boom\n"
            f"sys.path.insert(0, {HERE!r})\n"
            "import collect_market_data as c\n"
            f"sys.exit(c.main(['--dry-run', '--output', {out!r}]))\n")
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=dict(os.environ, **fake), cwd=HERE)
    assert p.returncode == 0, p.stdout[-800:] + p.stderr[-800:]
    assert "kalshi_ws_auth_configured=True" in p.stdout and "cfb_direct_configured=True" in p.stdout
    assert "no network connection was made" in p.stdout and os.listdir(out) == []
    for v in fake.values():
        assert v not in p.stdout and v not in p.stderr, v
    # a real (synthetic) session's manifest carries presence flags only
    import collect_market_data as cmd
    old = {k: os.environ.get(k) for k in fake}
    os.environ.update(fake)
    try:
        cred = cmd.credentials()
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    d = tmpdir()
    col = Collector(d, ["BTC"], {"coinbase": {"enabled": True}, "_credentials": cred}, FakeClock(), fsync=False,
                    live_features=False)
    col.close()
    text = open(os.path.join(col.dir, "manifest.json")).read()
    for v in fake.values():
        assert v not in text
    m = load_manifest(col.dir)
    assert m["research_only"] is True and m["status"] == "COMPLETED" and m["feature_set_version"] == FEATURE_SET_VERSION
    assert set(m["fingerprints"]) == {"legacy_strategy_fingerprint", "extended_strategy_fingerprint",
                                      "settlement_fingerprint", "market_data_fingerprint"} and all(m["fingerprints"].values())
    for bad in ({"api_secret": 1}, {"x": {"Authorization": "Basic abc"}}, {"l": [{"private_key": "k"}]}):
        try:
            assert_no_secrets(bad); raise AssertionError(bad)
        except ValueError:
            pass


# ═══════════════════ 16-17 gaps + backfill ═══════════════════
def test_gap_detection():
    cd = CadenceGapDetector("cf", "BTC", "BRTI", 1000, 2.5)
    assert [cd.observe(t) for t in (0, 1000, 2000, 2400)] == [None] * 4
    g = cd.observe(6000)
    assert g.kind == "CADENCE" and (g.start_ts_ms, g.end_ts_ms) == (2400, 6000) and cd.observe(5000) is None
    idg = IdGapDetector("coinbase", "BTC", "trades", True)
    assert idg.observe(10, 1) is None and idg.observe(11, 2) is None and idg.observe(11, 3) is None
    g = idg.observe(15, 9)
    assert g.missing_count == 3 and g.recoverable and "12..14" in g.detail
    # collector: a heartbeat that ARRIVES BEFORE the trade it mentions is not a gap; a trade that never comes is
    clock = FakeClock(1_790_000_000_000)
    col = Collector(tmpdir(), ["BTC"], {}, clock, fsync=False, live_features=False)
    ad = CoinbaseAdapter(["BTC"])
    t = 1_790_000_000_000
    hb = lambda ts, lt: json.dumps({"type": "heartbeat", "product_id": "BTC-USD", "time": iso(ts), "last_trade_id": lt})  # noqa: E731
    seq = [_cb(t, 1), hb(t + 1000, 2), _cb(t + 900, 2), hb(t + 2000, 2), hb(t + 3000, 4), hb(t + 4000, 4), _cb(t + 4500, 6)]
    for i, m in enumerate(seq):
        clock.advance(1000)
        col.on_message(ad, m, clock.wall_ms(), clock.mono_ns())
    gaps = col.manifest.gaps
    assert [(g["kind"], g["missing_count"]) for g in gaps] == [("TRADE_ID", 2), ("TRADE_ID", 1)], gaps
    assert all(g["known_at_ms"] is not None and g["ingest_seq"] is not None for g in gaps)
    # gaps are recorded, never filled: a price grid across an outage is MISSING (no interpolation / forward fill)
    eng = FeatureEngine("BTC", sources_enabled=("coinbase",))
    ts = [t + i * 500 for i in range(0, 300)]
    for x in ts:
        if not (t + 100_000 <= x < t + 130_000):
            eng.ingest(E("coinbase", EventType.QUOTE, x, x + 20, bid=99.0, ask=101.0, mid=100.0 + (x - t) / 1e6))
    row = eng.features_at(t + 150_000)                                   # ingested up to t + 149.5 s (+20 ms)
    assert row.status["vol.coinbase.rv.60s"] == Status.MISSING and row.values["vol.coinbase.rv.60s"] is None
    assert row.status["vol.coinbase.rv.15s"] == Status.READY


def test_backfill_semantics():
    d, col, loaded, _m = shared_session()
    b = START + 215_000
    bf = [e for e in loaded.events if e.mode == IngestMode.BACKFILLED and e.source == "coinbase"]
    assert bf and all(e.receive_ts_ms == b and e.event_ts_ms < b for e in bf)          # retrieval time kept, event time kept
    assert all(START + 200_000 - 100 <= e.event_ts_ms < b for e in bf)
    mid_t = (START + 200_000 + b) // 2
    assert not [e for e in bf if available(e, mid_t)]                                 # not visible before retrieval
    raw = [r for r in load_sessions([col.dir], include_raw=True).raw if r.stream == "rest_backfill"]
    assert raw and raw[0].receive_ts_ms == b
    # a backfilled duplicate of a live trade collapses (first arrival wins)
    eng = FeatureEngine("BTC")
    live = E("coinbase", EventType.TRADE, 1000, 1100, price=10.0, size=1.0, aggressor="buy", trade_id=77)
    dup = E("coinbase", EventType.TRADE, 1000, 5000, mode=IngestMode.BACKFILLED, price=10.0, size=1.0, aggressor="buy",
            trade_id=77)
    assert eng.ingest(live) and not eng.ingest(dup) and eng.duplicates == 1 and len(eng.trades["coinbase"]) == 1


# ═══════════════════ 18-19 replay determinism + single pass == batch ═══════════════════
def test_replay_determinism():
    a, b = tmpdir(), tmpdir()
    c1 = run_session(a, assets=("ETH",), start_ms=START, duration_s=120, seed=5)
    c2 = run_session(b, assets=("ETH",), start_ms=START, duration_s=120, seed=5)
    l1, l2 = load_sessions([c1.dir], include_raw=True), load_sessions([c2.dir], include_raw=True)
    assert [e.to_dict() for e in l1.events] == [e.to_dict() for e in l2.events] and l1.events
    assert [r.to_dict() for r in l1.raw] == [r.to_dict() for r in l2.raw]
    assert [e.ingest_seq for e in l1.events] == sorted(e.ingest_seq for e in l1.events)
    slept = []
    ev = l1.events[:400]
    assert list(Replayer(ev, sleeper=slept.append)) == ev and slept == []                # event-by-event, no sleeping
    span = (ev[-1].receive_ts_ms - ev[0].receive_ts_ms) / 1000
    r1 = Replayer(ev, speed=1.0, sleeper=slept.append)
    assert list(r1) == ev and abs(r1.slept_s - span) < 1e-6
    r20 = Replayer(ev, speed=20.0, sleeper=lambda s: None)
    list(r20)
    assert abs(r20.slept_s - span / 20) < 1e-6
    try:
        Replayer(ev, speed=0); raise AssertionError
    except ValueError:
        pass
    ds1, ds2 = build_dataset(l1, ["ETH"]), build_dataset(l2, ["ETH"])
    assert rows_digest(ds1.rows) == rows_digest(ds2.rows)
    p = subprocess.run([sys.executable, os.path.join(HERE, "scripts", "replay_market_data.py"), c1.dir, "--digest"],
                       capture_output=True, text=True)
    q = subprocess.run([sys.executable, os.path.join(HERE, "scripts", "replay_market_data.py"), c2.dir, "--digest"],
                       capture_output=True, text=True)
    dg = [ln for ln in p.stdout.splitlines() if "digest" in ln]
    assert p.returncode == 0 and dg and dg == [ln for ln in q.stdout.splitlines() if "digest" in ln]


def test_single_pass_equals_batch():
    d, col, loaded, market = shared_session()
    ds = build_dataset(loaded, ["BTC"])
    assert ds.rows and ds.skipped.get("before_data") and all(r["values"].keys() == set(FEATURE_NAMES) or
                                                               tuple(r["values"]) == FEATURE_NAMES for r in ds.rows)
    mk = discover_markets(loaded.events, ["BTC"])
    for r in ds.rows:
        row = compute_at("BTC", loaded.events, r["checkpoint_ts_ms"], mk[r["market_ticker"]][0], loaded.gaps)
        assert row.values == r["values"] and status_map(row) == r["status"], (r["market_ticker"], r["checkpoint_seconds_remaining"])
    # the incremental engine at arbitrary instants == the batch path
    eng = FeatureEngine("BTC")
    gaps = sorted(loaded.gaps, key=lambda g: g.known_at_ms)
    ev = sorted(loaded.events, key=lambda e: (e.receive_ts_ms, e.ingest_seq))
    probes = [START + 30_000 + 37_000 * k for k in range(13)]
    i = gi = 0
    for T in probes:
        while i < len(ev) and ev[i].receive_ts_ms <= T:
            eng.ingest(ev[i]); i += 1
        while gi < len(gaps) and gaps[gi].known_at_ms <= T:
            eng.ingest_gap(gaps[gi]); gi += 1
        a, b = eng.features_at(T, market), compute_at("BTC", loaded.events, T, market, loaded.gaps)
        assert a.values == b.values and status_map(a) == status_map(b), T


# ═══════════════════ 20-22 windows, values, missing, warm-up, partial ═══════════════════
def test_windows_and_values():
    sb = StreamBuffer()
    for t, v in ((1000, "a"), (5000, "b"), (6000, "c"), (10_000, "d"), (10_001, "e")):
        sb.add(t, t, t, v)
    assert sb.range(5000, 10_000) == ["c", "d"]                           # (lo, hi]: start excluded, end included
    assert sb.range(0, 999) == [] and sb.range(10_000, 20_000) == ["e"]
    assert sb.asof(9999, 5000) == (6000, "c") and sb.asof(9999, 3000) is None and sb.asof(999, 10**9) is None
    sb.add(5500, 11_000, 1, "late")                                        # late arrival lands in its series place
    assert sb.range(5000, 6000) == ["late", "c"]
    # engine values on known inputs: a quote every 500 ms with a +1 bp step per second, trades at known times
    T0 = 1_790_000_000_000
    eng = FeatureEngine("BTC", sources_enabled=("coinbase",))
    px = lambda t: 100.0 * math.exp(1e-4 * (t - T0) / 1000)  # noqa: E731
    for k in range(0, 1300):
        t = T0 + k * 500
        eng.ingest(E("coinbase", EventType.QUOTE, t, t, bid=px(t) - 0.01, ask=px(t) + 0.01, mid=px(t)))
    T = T0 + 649_500
    trades = [(T - 5000, 1.0, "buy"), (T - 4999, 2.0, "sell"), (T - 100, 4.0, "buy"), (T, 8.0, "buy")]
    for i, (t, s, side) in enumerate(trades):
        eng.ingest(E("coinbase", EventType.TRADE, t, T, price=px(t), size=s, aggressor=side, trade_id=900 + i))
    row = eng.features_at(T)
    v = row.values
    for h, sec in (("1s", 1), ("5s", 5), ("60s", 60), ("5m", 300)):
        assert abs(v[f"price.coinbase.logret.{h}"] - 1e-4 * sec) < 1e-12, h
        assert abs(v[f"price.coinbase.accel.{h}"]) < 1e-12
    assert abs(v["vol.coinbase.rv.60s"] - math.sqrt(60) * 1e-4) < 1e-12 and abs(v["vol.coinbase.absvol.60s"] - 1e-4) < 1e-12
    assert abs(v["vol.coinbase.ratio.15s_5m"] - 1.0) < 1e-9 and abs(v["price.coinbase.persistence.60s"] - 1.0) < 1e-9
    assert abs(v["price.coinbase.slope.60s"] - 1e-4) < 1e-12 and abs(v["price.coinbase.path.60s"] - 60e-4) < 1e-12
    assert v["volume.coinbase.vol.5s"] == 14.0 and v["volume.coinbase.count.5s"] == 3.0     # T-5000 excluded, T included
    assert v["flow.coinbase.signed_vol.5s"] == 10.0 and v["volume.coinbase.max_size.5s"] == 8.0
    assert abs(v["flow.coinbase.imbalance.5s"] - 10 / 14) < 1e-12 and abs(v["flow.coinbase.count_imbalance.5s"] - 1 / 3) < 1e-12
    assert v["volume.coinbase.vol.15s"] == 15.0
    # arbitrary window lengths (not in the registry) through the same causal machinery
    w = eng.window_features(T, 42_000, sources=("coinbase",))
    assert abs(w.values["price.coinbase.logret.42s"] - 42e-4) < 1e-12 and w.values["volume.coinbase.vol.42s"] == 15.0
    assert abs(w.values["vol.coinbase.rv.42s"] - math.sqrt(42) * 1e-4) < 1e-12
    w7 = eng.window_features(T, 7_000, sources=("coinbase",))
    assert w7.values["volume.coinbase.count.7s"] == 4.0 and w7.values["flow.coinbase.signed_vol.7s"] == 11.0
    try:
        eng.window_features(T, 1500); raise AssertionError
    except ValueError:
        pass
    # a trade received AFTER T is invisible even with an event time inside the window
    assert compute_at("BTC", [E("coinbase", EventType.TRADE, T - 1000, T + 1, price=1.0, size=5.0, aggressor="buy", trade_id=1)],
                      T, sources_enabled=("coinbase",)).values["volume.coinbase.vol.5s"] is None


def test_missing_is_none_never_zero():
    r = FeatureRow(0, "BTC", "")
    for st in (Status.MISSING, Status.NOT_READY, Status.UNAVAILABLE, Status.UNDEFINED):
        r.set("x", 5.0, st)
        assert r.values["x"] is None and r.status["x"] == st
    r.set("x", float("nan")); assert r.values["x"] is None and r.status["x"] == Status.UNDEFINED
    r.set("x", None); assert r.status["x"] == Status.UNDEFINED
    r.set("x", 0.0); assert r.values["x"] == 0.0 and r.status["x"] == Status.READY          # a real zero stays a zero
    # no data at all: every feature None with a non-READY status (never 0)
    row = FeatureEngine("BTC").features_at(1_790_000_000_000)
    counts = {"xex.n_sources"}                                  # a COUNT of fresh venues: 0 is a true value here
    assert row.values["xex.n_sources"] == 0.0 and row.status["xex.n_sources"] == Status.READY
    assert all(v is None for k, v in row.values.items() if k not in counts)
    assert all(s != Status.READY for k, s in row.status.items() if k not in counts)
    # no trades in a window: volume is a real 0 (observed, nothing traded) but ratios are UNDEFINED, not 0
    T0 = 1_790_000_000_000
    evs = [E("coinbase", EventType.QUOTE, T0 + k * 500, T0 + k * 500, bid=1.0, ask=3.0, mid=2.0) for k in range(200)]
    evs.append(E("coinbase", EventType.TRADE, T0, T0 + 1000, price=2.0, size=1.0, aggressor="buy", trade_id=1))
    row = compute_at("BTC", evs, T0 + 99_500, sources_enabled=("coinbase",))
    assert row.values["volume.coinbase.vol.5s"] == 0.0 and row.status["volume.coinbase.vol.5s"] == Status.READY
    for n in ("flow.coinbase.imbalance.5s", "volume.coinbase.avg_size.5s", "price.coinbase.persistence.15s"):
        assert row.values[n] is None and row.status[n] == Status.UNDEFINED, n
    # the whole synthetic dataset: every non-READY/PARTIAL value is None, and the CSV writes it as an empty cell
    d, col, loaded, market = shared_session()
    ds = build_dataset(loaded, ["BTC"])
    n_none = 0
    for rr in ds.rows:
        for k, s in rr["status"].items():
            if s not in ("READY", "PARTIAL"):
                assert rr["values"][k] is None, (k, s, rr["values"][k])
                n_none += 1
    assert n_none > 0
    out = tmpdir()
    write_dataset(ds, out)
    import csv
    rows = list(csv.DictReader(open(os.path.join(out, "features.csv"))))
    for rr, cr in zip(ds.rows, rows):
        for k, s in rr["status"].items():
            assert cr[f"{k}__status"] == s and (cr[k] == "") == (rr["values"][k] is None), k


def test_warmup_and_partial_mode():
    T0 = 1_790_000_000_000
    evs = [E("coinbase", EventType.QUOTE, T0 + k * 1000, T0 + k * 1000, bid=1.0, ask=3.0, mid=2.0 + k * 1e-3)
           for k in range(400) if not (300 <= k < 310)]                    # a 10-s hole at +300 s
    for T, want in ((T0 + 10_000, Status.NOT_READY), (T0 + 400_000 - 1000, Status.MISSING)):
        row = compute_at("BTC", evs, T, sources_enabled=("coinbase",))
        assert row.status["vol.coinbase.rv.5m"] == want and row.values["vol.coinbase.rv.5m"] is None, (T, row.status["vol.coinbase.rv.5m"])
    strict = compute_at("BTC", evs, T0 + 399_000, sources_enabled=("coinbase",))
    assert Status.PARTIAL not in strict.status.values()
    part = compute_at("BTC", evs, T0 + 399_000, sources_enabled=("coinbase",), config=FeatureConfig(partial_windows=True))
    assert part.status["vol.coinbase.rv.5m"] == Status.PARTIAL and part.values["vol.coinbase.rv.5m"] > 0
    assert part.status["vol.coinbase.rv.60s"] == Status.READY                   # untouched window stays READY
    assert compute_at("BTC", evs, T0 + 10_000, sources_enabled=("coinbase",),
                      config=FeatureConfig(partial_windows=True)).status["vol.coinbase.rv.5m"] == Status.NOT_READY


# ═══════════════════ 23-25 LEAKAGE ═══════════════════
def _feats(events, T, market, gaps=()):
    row = compute_at("BTC", events, T, market, gaps)
    return json.dumps({"v": row.values, "s": status_map(row)}, sort_keys=True, default=str)


def _perturbations(T, market, loaded, before=False):
    """Each item: (name, extra events, extra gaps). `before` shifts availability to <= T (the canary)."""
    rx = (lambda dt: T) if before else (lambda dt: T + dt)          # canary: received exactly at T (available)
    cf_at = [e for e in loaded.events if e.event_type == EventType.INDEX_VALUE and e.event_ts_ms <= T - 3000][-1]
    p = cf_at.payload
    return [
        ("delayed exchange trade", [E("coinbase", EventType.TRADE, T - 2000, rx(500), price=150000.0, size=500.0,
                                      aggressor="buy", trade_id=10**9)], []),
        ("amended CF observation", [cf_ev(cf_at.event_ts_ms, p["value"] + 250.0, rx(400), amend=T + 400)], []),
        ("post-checkpoint Kalshi book", [E("kalshi", EventType.BOOK, None, rx(300), symbol=market.ticker,
                                           bids=[[1.0, 9999.0]], asks=[[99.0, 1.0]])], []),
        ("post-checkpoint Kalshi state", [E("kalshi", EventType.MARKET_STATE, None, rx(300), symbol=market.ticker,
                                            ticker=market.ticker, strike=market.strike, strike_source="floor_strike",
                                            close_ts_ms=market.close_ts_ms, open_ts_ms=market.open_ts_ms, yes_bid=97.0,
                                            yes_ask=99.0, no_bid=1.0, no_ask=3.0, status="active")], []),
        ("future price", [E("coinbase", EventType.QUOTE, T if before else T + 1000, rx(1040), bid=1.0, ask=3.0,
                            mid=2.0), E("kraken", EventType.QUOTE, None, rx(1100), symbol="BTC/USD", bid=1.0, ask=3.0,
                                        mid=2.0)], []),
        ("post-close data", [cf_ev(market.close_ts_ms + 5000 if not before else T - 60, 1.0, rx(market.close_ts_ms - T + 5100)),
                             E("coinbase", EventType.TRADE, market.close_ts_ms + 1000 if not before else T - 60,
                               rx(market.close_ts_ms - T + 1100), price=1.0, size=1e6, aggressor="sell", trade_id=10**9 + 1)], []),
        ("gap learned after T", [], [Gap("coinbase", "BTC", "trades", "TRADE_ID", T - 4000, T - 3000, 1000, 5, True, "",
                                         known_at_ms=rx(200), ingest_seq=1)]),
    ]


def leak_check(builder, loaded, market, Ts):
    """Returns the perturbations that CHANGED a checkpoint's features (must be empty for an honest builder),
    and whether every perturbation is visible when it arrives before T (canaries: the check is sensitive)."""
    leaks, insensitive = [], []
    for T in Ts:
        base = builder(loaded.events, T, market, loaded.gaps)
        for (name, ev, gp), (_n, ev_b, gp_b) in zip(_perturbations(T, market, loaded),
                                                    _perturbations(T, market, loaded, before=True)):
            if builder(loaded.events + ev, T, market, list(loaded.gaps) + gp) != base:
                leaks.append((T - market.close_ts_ms, name))
            if builder(loaded.events + ev_b, T, market, list(loaded.gaps) + gp_b) == base:
                insensitive.append((T - market.close_ts_ms, name))
    return leaks, insensitive


class _LeakyEngine(FeatureEngine):
    """CANARY: aligns by EVENT time and disables the causality guards (what a naive pipeline does)."""

    def ingest(self, ev):
        self.max_receive = None
        return super().ingest(ev)

    def features_at(self, t, market=None):
        self.max_receive = None
        return super().features_at(t, market)


def _leaky_builder(events, T, market, gaps=()):
    eng = _LeakyEngine("BTC")
    for g in gaps:
        eng.ingest_gap(g)
    key = lambda e: e.event_ts_ms if e.event_ts_ms is not None else e.receive_ts_ms  # noqa: E731
    for e in sorted((e for e in events if key(e) <= T), key=key):
        eng.ingest(e)
    row = eng.features_at(T, market)
    return json.dumps({"v": row.values, "s": status_map(row)}, sort_keys=True, default=str)


def test_no_leakage():
    d, col, loaded, market = shared_session()
    Ts = [market.close_ts_ms - 120_000, market.close_ts_ms - 30_000, market.close_ts_ms]
    # the fundamental invariant: deleting everything received after T changes nothing at T
    for T in Ts + [market.close_ts_ms - 245_500, market.close_ts_ms - 61_000]:
        cut = [e for e in loaded.events if e.receive_ts_ms <= T]
        cut_g = [g for g in loaded.gaps if g.known_at_ms <= T]
        assert _feats(loaded.events, T, market, loaded.gaps) == _feats(cut, T, market, cut_g), T
        assert len(cut) < len(loaded.events)
    leaks, insensitive = leak_check(_feats, loaded, market, Ts)
    assert not leaks, leaks
    # canaries: every perturbation DOES change the features when it is available by T (so the check is sensitive)
    assert not insensitive, insensitive
    bad_leaks, _ = leak_check(_leaky_builder, loaded, market, Ts[1:2])
    assert {n for _t, n in bad_leaks} >= {"delayed exchange trade", "amended CF observation"}, bad_leaks
    # the single-pass dataset is invariant too: add every perturbation with receive times AFTER the last checkpoint
    T_last = market.close_ts_ms
    extra, extra_g = [], []
    for _n, ev, gp in _perturbations(T_last, market, loaded):
        extra += ev
        extra_g += gp
    from market_data.replay import LoadedSessions
    pert = LoadedSessions(events=loaded.events + extra, gaps=list(loaded.gaps) + extra_g, session_ids=loaded.session_ids)
    a = [r for r in build_dataset(loaded, ["BTC"]).rows if r["market_ticker"] == market.ticker]
    b = [r for r in build_dataset(pert, ["BTC"]).rows if r["market_ticker"] == market.ticker]
    assert a == b and len(a) >= 5


def test_labels_never_features():
    assert not set(FEATURE_NAMES) & set(LABEL_FIELDS)
    for n in FEATURE_NAMES:
        for w in ("result", "expiration", "official", "final", "outcome", "label"):
            assert w not in n, n
    eng = FeatureEngine("BTC")
    r = E("kalshi", EventType.RESOLUTION, None, 1000, symbol="T", ticker="T", result="yes", expiration_value=1.0)
    assert eng.ingest(r) is False and eng.ingested == 0
    d, col, loaded, market = shared_session()
    # a RESOLUTION (even a WRONG one, received before T) never changes features
    T = market.close_ts_ms - 30_000
    flip = E("kalshi", EventType.RESOLUTION, None, T - 10_000, symbol=market.ticker, ticker=market.ticker, result="no",
             expiration_value=1.0)
    assert _feats(loaded.events + [flip], T, market, loaded.gaps) == _feats(loaded.events, T, market, loaded.gaps)
    # settlement-state features equal the Step-2 reconstruction AS OF T (never the final / label mode)
    for T in (market.close_ts_ms - 90_000, market.close_ts_ms - 30_000, market.close_ts_ms):
        row = compute_at("BTC", loaded.events, T, market, loaded.gaps)
        obs = [SettlementObservation.from_dict(e.payload["observation"]) for e in loaded.events
               if e.event_type == EventType.INDEX_VALUE and e.receive_ts_ms <= T]
        st = reconstruct(market, obs, as_of_ms=T).state
        assert row.values["settle.accumulated_mean"] == st.accumulated_mean, T
        assert row.values["settle.phase"] == st.phase.value and row.values["settle.quality"] == st.quality.value
        assert row.values["settle.samples_filled"] == float(st.samples_filled)
    ds = build_dataset(loaded, ["BTC"])
    lab = ds.labels[market.ticker]
    assert lab["official_result"] in ("yes", "no") and lab["reconstruction_matches_official"] is True
    assert all(set(r) == set(KEY_COLS) | {"values", "status"} for r in ds.rows)


def test_cross_exchange_alignment():
    d, col, loaded, market = shared_session()
    T = market.close_ts_ms - 60_000
    late_cb = E("coinbase", EventType.QUOTE, T - 200, T + 300, bid=1.0, ask=3.0, mid=2.0)       # delayed venue quote
    base = compute_at("BTC", loaded.events, T, market, loaded.gaps)
    received = compute_at("BTC", [e for e in loaded.events if e.receive_ts_ms <= T], T, market, loaded.gaps)
    # venue quotes with event time <= T that ARRIVED after T (every venue has some at any T) are not used
    assert [e for e in loaded.events if e.event_type == EventType.QUOTE and e.source == "coinbase"
            and e.event_ts_ms <= T < e.receive_ts_ms]
    assert base.values == received.values and status_map(base) == status_map(received)
    with_late = compute_at("BTC", loaded.events + [late_cb], T, market, loaded.gaps)
    for n in ("xex.cb_minus_kr.mid", "xex.dispersion_bps", "price.ref.last", "cfspot.cf_minus_ref"):
        assert base.values[n] == with_late.values[n] and base.status[n] == Status.READY, n
    # once it ARRIVES (T + 300) the late quote takes its event-time place in the Coinbase series - not before
    def engine_until(t_end, extra):
        eng = FeatureEngine("BTC")
        for e in sorted((e for e in loaded.events + extra if e.receive_ts_ms <= t_end),
                        key=lambda e: (e.receive_ts_ms, e.ingest_seq)):
            eng.ingest(e)
        return eng
    assert engine_until(T + 300, [late_cb]).price_at("coinbase", T - 150) == 2.0
    assert engine_until(T, [late_cb]).price_at("coinbase", T - 150) not in (None, 2.0)
    assert base.values["xex.n_sources"] == 2.0 and base.status["xex.leadlag.corr"] in (Status.READY, Status.UNDEFINED)


# ═══════════════════ 26-28 settlement evidence, dataset, status page ═══════════════════
def test_settlement_evidence_capture():
    d, col, loaded, market = shared_session()
    from settlement.cache import load
    st = load([os.path.join(d, "settle.jsonl")])
    assert st.observations and market.ticker in st.markets and st.published and st.resolutions
    assert st.summary()["corrupt_records"] == 0
    assert st.sessions and all(x.get("synthetic") is True for x in st.sessions)        # the Step-2 report excludes it
    from settlement.resolution import verify_all
    rep = verify_all(list(st.markets.values()), st.resolutions, st.observations, ["cf_rti_60s_start_incl_asof_v1"])
    assert rep["rows"] and rep["policies"]["cf_rti_60s_start_incl_asof_v1"]
    assert window_policy().verified is False                                   # evidence never auto-verifies


def test_dataset_writer_and_provenance():
    d, col, loaded, market = shared_session()
    ds = build_dataset(loaded, ["BTC"], session_manifests=[load_manifest(col.dir)])
    out = tmpdir()
    for fmt in ("csv", "jsonl"):
        write_dataset(ds, out, fmt)
    assert sorted(os.listdir(out)) == ["features.csv", "features.jsonl", "labels.jsonl", "provenance.json"]
    hdr = open(os.path.join(out, "features.csv")).readline().strip().split(",")
    assert hdr[:len(KEY_COLS)] == list(KEY_COLS) and hdr[len(KEY_COLS):len(KEY_COLS) + len(FEATURE_NAMES)] == list(FEATURE_NAMES)
    assert hdr[-1] == FEATURE_NAMES[-1] + "__status" and not [h for h in hdr if h in LABEL_FIELDS]
    js = [json.loads(ln) for ln in open(os.path.join(out, "features.jsonl"))]
    assert js == json.loads(json.dumps(ds.rows))
    labels = [json.loads(ln) for ln in open(os.path.join(out, "labels.jsonl"))]
    assert {x["market_ticker"] for x in labels} == set(ds.labels)
    pv = json.load(open(os.path.join(out, "provenance.json")))
    for k in ("feature_set_version", "sessions", "sources_enabled", "feature_definitions", "windows_ms", "fingerprints",
              "checkpoints_s", "config", "rows_sha256", "causal_rule", "labels_are_separate", "session_fingerprints"):
        assert k in pv, k
    assert pv["feature_set_version"] == FEATURE_SET_VERSION and pv["sessions"] == [col.session_id]
    assert all(pv["fingerprints"].values()) and len(pv["feature_definitions"]) == len(FEATURES)
    assert pv["rows_sha256"] == rows_digest(ds.rows) and pv["settlement_window_policy_verified"] is False
    assert pv["session_fingerprints"][col.session_id] == load_manifest(col.dir)["fingerprints"]
    assert not [f for f in os.listdir(out) if f.startswith(".tmp")]
    p = subprocess.run([sys.executable, os.path.join(HERE, "scripts", "build_market_features.py"), col.dir, "--assets", "BTC",
                        "--out", os.path.join(out, "cli")], capture_output=True, text=True)
    assert p.returncode == 0 and "SYNTHETIC" in p.stdout, p.stdout + p.stderr
    assert json.load(open(os.path.join(out, "cli", "provenance.json")))["rows_sha256"] == pv["rows_sha256"]


def test_status_page():
    from market_data.status_server import serve
    clock = FakeClock(START + 400_000)
    d, col0, loaded, market = shared_session()
    col = Collector(tmpdir(), ["BTC"], {}, clock, fsync=False, live_features=True)
    ad = CoinbaseAdapter(["BTC"])
    for k in range(30):
        clock.advance(500)
        col.on_message(ad, json.dumps({"type": "ticker", "product_id": "BTC-USD", "time": iso(clock.wall_ms() - 20),
                                       "best_bid": "100", "best_ask": "101"}), clock.wall_ms(), clock.mono_ns())
    srv = serve(lambda: col.status({"coinbase": FeedHealth("coinbase").to_dict()}), 0)
    port = srv.server_address[1]
    try:
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        c.request("GET", "/status")
        r = c.getresponse()
        st = json.loads(r.read())
        assert r.status == 200 and st["research_only"] is True and st["counts"]["events"] == 30
        assert st["assets"]["BTC"]["features"]["price.coinbase.mid"]["value"] == 100.5
        c.request("GET", "/")
        r = c.getresponse()
        page = r.read().decode()
        assert r.status == 200 and "RESEARCH ONLY" in page and "price.coinbase.mid" in page and "<script" not in page
        c.request("POST", "/status", body="x")
        assert c.getresponse().status in (501, 405)
    finally:
        srv.shutdown()
    assert srv.server_address[0] == "127.0.0.1"


# ═══════════════════ 29-31 fingerprint, CLI, performance ═══════════════════
def test_market_data_fingerprint():
    ok, problems = mdfp.verify()
    assert ok, problems
    d = tmpdir()
    shutil.copytree(os.path.join(HERE, "market_data"), os.path.join(d, "market_data"),
                    ignore=shutil.ignore_patterns("__pycache__"))
    p = os.path.join(d, "market_data", "alignment.py")
    s = open(p).read()
    open(p, "w").write(s.replace("return event.receive_ts_ms <= t_ms", "return event.receive_ts_ms < t_ms"))
    ok2, pr2 = mdfp.verify(pkg_dir=os.path.join(d, "market_data"))
    assert not ok2 and "modules/alignment.py changed" in pr2, pr2
    open(p, "w").write(s.replace("Event-time alignment", "Event-time alignment (reworded)") + "\n# trailing comment\n")
    assert mdfp.verify(pkg_dir=os.path.join(d, "market_data"))[0]                 # cosmetic change: same fingerprint
    q = subprocess.run([sys.executable, "-m", "market_data.fingerprint", "--write"], cwd=HERE, capture_output=True, text=True)
    assert q.returncode == 2 and "Refusing" in q.stderr
    b = json.load(open(os.path.join(HERE, "config", "market_data_baseline.json")))
    assert b["feature_set_version"] == FEATURE_SET_VERSION and b["feature_count"] == len(FEATURE_NAMES)


def test_collector_cli():
    p = subprocess.run([sys.executable, "collect_market_data.py", "--assets", "BTC,DOGE", "--dry-run"], cwd=HERE,
                       capture_output=True, text=True)
    assert p.returncode == 2 and "unknown assets" in p.stderr
    code = ("import socket, sys\n"
            "def boom(*a, **k): raise RuntimeError('NETWORK USED')\n"
            "socket.socket.connect = boom; socket.create_connection = boom\n"
            f"sys.path.insert(0, {HERE!r})\n"
            "import collect_market_data as c\n"
            "sys.exit(c.main(['--assets', 'ETH,SOL', '--coinbase', '--kalshi', '--dry-run']))\n")
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=HERE,
                       env={k: v for k, v in os.environ.items() if not k.startswith(("KALSHI_API", "CFB_"))})
    assert p.returncode == 0, p.stdout + p.stderr
    assert "source coinbase" in p.stdout and "source kalshi" in p.stdout and "source kraken" not in p.stdout
    assert "source cf_" not in p.stdout and "ETH-USD" in p.stdout and "KXSOL15M" in p.stdout and "BTC-USD" not in p.stdout
    assert p.stdout.count("MATCHES") == 3
    # a short real run of the collector machinery against fake transports (no network)
    import collect_market_data as cmd
    a = cmd.parse_args(["--assets", "BTC", "--coinbase", "--secondary", "--kalshi", "--duration", "1.5", "--output", tmpdir(),
                        "--status-port", "0", "--settlement-store", "none", "--kalshi-interval", "0.2"])
    w = World(["BTC"], C - 900_000, C + 900_000)
    fclock = FakeClock(C - 100_000)
    fk = FakeKalshi(w, fclock)
    conns = []

    def connect(url, headers=None):
        conns.append(url)
        if "kraken" in url:
            raise ConnectionError("kraken down")
        return FakeWS([_cb(C - 100_000 + i, 1 + i) for i in range(20)])
    rc = cmd.run(a, clock=fclock, getter=fk, connect_fn=connect, install_signals=False)
    assert rc == 0 and fk.requests and all(r[0] == "GET" for r in fk.requests)
    sess = [os.path.join(a.output, x) for x in os.listdir(a.output)]
    m = load_manifest(sess[0])
    assert m["status"] == "COMPLETED" and m["code_verification"]["strategy"]["ok"] and m["code_verification"]["market_data"]["ok"]
    assert load_sessions(sess).events and m["disconnects"].get("kraken", 0) >= 1


def test_performance_smoke():
    d, col, loaded, market = shared_session()
    eng = FeatureEngine("BTC")
    t0 = time.perf_counter()
    for e in loaded.events:
        eng.ingest(e)
    per_event_us = (time.perf_counter() - t0) / len(loaded.events) * 1e6
    T = loaded.events[-1].receive_ts_ms
    t0 = time.perf_counter()
    for _ in range(5):
        eng.features_at(T, market)
    per_row_ms = (time.perf_counter() - t0) / 5 * 1000
    assert per_event_us < 500 and per_row_ms < 500, (per_event_us, per_row_ms)          # generous CI bounds
    print(f"  (ingest {per_event_us:.1f} us/event, features_at {per_row_ms:.1f} ms/row, {len(FEATURE_NAMES)} features)")


def test_previous_stages():
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 19):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-600:], p.stderr[-600:])


TESTS = [
    ("baselines", "1 Step-1 + Step-2 baselines untouched: artifacts, 47 fixtures, fingerprints, perp/production files",
     test_baselines_untouched),
    ("index_ids", "2 verified CF index ids (documented) routed per asset; window convention still unverified", test_index_ids),
    ("boundaries", "3 import boundaries, read-only (GET only, no order paths), no perp features, LIVE refused",
     test_import_boundaries_and_read_only),
    ("types", "4 event envelope + deterministic normalization (bad values fail, never 0)", test_types_and_normalization),
    ("coinbase", "5 Coinbase adapter: aggressor from maker side, BBO, heartbeat, backfill, failures", test_coinbase_adapter),
    ("kraken", "6 Kraken adapter: taker side, snapshot=BACKFILLED, ticker without event time", test_kraken_adapter),
    ("kalshi", "7 Kalshi adapter: market state, book (cents/dollars), trades, settled result", test_kalshi_adapter),
    ("cf", "8 CF adapters reuse the Step-2 parsers; CF is never substituted", test_cf_adapters),
    ("alignment", "9 the causal rule: usable at T iff receive_ts <= T; guards", test_alignment_rule),
    ("feed", "10 clock anomalies, 7 feed states, exponential backoff", test_clock_and_feed_states),
    ("ws", "11 websocket client on a loopback server: handshake, ping/pong, fragments, mid-frame timeouts",
     test_websocket_transport),
    ("reconnect", "12 reconnect + backoff + backfill-on-reconnect; auth missing; optional sources isolated; polling",
     test_reconnect_backoff_and_isolation),
    ("poller", "13 Kalshi read-only poller: roll-over, settled result after close, GET only", test_kalshi_poller),
    ("storage", "14 append-only store: CRC, torn writes, unknown kinds, new segments", test_storage_integrity),
    ("manifest", "15 manifest + dry run: credentials as presence flags only, no network", test_manifest_has_no_secrets),
    ("gaps", "16 gaps: cadence, trade ids, heartbeat (no false gaps), never filled", test_gap_detection),
    ("backfill", "17 backfill: BACKFILLED, event time kept, visible only from retrieval; duplicates collapse",
     test_backfill_semantics),
    ("replay", "18 deterministic replay: identical streams, 1x / 20x / event-by-event", test_replay_determinism),
    ("equivalence", "19 single-pass dataset == batch compute_at at every checkpoint and instant",
     test_single_pass_equals_batch),
    ("windows", "20 window bounds (T-w, T], feature values on known inputs, arbitrary windows", test_windows_and_values),
    ("missing", "21 missing -> None + status mask, never 0 (engine, dataset, CSV)", test_missing_is_none_never_zero),
    ("warmup", "22 warm-up NOT_READY; PARTIAL only in the named research mode", test_warmup_and_partial_mode),
    ("leakage", "23 LEAKAGE: delayed trade / amended CF / late book / future price / post-close / late gap; canaries",
     test_no_leakage),
    ("labels", "24 labels never features; settlement features == Step-2 reconstruction as of T", test_labels_never_features),
    ("cross", "25 cross-exchange features use only received data (no event-time alignment)", test_cross_exchange_alignment),
    ("evidence", "26 settlement evidence captured for the Step-2 tools; never auto-verified", test_settlement_evidence_capture),
    ("dataset", "27 dataset writer: features / labels / provenance separate, atomic, CLI", test_dataset_writer_and_provenance),
    ("status", "28 research status page: localhost, read-only, separate from production", test_status_page),
    ("fingerprint", "29 separate market-data fingerprint", test_market_data_fingerprint),
    ("cli", "30 collect_market_data.py: argument checks, dry run, fake-transport run", test_collector_cli),
    ("performance", "31 performance smoke bounds", test_performance_smoke),
    ("previous", "32 all previous stage suites", test_previous_stages),
]


if __name__ == "__main__":
    only = None
    if "--only" in sys.argv:
        only = set(sys.argv[sys.argv.index("--only") + 1].split(","))
    for key, name, fn in TESTS:
        if only is None or key in only:
            run(name, fn)
    if only is None:
        print("\nAll Stage 19 tests passed.")
    else:
        print(f"\nSelected Stage 19 tests passed: {','.join(sorted(only))}")
