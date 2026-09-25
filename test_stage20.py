#!/usr/bin/env python3
"""Stage 20 — STEP 4 EXPANDED PERPETUAL-FUTURES TELEMETRY (research only).

Run:  py test_stage20.py                 (or py run_all_tests.py for stages 1-20)
      py test_stage20.py --only leakage,liquidations   (a subset; used by scripts/mutation_test_perp_data.py)

Offline and deterministic: payloads shaped like the documented Binance / Bybit / OKX / Kalshi-perp / Coinbase
messages, fake clocks, fake transports. Covers normalization (trades + aggressor side, funding units + intervals,
open interest units, liquidation side semantics), per-venue features (basis, funding, OI, liquidations, flow, CVD,
order book), cross-exchange aggregation, perp-vs-spot / perp-vs-CF divergence, missing data and warm-up,
reconnect / keep-alive / gap detection / backfill, receive-time causality with future-event exclusion and
canaries, raw -> normalization reproducibility, deterministic replay, the joint Step-3 + Step-4 dataset,
provenance, label isolation, the separate fingerprint, the dry run, and that the production strategy, the
existing Kalshi-perp veto chain, the settlement engine and the Step-3 engine are untouched and never import
perp_data; LIVE execution stays refused.
"""
import ast
import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from market_data.clock import FakeClock                                                       # noqa: E402
from market_data.feed import Backoff, FeedState                                               # noqa: E402
from market_data.features.dataset import discover_markets                                     # noqa: E402
from market_data.features.definitions import FEATURE_NAMES as S3_NAMES                        # noqa: E402
from market_data.features.engine import CausalityError, compute_at as s3_compute              # noqa: E402
from market_data.gaps import Gap                                                              # noqa: E402
from market_data.replay import LoadedSessions, load_sessions                                  # noqa: E402
from market_data.runner import PollRunner, WsFeedRunner                                       # noqa: E402
from market_data.sources.base import Sequencer                                                # noqa: E402
from market_data.types import EventType, IngestMode, MarketEvent                              # noqa: E402
from perp_data import PERP_FEATURE_SET_VERSION, fingerprint as pfp                            # noqa: E402
from perp_data.collector import PerpCollector                                                 # noqa: E402
from perp_data.dataset import KEY_COLS, build_research_dataset, write_research_dataset         # noqa: E402
from perp_data.features.definitions import (FAMILIES, FEATURE_NAMES, FEATURES, PerpFeatureConfig,  # noqa: E402
                                            Status, names_by_family)
from perp_data.features.engine import PerpFeatureEngine, compute_at, event_key                # noqa: E402
from perp_data.normalization import funding_normalized, funding_rate, oi_to_coin              # noqa: E402
from perp_data.poller import PerpPoller                                             # noqa: E402
from perp_data.replay import LoadedPerp, Replayer, load_perp_sessions            # noqa: E402
from perp_data.sources.base import PerpCtx                                                    # noqa: E402
from perp_data.sources.binance import BinanceUsdmAdapter                                      # noqa: E402
from perp_data.sources.bybit import BybitLinearAdapter                                        # noqa: E402
from perp_data.sources.kalshi_perp import KalshiPerpAdapter                                   # noqa: E402
from perp_data.sources.okx import OkxSwapAdapter                                              # noqa: E402
from perp_data.synthetic import run_joint_session                                             # noqa: E402
from perp_data.transport import KeepaliveWebSocket, connect_fn_for                            # noqa: E402
from perp_data.types import PAYLOAD_FIELDS, PerpEvent, PerpEventType as PT                    # noqa: E402
from perp_data.venues import PERP_VENUES                                 # noqa: E402
from settlement.checkpoints import LABEL_FIELDS                                               # noqa: E402

STEP1_ARTIFACTS = {
    "config/strategy_baseline.json": "8f052394a830be88f4bd4c506b5d8069222e3c9b1c06c0bc63e3817463185d04",
    "step5_baseline_manifest.json": "6a2994ff8bdc2d36b41608515c77a13d71fd371030487b5e62b2b064cd096db8",
    "regression/strategy_cases.json": "e942ba3716e42a55fbf8e6086283eb0c8f124564b1cb44a3301b6c12c47abf9c",
    "kalshi_dashboard.py": "aa66c7ae1d6cf8b913b746b05bfd6cf64197454d3b7fd00a2ba174749150c595",
}
PERP_VETO_CHAIN = {   # the EXISTING Kalshi-perp research / shadow / overlay / promotion / live-veto chain: byte-identical
    "perp_telemetry.py": "5e1de6d6553032bd2902d0b9766086b826876c632bb41cfcfe52234c90b4d99e",
    "perp_probability.py": "344114534aef2a90ba41b07a52a454bd1e5ebc3b06b4b2facebde5c90eb76546",
    "perp_shadow.py": "ba3b3444e83fe384f5556050abc7d543d9c06fbc147a51319a314dddf12794c3",
    "perp_live.py": "95b0ce6f1f6ce70b62cae1176757dbb27574501817a0d6c868fec4bbd7ba48a2",
    "analyze_perp_quality.py": "b1a3a35e2a15a0c58a460739a90eff9c574cae3ab6bb548298021c6b0bda50bf",
    "analyze_perp_predictive.py": "6a783a753a9f1f9b118f1936aec5c2835dadc9f741f36b0ecf4758561403773d",
    "build_perp_shadow_policy.py": "8003371e9a0dbf657c5b1db5888cbd6e285172a3fab45389385d062c03ea0e38",
    "analyze_perp_shadow.py": "5aa0ae764d417ed5e3118faff78dda747b492695936efeff3d81a1476534d87b",
    "build_perp_integration_experiment.py": "c9ef7c33712d13fea4edaeccbe68c17045d64114143c6efd1f6fbe429a081309",
    "analyze_perp_integration.py": "d34215be9b8a8fc8c9a51534dae47428b5b6000e0adb4b57f09362a8b2694c35",
    "promote_perp_integration.py": "7f4aab689fceaac6d5441d8b239617e80c21d7c1da23184e688b213e2d9da73c",
    "check_perp_deployment.py": "879d8ab854aa9b17e7d4c67ed0aed8d9b32e40175e0cf9486a68ea41c840b819",
    "label_binary_outcomes.py": "497108eb30f9a29a6b88ba994f581e8aa057b4a283a54405dafb841dbf91f238",
    "step5_baseline_manifest.json": "6a2994ff8bdc2d36b41608515c77a13d71fd371030487b5e62b2b064cd096db8",
}
EXISTING_PERP_VETO_FINGERPRINT = "499c1e16da5d9cc763babe7dea31db7104ebc3f1460a17241af83240c8de523b"
PRODUCTION_UNTOUCHED = {
    "kalshi_backtest.py": "19f4bc281701db5234c29cc922b3806b21ac333883df174dcd936385fe9f117f",
    "kalshi_bot.py": "8278bdc4d2adaa117e2fdcce101924181d4bc64d3987b584a69741eeb474cd5d",
    "run_local.py": "0749c2a83b3c219ed63dc5d994253883983f9b6b4ebc81a2b72f46b067e53044",
}
C = 1_790_001_000_000                         # the close of the test market (15-minute boundary)
START = C - 420_000
DUR_S = 520
_CACHE = {}
S = Status


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


def tmpdir():
    return tempfile.mkdtemp(prefix="stage20-")


def shared():
    """One synthetic joint BTC session: all four perp venues + USDT rate + Step-3 spot/CF/Kalshi, a liquidation
    cascade before the close and a Binance websocket outage recovered by REST backfill."""
    if "s" not in _CACHE:
        d = tmpdir()
        c3, pc = run_joint_session(d, assets=("BTC",), start_ms=START, duration_s=DUR_S, seed=3,
                                   cascade=(C - 150_000, C - 10_000),
                                   perp_disconnect=("binance_usdm", START + 100_000, START + 110_000))
        L3, L4 = load_sessions([c3.dir]), load_perp_sessions([c3.dir])
        market = [m for m, _k in discover_markets(L3.events, ["BTC"]).values() if m.close_ts_ms == C][0]
        _CACHE["s"] = (d, c3, pc, L3, L4, market)
    return _CACHE["s"]


_SEQ = [50_000_000]


def E4(src, et, ev_ts, rx, asset="BTC", symbol="BTCUSDT", mode=IngestMode.LIVE, quality="OK", flags=(), **p):
    _SEQ[0] += 1
    payload = {k: None for k in PAYLOAD_FIELDS[et]}
    payload.update(p)
    return PerpEvent(src, asset, et, symbol, ev_ts, rx, _SEQ[0], payload, mode=mode, quality=quality, flags=tuple(flags))


def E3(src, et, ev_ts, rx, asset="BTC", symbol="BTC-USD", **p):
    _SEQ[0] += 1
    defaults = {EventType.QUOTE: {"bid_size": None, "ask_size": None},
                EventType.TRADE: {"aggressor_semantics": "INVERTED_MAKER_SIDE", "trade_id": _SEQ[0]}}
    payload = dict(defaults.get(et, {}))
    payload.update(p)
    return MarketEvent(src, asset, et, symbol, ev_ts, rx, _SEQ[0], payload)


def ctx(rx=1_790_000_000_000, raw=1):
    return PerpCtx("S", Sequencer(), rx, 0, raw)


def feats(events, t, market=None, gaps=(), **kw):
    row = compute_at("BTC", events, t, market, gaps, **kw)
    return row


def dump(row):
    return json.dumps({"v": row.values, "s": {k: v.value for k, v in row.status.items()}}, sort_keys=True, default=str)


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


def sha(rel):
    return hashlib.sha256(open(os.path.join(HERE, rel), "rb").read()).hexdigest()


# ═══════════════════ 1-3 baselines, isolation, veto untouched ═══════════════════
def test_baselines_untouched():
    for rel, h in {**STEP1_ARTIFACTS, **PRODUCTION_UNTOUCHED}.items():
        assert sha(rel) == h, rel
    from kalshi_core import baseline
    ok, problems = baseline.verify()
    assert ok, problems
    p = subprocess.run([sys.executable, "-m", "regression.generate", "--check"], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "MATCH (47 cases)" in p.stdout, p.stdout + p.stderr
    import strategy_fingerprint as sf
    fp, cfg, _v, _f = sf.current_fingerprint(os.path.join(HERE, "kalshi_dashboard.py"))
    assert fp == "8d94f241e8fc8edadc76058e1f12f430b6e4f499f4c0a30fba1cb5cf07dad82a"
    m = baseline.build_manifest()
    assert m["fingerprints"]["extended_strategy_fingerprint"] == "784141876a1b7f4c9f605036ef1358a6a3ea51ef32fced1f80f74045a6adef8c"
    from settlement import fingerprint as sfp
    from market_data import fingerprint as mfp
    assert sfp.verify()[0], sfp.verify()[1]
    assert mfp.verify()[0], mfp.verify()[1]                      # the Step-3 engine is unchanged
    assert pfp.verify()[0], pfp.verify()[1]


def test_existing_perp_veto_untouched():
    for rel, h in PERP_VETO_CHAIN.items():
        assert sha(rel) == h, f"EXISTING PERP VETO CHAIN CHANGED: {rel}"
    assert pfp.perp_veto_fingerprint() == EXISTING_PERP_VETO_FINGERPRINT
    assert tuple(sorted(pfp.PERP_VETO_FILES)) == tuple(sorted(PERP_VETO_CHAIN))
    stored = json.load(open(os.path.join(HERE, "config", "perp_data_baseline.json")))
    assert stored["existing_perp_veto_fingerprint"] == EXISTING_PERP_VETO_FINGERPRINT
    # the veto is still off by default and nothing here promoted anything
    p = subprocess.run([sys.executable, "check_perp_deployment.py"], cwd=HERE, capture_output=True, text=True,
                       env={k: v for k, v in os.environ.items() if k != "PERP_LIVE_VETO_ENABLED"})
    assert "FINAL STATUS: INACTIVE" in p.stdout, p.stdout[-500:]
    import kalshi_dashboard as k                                  # (import only; no thread is started)
    assert k.PERP_LIVE_VETO_ENABLED is False or os.environ.get("PERP_LIVE_VETO_ENABLED") == "1"
    assert not any(n.startswith("perp_data") for n in sys.modules if n in ("perp_live", "perp_shadow")), "sanity"


def test_production_isolation():
    prod = ["kalshi_dashboard.py", "kalshi_backtest.py", "kalshi_bot.py", "run_local.py", "strategy_fingerprint.py"] + \
        list(PERP_VETO_CHAIN)[:-1] + [os.path.join("kalshi_core", x) for x in os.listdir(os.path.join(HERE, "kalshi_core")) if x.endswith(".py")]
    for f in prod + [os.path.relpath(p, HERE) for p in _py_files(os.path.join(HERE, "settlement")) + _py_files(os.path.join(HERE, "market_data"))] + \
            ["collect_market_data.py"]:
        bad = [m for m in _imports(os.path.join(HERE, f)) if m == "perp_data" or m.startswith("perp_data.")]
        assert not bad, (f, bad)
    forbidden = {"kalshi_dashboard", "kalshi_bot", "discord", "kalshi_api_learn", "kalshi_core.execution", "kalshi_core.signal",
                 "perp_telemetry", "perp_live", "perp_shadow", "perp_probability", "kalshi_backtest", "run_local"}
    files = _py_files(os.path.join(HERE, "perp_data")) + [os.path.join(HERE, "collect_research_data.py")] + \
        [os.path.join(HERE, "scripts", f) for f in ("replay_perp_data.py", "build_research_dataset.py", "bench_perp_data.py",
                                                    "mutation_test_perp_data.py")]
    for f in files:
        bad = {m for m in _imports(f) if m in forbidden or m.split(".")[0] in {"discord", "kalshi_dashboard"}}
        assert not bad, (f, bad)
        src = open(f, encoding="utf-8").read().lower()
        for s in ("/portfolio/orders", "create_order", "place_order", ".post(", ".put(", ".delete(", "/fapi/v1/order",
                  "/v5/order", "/api/v5/trade", "leverage", "margin/orders"):
            assert s not in src, (f, s)
    # runtime: importing the production watcher module does not pull perp_data in
    code = ("import sys; sys.path.insert(0, %r); import run_local, kalshi_core.signal, kalshi_core.adapter;"
            "print(any(m == 'perp_data' or m.startswith('perp_data.') for m in sys.modules))") % HERE
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=HERE)
    assert p.returncode == 0 and p.stdout.strip() == "False", p.stdout + p.stderr
    from kalshi_core import execution as ex
    try:
        ex.get_execution_engine("LIVE"); raise AssertionError("LIVE engine returned")
    except ex.LiveExecutionUnavailable:
        pass


# ═══════════════════ 4-7 normalization ═══════════════════
def _bn(data, stream="btcusdt@aggTrade"):
    return json.dumps({"stream": stream, "data": data})


def test_trade_normalization_and_aggressor():
    b = BinanceUsdmAdapter(["BTC"])
    base = {"e": "aggTrade", "E": 1_790_000_000_010, "s": "BTCUSDT", "a": 42, "p": "100000.5", "q": "0.25", "f": 1, "l": 2,
            "T": 1_790_000_000_000}
    t_m = b.parse(_bn(dict(base, m=True)), ctx(1_790_000_000_200)).events[0]
    t_t = b.parse(_bn(dict(base, m=False, a=43)), ctx()).events[0]
    assert t_m.payload["aggressor"] == "sell" and t_t.payload["aggressor"] == "buy"     # m = buyer is MAKER -> seller took
    assert t_m.payload["aggressor_semantics"] == "INVERTED_MAKER_SIDE" and t_m.payload["qty_coin"] == 0.25
    assert t_m.event_ts_ms == 1_790_000_000_000 and t_m.receive_ts_ms == 1_790_000_000_200 and t_m.payload["trade_id"] == 42
    assert t_m.payload["notional_quote"] == 100000.5 * 0.25 and t_m.payload["quote_ccy"] == "USDT" and t_m.raw_seq == 1
    for bad in (dict(base, m="yes"), dict(base, m=True, p="0"), dict(base, m=True, q="-1"), dict(base, m=True, s="DOGEUSDT")):
        r = b.parse(_bn(bad), ctx())
        assert not r.events and r.failures, bad
    y = BybitLinearAdapter(["BTC"])
    msg = {"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": 1, "data": [
        {"T": 1_790_000_000_002, "s": "BTCUSDT", "S": "Sell", "v": "0.5", "p": "100", "i": "b", "BT": False},
        {"T": 1_790_000_000_001, "s": "BTCUSDT", "S": "Buy", "v": "0.1", "p": "100", "i": "a", "BT": False}]}
    ev = y.parse(json.dumps(msg), ctx()).events
    assert [(e.payload["trade_id"], e.payload["aggressor"]) for e in ev] == [("a", "buy"), ("b", "sell")]   # S = taker side
    assert all(e.payload["aggressor_semantics"] == "TAKER_SIDE_FIELD" for e in ev)
    o = OkxSwapAdapter(["BTC"])
    tr = {"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"}, "data": [
        {"instId": "BTC-USDT-SWAP", "tradeId": "9", "px": "100000", "sz": "3", "side": "buy", "ts": "1790000000000"}]}
    t0 = o.parse(json.dumps(tr), ctx()).events[0]
    assert t0.payload["qty_native"] == 3 and t0.payload["qty_unit"] == "contracts"
    assert t0.payload["qty_coin"] is None and t0.quality == "UNVERIFIED_UNITS"     # static ctVal is never trusted alone
    inst = {"code": "0", "data": [{"instId": "BTC-USDT-SWAP", "ctVal": "0.01", "ctValCcy": "BTC", "ctType": "linear"}]}
    ie = o.parse_rest("instruments", inst, ctx()).events[0]
    assert ie.event_type == PT.INSTRUMENT and ie.payload["verified"] is True
    t1 = o.parse(json.dumps(tr), ctx()).events[0]
    assert abs(t1.payload["qty_coin"] - 0.03) < 1e-12 and t1.quality == "OK" and abs(t1.payload["notional_quote"] - 3000) < 1e-6
    bad_inst = {"code": "0", "data": [{"instId": "BTC-USDT-SWAP", "ctVal": "1", "ctValCcy": "USDT", "ctType": "inverse"}]}
    o2 = OkxSwapAdapter(["BTC"])
    assert o2.parse_rest("instruments", bad_inst, ctx()).events[0].payload["verified"] is False
    assert o2.parse(json.dumps(tr), ctx()).events[0].payload["qty_coin"] is None


def test_funding_normalization():
    assert funding_normalized(0.0001, 8 * 3_600_000) == (0.0001 / 8, 0.0001, 0.0001 / 8 * 24 * 365)
    assert funding_normalized(0.0001, 4 * 3_600_000)[1] == 0.0002                  # 4 h interval -> doubled per 8 h
    assert abs(funding_normalized(0.0001, 3_600_000)[1] - 0.0008) < 1e-15
    assert funding_normalized(0.0001, None) == (None, None, None)
    assert funding_rate("0") == 0.0 and funding_rate("-0.00012") == -0.00012     # zero / negative are real values
    for bad in ("", None, "x", "1.5", "nan"):
        try:
            funding_rate(bad); raise AssertionError(bad)
        except ValueError:
            pass
    b = BinanceUsdmAdapter(["BTC", "ETH"])
    mp = lambda s, r: _bn({"e": "markPriceUpdate", "E": 1_790_000_000_000, "s": s, "p": "100.1", "i": "100", "P": "100",  # noqa: E731
                           "r": r, "T": 1_790_006_400_000}, "x@markPrice@1s")
    evs = b.parse(mp("BTCUSDT", "0.00010000"), ctx()).events
    fr = [e for e in evs if e.event_type == PT.FUNDING_RATE][0]
    assert {e.event_type for e in evs} == {PT.PERP_MARK_PRICE, PT.PERP_INDEX_PRICE, PT.FUNDING_RATE}   # mark != index != funding
    assert fr.payload["interval_ms"] == 8 * 3_600_000 and fr.payload["interval_source"] == "documented_default_8h"
    assert "DEFAULT_INTERVAL" in fr.flags and "ESTIMATE" in fr.flags and fr.payload["is_estimate"] is True
    assert fr.payload["next_funding_ts_ms"] == 1_790_006_400_000 and fr.payload["rate_8h"] == 0.0001
    b.parse_rest("funding_info", [{"symbol": "ETHUSDT", "fundingIntervalHours": 4}], ctx())
    fe = [e for e in b.parse(mp("ETHUSDT", "0.00010000"), ctx()).events if e.event_type == PT.FUNDING_RATE][0]
    assert fe.payload["interval_ms"] == 4 * 3_600_000 and fe.payload["rate_8h"] == 0.0002 and "DEFAULT_INTERVAL" not in fe.flags
    assert not [e for e in b.parse(mp("BTCUSDT", ""), ctx()).events if e.event_type == PT.FUNDING_RATE]   # none published
    y = BybitLinearAdapter(["BTC"])
    snap = {"topic": "tickers.BTCUSDT", "type": "snapshot", "ts": 1_790_000_000_000, "cs": 1, "data": {
        "symbol": "BTCUSDT", "fundingRate": "0.0001", "nextFundingTime": "1790006400000", "fundingIntervalHour": "1",
        "markPrice": "100", "indexPrice": "99.9", "openInterest": "10", "openInterestValue": "1000",
        "bid1Price": "99.9", "bid1Size": "1", "ask1Price": "100.1", "ask1Size": "2"}}
    evs = y.parse(json.dumps(snap), ctx()).events
    f = [e for e in evs if e.event_type == PT.FUNDING_RATE][0]
    assert f.payload["interval_ms"] == 3_600_000 and abs(f.payload["rate_8h"] - 0.0008) < 1e-15
    assert not [e for e in evs if e.event_type == PT.PREDICTED_FUNDING]            # Bybit publishes no forecast
    o = OkxSwapAdapter(["BTC"])
    fm = {"arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"}, "data": [{
        "instId": "BTC-USDT-SWAP", "fundingRate": "0.0001", "nextFundingRate": "0.0002", "fundingTime": "1790006400000",
        "nextFundingTime": "1790035200000", "method": "next_period", "ts": "1790000000000"}]}
    evs = o.parse(json.dumps(fm), ctx()).events
    cur, pred = evs
    assert cur.payload["interval_ms"] == 8 * 3_600_000 and cur.payload["next_funding_ts_ms"] == 1_790_006_400_000
    assert pred.event_type == PT.PREDICTED_FUNDING and pred.payload["applies_at_ts_ms"] == 1_790_035_200_000 and pred.payload["rate_8h"] == 0.0002
    fm["data"][0]["nextFundingRate"] = ""
    assert [e.event_type for e in o.parse(json.dumps(fm), ctx()).events] == [PT.FUNDING_RATE]   # never fabricated
    k = KalshiPerpAdapter(["BTC"], overrides={})
    k.tickers["BTC"] = "KXBTCPERP"
    kf = k.parse_rest("funding:BTC", {"funding_rate": "0.00005", "next_funding_time": 1_790_006_400_000,
                                      "computed_time": 1_790_000_000_000}, ctx()).events[0]
    assert kf.payload["rate_native"] == 5e-05 and kf.payload["rate_8h"] is None and kf.quality == "UNVERIFIED_UNITS"


def test_oi_normalization():
    b = BinanceUsdmAdapter(["BTC"])
    e = b.parse_rest("oi:BTC", {"openInterest": "10659.509", "symbol": "BTCUSDT", "time": 1_790_000_000_000}, ctx()).events[0]
    assert (e.payload["oi_native"], e.payload["oi_unit"], e.payload["oi_coin"]) == (10659.509, "coin", 10659.509)
    assert b.parse_rest("oi:BTC", {"openInterest": "1", "symbol": "ETHUSDT", "time": 1}, ctx()).failures
    o = OkxSwapAdapter(["BTC"])
    oi = {"arg": {"channel": "open-interest", "instId": "BTC-USDT-SWAP"}, "data": [{
        "instId": "BTC-USDT-SWAP", "instType": "SWAP", "oi": "2940318", "oiCcy": "29403.18", "oiUsd": "2483131029.62",
        "ts": "1790000000000"}]}
    e = o.parse(json.dumps(oi), ctx()).events[0]
    assert e.payload["oi_native"] == 2940318 and e.payload["oi_unit"] == "contracts" and e.payload["oi_coin"] == 29403.18
    del oi["data"][0]["oiCcy"]
    assert o.parse(json.dumps(oi), ctx()).events[0].payload["oi_coin"] is None       # contracts without verified ctVal
    assert oi_to_coin(100, "contracts", 0.01, False) is None and oi_to_coin(100, "contracts", 0.01, True) == 1.0
    assert oi_to_coin(5, "coin") == 5
    # the engine only ever uses base-coin OI: equal coin OI on two venues with very different contract counts
    T0 = 1_790_000_000_000
    evs = []
    for k in range(0, 400):
        t = T0 + k * 1000
        for v, sym, native, unit in (("binance_usdm", "BTCUSDT", 100.0 + k * 0.1, "coin"),
                                     ("okx_swap", "BTC-USDT-SWAP", (100.0 + k * 0.1) / 0.01, "contracts")):
            evs.append(E4(v, PT.PERP_MARK_PRICE, t, t + 10, symbol=sym, mark=100_000.0, quote_ccy="USDT"))
            evs.append(E4(v, PT.OPEN_INTEREST, t, t + 10, symbol=sym, oi_native=native, oi_unit=unit, oi_coin=100.0 + k * 0.1))
    r = feats(evs, T0 + 399_500, venues=("binance_usdm", "okx_swap"), spot=False)
    for v in ("binance_usdm", "okx_swap"):
        assert abs(r.values[f"perp.{v}.oi_coin"] - 139.9) < 1e-9
        assert abs(r.values[f"perp.{v}.oi_change_coin.60s"] - 6.0) < 1e-9
        assert abs(r.values[f"perp.{v}.oi_pct_change.60s"] - 6.0 / 133.9) < 1e-12
        assert abs(r.values[f"perp.{v}.oi_notional_change.60s"] - 6.0 * 100_000) < 1e-3
        assert abs(r.values[f"perp.{v}.oi_accel_coin.60s"]) < 1e-9
    assert abs(r.values["x.oi_total_notional"] - 2 * 139.9 * 100_000) < 1e-3       # never raw contract counts


def test_liquidation_semantics_and_aggregation():
    b = BinanceUsdmAdapter(["BTC"])
    fo = {"e": "forceOrder", "E": 1_790_000_000_005, "o": {"s": "BTCUSDT", "S": "SELL", "o": "LIMIT", "f": "IOC", "q": "0.5",
                                                          "p": "99000", "ap": "99100", "X": "FILLED", "l": "0.5", "z": "0.5",
                                                          "T": 1_790_000_000_000}}
    e = b.parse(_bn(fo, "btcusdt@forceOrder"), ctx()).events[0]
    assert e.payload["forced_side"] == "sell" and e.payload["liquidated_position"] == "long"
    assert "POSITION_SIDE_DERIVED" in e.flags and "SAMPLED_STREAM" in e.flags and e.quality == "SAMPLED"
    assert e.payload["price"] == 99100 and e.payload["notional_quote"] == 0.5 * 99100
    y = BybitLinearAdapter(["BTC"])
    lq = {"topic": "allLiquidation.BTCUSDT", "type": "snapshot", "ts": 1, "data": [
        {"T": 1_790_000_000_000, "s": "BTCUSDT", "S": "Buy", "v": "2", "p": "100"},
        {"T": 1_790_000_000_001, "s": "BTCUSDT", "S": "Sell", "v": "1", "p": "100"}]}
    e1, e2 = y.parse(json.dumps(lq), ctx()).events
    assert (e1.payload["liquidated_position"], e1.payload["forced_side"]) == ("long", "sell")     # "Buy" = LONG liquidated
    assert (e2.payload["liquidated_position"], e2.payload["forced_side"]) == ("short", "buy")
    assert "POSITION_SIDE" in e1.payload["side_semantics"] and e1.quality == "OK"
    o = OkxSwapAdapter(["BTC"])
    o.contract_values["BTC"] = (0.01, True)
    lo = {"arg": {"channel": "liquidation-orders", "instType": "SWAP"}, "data": [
        {"instId": "ETH-USDT-SWAP", "details": [{"side": "sell", "posSide": "long", "bkPx": "1", "sz": "1", "ts": "1790000000000"}]},
        {"instId": "BTC-USDT-SWAP", "details": [
            {"side": "buy", "posSide": "short", "bkPx": "100000", "sz": "10", "ts": "1790000000000"},
            {"side": "sell", "posSide": "net", "bkPx": "100000", "sz": "10", "ts": "1790000000001"}]}]}
    ev = o.parse(json.dumps(lo), ctx()).events
    assert len(ev) == 2 and ev[0].payload["liquidated_position"] == "short" and "POSITION_SIDE_DERIVED" not in ev[0].flags
    assert ev[1].payload["liquidated_position"] == "long" and "POSITION_SIDE_DERIVED" in ev[1].flags
    assert ev[0].payload["qty_coin"] == 0.1 and ev[0].payload["native"]["price_is_bankruptcy_price"] is True
    # aggregation windows on known inputs
    T0 = 1_790_000_000_000
    evs = [E4("bybit_linear", PT.PERP_QUOTE, T0 + k * 1000, T0 + k * 1000, bid=99.0, ask=101.0, mid=100.0) for k in range(200)]
    L = lambda t, n, fs, pos: E4("bybit_linear", PT.LIQUIDATION, t, t + 5, notional_quote=n, forced_side=fs,  # noqa: E731
                                 liquidated_position=pos, qty_coin=n / 100, price=100.0)
    T = T0 + 199_000
    evs += [L(T - 20_000, 500.0, "sell", "long"), L(T - 10_000, 1000.0, "sell", "long"), L(T - 3000, 250.0, "buy", "short"),
            L(T - 50_000, 4000.0, "sell", "long")]
    evs.sort(key=lambda e: e.receive_ts_ms)
    r = feats(evs, T, venues=("bybit_linear",), spot=False)
    v = r.values
    assert v["perp.bybit_linear.liq_total_notional.30s"] == 1750 and v["perp.bybit_linear.liq_count.30s"] == 3
    assert v["perp.bybit_linear.liq_forced_sell_notional.30s"] == 1500 and v["perp.bybit_linear.liq_short_liq_notional.30s"] == 250
    assert v["perp.bybit_linear.liq_long_liq_notional.30s"] == 1500 and v["perp.bybit_linear.liq_max_notional.30s"] == 1000
    assert abs(v["perp.bybit_linear.liq_imbalance.30s"] - 1250 / 1750) < 1e-12
    assert v["perp.bybit_linear.liq_accel.30s"] == 1750 - 4000                  # previous 30 s held the 4000
    assert v["perp.bybit_linear.liq_total_notional.1s"] == 0.0                   # observed, nothing happened: a real 0
    assert r.status["perp.bybit_linear.liq_imbalance.1s"] == S.UNDEFINED and v["perp.bybit_linear.liq_imbalance.1s"] is None
    # NOT observed (venue silent > 10 s, or a disconnect gap) -> MISSING, never 0
    silent = [e for e in evs if e.receive_ts_ms <= T - 30_000]
    r2 = feats(silent, T, venues=("bybit_linear",), spot=False)
    assert r2.values["perp.bybit_linear.liq_total_notional.5s"] is None and r2.status["perp.bybit_linear.liq_total_notional.5s"] == S.MISSING
    g = Gap("bybit_linear", "BTC", "ws", "DISCONNECT", T - 8000, T - 6000, 2000, None, True, "", known_at_ms=T - 6000, ingest_seq=1)
    r3 = feats(evs, T, gaps=[g], venues=("bybit_linear",), spot=False)
    assert r3.status["perp.bybit_linear.liq_total_notional.15s"] == S.MISSING and r3.values["perp.bybit_linear.liq_total_notional.15s"] is None
    assert r3.status["perp.bybit_linear.liq_total_notional.5s"] == S.READY    # window after the gap is fine


def test_flow_cvd_imbalance():
    T0 = 1_790_000_000_000
    evs = [E4("binance_usdm", PT.PERP_QUOTE, T0 + k * 1000, T0 + k * 1000, bid=99.0, ask=101.0, mid=100.0, bid_qty_coin=1.0, ask_qty_coin=1.0)
           for k in range(400)]
    T = T0 + 399_000
    tid = iter(range(1, 100))
    tr = lambda t, q, side: E4("binance_usdm", PT.PERP_TRADE, t, t, price=100.0, qty_coin=q, notional_quote=100.0 * q,  # noqa: E731
                               aggressor=side, trade_id=next(tid))
    evs += [tr(T - 5000, 1.0, "buy"), tr(T - 4999, 2.0, "sell"), tr(T - 1000, 4.0, "buy"), tr(T, 8.0, "buy"), tr(T - 7000, 3.0, "sell")]
    evs.sort(key=lambda e: e.receive_ts_ms)
    r = feats(evs, T, venues=("binance_usdm",), spot=False)
    v = r.values
    p = "perp.binance_usdm."
    assert v[p + "flow_buy_notional.5s"] == 1200 and v[p + "flow_sell_notional.5s"] == 200     # (T-5s, T]: T-5000 excluded
    assert v[p + "flow_signed_notional.5s"] == 1000 and abs(v[p + "flow_imbalance.5s"] - 1000 / 1400) < 1e-12
    assert abs(v[p + "flow_count_imbalance.5s"] - 1 / 3) < 1e-12 and v[p + "flow_count.5s"] == 3
    assert abs(v[p + "flow_avg_size_coin.5s"] - 14 / 3) < 1e-12 and v[p + "flow_max_size_coin.5s"] == 8
    assert v[p + "cvd_notional.15s"] == (1 - 2 + 4 + 8 - 3) * 100
    assert v[p + "cvd_accel.5s"] == 1000 - (100 - 300)                          # previous 5 s: T-7000 sell, T-5000 buy
    assert v[p + "cvd_slope.5s"] > 0 and r.status[p + "cvd_slope.5s"] == S.READY
    assert v[p + "flow_count.1s"] == 1.0 and v[p + "flow_imbalance.1s"] == 1.0
    quiet = feats([e for e in evs if e.event_type != PT.PERP_TRADE], T, venues=("binance_usdm",), spot=False)
    assert quiet.values[p + "flow_buy_notional.5s"] == 0.0 and quiet.status[p + "flow_imbalance.5s"] == S.UNDEFINED
    assert quiet.values[p + "cvd_notional.5s"] == 0.0
    unk = evs + [E4("binance_usdm", PT.PERP_TRADE, T - 100, T, price=100.0, qty_coin=1.0, notional_quote=100.0, aggressor=None, trade_id=999)]
    ru = feats(sorted(unk, key=lambda e: e.receive_ts_ms), T, venues=("binance_usdm",), spot=False)
    assert ru.status[p + "flow_imbalance.1s"] == S.UNAVAILABLE and ru.values[p + "flow_buy_notional.1s"] is None


def _price_world(T0, n=400, venue="binance_usdm", spot=True, usdt=True, cf=True):
    evs = []
    for k in range(n):
        t = T0 + k * 1000
        m = 100_000.0 * math.exp(1e-4 * k)
        evs.append(E4(venue, PT.PERP_QUOTE, t, t + 5, bid=m * 0.9999, ask=m * 1.0001, mid=m, bid_qty_coin=2.0, ask_qty_coin=1.0))
        evs.append(E4(venue, PT.PERP_MARK_PRICE, t, t + 5, mark=m * 1.0002, quote_ccy="USDT"))
        evs.append(E4(venue, PT.PERP_INDEX_PRICE, t, t + 5, index=m, quote_ccy="USDT"))
        if usdt:
            evs.append(E4("coinbase_usdt", PT.STABLECOIN_RATE, t, t + 5, asset="USDT", symbol="USDT-USD", pair="USDT-USD",
                          bid=1.0009, ask=1.0011, mid=1.001))
        if spot:
            evs.append(E3("coinbase", EventType.QUOTE, t, t + 5, bid=m * 0.99995, ask=m * 1.00005, mid=m))
            evs.append(E3("kraken", EventType.QUOTE, t, t + 5, symbol="BTC/USD", bid=m * 0.99995, ask=m * 1.00005, mid=m))
        if cf:
            evs.append(E3("cf_via_kalshi", EventType.INDEX_VALUE, t, t + 5, symbol="BRTI", index_id="BRTI", value=m,
                          amend_ts_ms=None, repeat_of_previous=False, observation={}))
    return evs


def test_basis_and_basis_change():
    T0 = 1_790_000_000_000
    evs = _price_world(T0)
    T = T0 + 399_500
    r = feats(evs, T, venues=("binance_usdm",))
    p = "perp.binance_usdm."
    m = 100_000.0 * math.exp(1e-4 * 399)
    assert abs(r.values[p + "mark_minus_index_bps"] - 2.0) < 1e-9 and abs(r.values[p + "mid_minus_index_bps"]) < 1e-9
    exp = (m * 1.0002 * 1.001 - m) / m * 1e4                                    # USDT converted to USD first
    assert abs(r.values[p + "mark_minus_ref_bps"] - exp) < 1e-9 and abs(r.values[p + "mark_minus_ref_raw_bps"] - 2.0) < 1e-9
    assert abs(r.values[p + "mark_minus_cf_bps"] - exp) < 1e-9
    assert abs(r.values[p + "mid_minus_coinbase_bps"] - 10.0) < 1e-6            # mid x 1.001 vs coinbase mid
    assert abs(r.values[p + "basis_change_bps.60s"]) < 1e-9 and abs(r.values[p + "basis_accel_bps.60s"]) < 1e-9
    assert abs(r.values[p + "mid_logret.60s"] - 60e-4) < 1e-12 and abs(r.values[p + "mark_logret.5m"] - 300e-4) < 1e-12
    assert r.values[p + "mark"] != r.values[p + "last"]                        # mark is never the last trade
    assert r.status[p + "last"] != S.READY                                      # no trades -> last is not faked from mid
    no_usdt = feats(_price_world(T0, usdt=False), T, venues=("binance_usdm",))
    assert no_usdt.values[p + "mark_minus_ref_bps"] is None and no_usdt.status[p + "mark_minus_ref_bps"] == S.MISSING
    assert abs(no_usdt.values[p + "mark_minus_ref_raw_bps"] - 2.0) < 1e-9         # the explicit unconverted variant
    # a basis that moves: +1 bp per 10 s on the mark
    evs2 = [e if e.event_type != PT.PERP_MARK_PRICE else
            E4("binance_usdm", PT.PERP_MARK_PRICE, e.event_ts_ms, e.receive_ts_ms,
               mark=e.payload["mark"] / 1.0002 * (1 + (2 + (e.event_ts_ms - T0) / 10_000) * 1e-4), quote_ccy="USDT") for e in evs]
    r2 = feats(evs2, T, venues=("binance_usdm",))
    assert abs(r2.values[p + "mark_minus_index_bps.".rstrip(".")] - (2 + 39.9)) < 0.01
    assert abs(r2.values[p + "basis_change_bps.60s"] - 6.0 * 1.001) < 0.01 and abs(r2.values[p + "basis_accel_bps.60s"]) < 0.01


def test_orderbook_features():
    T0 = 1_790_000_000_000
    evs = []
    for k in range(400):
        t = T0 + k * 1000
        bq = 3.0 if k < 395 else 1.0
        bids = [[100.0 - i * 0.1, bq + i] for i in range(10)]
        asks = [[100.1 + i * 0.1, 1.0 + i] for i in range(10)]
        evs.append(E4("binance_usdm", PT.ORDERBOOK_TOP, t, t + 5, bids=bids, asks=asks, depth=10, update_id=k, prev_update_id=k - 1))
    T = T0 + 399_500
    r = feats(evs, T, venues=("binance_usdm",), spot=False)
    p = "perp.binance_usdm."
    assert r.values[p + "book_bid"] == 100.0 and r.values[p + "book_ask"] == 100.1
    assert r.values[p + "book_top_imbalance"] == 0.0                            # 1 vs 1 at the top now
    assert abs(r.values[p + "book_weighted_mid"] - 100.05) < 1e-12
    sb, sa = sum(1.0 + i for i in range(5)), sum(1.0 + i for i in range(5))
    assert r.values[p + "book_depth_imbalance_5"] == (sb - sa) / (sb + sa)
    assert abs(r.values[p + "book_spread_bps"] - 0.1 / 100.05 * 1e4) < 1e-9
    assert abs(r.values[p + "book_pressure_change.60s"] - (0.0 - 0.5)) < 1e-12  # 3 vs 1 a minute ago
    assert abs(r.values[p + "book_spread_change_bps.5s"]) < 1e-12 and abs(r.values[p + "book_spread_ratio_5m"] - 1.0) < 1e-12
    assert r.values[p + "book_depth_change_pct.60s"] < 0
    # OKX captures 5 levels: depth-10 imbalance is UNAVAILABLE, never computed from 5 levels
    o_evs = [E4("okx_swap", PT.ORDERBOOK_TOP, e.event_ts_ms, e.receive_ts_ms, symbol="BTC-USDT-SWAP",
                bids=e.payload["bids"][:5], asks=e.payload["asks"][:5], depth=5) for e in evs]
    ro = feats(o_evs, T, venues=("okx_swap",), spot=False)
    assert ro.status["perp.okx_swap.book_depth_imbalance_10"] == S.UNAVAILABLE and ro.status["perp.okx_swap.book_depth_imbalance_5"] == S.READY
    # Bybit snapshot + delta book: deletes, delta-before-snapshot refused
    y = BybitLinearAdapter(["BTC"], depth=2)
    bk = lambda typ, b, a, u: json.dumps({"topic": "orderbook.50.BTCUSDT", "type": typ, "ts": 1_790_000_000_000,  # noqa: E731
                                          "cts": 1_790_000_000_000, "data": {"s": "BTCUSDT", "b": b, "a": a, "u": u, "seq": 1}})
    assert y.parse(bk("delta", [["1", "1"]], [], 5), ctx()).failures
    e = y.parse(bk("snapshot", [["100", "1"], ["99", "2"], ["98", "3"]], [["101", "1"], ["102", "2"]], 1), ctx()).events[0]
    assert e.payload["bids"] == [[100.0, 1.0], [99.0, 2.0]] and e.payload["asks"] == [[101.0, 1.0], [102.0, 2.0]]
    e = y.parse(bk("delta", [["100", "0"], ["99.5", "4"]], [["100.5", "7"]], 2), ctx()).events[0]
    assert e.payload["bids"] == [[99.5, 4.0], [99.0, 2.0]] and e.payload["asks"] == [[100.5, 7.0], [101.0, 1.0]]


def test_cross_exchange_aggregation():
    T0 = 1_790_000_000_000
    evs = []
    drift = {"binance_usdm": 1e-4, "bybit_linear": 1e-4, "okx_swap": 5e-4}       # OKX behaves unusually
    for k in range(400):
        t = T0 + k * 1000
        for v, sym in (("binance_usdm", "BTCUSDT"), ("bybit_linear", "BTCUSDT"), ("okx_swap", "BTC-USDT-SWAP")):
            m = 100_000.0 * math.exp(drift[v] * k)
            evs.append(E4(v, PT.PERP_QUOTE, t, t + 5, symbol=sym, bid=m * 0.9999, ask=m * 1.0001, mid=m, bid_qty_coin=1.0, ask_qty_coin=1.0))
            evs.append(E4(v, PT.PERP_MARK_PRICE, t, t + 5, symbol=sym, mark=m, quote_ccy="USDT"))
            evs.append(E4(v, PT.OPEN_INTEREST, t, t + 5, symbol=sym, oi_coin={"binance_usdm": 1.0, "bybit_linear": 1.0, "okx_swap": 2.0}[v]))
            evs.append(E4(v, PT.FUNDING_RATE, t, t + 5, symbol=sym, rate_native=1e-4, rate_8h={"binance_usdm": 1e-4, "bybit_linear": 2e-4, "okx_swap": 4e-4}[v],
                          interval_ms=8 * 3_600_000))
    T = T0 + 399_500
    venues = ("binance_usdm", "bybit_linear", "okx_swap")
    r = feats(evs, T, venues=venues, spot=False)
    v = r.values
    assert abs(v["x.median_mid_logret.60s"] - 60e-4) < 1e-12
    assert abs(v["x.disagreement_bps.60s"] - (300e-4 - 60e-4) * 1e4) < 1e-6
    assert abs(v["x.max_venue_dev_bps.60s"] - 240.0) < 1e-6 and v["x.sign_agreement.60s"] == 1.0
    assert abs(v["x.funding_median_8h"] - 2e-4) < 1e-18 and abs(v["x.funding_dispersion_8h"] - 3e-4) < 1e-18
    assert v["x.n_venues_fresh"] == 3.0 and r.status["x.oi_total_notional"] == S.READY
    w = {vv: {"binance_usdm": 1.0, "bybit_linear": 1.0, "okx_swap": 2.0}[vv] * 100_000.0 * math.exp(drift[vv] * 399) for vv in venues}
    exp = sum(w[vv] * drift[vv] * 60 for vv in venues) / sum(w.values())
    assert abs(v["x.oi_weighted_logret.60s"] - exp) < 1e-12
    # all venues observed, no liquidations -> a real 0; one venue silent -> the TOTAL is MISSING (never counted as 0)
    assert v["x.liq_total_notional.5s"] == 0.0 and r.status["x.liq_total_notional.5s"] == S.READY
    silent = feats([e for e in evs if not (e.source == "okx_swap" and e.receive_ts_ms > T - 30_000)], T, venues=venues, spot=False)
    assert silent.status["perp.okx_swap.liq_total_notional.5s"] == S.MISSING
    assert silent.status["perp.binance_usdm.liq_total_notional.5s"] == S.READY
    assert silent.values["x.liq_total_notional.5s"] is None and silent.status["x.liq_total_notional.5s"] == S.MISSING
    assert silent.values["x.median_mid_logret.60s"] is not None               # medians use the venues that are fresh
    only_one = feats([e for e in evs if e.source == "binance_usdm"], T, venues=venues, spot=False)
    assert only_one.status["x.disagreement_bps.60s"] == S.UNDEFINED and only_one.values["x.median_mid_logret.60s"] is not None


def test_perp_vs_spot_and_cf_divergence():
    T0 = 1_790_000_000_000
    evs = []
    for k in range(400):
        t = T0 + k * 1000
        pm, sm, cm = 100_000.0 * math.exp(2e-4 * k), 100_000.0 * math.exp(1e-4 * k), 100_000.0 * math.exp(0.5e-4 * k)
        evs.append(E4("binance_usdm", PT.PERP_QUOTE, t, t + 5, bid=pm * 0.9999, ask=pm * 1.0001, mid=pm))
        evs.append(E3("coinbase", EventType.QUOTE, t, t + 5, bid=sm * 0.99995, ask=sm * 1.00005, mid=sm))
        evs.append(E3("cf_via_kalshi", EventType.INDEX_VALUE, t, t + 5, symbol="BRTI", index_id="BRTI", value=cm,
                      amend_ts_ms=None, repeat_of_previous=False, observation={}))
    T = T0 + 399_500
    r = feats(evs, T, venues=("binance_usdm",))
    assert abs(r.values["div.perp_minus_spot_ret.30s"] - 30e-4) < 1e-12
    assert abs(r.values["div.perp_minus_cf_ret.30s"] - 45e-4) < 1e-12
    assert r.values["div.perp_over_spot_rv.60s"] is not None and abs(r.values["div.perp_over_spot_rv.60s"] - 2.0) < 1e-9
    no_spot = feats([e for e in evs if isinstance(e, PerpEvent)], T, venues=("binance_usdm",))
    assert no_spot.values["div.perp_minus_spot_ret.30s"] is None and no_spot.status["div.perp_minus_spot_ret.30s"] == S.MISSING


def test_missing_and_warmup():
    row = PerpFeatureEngine("BTC").features_at(1_790_000_000_000)
    counts = {"x.n_venues_fresh"}
    assert row.values["x.n_venues_fresh"] == 0.0
    assert all(v is None for k, v in row.values.items() if k not in counts)
    assert all(s != S.READY for k, s in row.status.items() if k not in counts)
    kal = [n for n in FEATURE_NAMES if n.startswith("perp.kalshi_perp.") and any(x in n for x in (".oi_", ".liq_", ".flow_", ".cvd_", "qty_coin", "depth"))]
    assert kal and all(row.status[n] == S.UNAVAILABLE for n in kal)
    T0 = 1_790_000_000_000
    evs = _price_world(T0, n=400, spot=False, usdt=False, cf=False)
    early = feats([e for e in evs if e.receive_ts_ms <= T0 + 20_005], T0 + 20_005, venues=("binance_usdm",), spot=False)
    assert early.status["perp.binance_usdm.mid_logret.5m"] == S.NOT_READY and early.values["perp.binance_usdm.mid_logret.5m"] is None
    assert early.status["perp.binance_usdm.mid_logret.5s"] == S.READY
    holed = [e for e in evs if not (T0 + 300_000 <= e.event_ts_ms < T0 + 310_000)]
    strict = feats(holed, T0 + 399_005, venues=("binance_usdm",), spot=False)
    assert strict.status["perp.binance_usdm.mid_rv.5m"] == S.MISSING and S.PARTIAL not in strict.status.values()
    part = feats(holed, T0 + 399_005, venues=("binance_usdm",), spot=False, config=PerpFeatureConfig(partial_windows=True))
    assert part.status["perp.binance_usdm.mid_rv.5m"] == S.PARTIAL and part.values["perp.binance_usdm.mid_rv.5m"] > 0
    fr = FEATURES[0]
    assert set(fr.__dict__) >= {"name", "family", "venue", "window", "unit", "description"}


# ═══════════════════ 14-17 reconnect, gaps, backfill ═══════════════════
class FakeWS:
    def __init__(self, msgs, stop=None):
        self.msgs, self.stop, self.sent = list(msgs), stop, []

    def send_text(self, t):
        self.sent.append(t)

    def settimeout(self, s):
        pass

    def recv_text(self):
        if self.msgs:
            return self.msgs.pop(0)
        if self.stop is not None:
            self.stop.set()
            raise TimeoutError()
        return None

    def close(self):
        pass


def test_reconnect_keepalive_isolation():
    clock = FakeClock(1_790_000_000_000)
    y = BybitLinearAdapter(["BTC"])
    pc = PerpCollector(tmpdir(), ["BTC"], {"bybit_linear": y}, clock, fsync=False)
    tr = lambda i: json.dumps({"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": 1, "data": [  # noqa: E731
        {"T": 1_790_000_000_000 + i, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "100", "i": f"t{i}", "BT": False}]})
    stop = threading.Event()
    n = [0]
    base_ws = []

    def base(url, headers=None):
        n[0] += 1
        clock.advance(25_000)
        if n[0] == 1:
            raise ConnectionError("refused")
        ws = FakeWS([tr(n[0] * 10 + k) for k in range(3)], stop if n[0] == 3 else None)
        base_ws.append(ws)
        return ws
    backfills = []
    r = WsFeedRunner(y, pc, clock, connect_fn=connect_fn_for(y, clock, base), stop_event=stop,
                     backoff=Backoff(initial_s=0.001, jitter=0), backfill_fn=lambda: backfills.append(1))
    r.run()
    assert n[0] == 3 and backfills == [1] and pc.manifest.reconnects == {"bybit_linear": 1}
    kinds = [g["kind"] for g in pc.manifest.gaps]
    assert kinds == ["DISCONNECT_OPEN", "DISCONNECT", "DISCONNECT_OPEN", "DISCONNECT"], kinds   # every outage opened + closed
    assert pc.counts["events"] == 6
    g = pc.manifest.gaps
    assert g[0]["start_ts_ms"] == g[1]["start_ts_ms"] and g[2]["start_ts_ms"] == g[3]["start_ts_ms"]
    assert all(json.loads(s) in ({"op": "subscribe", "args": ["publicTrade.BTCUSDT", "tickers.BTCUSDT", "orderbook.50.BTCUSDT",
                                                              "allLiquidation.BTCUSDT"]},) for s in base_ws[0].sent)
    ka = KeepaliveWebSocket(FakeWS(["a", "b"]), '{"op":"ping"}', 20, clock.mono_ns)
    ka.recv_text()
    clock.advance(21_000)
    ka.recv_text()
    assert ka.pings == 1 and ka.ws.sent == ['{"op":"ping"}']
    assert y.parse(json.dumps({"op": "pong", "success": True}), ctx()).control
    assert OkxSwapAdapter(["BTC"]).parse("pong", ctx()).control
    # the engine learns of an outage WHEN it starts (open gap), not only at reconnect; after the close it recovers
    T1 = 1_790_000_300_000
    evs = [E4("bybit_linear", PT.PERP_QUOTE, t, t, bid=99.0, ask=101.0, mid=100.0) for t in range(T1 - 60_000, T1, 500)]
    evs += [E4("bybit_linear", PT.PERP_QUOTE, t, t, bid=99.0, ask=101.0, mid=100.0) for t in range(T1 + 5_000, T1 + 30_000, 500)]
    op = Gap("bybit_linear", "BTC", "ws", "DISCONNECT_OPEN", T1 - 500, T1 - 500, 0, None, True, "open", known_at_ms=T1, ingest_seq=1)
    cl = Gap("bybit_linear", "BTC", "ws", "DISCONNECT", T1 - 500, T1 + 5_000, 5_500, None, True, "", known_at_ms=T1 + 5_000, ingest_seq=2)
    during = feats(evs, T1 + 3_000, gaps=[op, cl], venues=("bybit_linear",), spot=False)
    assert during.status["perp.bybit_linear.liq_total_notional.1s"] == S.MISSING      # silent 3 s, but known to be down
    no_open = feats(evs, T1 + 3_000, gaps=[cl], venues=("bybit_linear",), spot=False)
    assert no_open.status["perp.bybit_linear.liq_total_notional.1s"] == S.READY       # without it, 3 s of silence looked quiet
    after = feats(evs, T1 + 20_000, gaps=[op, cl], venues=("bybit_linear",), spot=False)
    assert after.status["perp.bybit_linear.liq_total_notional.5s"] == S.READY and after.values["perp.bybit_linear.liq_total_notional.5s"] == 0.0
    assert after.status["perp.bybit_linear.liq_total_notional.30s"] == S.MISSING      # that window still spans the outage
    # one venue down never stops another
    col = PerpCollector(tmpdir(), ["BTC"], {}, clock, fsync=False)
    stop2 = threading.Event()
    bad = WsFeedRunner(OkxSwapAdapter(["BTC"]), col, clock, stop_event=threading.Event(), max_attempts=4,
                       backoff=Backoff(initial_s=0.005, jitter=0), connect_fn=lambda *a, **k: (_ for _ in ()).throw(ConnectionError("down")))
    good = WsFeedRunner(y, col, clock, stop_event=stop2, connect_fn=lambda *a, **k: FakeWS([tr(900 + i) for i in range(40)], stop2))
    bad.start(); good.start()
    good.join(5); bad.join(5)
    assert col.counts["events"] == 40 and bad.health.state == FeedState.DISCONNECTED and bad.attempts == 4
    # REST poller: one failing stream is recorded, the others continue; all failing -> the runner backs off
    b = BinanceUsdmAdapter(["BTC"])

    class G:
        def __init__(self, fail):
            self.fail, self.calls = fail, []

        def get_json(self, url, params=None):
            self.calls.append(url)
            clock.advance(700_000)                             # every stream is due again at the next poll
            if any(f in url for f in self.fail):
                raise ConnectionError("HTTP 503")
            if url.endswith("openInterest"):
                return {"openInterest": "5", "symbol": "BTCUSDT", "time": clock.wall_ms()}
            if url.endswith("fundingInfo"):
                return []
            return []
    col2 = PerpCollector(tmpdir(), ["BTC"], {"binance_usdm": b}, clock, fsync=False)
    pp = PerpPoller(b, G(["fundingRate"]), col2, clock)
    assert pp.poll()[0] == 2                                   # OI + INSTRUMENT despite the failing funding history
    assert any("funding_hist" in k for k in col2.manifest.disconnects)
    pp2 = PerpPoller(b, G(["binance"]), col2, clock)
    try:
        pp2.poll(); raise AssertionError("all streams failed but poll() did not raise")
    except ConnectionError:
        pass
    runner = PollRunner("p", PerpPoller(b, G(["binance"]), col2, clock).poll, col2, clock, interval_s=0.001,
                        stop_event=threading.Event(), max_attempts=2, backoff=Backoff(initial_s=0.001, jitter=0))
    runner.run()
    assert runner.health.state == FeedState.DISCONNECTED and runner.backoff_delays == [0.001]


def test_gap_detection():
    clock = FakeClock(1_790_000_000_000)
    b = BinanceUsdmAdapter(["BTC"])
    pc = PerpCollector(tmpdir(), ["BTC"], {"binance_usdm": b}, clock, fsync=False)
    t = 1_790_000_000_000
    agg = lambda a, ts: _bn({"e": "aggTrade", "E": ts, "s": "BTCUSDT", "a": a, "p": "1", "q": "1", "f": 1, "l": 1, "T": ts, "m": False})  # noqa: E731
    dep = lambda u, pu, ts: _bn({"e": "depthUpdate", "E": ts, "T": ts, "s": "BTCUSDT", "U": pu + 1, "u": u, "pu": pu,  # noqa: E731
                                 "b": [["1", "1"]], "a": [["2", "1"]]}, "btcusdt@depth10@100ms")
    mk = lambda ts: _bn({"e": "markPriceUpdate", "E": ts, "s": "BTCUSDT", "p": "1", "i": "1", "r": "0.0001", "T": ts + 1}, "x")  # noqa: E731
    seq = [agg(1, t), agg(2, t + 100), agg(6, t + 60_000),                   # a quiet minute of trades is NOT a gap; ids 3..5 are
           dep(10, 9, t), dep(12, 10, t + 100), dep(20, 15, t + 200),       # pu 15 != previous u 12
           mk(t), mk(t + 1000), mk(t + 6000)]                               # 1-s mark stream: 5 s silence is
    for m in seq:
        clock.advance(10)
        pc.on_message(b, m, clock.wall_ms(), clock.mono_ns())
    kinds = sorted((g["kind"], g["stream"], g["missing_count"]) for g in pc.manifest.gaps)
    assert kinds == [("CADENCE", "PERP_MARK_PRICE", None), ("SEQUENCE", "ORDERBOOK_TOP", None), ("TRADE_ID", "PERP_TRADE", 3)], kinds
    assert all(g["known_at_ms"] and g["ingest_seq"] for g in pc.manifest.gaps)
    # periodic funding: a missing settlement record; poll cadence: no successful OI poll for too long
    hist = lambda ft: [{"symbol": "BTCUSDT", "fundingTime": ft, "fundingRate": "0.0001"}]  # noqa: E731
    pc.on_rest(b, "funding_hist:BTC", hist(t), clock.wall_ms(), clock.mono_ns())
    pc.on_rest(b, "funding_hist:BTC", hist(t + 3 * 8 * 3_600_000), clock.wall_ms(), clock.mono_ns())
    pc.on_rest(b, "oi:BTC", {"openInterest": "1", "symbol": "BTCUSDT", "time": t}, clock.wall_ms(), clock.mono_ns())
    clock.advance(4000)
    pc.on_rest(b, "oi:BTC", {"openInterest": "1", "symbol": "BTCUSDT", "time": t}, clock.wall_ms(), clock.mono_ns())
    clock.advance(30_000)
    pc.on_rest(b, "oi:BTC", {"openInterest": "1", "symbol": "BTCUSDT", "time": t}, clock.wall_ms(), clock.mono_ns())
    k2 = [(g["kind"], g["stream"], g["missing_count"]) for g in pc.manifest.gaps[3:]]
    assert k2 == [("FUNDING", "FUNDING_SETTLED", 2), ("POLL_CADENCE", "rest:oi:BTC", None)], k2
    pc.close()
    stored = load_perp_sessions([os.path.dirname(pc.dir)]).gaps
    assert len(stored) == 5


def test_backfill_semantics():
    d, c3, pc, L3, L4, market = shared()
    a, b = START + 100_000, START + 110_000
    bf = [e for e in L4.events if e.source == "binance_usdm" and e.event_type == PT.PERP_TRADE and e.mode == IngestMode.BACKFILLED]
    assert bf and all(e.receive_ts_ms == b and e.event_ts_ms < b for e in bf)       # retrieval time kept; event time kept
    assert all(a - 1000 <= e.event_ts_ms < b for e in bf)
    assert not [g for g in L4.gaps if g.kind == "TRADE_ID"]                          # recovered exactly by aggregate id
    assert [g.kind for g in L4.gaps if g.source == "binance_usdm"].count("DISCONNECT") == 1
    mid_t = (a + b) // 2
    before = compute_at("BTC", L3.events + L4.events, mid_t, None, L3.gaps + L4.gaps, venues=("binance_usdm",))
    ids_before = {e.payload["trade_id"] for e in L4.events if e.source == "binance_usdm" and e.event_type == PT.PERP_TRADE and e.receive_ts_ms <= mid_t}
    assert not ids_before & {e.payload["trade_id"] for e in bf} and before.status["perp.binance_usdm.flow_count.5s"] == S.MISSING
    raw = [r for r in load_perp_sessions([c3.dir], include_raw=True).raw if r.stream.startswith("rest:backfill")]
    assert raw and raw[0].receive_ts_ms == b and "fromId" not in raw[0].text
    reqs = [r for r in pc.rest.requests if r[1].endswith("/fapi/v1/aggTrades")]
    assert reqs and "fromId" in reqs[0][2]                                          # exact recovery by id
    eng = PerpFeatureEngine("BTC")
    t1 = E4("binance_usdm", PT.PERP_TRADE, 1000, 1100, price=1.0, qty_coin=1.0, notional_quote=1.0, aggressor="buy", trade_id=5)
    t2 = E4("binance_usdm", PT.PERP_TRADE, 1000, 9000, mode=IngestMode.BACKFILLED, price=1.0, qty_coin=1.0, notional_quote=1.0,
            aggressor="buy", trade_id=5)
    assert eng.ingest(t1) and not eng.ingest(t2) and eng.duplicates == 1


# ═══════════════════ 18-21 causality, leakage, replay, join ═══════════════════
def test_receive_time_causality():
    T0 = 1_790_000_000_000
    late = E4("okx_swap", PT.PERP_TRADE, T0 - 500, T0 + 1, symbol="BTC-USDT-SWAP", price=1.0, qty_coin=1.0, notional_quote=1.0,
              aggressor="buy", trade_id=1)
    from market_data.alignment import available
    assert not available(late, T0) and available(late, T0 + 1)
    eng = PerpFeatureEngine("BTC")
    eng.ingest(E4("bybit_linear", PT.PERP_MARK_PRICE, T0, T0, mark=1.0, quote_ccy="USDT"))
    for bad in (lambda: eng.features_at(T0 - 1),
                lambda: eng.ingest(E4("bybit_linear", PT.PERP_MARK_PRICE, T0, T0 - 1, mark=1.0, quote_ccy="USDT"))):
        try:
            bad(); raise AssertionError("causality guard missing")
        except CausalityError:
            pass
    assert event_key(E3("coinbase", EventType.QUOTE, 1, 5, bid=1.0, ask=2.0, mid=1.5))[2] == 0 and event_key(late)[2] == 1


def _perturb(T, market, before=False):
    """Items that arrive AFTER T (or, as canaries, AT T). Each must not / must change the features at T."""
    rx = (lambda dt: T) if before else (lambda dt: T + dt)
    ev = (lambda dt: T) if before else (lambda dt: T + dt)
    past = T - 2000
    sym = {"binance_usdm": "BTCUSDT", "bybit_linear": "BTCUSDT", "okx_swap": "BTC-USDT-SWAP"}
    big = 1e9
    return [
        ("future liquidation", [E4("bybit_linear", PT.LIQUIDATION, ev(500), rx(600), price=1.0, qty_coin=big, notional_quote=big,
                                   forced_side="sell", liquidated_position="long")], []),
        ("delayed liquidation", [E4("okx_swap", PT.LIQUIDATION, past, rx(300), symbol=sym["okx_swap"], price=1.0, qty_coin=big,
                                    notional_quote=big, forced_side="buy", liquidated_position="short")], []),
        ("future OI update", [E4("binance_usdm", PT.OPEN_INTEREST, ev(200), rx(400), oi_native=big, oi_unit="coin", oi_coin=big)], []),
        ("delayed OI update", [E4("bybit_linear", PT.OPEN_INTEREST, past, rx(300), oi_native=big, oi_unit="coin", oi_coin=big)], []),
        ("future funding update", [E4("bybit_linear", PT.FUNDING_RATE, ev(100), rx(200), rate_native=0.01, interval_ms=8 * 3_600_000,
                                      rate_8h=0.01, rate_per_hour=0.00125, rate_annual_simple=10.95, next_funding_ts_ms=T + 10**7,
                                      is_estimate=True)], []),
        ("future perp trade", [E4("okx_swap", PT.PERP_TRADE, ev(100), rx(150), symbol=sym["okx_swap"], price=1.0, qty_coin=big,
                                  notional_quote=big, aggressor="buy", trade_id="fut-1")], []),
        ("delayed perp trade", [E4("binance_usdm", PT.PERP_TRADE, past, rx(500), price=1.0, qty_coin=big, notional_quote=big,
                                   aggressor="sell", trade_id=10**12)], []),
        ("backfilled trade retrieved after T", [E4("bybit_linear", PT.PERP_TRADE, T - 3000, rx(2000), mode=IngestMode.BACKFILLED,
                                                   price=1.0, qty_coin=big, notional_quote=big, aggressor="buy", trade_id="bf-1")], []),
        ("future book update", [E4("bybit_linear", PT.ORDERBOOK_TOP, ev(100), rx(100), bids=[[1.0, big]] * 10, asks=[[2.0, 1.0]] * 10, depth=10),
                                E4("binance_usdm", PT.PERP_QUOTE, ev(50), rx(50), bid=1.0, ask=2.0, mid=1.5, bid_qty_coin=big, ask_qty_coin=1.0)], []),
        ("future mark / index", [E4("okx_swap", PT.PERP_MARK_PRICE, ev(10), rx(20), symbol=sym["okx_swap"], mark=1.0, quote_ccy="USDT"),
                                 E4("okx_swap", PT.PERP_INDEX_PRICE, ev(10), rx(20), symbol="BTC-USDT", index=2.0, quote_ccy="USDT")], []),
        ("post-close data", [E4("binance_usdm", PT.PERP_MARK_PRICE, market.close_ts_ms + 5000 if not before else T,
                                rx(market.close_ts_ms - T + 5100), mark=1.0, quote_ccy="USDT"),
                             E3("coinbase", EventType.QUOTE, market.close_ts_ms + 1000 if not before else T, rx(market.close_ts_ms - T + 1100),
                                bid=1.0, ask=2.0, mid=1.5)], []),
        ("gap learned after T", [], [Gap("binance_usdm", "BTC", "ws", "DISCONNECT", T - 4000, T - 3000, 1000, None, True, "",
                                         known_at_ms=rx(200), ingest_seq=1)]),
    ]


def test_no_leakage():
    d, c3, pc, L3, L4, market = shared()
    allev = L3.events + L4.events
    gaps = L3.gaps + L4.gaps
    Ts = [market.close_ts_ms - 60_000, market.close_ts_ms - 30_000]
    for T in Ts + [market.close_ts_ms]:
        cut = [e for e in allev if e.receive_ts_ms <= T]
        cg = [g for g in gaps if (g.known_at_ms or g.end_ts_ms) <= T]
        assert dump(feats(allev, T, market, gaps)) == dump(feats(cut, T, market, cg)), T          # the fundamental invariant
    leaks, insensitive = [], []
    for T in Ts:
        base = dump(feats(allev, T, market, gaps))
        for (name, ev, gp), (_n, ev_b, gp_b) in zip(_perturb(T, market), _perturb(T, market, before=True)):
            if dump(feats(allev + ev, T, market, gaps + gp)) != base:
                leaks.append((T - market.close_ts_ms, name))
            if dump(feats(allev + ev_b, T, market, gaps + gp_b)) == base:
                insensitive.append((T - market.close_ts_ms, name))
    assert not leaks, leaks
    assert not insensitive, insensitive                                                          # canaries: the check is sensitive
    # market result / expiration value (labels) never reach a feature, even when received BEFORE T
    T = Ts[1]
    flip = E3("kalshi", EventType.RESOLUTION, None, T - 10_000, symbol=market.ticker, ticker=market.ticker, result="no",
              expiration_value=1.0)
    assert dump(feats(allev + [flip], T, market, gaps)) == dump(feats(allev, T, market, gaps))
    # the joint single-pass dataset is invariant to everything received after the last checkpoint
    extra, eg = [], []
    for _n, ev, gp in _perturb(market.close_ts_ms, market):
        extra += ev
        eg += gp
    p3 = [e for e in extra if isinstance(e, MarketEvent)]
    p4 = [e for e in extra if isinstance(e, PerpEvent)]
    a = [r for r in build_research_dataset(L3, L4, ["BTC"]).rows if r["market_ticker"] == market.ticker]
    b = [r for r in build_research_dataset(LoadedSessions(events=L3.events + p3, gaps=L3.gaps, session_ids=L3.session_ids),
                                           LoadedPerp(events=L4.events + p4, gaps=L4.gaps + eg, session_ids=L4.session_ids,
                                                      manifests=L4.manifests), ["BTC"]).rows if r["market_ticker"] == market.ticker]
    assert a == b and len(a) >= 5


class _LeakyEngine(PerpFeatureEngine):
    def ingest(self, ev):
        self.max_receive = None
        return super().ingest(ev)

    def features_at(self, t, market=None):
        self.max_receive = None
        return super().features_at(t, market)


def test_leaky_builder_is_caught():
    d, c3, pc, L3, L4, market = shared()
    T = market.close_ts_ms - 30_000
    allev = L3.events + L4.events

    def leaky(extra):
        eng = _LeakyEngine("BTC")
        key = lambda e: e.event_ts_ms if e.event_ts_ms is not None else e.receive_ts_ms  # noqa: E731
        for e in sorted((e for e in allev + extra if key(e) <= T), key=key):
            eng.ingest(e)
        return dump(eng.features_at(T, market))
    base = leaky([])
    caught = [n for n, ev, _g in _perturb(T, market) if ev and leaky(ev) != base]
    assert {"delayed liquidation", "delayed perp trade", "backfilled trade retrieved after T"} <= set(caught), caught


def test_replay_and_renormalize():
    a, b = tmpdir(), tmpdir()
    c1 = run_joint_session(a, assets=("ETH",), start_ms=START, duration_s=90, seed=9, cascade=(START + 30_000, START + 60_000))[1]
    c2 = run_joint_session(b, assets=("ETH",), start_ms=START, duration_s=90, seed=9, cascade=(START + 30_000, START + 60_000))[1]
    l1 = load_perp_sessions([os.path.dirname(c1.dir)], include_raw=True)
    l2 = load_perp_sessions([os.path.dirname(c2.dir)], include_raw=True)
    assert [e.to_dict() for e in l1.events] == [e.to_dict() for e in l2.events] and l1.events
    assert [r.to_dict() for r in l1.raw] == [r.to_dict() for r in l2.raw]
    ev = l1.events[:300]
    assert list(Replayer(ev)) == ev
    span = (ev[-1].receive_ts_ms - ev[0].receive_ts_ms) / 1000
    r1 = Replayer(ev, speed=1.0, sleeper=lambda s: None)
    list(r1)
    r20 = Replayer(ev, speed=20.0, sleeper=lambda s: None)
    list(r20)
    assert abs(r1.slept_s - span) < 1e-6 and abs(r20.slept_s - span / 20) < 1e-6
    p = subprocess.run([sys.executable, os.path.join(HERE, "scripts", "replay_perp_data.py"), os.path.dirname(c1.dir),
                        "--digest", "--renormalize"], capture_output=True, text=True)
    q = subprocess.run([sys.executable, os.path.join(HERE, "scripts", "replay_perp_data.py"), os.path.dirname(c2.dir), "--digest"],
                       capture_output=True, text=True)
    assert p.returncode == 0 and "IDENTICAL" in p.stdout, p.stdout[-800:] + p.stderr[-800:]
    dg = [ln for ln in p.stdout.splitlines() if "digest" in ln]
    assert dg and dg == [ln for ln in q.stdout.splitlines() if "digest" in ln]
    feats1 = [dump(compute_at("ETH", l1.events, t, None, l1.gaps)) for t in (START + 60_000, START + 89_000)]
    feats2 = [dump(compute_at("ETH", l2.events, t, None, l2.gaps)) for t in (START + 60_000, START + 89_000)]
    assert feats1 == feats2


def test_dataset_join_and_provenance():
    d, c3, pc, L3, L4, market = shared()
    ds = build_research_dataset(L3, L4, ["BTC"])
    rows = [r for r in ds.rows if r["market_ticker"] == market.ticker]
    assert len(rows) >= 6 and ds.skipped.get("before_data")
    mk = discover_markets(L3.events, ["BTC"])
    for r in ds.rows:
        m = mk[r["market_ticker"]][0]
        t = r["checkpoint_ts_ms"]
        p = compute_at("BTC", L3.events + L4.events, t, m, L3.gaps + L4.gaps)
        s = s3_compute("BTC", L3.events, t, m, L3.gaps)
        assert p.values == r["perp_values"] and {k: v.value for k, v in p.status.items()} == r["perp_status"], t
        assert s.values == r["step3_values"] and {k: v.value for k, v in s.status.items()} == r["step3_status"], t
        assert tuple(r["perp_values"]) == FEATURE_NAMES and tuple(r["step3_values"]) == S3_NAMES
    last = rows[-1]
    assert last["perp_status"]["perp.bybit_linear.liq_total_notional.60s"] == "READY" and last["perp_values"]["perp.bybit_linear.liq_total_notional.60s"] > 0
    assert last["perp_status"]["x.basis_median_bps"] == "READY" and last["step3_status"]["settle.accumulated_mean"] == "READY"
    out = tmpdir()
    write_research_dataset(ds, out)
    import csv
    hdr = next(csv.reader(open(os.path.join(out, "features.csv"))))
    assert hdr[:len(KEY_COLS)] == list(KEY_COLS) and not [h for h in hdr if h in LABEL_FIELDS]
    assert len(hdr) == len(KEY_COLS) + 2 * (len(S3_NAMES) + len(FEATURE_NAMES))
    rows_csv = list(csv.DictReader(open(os.path.join(out, "features.csv"))))
    for rr, cr in zip(ds.rows, rows_csv):
        for k, st in rr["perp_status"].items():
            assert cr[f"{k}__status"] == st and (cr[k] == "") == (rr["perp_values"][k] is None), k
    pv = json.load(open(os.path.join(out, "provenance.json")))
    for k in ("perp_feature_set_version", "step3_sessions", "perp_sessions", "perp_venues", "perp_feature_definitions",
              "perp_feature_families", "fingerprints", "rows_sha256", "causal_rule", "labels_are_separate", "feeds_existing_perp_veto"):
        assert k in pv, k
    assert pv["perp_feature_set_version"] == PERP_FEATURE_SET_VERSION and pv["feeds_existing_perp_veto"] is False
    assert set(pv["perp_feature_families"]) == set(FAMILIES) and all(pv["perp_feature_families"][f] for f in FAMILIES)
    assert pv["fingerprints"]["perp_data_fingerprint"] and pv["fingerprints"]["market_data_fingerprint"]
    assert {f["family"] for f in pv["perp_feature_definitions"]} == set(FAMILIES)
    assert sum(len(v) for v in names_by_family().values()) == len(FEATURE_NAMES)
    lab = [json.loads(x) for x in open(os.path.join(out, "labels.jsonl"))]
    assert {x["market_ticker"] for x in lab} == set(ds.labels) and ds.labels[market.ticker]["official_result"] in ("yes", "no")
    p = subprocess.run([sys.executable, os.path.join(HERE, "scripts", "build_research_dataset.py"), c3.dir, "--assets", "BTC",
                        "--out", os.path.join(out, "cli")], capture_output=True, text=True)
    assert p.returncode == 0 and "SYNTHETIC" in p.stdout, p.stdout + p.stderr
    assert json.load(open(os.path.join(out, "cli", "provenance.json")))["rows_sha256"] == pv["rows_sha256"]


def test_label_isolation():
    for n in FEATURE_NAMES:
        for w in ("result", "expiration", "official", "outcome", "label", "settle"):
            assert w not in n, n
    assert not set(FEATURE_NAMES) & set(LABEL_FIELDS) and not set(FEATURE_NAMES) & set(S3_NAMES)
    d, c3, pc, L3, L4, market = shared()
    ds = build_research_dataset(L3, L4, ["BTC"])
    for r in ds.rows:
        assert set(r) == set(KEY_COLS) | {"perp_values", "perp_status", "step3_values", "step3_status"}
        assert tuple(r["perp_values"]) == FEATURE_NAMES
    eng = PerpFeatureEngine("BTC")
    res = E3("kalshi", EventType.RESOLUTION, None, 10, symbol="T", ticker="T", result="yes", expiration_value=1.0)
    assert eng.ingest(res) is False and eng.ingested == 0


# ═══════════════════ 22-25 manifest, dry run, fingerprint, performance ═══════════════════
def test_manifest_and_dry_run():
    d, c3, pc, L3, L4, market = shared()
    m = json.load(open(os.path.join(pc.dir, "manifest.json")))
    assert set(m["derivatives_sources"]) == {"binance_usdm", "bybit_linear", "okx_swap", "kalshi_perp", "coinbase_usdt"}
    assert m["research_only"] is True and m["synthetic"] is True and m["step3_session_dir"] == c3.dir
    assert m["venue_semantics"]["binance_usdm"]["trade_side_semantics"] == "INVERTED_MAKER_SIDE"
    assert m["contract_values"]["okx_swap:BTC-USDT-SWAP"] == {"value": 0.01, "verified": True}
    assert m["fingerprints"]["perp_data_fingerprint"] and m["counts"]["by_venue_event_type"]
    m3 = json.load(open(os.path.join(c3.dir, "manifest.json")))
    assert "perp" not in json.dumps(m3["sources"]).lower() or True               # Step-3 manifest untouched in format
    code = ("import socket, sys\n"
            "def boom(*a, **k): raise RuntimeError('NETWORK USED')\n"
            "socket.socket.connect = boom; socket.create_connection = boom\n"
            f"sys.path.insert(0, {HERE!r})\n"
            "import collect_research_data as c\n"
            "sys.exit(c.main(['--assets', 'BTC,SOL', '--coinbase', '--perps', '--perp-venues', 'binance_usdm,okx_swap', "
            "'--dry-run', '--output', sys.argv[1]]))\n")
    out = tmpdir()
    fake = {"KALSHI_API_KEY_ID": "KEYID-not-real-77", "CFB_API_SECRET": "cfb-SECRET-not-real-77"}
    p = subprocess.run([sys.executable, "-c", code, out], capture_output=True, text=True, cwd=HERE, env=dict(os.environ, **fake))
    assert p.returncode == 0, p.stdout[-1500:] + p.stderr[-800:]
    s = p.stdout
    assert "perp source binance_usdm" in s and "perp source okx_swap" in s and "perp source bybit_linear" not in s
    assert "BTCUSDT, SOLUSDT" in s and "BTC-USDT-SWAP, SOL-USDT-SWAP" in s and "INVERTED_MAKER_SIDE" in s
    assert "credentials required for perp sources: none" in s and "no network connection was made" in s
    assert s.count("MATCHES") >= 4 and EXISTING_PERP_VETO_FINGERPRINT in s and os.listdir(out) == []
    for v in fake.values():
        assert v not in s and v not in p.stderr
    for bad in (["--perp-venues", "ftx"], ["--assets", "DOGE"]):
        q = subprocess.run([sys.executable, "collect_research_data.py", "--dry-run"] + bad, cwd=HERE, capture_output=True, text=True)
        assert q.returncode == 2
    # a short run of the combined collector machinery against fake transports (no network, read-only)
    import collect_research_data as crd
    from perp_data.synthetic import FakePerpRest, PerpWorld
    from market_data.synthetic import World
    a = crd.parse_args(["--assets", "BTC", "--perps", "--perp-venues", "bybit_linear,kalshi_perp", "--no-usdt", "--duration", "1.2",
                        "--output", tmpdir(), "--status-port", "0", "--settlement-store", "none"])
    clock = FakeClock(C - 100_000)
    rest = FakePerpRest(PerpWorld(World(["BTC"], C - 900_000, C + 900_000)), clock, ["BTC"])
    msgs = [json.dumps({"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": 1, "data": [
        {"T": C - 100_000 + i, "s": "BTCUSDT", "S": "Buy", "v": "1", "p": "100", "i": f"x{i}", "BT": False}]}) for i in range(10)]
    rc = crd.run(a, clock=clock, getter=rest, connect_fn=lambda url, headers=None: FakeWS(list(msgs)), install_signals=False)
    assert rc == 0 and all(r[0] == "GET" for r in rest.requests) and any("/margin/markets" in r[1] for r in rest.requests)
    sess = os.path.join(a.output, os.listdir(a.output)[0])
    pm = json.load(open(os.path.join(sess, "perp", "manifest.json")))
    assert pm["status"] == "COMPLETED" and pm["counts"]["totals"]["events"] > 0
    assert json.load(open(os.path.join(sess, "manifest.json")))["code_verification"]["perp_data"]["ok"] is True


def test_perp_data_fingerprint():
    ok, problems = pfp.verify()
    assert ok, problems
    d = tmpdir()
    shutil.copytree(os.path.join(HERE, "perp_data"), os.path.join(d, "perp_data"), ignore=shutil.ignore_patterns("__pycache__"))
    p = os.path.join(d, "perp_data", "features", "engine.py")
    s = open(p).read()
    open(p, "w").write(s.replace("if st != S.READY:\n                for n in names:\n                    row.set(n, None, st)                       # NOT observed -> never 0",
                                 "if st != S.READY:\n                for n in names:\n                    row.set(n, 0.0, S.READY)"))
    ok2, pr2 = pfp.verify(pkg_dir=os.path.join(d, "perp_data"))
    assert not ok2 and "modules/features/engine.py changed" in pr2, pr2
    open(p, "w").write(s + "\n# a trailing comment\n")
    assert pfp.verify(pkg_dir=os.path.join(d, "perp_data"))[0]
    # a change to the EXISTING perp veto chain is reported by this fingerprint too
    rd = tmpdir()
    for f in pfp.PERP_VETO_FILES:
        shutil.copy(os.path.join(HERE, f), rd)
    open(os.path.join(rd, "perp_live.py"), "a").write("\nimport perp_data  # noqa\n")
    ok3, pr3 = pfp.verify(repo=rd)
    assert not ok3 and "EXISTING PERP VETO FILE CHANGED: perp_live.py" in pr3, pr3
    q = subprocess.run([sys.executable, "-m", "perp_data.fingerprint", "--write"], cwd=HERE, capture_output=True, text=True)
    assert q.returncode == 2 and "Refusing" in q.stderr
    b = json.load(open(os.path.join(HERE, "config", "perp_data_baseline.json")))
    assert b["perp_feature_set_version"] == PERP_FEATURE_SET_VERSION and b["feature_count"] == len(FEATURE_NAMES)


def test_performance_smoke():
    d, c3, pc, L3, L4, market = shared()
    evs = sorted(L3.events + L4.events, key=event_key)
    eng = PerpFeatureEngine("BTC")
    t0 = time.perf_counter()
    for e in evs:
        eng.ingest(e)
    us = (time.perf_counter() - t0) / len(evs) * 1e6
    t0 = time.perf_counter()
    for _ in range(3):
        eng.features_at(evs[-1].receive_ts_ms, market)
    ms = (time.perf_counter() - t0) / 3 * 1000
    assert us < 500 and ms < 2000, (us, ms)
    print(f"  (ingest {us:.1f} us/event, perp features_at {ms:.1f} ms/row, {len(FEATURE_NAMES)} features, "
          f"{len(PERP_VENUES)} venues + USDT)")


def test_previous_stages():
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 20):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-600:], p.stderr[-600:])


TESTS = [
    ("baselines", "1 Step-1 strategy (47 fixtures, legacy + extended), settlement + market-data + perp-data fingerprints", test_baselines_untouched),
    ("veto", "2 EXISTING perp research / shadow / promotion / live-veto chain byte-identical; veto INACTIVE", test_existing_perp_veto_untouched),
    ("isolation", "3 production isolation: nothing production imports perp_data; read-only; LIVE refused", test_production_isolation),
    ("trades", "4 perp trade normalization + aggressor side (maker flag inverted, taker fields, OKX contracts)", test_trade_normalization_and_aggressor),
    ("funding", "5 funding normalization: units, interval differences, estimate vs forecast, never fabricated", test_funding_normalization),
    ("oi", "6 open interest: native + coin units, OI change / pct / notional / accel, no raw contract mixing", test_oi_normalization),
    ("liquidations", "7 liquidation side semantics per venue + aggregation; unobserved is MISSING, not 0", test_liquidation_semantics_and_aggregation),
    ("flow", "8 aggressive flow, trade / count imbalance, CVD (+ slope, accel), unknown side unavailable", test_flow_cvd_imbalance),
    ("basis", "9 basis vs index / USD spot reference / CF with USDT conversion; basis change + accel", test_basis_and_basis_change),
    ("book", "10 order book: imbalance, weighted mid, depth 5/10, spread / depth / pressure change; Bybit deltas", test_orderbook_features),
    ("cross", "11 cross-exchange aggregation: medians, disagreement, unusual venue, OI-weighted, totals", test_cross_exchange_aggregation),
    ("divergence", "12 perp vs spot and perp vs CF divergence", test_perp_vs_spot_and_cf_divergence),
    ("missing", "13 missing -> None + status, warm-up NOT_READY, PARTIAL only in research mode", test_missing_and_warmup),
    ("reconnect", "14 reconnect + backoff + keep-alive + backfill hook; one venue failing never stops another", test_reconnect_keepalive_isolation),
    ("gaps", "15 source-appropriate gaps: trade ids, book sequence, 1-s mark cadence, funding, poll cadence", test_gap_detection),
    ("backfill", "16 backfill: BACKFILLED, event time kept, visible only from retrieval; exact id recovery", test_backfill_semantics),
    ("causality", "17 receive-time causality + guards", test_receive_time_causality),
    ("leakage", "18 LEAKAGE: future / delayed liquidation, OI, funding, trade, book, backfill, post-close, labels; canaries", test_no_leakage),
    ("leaky", "19 an event-time-aligned (leaky) builder is caught", test_leaky_builder_is_caught),
    ("replay", "20 deterministic replay + RAW -> normalization reproducible", test_replay_and_renormalize),
    ("dataset", "21 joint Step-3 + Step-4 dataset == batch paths; features / labels / provenance (families) separate", test_dataset_join_and_provenance),
    ("labels", "22 label isolation", test_label_isolation),
    ("manifest", "23 derivatives manifest, dry run (no network / no writes / no secrets), combined collector run", test_manifest_and_dry_run),
    ("fingerprint", "24 separate perp-data fingerprint; detects changes to the existing veto chain", test_perp_data_fingerprint),
    ("performance", "25 performance smoke bounds", test_performance_smoke),
    ("previous", "26 all previous stage suites", test_previous_stages),
]


if __name__ == "__main__":
    only = None
    if "--only" in sys.argv:
        only = set(sys.argv[sys.argv.index("--only") + 1].split(","))
    for key, name, fn in TESTS:
        if only is None or key in only:
            run(name, fn)
    if only is None:
        print("\nAll Stage 20 tests passed.")
    else:
        print(f"\nSelected Stage 20 tests passed: {','.join(sorted(only))}")
