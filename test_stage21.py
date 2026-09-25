#!/usr/bin/env python3
"""Stage 21 — STEP 5 MARKET MICROSTRUCTURE (research only).

Run:  py test_stage21.py                 (or py run_all_tests.py for stages 1-21)
      py test_stage21.py --only causality,sequence   (a subset; used by scripts/mutation_test_microstructure.py)

Offline and deterministic: payloads shaped like the documented Coinbase Advanced Trade level2, Kraken v2 book,
Binance diff depth, Bybit orderbook, OKX books and Kalshi orderbook_delta / trade messages; fake clocks and
transports. Covers the deterministic local book (snapshot, insert / modify / delete, sequence ids, crossed and stale
books, reset / resnapshot, WARMING_UP), each venue's sequence policy, Kalshi YES / NO conventions, price-level vs
queue, the book features (depth, imbalance, microprice, OFI / MLOFI, depth deltas, cancel ESTIMATES, sweeps,
REPLENISHMENT_PATTERN, vacuum, VPIN-style, adverse move, trade intensity), missing / warm-up statuses, receive-time
causality, no retroactive repair, leakage, post-event labels, the offline lead-lag tool, sub-second grids, the joint
Step 2-5 dataset, the collector (raw first, resnapshot, storage controls, dry run), replay / renormalization, the
separate fingerprint and that the production strategy, the existing perp veto and the earlier layers never import
microstructure; LIVE execution stays refused.
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
import time
from itertools import count

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from market_data.clock import FakeClock                                                          # noqa: E402
from market_data.features.definitions import FEATURE_NAMES as S3_NAMES, Status                   # noqa: E402
from market_data.features.engine import CausalityError                                           # noqa: E402
from market_data.normalization import NormalizationError                                         # noqa: E402
from market_data.replay import load_sessions                                                     # noqa: E402
from market_data.storage import read_session                                                     # noqa: E402
from market_data.types import AggressorSemantics, EventType, MarketEvent                         # noqa: E402
from microstructure import MICRO_FEATURE_SET_VERSION, fingerprint as mfp                         # noqa: E402
from microstructure import kalshi as K                                                           # noqa: E402
from microstructure.book import LocalBook, NegativeLevel, kraken_checksum                        # noqa: E402
from microstructure.collector import MicroCollector, ResnapshotRequired                          # noqa: E402
from microstructure.dataset import KEY_COLS, build_joint_dataset, write_joint_dataset            # noqa: E402
from microstructure.features.definitions import (FAMILIES, FEATURE_NAMES, FEATURES,              # noqa: E402
                                                 MicroFeatureConfig, counts_by_family, names_by_family)
from microstructure.features.engine import MicroFeatureEngine, compute_at, event_key             # noqa: E402
from microstructure.grid import eligible, subsecond_rows                                         # noqa: E402
from microstructure.labels import checkpoint_forward_labels, trade_impact_labels                 # noqa: E402
from microstructure.leadlag import lead_lag                                                      # noqa: E402
from microstructure.poller import SnapshotPoller                                                 # noqa: E402
from microstructure.reconstruction import BookReconstructor, BookStatus as BS                    # noqa: E402
from microstructure.replay import load_micro_sessions, rebuild_books, renormalize                # noqa: E402
from microstructure.sources.base import MicroCtx                                                 # noqa: E402
from microstructure.sources.binance_depth import BinanceDepthAdapter                             # noqa: E402
from microstructure.sources.bybit_book import BybitBookAdapter                                   # noqa: E402
from microstructure.sources.coinbase_l2 import CoinbaseL2Adapter                                 # noqa: E402
from microstructure.sources.kalshi_ws import KalshiWsAdapter                                     # noqa: E402
from microstructure.sources.kraken_book import KrakenBookAdapter                                 # noqa: E402
from microstructure.sources.okx_books import OkxBooksAdapter                                     # noqa: E402
from microstructure.storage import MicroStoreWriter, prune_sessions, storage_estimate            # noqa: E402
from microstructure.synthetic import ADAPTERS, VenueFeed, run_research_session                   # noqa: E402
from microstructure.types import MicroEvent, MicroEventType as MT                                # noqa: E402
from microstructure.venues import BOOK_VENUES, VENUES                                            # noqa: E402
from perp_data import fingerprint as pfp                                                         # noqa: E402
from perp_data.dataset import build_research_dataset                                             # noqa: E402
from perp_data.features.definitions import FEATURE_NAMES as P4_NAMES                             # noqa: E402
from perp_data.replay import load_perp_sessions                                                  # noqa: E402
from perp_data.types import PerpEvent, PerpEventType as PT                                       # noqa: E402

STEP1_ARTIFACTS = {
    "config/strategy_baseline.json": "8f052394a830be88f4bd4c506b5d8069222e3c9b1c06c0bc63e3817463185d04",
    "step5_baseline_manifest.json": "6a2994ff8bdc2d36b41608515c77a13d71fd371030487b5e62b2b064cd096db8",
    "regression/strategy_cases.json": "e942ba3716e42a55fbf8e6086283eb0c8f124564b1cb44a3301b6c12c47abf9c",
    "kalshi_dashboard.py": "aa66c7ae1d6cf8b913b746b05bfd6cf64197454d3b7fd00a2ba174749150c595",
}
PERP_VETO_CHAIN = {
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
EARLIER_FINGERPRINTS = {"market_data": "969cec83e8b9912e1fb91d02e8663acc943424dce7d33456b8850b0fb58297fd",
                        "perp_data": "90543ddff68e9dece7443fd9a7d876070f14a06d008a013e8980cb6aa292758c",
                        "settlement": "3eba791cfe8163cc17406ecdbd0c0e04abfe178afb7b6695565a3484318ff2be"}
C = 1_790_001_000_000                         # the close of the test market (15-minute boundary)
START = C - 420_000
DUR_S = 520
S = Status
_CACHE = {}
_N = count(1)
STEP5_SCRIPTS = ("replay_microstructure.py", "build_micro_dataset.py", "lead_lag_analysis.py", "prune_sessions.py",
                 "bench_microstructure.py", "mutation_test_microstructure.py")


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


def tmpdir():
    return tempfile.mkdtemp(prefix="stage21-")


def sha(rel):
    return hashlib.sha256(open(os.path.join(HERE, rel), "rb").read()).hexdigest()


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


# ---------------- event builders ----------------
def mev(src, et, sym, r, payload, asset="BTC", ets=None):
    return MicroEvent(source=src, asset=asset, event_type=et, symbol=sym, event_ts_ms=ets, receive_ts_ms=r,
                      ingest_seq=next(_N), payload=payload)


def units(src):
    return {"price_unit": VENUES[src].price_unit, "qty_unit": VENUES[src].qty_unit}


def snap(src, sym, r, bids, asks, uid=None, prev=None, chain=None, depth=None, checksum=None, asset="BTC"):
    return mev(src, MT.BOOK_SNAPSHOT, sym, r, dict(book=sym, bids=bids, asks=asks, update_id=uid, prev_update_id=prev, depth=depth,
                                                  chain=chain, checksum=checksum, **units(src)), asset)


def delta(src, sym, r, changes, uid=None, prev=None, first=None, chain=None, checksum=None, asset="BTC", ets=None):
    return mev(src, MT.BOOK_DELTA, sym, r, dict(book=sym, changes=[list(c) for c in changes], update_id=uid, prev_update_id=prev,
                                               first_update_id=first, checksum=checksum, chain=chain, **units(src)), asset, ets)


def reset(src, r, book="*", reason="DISCONNECT"):
    return mev(src, MT.BOOK_RESET, book, r, {"book": book, "reason": reason}, "*")


def cb_trade(r, px, qty, aggr, tid=None):
    return MarketEvent(source="coinbase", asset="BTC", event_type=EventType.TRADE, symbol="BTC-USD", event_ts_ms=r - 3,
                       receive_ts_ms=r, ingest_seq=next(_N),
                       payload={"price": px, "size": qty, "aggressor": aggr,
                                "aggressor_semantics": AggressorSemantics.INVERTED_MAKER_SIDE.value,
                                "trade_id": tid if tid is not None else next(_N)})


CB, CBS = "coinbase_l2", "BTC-USD"


def keepalive(src, sym, times, px=50.0, chain=None):
    """Deep-level no-op-ish deltas keeping a book fresh (never touch the best levels)."""
    return [delta(src, sym, t, [("bid", px, 1.0 + (i % 2), "abs")], chain=chain) for i, t in enumerate(times)]


def feats(events, t, venues=None, market=None, cfg=None):
    eng = MicroFeatureEngine("BTC", cfg, venues) if venues else MicroFeatureEngine("BTC", cfg)
    for e in sorted(events, key=event_key):
        if e.receive_ts_ms <= t:
            eng.ingest(e)
    return eng.features_at(t, market)


def shared():
    """One synthetic joint BTC session: Step 3 (spot / CF / Kalshi REST) + Step 4 (perps) + Step 5 (all six books)."""
    if "s" not in _CACHE:
        root = tmpdir()
        c3, c4, c5 = run_research_session(root, ("BTC",), start_ms=START, duration_s=DUR_S, seed=7)
        s3 = load_sessions([c3.dir]); p4 = load_perp_sessions([c3.dir]); m5 = load_micro_sessions([c3.dir], include_raw=True)
        _CACHE["s"] = (root, c3, c4, c5, s3, p4, m5)
    return _CACHE["s"]


def faulted():
    """A micro-only session with one fault per venue (sequence drops, crossed books, an outage)."""
    if "f" not in _CACHE:
        root = tmpdir()
        t0 = C - 200_000
        faults = [("drop", "coinbase_l2", t0 + 50_000), ("drop", "binance_usdm_book", t0 + 60_000),
                  ("drop", "okx_swap_book", t0 + 70_000), ("drop", "kraken_book", t0 + 80_000),
                  ("drop", "kalshi_ws", t0 + 90_000), ("crossed", "bybit_linear_book", t0 + 100_000),
                  ("drop", "bybit_linear_book", t0 + 110_000), ("disconnect", "coinbase_l2", t0 + 140_000, t0 + 150_000)]
        c3, c4, c5 = run_research_session(root, ("BTC",), start_ms=t0, duration_s=240, seed=11, step3=False, perps=False,
                                           faults=faults)
        _CACHE["f"] = (root, c5, t0)
    return _CACHE["f"]


# ═══════════════════ 1-3 baselines, veto, isolation ═══════════════════
def test_baselines_untouched():
    for rel, h in {**STEP1_ARTIFACTS, **PRODUCTION_UNTOUCHED}.items():
        assert sha(rel) == h, rel
    from kalshi_core import baseline
    ok, problems = baseline.verify()
    assert ok, problems
    p = subprocess.run([sys.executable, "-m", "regression.generate", "--check"], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "MATCH (47 cases)" in p.stdout, p.stdout + p.stderr
    import strategy_fingerprint as sf
    fp, _cfg, _v, _f = sf.current_fingerprint(os.path.join(HERE, "kalshi_dashboard.py"))
    assert fp == "8d94f241e8fc8edadc76058e1f12f430b6e4f499f4c0a30fba1cb5cf07dad82a"
    assert baseline.build_manifest()["fingerprints"]["extended_strategy_fingerprint"] == \
        "784141876a1b7f4c9f605036ef1358a6a3ea51ef32fced1f80f74045a6adef8c"
    from market_data import fingerprint as mdfp
    from settlement import fingerprint as sfp
    for name, mod in (("settlement", sfp), ("market_data", mdfp), ("perp_data", pfp)):
        ok, problems = mod.verify()
        assert ok, (name, problems)
    assert json.load(open(os.path.join(HERE, "config", "market_data_baseline.json")))["market_data_fingerprint"] == EARLIER_FINGERPRINTS["market_data"]
    assert json.load(open(os.path.join(HERE, "config", "perp_data_baseline.json")))["perp_data_fingerprint"] == EARLIER_FINGERPRINTS["perp_data"]
    assert json.load(open(os.path.join(HERE, "config", "settlement_baseline.json")))["settlement_fingerprint"] == EARLIER_FINGERPRINTS["settlement"]


def test_existing_perp_veto_untouched():
    for rel, h in PERP_VETO_CHAIN.items():
        assert sha(rel) == h, f"EXISTING PERP VETO CHAIN CHANGED: {rel}"
    assert pfp.perp_veto_fingerprint() == EXISTING_PERP_VETO_FINGERPRINT
    for rel in list(PERP_VETO_CHAIN)[:-1]:
        bad = [m for m in _imports(os.path.join(HERE, rel)) if m == "microstructure" or m.startswith("microstructure.")]
        assert not bad, f"the existing perp veto imports Step-5 code: {rel} {bad}"
    p = subprocess.run([sys.executable, "check_perp_deployment.py"], cwd=HERE, capture_output=True, text=True,
                       env={k: v for k, v in os.environ.items() if k != "PERP_LIVE_VETO_ENABLED"})
    assert "FINAL STATUS: INACTIVE" in p.stdout, p.stdout[-500:]
    stored = json.load(open(os.path.join(HERE, "config", "microstructure_baseline.json")))
    assert stored["existing_perp_veto_fingerprint"] == EXISTING_PERP_VETO_FINGERPRINT


def test_production_isolation():
    prod = ["kalshi_dashboard.py", "kalshi_backtest.py", "kalshi_bot.py", "run_local.py", "strategy_fingerprint.py",
            "collect_market_data.py"] + [os.path.join("kalshi_core", x) for x in os.listdir(os.path.join(HERE, "kalshi_core"))
                                         if x.endswith(".py")]
    layers = [os.path.relpath(p, HERE) for d in ("settlement", "market_data", "perp_data") for p in _py_files(os.path.join(HERE, d))]
    for f in prod + layers:
        bad = [m for m in _imports(os.path.join(HERE, f)) if m == "microstructure" or m.startswith("microstructure.")]
        assert not bad, f"Step-5 feature imported by {f}: {bad}"
    forbidden = {"kalshi_dashboard", "kalshi_bot", "discord", "kalshi_api_learn", "kalshi_core.execution", "kalshi_core.signal",
                 "perp_telemetry", "perp_live", "perp_shadow", "perp_probability", "kalshi_backtest", "run_local"}
    files = _py_files(os.path.join(HERE, "microstructure")) + [os.path.join(HERE, "collect_research_data.py")] + \
        [os.path.join(HERE, "scripts", f) for f in STEP5_SCRIPTS]
    for f in files:
        bad = {m for m in _imports(f) if m in forbidden or m == "urllib.request" or
               m.split(".")[0] in {"discord", "kalshi_dashboard", "requests", "http", "socket"}}
        assert not bad, (f, bad)
        src = open(f, encoding="utf-8").read().lower()
        for s in ("/portfolio/orders", "create_order", "place_order", ".post(", ".put(", ".delete(", "/fapi/v1/order",
                  "/v5/order", "/api/v5/trade", "leverage", "margin/orders", "private_key", "api_secret"):
            assert s not in src, (f, s)
    # the Kalshi websocket adapter only ever sends subscription commands
    k = KalshiWsAdapter(["BTC"])
    k.markets = {"KXBTC15M-26SEP211030": C}
    for m in k.subscribe_messages() + k.pending_messages(C + 10 ** 7):
        assert json.loads(m)["cmd"] in ("subscribe", "unsubscribe"), m
    # runtime: the production watcher modules never pull microstructure in
    code = ("import sys; sys.path.insert(0, %r); import run_local, kalshi_core.signal, kalshi_core.adapter, perp_live;"
            "print(any(m == 'microstructure' or m.startswith('microstructure.') for m in sys.modules))") % HERE
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=HERE)
    assert p.returncode == 0 and p.stdout.strip() == "False", p.stdout + p.stderr
    # features never import labels / lead-lag (post-event data)
    for f in _py_files(os.path.join(HERE, "microstructure", "features")):
        assert not {m for m in _imports(f) if m in ("microstructure.labels", "microstructure.leadlag", "microstructure.dataset")}, f
    from kalshi_core import execution as ex
    try:
        ex.get_execution_engine("LIVE"); raise AssertionError("LIVE engine returned")
    except ex.LiveExecutionUnavailable:
        pass


# ═══════════════════ 4-9 book mechanics ═══════════════════
def test_audit_and_price_level_vs_queue():
    for n, v in VENUES.items():
        assert v.book_classification == "TRUE_INCREMENTAL_BOOK" and v.level_type == "PRICE_LEVEL", n
    b = LocalBook()
    assert not any("queue" in a for a in dir(b)), "a price-level book must not expose queue positions"
    assert not any("queue" in f.name or "iceberg" in f.name for f in FEATURES)
    doc = open(os.path.join(HERE, "docs", "MICROSTRUCTURE.md"), encoding="utf-8").read()
    for s in ("TOP-OF-BOOK ONLY", "TRUE INCREMENTAL BOOK", "SNAPSHOT", "POLLING", "PRICE-LEVEL DEPTH", "ORDER-LEVEL QUEUE"):
        assert s in doc, s


def test_local_book():
    b = LocalBook()
    b.load([(100.0, 1.0), (99.0, 2.0), (98.0, 0.0)], [(101.0, 1.5), (103.0, 1.0)])
    assert b.best_bid() == (100.0, 1.0) and b.best_ask() == (101.0, 1.5) and b.levels() == (2, 2)
    assert b.set_level("bid", 99.5, 4.0) == (0.0, 4.0)                      # insert
    assert b.set_level("bid", 99.5, 3.0) == (4.0, 3.0)                      # modify
    assert b.set_level("ask", 101.0, 0.0) == (1.5, 0.0)                     # delete
    assert b.best_ask() == (103.0, 1.0) and b.top("bid", 3) == [(100.0, 1.0), (99.5, 3.0), (99.0, 2.0)]
    assert b.rank("bid", 99.2) == 2 and b.rank("ask", 102.0) == 0 and b.rank("ask", 104.0) == 1
    assert b.add_level("ask", 102.0, 2.0) == (0.0, 2.0) and b.add_level("ask", 102.0, -2.0) == (2.0, 0.0)
    try:
        b.add_level("bid", 100.0, -5.0); raise AssertionError("negative level accepted")
    except NegativeLevel:
        pass
    assert not b.crossed()
    b.set_level("bid", 103.0, 1.0)
    assert b.crossed()
    b.load([(p, 1.0) for p in range(1, 40)], [(p, 1.0) for p in range(50, 90)])
    b.truncate(10)
    assert b.levels() == (10, 10) and b.best_bid()[0] == 39 and b.top("ask", 20)[-1][0] == 59


def test_book_states_and_resnapshot():
    rc = BookReconstructor(warmup_ms=1000)
    k = (CB, CBS)
    assert rc.status(k, 0) == BS.NO_BOOK
    rc.apply(delta(CB, CBS, 500, [("bid", 100.0, 1.0, "abs")], uid=1, prev=0, chain="c1"))
    assert rc.status(k, 500) == BS.AWAITING_SNAPSHOT                         # a delta without a snapshot is ignored
    rc.apply(snap(CB, CBS, 1000, [(100.0, 1.0)], [(101.0, 1.0)], uid=2, prev=1, chain="c1"))
    assert rc.status(k, 1500) == BS.WARMING_UP and rc.status(k, 2000) == BS.READY
    rc.apply(delta(CB, CBS, 3000, [("ask", 100.5, 2.0, "abs")], uid=3, prev=2, chain="c1"))
    assert rc.status(k, 3000) == BS.READY and rc.tracks[k].book.best_ask() == (100.5, 2.0)
    assert rc.status(k, 3000 + VENUES[CB].stale_after_ms + 1) == BS.STALE
    rc.apply(delta(CB, CBS, 4000, [("bid", 100.7, 1.0, "abs")], uid=4, prev=3, chain="c1"))   # bid above ask
    assert rc.status(k, 4000) == BS.INVALID and rc.tracks[k].reason == "CROSSED_OR_LOCKED_BOOK"
    u = rc.apply(delta(CB, CBS, 4100, [("bid", 99.0, 1.0, "abs")], uid=5, prev=4, chain="c1"))
    assert u.kind == "ignored" and rc.status(k, 4100) == BS.INVALID            # never continued after INVALID
    rc.apply(snap(CB, CBS, 6000, [(100.0, 1.0)], [(101.0, 1.0)], uid=1, prev=None, chain="c2"))
    assert rc.status(k, 6500) == BS.WARMING_UP and rc.status(k, 7000) == BS.READY
    assert rc.tracks[k].intervals == [[1000, 4000], [6000, None]] and rc.tracks[k].counts["resnapshots"] == 1
    assert rc.valid_since(k, 3000) == 1000 and rc.valid_since(k, 5000) is None and rc.valid_since(k, 7000) == 6000
    assert not rc.valid_throughout(k, 3000, 7000) and rc.valid_throughout(k, 6000, 9000)
    rc.apply(reset(CB, 8000))
    assert rc.status(k, 8000) == BS.INVALID and rc.tracks[k].reason.startswith("RESET")
    try:
        rc.apply(delta(CB, CBS, 7000, [], uid=9))
        raise AssertionError("out-of-order apply accepted")
    except ValueError:
        pass


def test_sequence_policies():
    # --- chain_contiguous (Coinbase connection / Kalshi subscription): a gap on the chain invalidates ALL its books ---
    rc = BookReconstructor(warmup_ms=0)
    rc.apply(snap(CB, "BTC-USD", 1000, [(100.0, 1.0)], [(101.0, 1.0)], uid=1, prev=0, chain="cb:c1"))
    rc.apply(snap(CB, "ETH-USD", 1001, [(10.0, 1.0)], [(11.0, 1.0)], uid=2, prev=1, chain="cb:c1", asset="ETH"))
    u = rc.apply(delta(CB, "BTC-USD", 1100, [("bid", 99.0, 1.0, "abs")], uid=4, prev=2, chain="cb:c1"))   # seq 3 lost
    assert u.kind == "gap" and rc.status(("coinbase_l2", "BTC-USD"), 1200) == BS.NEEDS_RESNAPSHOT
    assert rc.status(("coinbase_l2", "ETH-USD"), 1200) == BS.NEEDS_RESNAPSHOT        # the other product too
    assert rc.apply(delta(CB, "ETH-USD", 1300, [("bid", 9.0, 1.0, "abs")], uid=5, prev=4, chain="cb:c1")).kind == "ignored"
    rc.apply(snap(CB, "BTC-USD", 2000, [(100.0, 1.0)], [(101.0, 1.0)], uid=1, prev=0, chain="cb:c2"))
    assert rc.status(("coinbase_l2", "BTC-USD"), 2000) == BS.READY
    # the Coinbase adapter reports the last seq before a discontinuity even across heartbeats / other products
    ad = CoinbaseL2Adapter(["BTC"])
    n = count(1)

    def ctx(r):
        return MicroCtx("s", lambda: next(n), r, None, None, "ws", 1)
    m = lambda seq, ch="l2_data", typ="update": json.dumps({"channel": ch, "timestamp": "2026-09-21T10:00:00Z", "sequence_num": seq,  # noqa: E731
                                                          "events": [{"type": typ, "product_id": "BTC-USD", "updates": [
                                                              {"side": "bid", "event_time": "2026-09-21T10:00:00Z",
                                                               "price_level": "100", "new_quantity": "1"}]}] if ch == "l2_data" else []})
    e0 = ad.parse(m(0, typ="snapshot"), ctx(1)).events[0]
    ad.parse(m(1, ch="heartbeats"), ctx(2))
    e2 = ad.parse(m(2), ctx(3)).events[0]
    ad.parse(m(4, ch="heartbeats"), ctx(4))                                       # seq 3 lost, then a heartbeat
    e5 = ad.parse(m(5), ctx(5)).events[0]
    assert e0.payload["prev_update_id"] is None and e2.payload["prev_update_id"] == 1 and e5.payload["prev_update_id"] == 2
    assert e5.payload["update_id"] == 5 and e5.payload["chain"] == "coinbase_l2:c1"
    # --- Binance: buffered deltas, drop u < L, first U <= L <= u, pu chain, snapshot too old ---
    B, BS_ = "binance_usdm_book", "BTCUSDT"
    rc = BookReconstructor(warmup_ms=0)
    k = (B, BS_)
    for i, (U, u, pu) in enumerate([(90, 95, 89), (96, 100, 95), (101, 105, 100)]):
        assert rc.apply(delta(B, BS_, 1000 + i, [("bid", 100.0 + i, 1.0, "abs")], uid=u, prev=pu, first=U)).kind == "buffered"
    rc.apply(snap(B, BS_, 1100, [(100.0, 2.0)], [(110.0, 1.0)], uid=98))          # L = 98: drop (90,95); apply (96,100)
    tr = rc.tracks[k]
    assert rc.status(k, 1100) == BS.READY and tr.update_id == 105 and tr.counts["dropped"] == 1
    assert tr.intervals[-1][0] == 1100                                              # valid only from the snapshot's receive time
    rc.apply(delta(B, BS_, 1200, [("ask", 109.0, 1.0, "abs")], uid=110, prev=105, first=106))
    assert rc.status(k, 1200) == BS.READY
    u = rc.apply(delta(B, BS_, 1300, [("ask", 108.0, 1.0, "abs")], uid=120, prev=115, first=116))   # pu != previous u
    assert u.kind == "gap" and rc.status(k, 1300) == BS.NEEDS_RESNAPSHOT
    assert rc.apply(delta(B, BS_, 1400, [("ask", 108.0, 2.0, "abs")], uid=125, prev=120, first=121)).kind == "buffered"
    rc.apply(snap(B, BS_, 1500, [(100.0, 2.0)], [(110.0, 1.0)], uid=123))
    assert rc.status(k, 1500) == BS.READY and tr.update_id == 125
    rc2 = BookReconstructor(warmup_ms=0)
    rc2.apply(delta(B, BS_, 1000, [("bid", 1.0, 1.0, "abs")], uid=130, prev=125, first=126))
    u = rc2.apply(snap(B, BS_, 1100, [(1.0, 1.0)], [(2.0, 1.0)], uid=120))          # U 126 > L 120
    assert u.kind == "gap" and "SNAPSHOT_TOO_OLD" in u.reason and rc2.status(k, 1100) == BS.NEEDS_RESNAPSHOT
    # --- OKX prevSeqId chain ---
    OK_, OS = "okx_swap_book", "BTC-USDT-SWAP"
    rc = BookReconstructor(warmup_ms=0)
    rc.apply(snap(OK_, OS, 1000, [(100.0, 5.0)], [(101.0, 5.0)], uid=10, prev=-1))
    assert rc.apply(delta(OK_, OS, 1100, [("bid", 100.0, 4.0, "abs")], uid=11, prev=10)).kind == "delta"
    assert rc.apply(delta(OK_, OS, 1200, [], uid=11, prev=11)).kind == "delta"        # no-change update: prev == seq
    assert rc.apply(delta(OK_, OS, 1300, [("bid", 100.0, 3.0, "abs")], uid=14, prev=13)).kind == "gap"
    # --- Kraken checksum: verified with instrument precisions, mismatch INVALID, unknown precision flagged ---
    KR, KS = "kraken_book", "BTC/USD"
    lb = LocalBook()
    lb.load([(100.1, 1.5), (100.0, 2.0)], [(100.2, 0.5)])
    good = kraken_checksum(lb, 1, 8)
    rc = BookReconstructor(warmup_ms=0)
    rc.apply(snap(KR, KS, 1000, [(100.1, 1.5), (100.0, 2.0)], [(100.2, 0.5)], depth=10, checksum=good))
    assert "CHECKSUM_UNVERIFIED" in rc.tracks[(KR, KS)].flags and rc.status((KR, KS), 1000) == BS.READY
    rc.apply(mev(KR, MT.INSTRUMENT, KS, 1050, {"book": KS, "info": {"price_precision": 1, "qty_precision": 8}}))
    lb.set_level("ask", 100.3, 1.0)
    rc.apply(delta(KR, KS, 1100, [("ask", 100.3, 1.0, "abs")], checksum=kraken_checksum(lb, 1, 8)))
    assert rc.tracks[(KR, KS)].counts["checksum_ok"] == 1 and "CHECKSUM_UNVERIFIED" not in rc.tracks[(KR, KS)].flags
    rc.apply(delta(KR, KS, 1200, [("ask", 100.4, 1.0, "abs")], checksum=12345))
    assert rc.status((KR, KS), 1200) == BS.INVALID and rc.tracks[(KR, KS)].reason == "CHECKSUM_MISMATCH"
    # --- Bybit: monotonic ids only; restart snapshot resets ---
    Y, YS = "bybit_linear_book", "BTCUSDT"
    rc = BookReconstructor(warmup_ms=0)
    rc.apply(snap(Y, YS, 1000, [(100.0, 1.0)], [(101.0, 1.0)], uid=500))
    assert rc.apply(delta(Y, YS, 1100, [("bid", 99.0, 1.0, "abs")], uid=510)).kind == "delta"   # jumps are not detectable
    assert rc.apply(delta(Y, YS, 1200, [("bid", 98.0, 1.0, "abs")], uid=505)).kind == "invalid"
    rc.apply(snap(Y, YS, 1300, [(100.0, 1.0)], [(101.0, 1.0)], uid=1))
    assert rc.status((Y, YS), 1300) == BS.READY
    assert "not documented" in VENUES[Y].limitations or "SEQUENCE_UNVERIFIABLE" in VENUES[Y].limitations
    # --- Kalshi: seq per subscription id ---
    KW, T1 = "kalshi_ws", "KXBTC15M-26SEP211030"
    rc = BookReconstructor(warmup_ms=0)
    rc.apply(snap(KW, T1, 1000, [(40.0, 10.0)], [(45.0, 5.0)], uid=1, chain="k:c1:sid1"))
    assert rc.apply(delta(KW, T1, 1100, [("bid", 41.0, 3.0, "rel")], uid=2, prev=1, chain="k:c1:sid1")).kind == "delta"
    assert rc.apply(delta(KW, T1, 1200, [("bid", 41.0, 3.0, "rel")], uid=4, prev=2, chain="k:c1:sid1")).kind == "gap"


def test_kalshi_conventions():
    assert K.yes_ask_from_no_bid(55) == 45.0 and K.no_ask_from_yes_bid(40) == 60.0
    assert K.no_bid_from_yes_ask(45) == 55.0 and K.yes_bid_from_no_ask(60) == 40.0
    assert K.dollars_to_cents("0.4500") == 45.0 and K.cents_to_prob(45) == 0.45 and K.prob_to_cents(0.45) == 45.0
    for bad in (lambda: K.dollars_to_cents("1.5"), lambda: K.cents_to_prob(-1), lambda: K.prob_to_cents(45)):
        try:
            bad(); raise AssertionError("out-of-range price accepted (units mixed)")
        except NormalizationError:
            pass
    assert K.native_to_yes_terms("yes", 40) == ("bid", 40.0) and K.native_to_yes_terms("no", 55) == ("ask", 45.0)
    ex = K.executable_state([(40.0, 10.0), (39.0, 5.0)], [(45.0, 7.0)])
    assert ex == {"yes_bid_cents": 40.0, "yes_bid_size": 10.0, "yes_ask_cents": 45.0, "yes_ask_size": 7.0,
                  "no_bid_cents": 55.0, "no_bid_size": 7.0, "no_ask_cents": 60.0, "no_ask_size": 10.0}
    assert ex["yes_ask_cents"] - ex["yes_bid_cents"] == ex["no_ask_cents"] - ex["no_bid_cents"]    # YES spread == NO spread
    ad = KalshiWsAdapter(["BTC"])
    n = count(1)
    ctx = lambda r: MicroCtx("s", lambda: next(n), r, None, None, "ws", 1)  # noqa: E731
    tk = "KXBTC15M-26SEP211030"
    s = ad.parse(json.dumps({"type": "orderbook_snapshot", "sid": 3, "seq": 1, "msg": {
        "market_ticker": tk, "yes_dollars_fp": [["0.4000", "10.00"], ["0.3900", "5.00"]],
        "no_dollars_fp": [["0.5500", "7.00"], ["0.5400", "2.00"]]}}), ctx(1000)).events[0]
    assert s.event_type == MT.BOOK_SNAPSHOT and s.payload["bids"] == [[40.0, 10.0], [39.0, 5.0]]
    assert sorted(s.payload["asks"]) == [[45.0, 7.0], [46.0, 2.0]] and s.payload["price_unit"] == "YES_CENTS"
    assert s.payload["native_no_bids"] == [[55.0, 7.0], [54.0, 2.0]] and s.payload["chain"] == "kalshi_ws:c1:sid3"
    d = ad.parse(json.dumps({"type": "orderbook_delta", "sid": 3, "seq": 2, "msg": {
        "market_ticker": tk, "price_dollars": "0.5500", "delta_fp": "-3.00", "side": "no", "ts_ms": C - 1}}), ctx(1001)).events[0]
    assert d.payload["changes"] == [["ask", 45.0, -3.0, "rel"]] and d.payload["prev_update_id"] == 1
    old = ad.parse(json.dumps({"type": "orderbook_delta", "sid": 3, "seq": 3, "msg": {
        "market_ticker": tk, "price": 41, "delta": 4, "side": "yes"}}), ctx(1002)).events[0]      # legacy cent fields
    assert old.payload["changes"] == [["bid", 41.0, 4.0, "rel"]]
    tr = ad.parse(json.dumps({"type": "trade", "sid": 9, "msg": {"trade_id": "x1", "market_ticker": tk, "yes_price_dollars": "0.4300",
                                                                 "count_fp": "12.00", "taker_side": "no", "ts_ms": C - 3}}), ctx(1004)).events[0]
    assert tr.payload["price"] == 43.0 and tr.payload["aggressor"] == "sell" and tr.payload["qty"] == 12.0
    # engine: executable state and the YES-terms book
    from market_data.features.dataset import discover_markets  # noqa: F401  (vocabulary check only)

    class Mk:
        ticker = tk
    row = feats([s, d, old], 2100, market=Mk())
    V = row.values
    assert V["micro.kalshi.yes_bid"] == 41.0 and V["micro.kalshi.yes_ask"] == 45.0 and V["micro.kalshi.no_bid"] == 55.0
    assert V["micro.kalshi.no_ask"] == 59.0 and V["micro.kalshi.spread_cents"] == 4.0 and V["micro.kalshi.yes_bid_size"] == 4.0
    assert V["micro.kalshi.no_bid_size"] == 4.0 and V["micro.kalshi.yes_ask_size"] == 4.0 and V["micro.kalshi.no_ask_size"] == 4.0
    assert V["micro.kalshi.no_depth_5"] == 6.0 and V["micro.kalshi.yes_depth_5"] == 19.0


def test_adapters_parse_native_messages():
    from market_data.synthetic import World
    from perp_data.synthetic import PerpWorld
    w = World(["BTC"], C - 100_000, C + 100_000)
    pw = PerpWorld(w)
    n = count(1)
    for v in ("coinbase_l2", "kraken_book", "bybit_linear_book", "okx_swap_book", "binance_usdm_book", "kalshi_ws"):
        f = VenueFeed(v, ["BTC"], w, pw, 5)
        ad = ADAPTERS[v](["BTC"])
        msgs = f.connect(C - 50_000, 1) + f.step(C - 49_900) + f.step(C - 49_800)
        evs, fails = [], []
        for i, m in enumerate(msgs):
            res = ad.parse(m, MicroCtx("s", lambda: next(n), C - 50_000 + i, None, None, "ws", 1))
            evs += res.events
            fails += res.failures
        assert not fails and evs, (v, fails[:1])
        assert all(e.payload.get("price_unit") in (VENUES[v].price_unit, "YES_CENTS") for e in evs if e.event_type != MT.INSTRUMENT)
        if v == "binance_usdm_book":
            res = ad.parse_rest("snapshot:BTC", f.binance_snapshot("BTC"), MicroCtx("s", lambda: next(n), C, None, None, "rest", 1))
            assert res.events[0].event_type == MT.BOOK_SNAPSHOT and res.events[0].payload["update_id"] == f.u["BTC"]
            assert ad.snapshot_url("BTC")[1] == {"symbol": "BTCUSDT", "limit": 1000}
    # failures are recorded, never guessed
    for ad, bad in ((CoinbaseL2Adapter(["BTC"]), '{"channel":"l2_data","sequence_num":1,"events":[{"type":"update","product_id":"DOGE-USD","updates":[]}]}'),
                    (KrakenBookAdapter(["BTC"]), '{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":"x","qty":1}]}]}'),
                    (BinanceDepthAdapter(["BTC"]), "{not json"),
                    (BybitBookAdapter(["BTC"]), '{"topic":"orderbook.200.BTCUSDT","type":"weird","data":{"s":"BTCUSDT","u":1}}'),
                    (OkxBooksAdapter(["BTC"]), '{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"update","data":[{"bids":[],"asks":[]}]}'),
                    (KalshiWsAdapter(["BTC"]), '{"type":"orderbook_delta","sid":1,"seq":1,"msg":{"market_ticker":"KXBTC15M-X","price_dollars":"1.7","delta_fp":"1","side":"yes"}}')):
        res = ad.parse(bad, MicroCtx("s", lambda: next(n), C, None, None, "ws", 1))
        assert res.failures and not res.events, (ad.source, res.events)
    assert OkxBooksAdapter(["BTC"]).parse("pong", MicroCtx("s", lambda: next(n), C, None, None, "ws", 1)).control == ["pong"]
    for bad_depth in ((KrakenBookAdapter, 7), (BybitBookAdapter, 7), (BinanceDepthAdapter, 7)):
        try:
            bad_depth[0](["BTC"], depth=bad_depth[1]); raise AssertionError("invalid depth accepted")
        except ValueError:
            pass


# ═══════════════════ 10-12 collector, faults, replay ═══════════════════
class FakeGetter:
    def __init__(self, fn):
        self.fn, self.requests = fn, []

    def get_json(self, url, params=None):
        self.requests.append(("GET", url, dict(params or {})))
        return self.fn(url, params)


def test_collector_resnapshot_and_raw_first():
    clock = FakeClock(C)
    ad = CoinbaseL2Adapter(["BTC"])
    col = MicroCollector(tmpdir(), ["BTC"], {CB: ad}, clock, fsync=False)
    col.on_connect(ad, clock.wall_ms())
    base = {"channel": "l2_data", "timestamp": "2026-09-21T10:00:00Z"}
    snapm = dict(base, sequence_num=0, events=[{"type": "snapshot", "product_id": "BTC-USD", "updates": [
        {"side": "bid", "event_time": "x", "price_level": "100", "new_quantity": "1"},
        {"side": "offer", "event_time": "x", "price_level": "101", "new_quantity": "1"}]}])
    col.on_message(ad, json.dumps(snapm), clock.wall_ms(), clock.mono_ns())
    upd = lambda s: json.dumps(dict(base, sequence_num=s, events=[{"type": "update", "product_id": "BTC-USD", "updates": [  # noqa: E731
        {"side": "bid", "event_time": "x", "price_level": "99", "new_quantity": str(s)}]}]))
    clock.advance(100)
    col.on_message(ad, upd(1), clock.wall_ms(), clock.mono_ns())
    clock.advance(100)
    try:
        col.on_message(ad, upd(3), clock.wall_ms(), clock.mono_ns())
        raise AssertionError("gap did not demand a resnapshot")
    except ResnapshotRequired as e:
        assert "SEQUENCE_GAP" in str(e)
    col.on_disconnect(ad, clock.wall_ms(), "coinbase_l2: resnapshot")
    col.on_connect(ad, clock.wall_ms(), reconnect=True)
    col.on_message(ad, json.dumps(dict(snapm, sequence_num=0)), clock.wall_ms(), clock.mono_ns())
    col.close()
    recs = read_session(col.dir).records
    kinds = [k for k, _ in recs]
    first_raw = kinds.index("raw")
    assert first_raw < kinds.index("event")                                      # raw first
    raws = [d for k, d in recs if k == "raw"]
    assert raws[0]["stream"] == "ws#1" and raws[-1]["stream"] == "ws#2"          # connection number recorded
    evs = [MicroEvent.from_dict(d) for k, d in recs if k == "event"]
    assert any(e.event_type == MT.BOOK_RESET and e.payload["reason"] == "RESNAPSHOT" for e in evs)
    gaps = [d for k, d in recs if k == "gap"]
    assert any(g["kind"] == "BOOK_SEQUENCE" for g in gaps) and any(g["kind"] == "DISCONNECT" for g in gaps)
    m = json.load(open(os.path.join(col.dir, "manifest.json")))
    assert m["status"] == "COMPLETED" and m["resnapshot_requests"]["coinbase_l2"] == 1
    assert m["books"]["coinbase_l2:BTC-USD"]["status"] in ("READY", "WARMING_UP") and m["research_only"] is True
    # the replayed books equal the live collector's
    rc = rebuild_books(evs)
    assert rc.tracks[(CB, CBS)].counts == col.recon.tracks[(CB, CBS)].counts
    assert rc.tracks[(CB, CBS)].intervals == col.recon.tracks[(CB, CBS)].intervals
    # Binance: snapshots are fetched with GET only when needed (connect / gap), at most one per interval
    clock = FakeClock(C)
    bad = BinanceDepthAdapter(["BTC", "ETH"])
    bcol = MicroCollector(tmpdir(), ["BTC", "ETH"], {"binance_usdm_book": bad}, clock, fsync=False)
    g = FakeGetter(lambda url, p: {"lastUpdateId": 10, "bids": [["100", "1"]], "asks": [["101", "1"]]})
    sp = SnapshotPoller(bad, g, bcol, clock, min_interval_s=1.0)
    assert sp.poll() == (0, 0, 0) and not g.requests                              # nothing needed before connect
    bcol.on_connect(bad, clock.wall_ms())
    sp.poll()
    assert [(r[0], r[2]["symbol"]) for r in g.requests] == [("GET", "BTCUSDT"), ("GET", "ETHUSDT")] and not bcol.snapshot_needed
    clock.advance(100)
    sp.poll()
    assert len(g.requests) == 2                                                   # satisfied: no further requests
    bcol.record_overload(bad, clock.wall_ms(), "queue overflow (test)")
    assert bcol.manifest.drops["binance_usdm_book:overload"] == 1 and bcol.snapshot_needed
    bcol.close()


def test_fault_injection_and_recovery():
    root, c5, t0 = faulted()
    log = c5.fault_log
    res = {x[1] for x in log if x[0] == "resnapshot"}
    for v in ("coinbase_l2", "okx_swap_book", "kraken_book", "kalshi_ws", "bybit_linear_book"):
        assert v in res, (v, log)
    books = c5.book_status()
    assert books["binance_usdm_book:BTCUSDT"]["gaps"] >= 1 and books["binance_usdm_book:BTCUSDT"]["resnapshots"] >= 1
    assert books["bybit_linear_book:BTCUSDT"]["crossed"] >= 1
    # a lost Bybit delta is not detectable by any SEQUENCE rule (documented); it can only surface indirectly (e.g. a
    # later crossed book caused by a level that should have been deleted)
    assert not any(x[0] == "resnapshot" and x[1] == "bybit_linear_book" and "SEQUENCE" in x[3] for x in log)
    for k, b in books.items():
        if not k.startswith("kalshi_ws:") or b["snapshots"] > 1:
            assert b["status"] == "READY", (k, b)
    cb = books["coinbase_l2:BTC-USD"]
    assert cb["resnapshots"] >= 2                                                # the sequence gap AND the outage
    L = load_micro_sessions([root + "/" + os.listdir(root)[0]], include_raw=True)
    rc = rebuild_books(L.events)
    for k, tr in c5.recon.tracks.items():
        assert rc.tracks[k].counts == tr.counts and rc.tracks[k].intervals == tr.intervals, k
    again = renormalize(L.raw, {v: ADAPTERS[v](["BTC"]) for v in BOOK_VENUES}, L.session_ids[0])
    strip = lambda e: json.dumps({k: v for k, v in e.to_dict().items() if k != "ingest_seq"}, sort_keys=True, default=str)  # noqa: E731
    assert [strip(e) for e in L.events if e.channel != "collector"] == [strip(e) for e in again]


def test_replay_deterministic():
    root, c3, c4, c5, s3, p4, m5 = shared()
    a = rebuild_books(m5.events)
    b = rebuild_books(list(reversed(m5.events)))                                 # order restored by (receive, seq)
    for k in a.tracks:
        assert a.tracks[k].book.top("bid", 50) == b.tracks[k].book.top("bid", 50) and a.tracks[k].intervals == b.tracks[k].intervals
    p = subprocess.run([sys.executable, "scripts/replay_microstructure.py", c3.dir, "--digest", "--renormalize"], cwd=HERE,
                       capture_output=True, text=True)
    q = subprocess.run([sys.executable, "scripts/replay_microstructure.py", c3.dir, "--digest"], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "IDENTICAL to the stored events" in p.stdout, p.stdout[-800:] + p.stderr[-800:]
    dig = lambda s: [ln for ln in s.splitlines() if "digest" in ln]  # noqa: E731
    assert dig(p.stdout) == dig(q.stdout) and len(dig(p.stdout)) == 2


# ═══════════════════ 13-21 features ═══════════════════
def test_feature_registry():
    assert FAMILIES == ("SPOT_BOOK", "PERP_BOOK", "KALSHI_BOOK", "ORDER_FLOW", "TRADE_INTENSITY", "LIQUIDITY", "SWEEP",
                        "REPLENISHMENT", "TOXICITY", "CROSS_VENUE_MICROSTRUCTURE")
    fam = names_by_family()
    assert all(fam[f] for f in FAMILIES) and sum(counts_by_family().values()) == len(FEATURES) == len(set(FEATURE_NAMES))
    assert len(FEATURES) <= 650, "feature count must stay controlled"
    assert not (set(FEATURE_NAMES) & (set(S3_NAMES) | set(P4_NAMES)))
    for f in FEATURES:
        assert "cancel" not in f.name or f.name.split(".")[-2].startswith("est_cancel"), f.name
        if "depth_removed" in f.name:
            assert "not 'cancellations'" in f.description
        if ".est_" in f.name:
            assert "ESTIMATE" in f.description and "estimate" in f.completeness
        if "vpin" in f.name:
            assert "not canonical" in f.description.lower()
        if f.family == "REPLENISHMENT":
            assert "replenishment" in f.name and "iceberg" not in f.name
    assert MICRO_FEATURE_SET_VERSION == "micro_features_v1"


def _ofi_events():
    evs = [snap(CB, CBS, 1000, [(100.0, 1.0), (99.0, 2.0)], [(101.0, 1.0), (102.0, 3.0)], chain=None),
           delta(CB, CBS, 3000, [("bid", 100.0, 3.0, "abs")]),
           delta(CB, CBS, 4000, [("ask", 101.0, 0.0, "abs")]),
           delta(CB, CBS, 4500, [("bid", 100.5, 2.0, "abs")]),
           cb_trade(1000, 100.0, 0.1, "sell"), cb_trade(3900, 101.0, 0.4, "buy")]
    return evs


def test_book_features_math():
    row = feats(_ofi_events(), 6500, venues=(CB,))
    V, St = row.values, row.status
    p = "micro.coinbase."
    assert V[p + "book_ready"] == 1.0 and V[p + "best_bid"] == 100.5 and V[p + "best_ask"] == 102.0
    assert V[p + "mid"] == 101.25 and V[p + "spread"] == 1.5 and abs(V[p + "spread_bps"] - 1.5 / 101.25 * 1e4) < 1e-9
    assert abs(V[p + "microprice"] - 101.1) < 1e-12 and abs(V[p + "microprice_minus_mid_bps"] - (101.1 - 101.25) / 101.25 * 1e4) < 1e-9
    assert V[p + "bid_depth_1"] == 2.0 and V[p + "bid_depth_5"] == 7.0 and V[p + "ask_depth_5"] == 3.0
    assert abs(V[p + "imbalance_1"] - (-0.2)) < 1e-12 and abs(V[p + "imbalance_5"] - 0.4) < 1e-12
    assert abs(V[p + "depth_concentration_bid"] - 2 / 7) < 1e-12 and V[p + "total_depth_10"] == 10.0
    assert abs(V[p + "next_level_gap_bid_bps"] - 0.5 / 101.25 * 1e4) < 1e-9
    assert V[p + "next_level_gap_ask_bps"] is None and St[p + "next_level_gap_ask_bps"] == S.UNDEFINED
    lim = 101.25 * 25 / 1e4                                          # 25 bps ~ 0.253: no level within
    assert V[p + "bid_liquidity_25bps"] == 0.0 and lim < 1.0
    assert St[p + "depth_slope_bid"] == S.READY and V[p + "depth_slope_bid"] > 0


def test_ofi_and_depth_deltas():
    row = feats(_ofi_events(), 6500, venues=(CB,))
    V, St = row.values, row.status
    p = "micro.coinbase."
    assert V[p + "ofi_l1.5s"] == 5.0 and V[p + "mlofi_5.5s"] == 8.0            # e = 2, 1, 2 ; level sums 2, 1, 5
    assert V[p + "ofi_l1.1s"] == 0.0 and St[p + "ofi_l1.60s"] == S.NOT_READY  # book valid only since 1000
    assert V[p + "bid_depth_added.5s"] == 4.0 and V[p + "bid_depth_removed.5s"] == 0.0
    assert V[p + "ask_depth_added.5s"] == 0.0 and V[p + "ask_depth_removed.5s"] == 1.0
    assert abs(V[p + "est_cancel_ask.5s"] - 0.6) < 1e-12 and V[p + "est_cancel_bid.5s"] == 0.0   # removed - same-side trades
    assert St[p + "est_cancel_ask.60s"] == S.NOT_READY
    assert V[p + "microprice_change_bps.5s"] is not None and St[p + "imbalance_10_change.5s"] == S.READY
    assert V[p + "net_bid_depth_change_10.5s"] == 4.0 and V[p + "net_ask_depth_change_10.5s"] == -1.0


def test_trade_intensity_and_large_trades():
    evs = [snap(CB, CBS, 1000, [(100.0, 1.0)], [(101.0, 1.0)])] + keepalive(CB, CBS, range(4000, 1_000_000, 4000))
    sizes = []
    for i in range(0, 900):
        r = 1000 + i * 1000
        q = 1.0 + (i % 20)
        sizes.append(q)
        evs.append(cb_trade(r, 100.5, q, "buy" if i % 3 else "sell"))
    T = 900_000
    V, St = feats(evs, T, venues=(CB,)).values, feats(evs, T, venues=(CB,)).status
    p = "micro.coinbase."
    assert V[p + "trade_count.60s"] == 60.0 and V[p + "trade_count.1s"] == 1.0 and V[p + "trade_rate_ratio_5s_60s"] == 1.0
    assert V[p + "median_interarrival_ms.60s"] == 1000.0 and St[p + "large_trade_count.60s"] == S.READY
    prior = sorted(q for i, q in enumerate(sizes) if T - 900_000 < 1000 + i * 1000 <= T - 60_000)
    thr = prior[int(math.floor(0.95 * len(prior)))]
    last = [q for i, q in enumerate(sizes) if T - 60_000 < 1000 + i * 1000 <= T]
    assert V[p + "large_trade_count.60s"] == float(sum(1 for q in last if q > thr))
    # the threshold is causal: sizes AFTER T never move it
    evs2 = evs + [cb_trade(T + 500 + i, 100.5, 10_000.0, "buy") for i in range(100)]
    assert feats(evs2, T, venues=(CB,)).values == V
    early = feats(evs, 30_000, venues=(CB,))
    assert early.status[p + "large_trade_count.60s"] == S.NOT_READY          # not enough history yet


def test_sweeps_and_replenishment():
    ka = keepalive(CB, CBS, range(6000, 70_000, 4000), px=90.0)
    evs = [snap(CB, CBS, 1000, [(100.0, 1.0), (99.0, 1.0), (90.0, 1.0)], [(101.0, 1.0), (102.0, 1.0), (103.0, 1.0), (104.0, 5.0)]),
           cb_trade(900, 100.0, 0.01, "sell"),
           cb_trade(2940, 101.0, 1.0, "buy"), cb_trade(2960, 102.0, 1.0, "buy"), cb_trade(2980, 103.0, 1.0, "buy"),
           delta(CB, CBS, 3000, [("ask", 101.0, 0.0, "abs"), ("ask", 102.0, 0.0, "abs"), ("ask", 103.0, 0.0, "abs")])] + ka
    V = feats(evs, 62_000, venues=(CB,)).values
    p = "micro.coinbase."
    assert V[p + "sweep_count.60s"] == 1.0 and V[p + "last_sweep_direction"] == 1.0 and V[p + "last_sweep_levels"] == 3.0
    assert V[p + "last_sweep_notional"] == 306.0 and V[p + "last_sweep_duration_ms"] == 20.0
    assert abs(V[p + "last_sweep_impact_bps"] - (102.0 - 100.5) / 100.5 * 1e4) < 1e-9
    no_trades = [e for e in evs if not isinstance(e, MarketEvent)] + [cb_trade(900, 100.0, 0.01, "sell")]
    V2, St2 = feats(no_trades, 62_000, venues=(CB,)).values, feats(no_trades, 62_000, venues=(CB,)).status
    assert V2[p + "sweep_count.60s"] == 0.0 and St2[p + "last_sweep_direction"] == S.UNDEFINED   # a repricing is not a sweep
    # REPLENISHMENT_PATTERN: the best bid is depleted by sells and restored at the same price (not an iceberg claim)
    evs = [snap(CB, CBS, 1000, [(100.0, 5.0), (99.0, 1.0), (90.0, 1.0)], [(101.0, 5.0)]), cb_trade(900, 100.0, 0.01, "buy"),
           cb_trade(2900, 100.0, 2.0, "sell"), delta(CB, CBS, 3000, [("bid", 100.0, 3.0, "abs")]),
           delta(CB, CBS, 4000, [("bid", 100.0, 5.0, "abs")]),
           cb_trade(4900, 100.0, 3.0, "sell"), delta(CB, CBS, 5000, [("bid", 100.0, 2.0, "abs")]),
           delta(CB, CBS, 6000, [("bid", 100.0, 4.5, "abs")])] + ka
    V = feats(evs, 62_000, venues=(CB,)).values
    assert V[p + "replenishment_bid_count.60s"] == 2.0 and V[p + "replenishment_max_run.60s"] == 2.0
    assert V[p + "replenishment_ask_count.60s"] == 0.0
    # a restore WITHOUT a trade at that price is not a replenishment
    evs3 = [e for e in evs if not (isinstance(e, MarketEvent) and e.payload["size"] > 1)]
    assert feats(evs3, 62_000, venues=(CB,)).values[p + "replenishment_bid_count.60s"] == 0.0


def test_vacuum_toxicity_adverse():
    p = "micro.coinbase."
    # VPIN-style: NOT_READY during calibration and until 50 buckets complete; all-buy flow -> 1.0
    evs = [snap(CB, CBS, 500, [(100.0, 1.0)], [(101.0, 1.0)])] + [cb_trade(1000 + i * 1000, 100.5, 1.0, "buy") for i in range(900)]
    assert feats(evs, 1000 + 893_000, venues=(CB,)).status[p + "vpin_style_50"] == S.NOT_READY
    r = feats(evs, 1000 + 899_000, venues=(CB,))
    assert r.status[p + "vpin_style_50"] == S.READY and abs(r.values[p + "vpin_style_50"] - 1.0) < 1e-12
    alt = [evs[0]] + [cb_trade(1000 + i * 1000, 100.5, 1.0, "buy" if i % 2 else "sell") for i in range(900)]
    assert abs(feats(alt, 1000 + 899_000, venues=(CB,)).values[p + "vpin_style_50"]) < 1e-12
    auto = feats(alt, 1000 + 899_000, venues=(CB,))
    assert auto.values[p + "trade_sign_autocorr.60s"] < -0.99                     # perfectly alternating signs
    # adverse move: trades in [T-65 s, T-5 s], mid 5 s later, every mid received <= T
    base = [snap(CB, CBS, 1000, [(100.0, 1.0), (90.0, 1.0)], [(101.0, 1.0)]), cb_trade(1000, 100.0, 0.01, "sell")]
    base += keepalive(CB, CBS, range(4000, 100_000, 4000), px=90.0)
    base += [cb_trade(70_000 + i * 1000, 100.5, 1.0, "buy") for i in range(12)]
    base += [delta(CB, CBS, 84_000, [("bid", 101.0, 1.0, "abs"), ("ask", 102.0, 1.0, "abs"), ("ask", 101.0, 0.0, "abs")])]
    r = feats(base, 90_000, venues=(CB,))
    assert abs(r.values[p + "adverse_move_bps"] - 3 * (1.0 / 100.5 * 1e4) / 12) < 1e-9, r.values[p + "adverse_move_bps"]
    fut = base + [delta(CB, CBS, 90_500, [("bid", 150.0, 1.0, "abs"), ("ask", 151.0, 1.0, "abs")])]
    assert feats(fut, 90_000, venues=(CB,)).values[p + "adverse_move_bps"] == r.values[p + "adverse_move_bps"]
    # liquidity vacuum: needs 5 minutes of per-second history; then depth collapse + spread blow-out -> 1
    evs = [snap(CB, CBS, 1000, [(100.0, 10.0), (99.9, 10.0)], [(100.1, 10.0), (100.2, 10.0)])] + \
        keepalive(CB, CBS, range(3000, 400_000, 3000), px=90.0)
    assert feats(evs, 200_000, venues=(CB,)).status[p + "liquidity_vacuum"] == S.NOT_READY
    evs.append(delta(CB, CBS, 399_500, [("bid", 100.0, 0.0, "abs"), ("bid", 99.9, 1.0, "abs"), ("ask", 100.1, 0.0, "abs"),
                                        ("ask", 100.2, 1.0, "abs"), ("bid", 90.0, 0.0, "abs")]))
    r = feats(evs, 400_000, venues=(CB,))
    assert r.values[p + "liquidity_vacuum"] == 1.0 and r.values[p + "depth10_vs_5m_median"] < 0.5


def test_missing_and_warmup():
    evs = _ofi_events()
    row = feats(evs, 6500, venues=(CB, "kraken_book", "okx_swap_book"))
    V, St = row.values, row.status
    assert V["micro.kraken.imbalance_10"] is None and St["micro.kraken.imbalance_10"] == S.MISSING   # never 0
    assert St["micro.kraken.book_ready"] == S.MISSING and V["micro.okx.ofi_l1.5s"] is None
    assert St["micro.bybit.imbalance_10"] == S.UNAVAILABLE                      # venue not enabled
    w = feats(evs, 1500, venues=(CB,))
    assert w.status["micro.coinbase.imbalance_1"] == S.NOT_READY and w.values["micro.coinbase.book_ready"] == 0.0
    inv = evs + [reset(CB, 7000)]
    r = feats(inv, 7500, venues=(CB,))
    assert r.values["micro.coinbase.imbalance_10"] is None and r.status["micro.coinbase.imbalance_10"] == S.MISSING
    assert r.values["micro.coinbase.ofi_l1.1s"] is None and r.values["micro.coinbase.book_ready"] == 0.0
    st = feats(evs, 4500 + 5001, venues=(CB,))
    assert st.status["micro.coinbase.mid"] == S.MISSING                          # stale book
    # OKX sizes are contracts: coin features UNAVAILABLE until a VERIFIED contract value, unit-free features READY
    OK_, OS = "okx_swap_book", "BTC-USDT-SWAP"
    ok = [snap(OK_, OS, 1000, [(100.0, 10.0)], [(101.0, 30.0)], uid=1, prev=-1)]
    r = feats(ok, 3000, venues=(OK_,))
    assert r.status["micro.okx.imbalance_1"] == S.READY and r.values["micro.okx.imbalance_1"] == -0.5
    assert r.status["micro.okx.bid_depth_1"] == S.UNAVAILABLE and r.values["micro.okx.bid_depth_1"] is None
    ins = PerpEvent(source="okx_swap", asset="BTC", event_type=PT.INSTRUMENT, symbol=OS, event_ts_ms=None, receive_ts_ms=900,
                    ingest_seq=next(_N), payload={"contract_value": 0.01, "contract_value_ccy": "BTC", "funding_interval_ms": None,
                                                  "verified": True}, channel="rest")
    r = feats(ok + [ins], 3000, venues=(OK_,))
    assert r.status["micro.okx.bid_depth_1"] == S.READY and abs(r.values["micro.okx.bid_depth_1"] - 0.1) < 1e-12


# ═══════════════════ 22-26 causality, leakage, labels ═══════════════════
def test_receive_time_causality():
    evs = _ofi_events() + keepalive(CB, CBS, range(7000, 20_000, 2000))
    late = delta(CB, CBS, 10_300, [("bid", 100.9, 50.0, "abs")], ets=9_900)       # event 9.9 s, received 10.3 s
    evs.append(late)
    T = 10_000
    a = compute_at("BTC", evs, T, venues=(CB,))
    b = compute_at("BTC", [e for e in evs if e is not late], T, venues=(CB,))
    assert a.values == b.values and a.values["micro.coinbase.best_bid"] == 100.5   # not visible before receipt
    assert compute_at("BTC", evs, 10_300, venues=(CB,)).values["micro.coinbase.best_bid"] == 100.9
    stream = MicroFeatureEngine("BTC", venues=(CB,))
    evs_sorted = sorted(evs, key=event_key)
    i = 0
    for t in (5000, 8000, 10_000, 12_345, 19_000):
        while i < len(evs_sorted) and evs_sorted[i].receive_ts_ms <= t:
            stream.ingest(evs_sorted[i]); i += 1
        assert stream.features_at(t).values == compute_at("BTC", evs, t, venues=(CB,)).values, t
    try:
        stream.features_at(1000); raise AssertionError("features before the last ingested receive time")
    except CausalityError:
        pass
    try:
        stream.ingest(delta(CB, CBS, 100, [])); raise AssertionError("out-of-order ingest accepted")
    except CausalityError:
        pass


def test_no_retroactive_repair():
    evs = [snap(CB, CBS, 1000, [(100.0, 1.0), (90.0, 1.0)], [(101.0, 1.0)], chain="c1", uid=0)]
    evs += [delta(CB, CBS, t, [("bid", 90.0, 1.0 + (i % 2), "abs")], chain="c1", uid=i + 1, prev=i)
            for i, t in enumerate(range(3000, 30_000, 3000))]
    evs.append(reset(CB, 30_000))
    evs.append(snap(CB, CBS, 40_000, [(100.0, 1.0), (90.0, 1.0)], [(101.0, 1.0)], chain="c2", uid=0))
    evs += [delta(CB, CBS, t, [("bid", 90.0, 1.0 + (i % 2), "abs")], chain="c2", uid=i + 1, prev=i)
            for i, t in enumerate(range(43_000, 120_000, 3000))]
    p = "micro.coinbase."
    r = compute_at("BTC", evs, 35_000, venues=(CB,))
    assert r.status[p + "imbalance_10"] == S.MISSING                              # still invalid; nothing repaired
    r = compute_at("BTC", evs, 70_000, venues=(CB,))
    assert r.status[p + "ofi_l1.60s"] == S.NOT_READY and r.status[p + "ofi_l1.15s"] == S.READY
    assert r.status[p + "net_bid_depth_change_10.60s"] != S.READY and r.status[p + "sweep_count.60s"] == S.NOT_READY
    r = compute_at("BTC", evs, 101_000, venues=(CB,))
    assert r.status[p + "ofi_l1.60s"] == S.READY                                  # a full window after the resnapshot


def test_no_leakage():
    root, c3, c4, c5, s3, p4, m5 = shared()
    evs = list(s3.events) + list(p4.events) + list(m5.events)
    from market_data.features.dataset import discover_markets
    markets = discover_markets(s3.events, ["BTC"])
    m = [mm for mm, _k in markets.values()][-1]
    for T in (C - 300_000, C - 60_000, C - 1000):
        full = compute_at("BTC", evs, T, m)
        past = compute_at("BTC", [e for e in evs if e.receive_ts_ms <= T], T, m)
        assert full.values == past.values, T
        canary = evs + [snap(CB, CBS, T + 1, [(1.0, 1e9)], [(2.0, 1e9)], chain="zz"),
                        cb_trade(T + 1, 1.0, 1e9, "buy"),
                        mev("kalshi_ws", MT.TRADE, m.ticker, T + 1, {"price": 99.0, "qty": 1e9, "price_unit": "YES_CENTS",
                                                                   "qty_unit": "contracts", "aggressor": "buy",
                                                                   "aggressor_semantics": "x", "trade_id": "canary"})]
        assert compute_at("BTC", canary, T, m).values == full.values, T
    # post-event labels are never features and are only known after T
    labs = checkpoint_forward_labels(evs, "BTC", [C - 60_000])
    assert labs[0]["available_at_ms"] > C - 60_000 and not (set(labs[0]) & set(FEATURE_NAMES))
    assert all(k.startswith("micro_label.") or k in ("asset", "checkpoint_ts_ms", "label_kind", "available_at_ms") for k in labs[0])


def test_post_event_labels():
    evs = [snap(CB, CBS, 1000, [(100.0, 1.0), (90.0, 1.0)], [(101.0, 1.0)]), cb_trade(2000, 101.0, 1.0, "buy"),
           cb_trade(3000, 100.0, 1.0, "sell"),
           delta(CB, CBS, 2400, [("bid", 100.5, 1.0, "abs")]), delta(CB, CBS, 3200, [("ask", 100.8, 1.0, "abs")])]
    labs = trade_impact_labels(evs, "BTC", CB)
    assert len(labs) == 2 and labs[0]["label_kind"] == "POST_EVENT_RESEARCH_LABEL"
    m0 = 100.5
    assert abs(labs[0]["impact_bps.500ms"] - (100.75 - m0) / m0 * 1e4) < 1e-9 and labs[0]["impact_bps.250ms"] == 0.0
    assert labs[0]["available_at_ms.5000ms"] == 7000
    assert abs(labs[1]["impact_bps.250ms"] - (-(100.65 - 100.75) / 100.75 * 1e4)) < 1e-9     # sell: signed
    fl = checkpoint_forward_labels(evs, "BTC", [2000], venues=(CB,))
    assert abs(fl[0]["micro_label.coinbase.fwd_mid_bps.1000ms"] - (100.75 / 100.5 - 1) * 1e4) < 1e-9
    assert abs(fl[0]["micro_label.coinbase.fwd_mid_bps.5000ms"] - (100.65 / 100.5 - 1) * 1e4) < 1e-9


# ═══════════════════ 27-29 lead-lag, grid, dataset ═══════════════════
def test_lead_lag_offline():
    import random
    rnd = random.Random(3)
    step, n = 250, 4000
    t = [i * step for i in range(n)]
    y = [100.0]
    for _ in range(n - 1):
        y.append(y[-1] * math.exp(rnd.gauss(0, 1e-4)))
    x = [y[max(0, i - 4)] for i in range(n)]                                      # X lags Y by 1 s
    ser = {"binance_usdm_book": (t, x), "coinbase_l2": (t, y)}
    r = lead_lag(ser, 0, t[-1], step, 5000, 0.6)["pairs"]["binance_usdm_book|coinbase_l2"]
    assert r["best_lag_ms_discovery"] == -1000 and r["holdout_corr_at_discovery_best"] > 0.9
    assert "coinbase_l2 leads binance_usdm_book" in r["interpretation"]
    # the holdout never influences the chosen lag: scramble it and the choice is unchanged
    split = int(n * 0.6)
    y2 = y[:split] + [100.0 * math.exp(rnd.gauss(0, 1e-3)) for _ in range(n - split)]
    r2 = lead_lag({"binance_usdm_book": (t, x), "coinbase_l2": (t, y2)}, 0, t[-1], step, 5000, 0.6)["pairs"]["binance_usdm_book|coinbase_l2"]
    assert r2["best_lag_ms_discovery"] == -1000 and r2["discovery_corr_at_best"] == r["discovery_corr_at_best"]
    assert r2["holdout_corr_at_discovery_best"] != r["holdout_corr_at_discovery_best"]


def test_subsecond_grid():
    inc, exc = eligible(250)
    assert "cf" in exc and "kalshi_rest" in exc and "coinbase_l2" in inc and "binance_usdm_book" in inc
    assert "cf" in eligible(1000)[0]
    try:
        eligible(300); raise AssertionError("arbitrary step accepted")
    except ValueError:
        pass
    root, c3, c4, c5, s3, p4, m5 = shared()
    evs = list(s3.events) + list(p4.events) + list(m5.events)
    g = subsecond_rows("BTC", evs, C - 120_000, C - 118_000, 250)
    assert len(g["rows"]) == 9 and "cf" in g["excluded_sources"]
    for row in g["rows"]:
        assert all(n.startswith(("micro.coinbase.", "micro.kraken.", "micro.binance.", "micro.bybit.", "micro.okx."))
                   for n in row["values"])
    T = g["rows"][4]["t_ms"]
    ref = compute_at("BTC", evs, T, venues=tuple(g["included_sources"]))
    assert all(ref.values[n] == v for n, v in g["rows"][4]["values"].items())


def test_joint_dataset():
    root, c3, c4, c5, s3, p4, m5 = shared()
    ds = build_joint_dataset(s3, p4, m5, ["BTC"])
    assert ds.rows and ds.labels and len(ds.micro_labels) == len(ds.rows)
    evs = list(s3.events) + list(p4.events) + list(m5.events)
    from market_data.features.dataset import discover_markets
    markets = discover_markets(s3.events, ["BTC"])
    for r in ds.rows[::3]:
        ref = compute_at("BTC", evs, r["checkpoint_ts_ms"], markets[r["market_ticker"]][0])
        assert r["micro_values"] == ref.values, r["checkpoint_ts_ms"]
        assert set(r["micro_values"]) == set(FEATURE_NAMES)
    ds4 = build_research_dataset(s3, p4, ["BTC"])                                  # Step-3 / Step-4 features unchanged by the join
    by = {(r["market_ticker"], r["checkpoint_seconds_remaining"]): r for r in ds4.rows}
    for r in ds.rows:
        o = by[(r["market_ticker"], r["checkpoint_seconds_remaining"])]
        assert r["perp_values"] == o["perp_values"] and r["step3_values"] == o["step3_values"]
    for lab in ds.micro_labels:
        assert lab["available_at_ms"] > lab["checkpoint_ts_ms"]
    pv = ds.provenance
    assert pv["labels_are_separate"] and pv["feeds_production"] is False and pv["feeds_existing_perp_veto"] is False
    assert pv["micro_feature_counts_by_family"] == counts_by_family() and pv["research_only"] is True
    out = tmpdir()
    write_joint_dataset(ds, out)
    assert sorted(os.listdir(out)) == ["features.csv", "labels.jsonl", "micro_labels.jsonl", "provenance.json"]
    head = open(os.path.join(out, "features.csv"), encoding="utf-8").readline().strip().split(",")
    assert tuple(head[:len(KEY_COLS)]) == KEY_COLS and not any(h.startswith("micro_label") or h in ("result", "official_result")
                                                               for h in head)


# ═══════════════════ 30-33 collector CLI, storage, fingerprint, docs ═══════════════════
class FakeWS:
    def __init__(self, msgs):
        self.msgs, self.sent = list(msgs), []

    def send_text(self, t):
        self.sent.append(t)

    def settimeout(self, s):
        pass

    def recv_text(self):
        return self.msgs.pop(0) if self.msgs else None

    def close(self):
        pass


def test_collector_cli():
    code = ("import socket, sys\n"
            "def boom(*a, **k): raise RuntimeError('NETWORK USED')\n"
            "socket.socket.connect = boom; socket.create_connection = boom\n"
            f"sys.path.insert(0, {HERE!r})\n"
            "import collect_research_data as c\n"
            "sys.exit(c.main(['--assets', 'BTC,SOL', '--micro', '--micro-venues', 'coinbase_l2,binance_usdm_book,kalshi_ws', "
            "'--compression-level', '9', '--dry-run', '--output', sys.argv[1]]))\n")
    out = tmpdir()
    fake = {"KALSHI_API_KEY_ID": "KEYID-not-real-88", "CFB_API_SECRET": "cfb-SECRET-not-real-88"}
    p = subprocess.run([sys.executable, "-c", code, out], capture_output=True, text=True, cwd=HERE, env=dict(os.environ, **fake))
    assert p.returncode == 0, p.stdout[-1500:] + p.stderr[-800:]
    s = p.stdout
    assert "book source coinbase_l2" in s and "book source binance_usdm_book" in s and "book source kraken_book" not in s
    assert "BTC-USD, SOL-USD" in s and "sequence policy binance_diff" in s and "gzip level 9" in s
    assert "code verification microstructure: MATCHES" in s and "no network connection was made" in s and os.listdir(out) == []
    for v in fake.values():
        assert v not in s and v not in p.stderr
    for bad in (["--micro-venues", "ftx_book"], ["--segment-mb", "0"], ["--compression-level", "11"]):
        q = subprocess.run([sys.executable, "collect_research_data.py", "--dry-run", "--micro"] + bad, cwd=HERE,
                           capture_output=True, text=True)
        assert q.returncode == 2, bad
    import collect_research_data as crd
    a = crd.parse_args(["--all-research"])
    assert a.cf and a.coinbase and a.secondary and a.kalshi and a.perps and a.micro and len(a.micro_venues) == 6
    a = crd.parse_args(["--perps"])
    assert not a.micro                                                            # Step-4-only runs are unchanged
    # a short combined run against fake transports (no network, read-only)
    from market_data.synthetic import World
    from perp_data.synthetic import PerpWorld
    a = crd.parse_args(["--assets", "BTC", "--micro", "--micro-venues", "coinbase_l2", "--duration", "1.2", "--output", tmpdir(),
                        "--status-port", "0", "--settlement-store", "none", "--compression-level", "1"])
    clock = FakeClock(C - 100_000)
    w = World(["BTC"], C - 900_000, C + 900_000)
    f = VenueFeed("coinbase_l2", ["BTC"], w, PerpWorld(w), 3)
    msgs = f.connect(C - 100_000, 1) + [m for k in range(20) for m in f.step(C - 99_000 + k * 200)]
    g = FakeGetter(lambda url, p: {})
    rc = crd.run(a, clock=clock, getter=g, connect_fn=lambda url, headers=None: FakeWS(list(msgs)), install_signals=False)
    assert rc == 0 and all(r[0] == "GET" for r in g.requests)
    sess = os.path.join(a.output, os.listdir(a.output)[0])
    mm = json.load(open(os.path.join(sess, "micro", "manifest.json")))
    assert mm["status"] == "COMPLETED" and mm["counts"]["totals"]["events"] > 0 and mm["storage_settings"]["compression_level"] == 1
    assert json.load(open(os.path.join(sess, "manifest.json")))["code_verification"]["microstructure"]["ok"] is True


def test_storage_controls():
    d1, d9 = tmpdir(), tmpdir()
    w1 = MicroStoreWriter(d1, compression_level=1, fsync=False, flush_lines=100)
    w9 = MicroStoreWriter(d9, compression_level=9, fsync=False, flush_lines=100)
    for i in range(2000):
        rec = {"source": "x", "i": i, "bids": [[100 + j * 0.1, 1.0 + (i * j) % 7] for j in range(10)]}
        w1.write("event", rec); w9.write("event", rec)
    w1.close(); w9.close()
    assert w9.bytes_compressed < w1.bytes_compressed and w1.bytes_uncompressed == w9.bytes_uncompressed
    assert len(read_session(d1).records) == len(read_session(d9).records) == 2000     # nothing dropped
    t = {"now": 0.0}
    dt_ = tmpdir()
    wt = MicroStoreWriter(dt_, segment_max_s=10, fsync=False, flush_lines=10, clock=lambda: t["now"])
    for i in range(100):
        t["now"] = i * 1.0
        wt.write("event", {"i": i})
    wt.close()
    assert wt.rotations >= 9 and len(read_session(dt_).segments) >= 9 and len(read_session(dt_).records) == 100
    try:
        MicroStoreWriter(tmpdir(), compression_level=0); raise AssertionError("invalid level accepted")
    except ValueError:
        pass
    root = tmpdir()
    import datetime as dtm
    now = dtm.datetime(2026, 9, 25, tzinfo=dtm.timezone.utc)
    for name, days, status in (("old", 30, "COMPLETED"), ("running", 30, "RUNNING"), ("new", 1, "COMPLETED")):
        os.makedirs(os.path.join(root, name, "micro"))
        with open(os.path.join(root, name, "manifest.json"), "w") as f:
            json.dump({"started_utc": (now - dtm.timedelta(days=days)).isoformat(), "status": status}, f)
    os.makedirs(os.path.join(root, "nomanifest"))
    now_ms = int(now.timestamp() * 1000)
    got = prune_sessions(root, 14, now_ms=now_ms, dry_run=True)
    assert [os.path.basename(d) for d, _ in got] == ["old"] and os.path.isdir(os.path.join(root, "old"))
    prune_sessions(root, 14, now_ms=now_ms, dry_run=False)
    assert sorted(os.listdir(root)) == ["new", "nomanifest", "running"]
    est = storage_estimate(3600, 36_000, 60, {"a": 3, "b": 1})
    assert est["compressed_bytes_per_hour"] == 216_000 and est["compression_ratio"] == 10 and est["record_share_by_source"]["a"] == 0.75


def test_microstructure_fingerprint():
    ok, problems = mfp.verify()
    assert ok, problems
    stored = json.load(open(mfp.BASELINE_PATH))
    assert stored["feature_count"] == len(FEATURES) and stored["micro_feature_set_version"] == MICRO_FEATURE_SET_VERSION
    assert stored["production_files"] == mfp.production_hashes()
    tmp = tmpdir()
    pkg = os.path.join(tmp, "microstructure")
    shutil.copytree(os.path.join(HERE, "microstructure"), pkg, ignore=shutil.ignore_patterns("__pycache__"))
    ok2, _ = mfp.verify(pkg_dir=pkg)
    assert ok2                                                                    # an identical copy verifies
    with open(os.path.join(pkg, "reconstruction.py"), "a", encoding="utf-8") as f:
        f.write("\nMUTATED = 1\n")
    ok3, problems3 = mfp.verify(pkg_dir=pkg)
    assert not ok3 and any("reconstruction.py" in p for p in problems3)
    p = subprocess.run([sys.executable, "-m", "microstructure.fingerprint", "--write"], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 2                                                      # refuses without the explicit intent flag


def test_docs_and_outputs():
    doc = open(os.path.join(HERE, "docs", "MICROSTRUCTURE.md"), encoding="utf-8").read()
    for s in ("Order-flow imbalance", "VPIN-STYLE", "REPLENISHMENT_PATTERN", "POST-EVENT", "lead-lag", "no retroactive repair",
              "Step 6", "feature-family evaluation", "sub-second", "Storage", "ESTIMATE"):
        assert s.lower() in doc.lower(), s
    for f in ("ARCHITECTURE.md", "ROADMAP.md"):
        assert "microstructure" in open(os.path.join(HERE, "docs", f), encoding="utf-8").read().lower(), f
    for f in ("README.md", "SETUP.txt"):
        assert "microstructure" in open(os.path.join(HERE, f), encoding="utf-8").read().lower(), f
    assert "microstructure" in open(os.path.join(HERE, "docs", "BASELINE.md"), encoding="utf-8").read().lower()
    perf = json.load(open(os.path.join(HERE, "analysis_output", "microstructure_performance.json")))
    assert perf["synthetic"] is True and "SYNTHETIC" in perf["note"]
    mut = json.load(open(os.path.join(HERE, "analysis_output", "microstructure_mutation_results.json")))
    assert mut["all_caught_and_controls_pass"] is True and len(mut["mutations"]) == 11


def test_performance_smoke():
    root, c3, c4, c5, s3, p4, m5 = shared()
    t0 = time.perf_counter()
    rebuild_books(m5.events)
    dt = time.perf_counter() - t0
    assert len(m5.events) / dt > 5000, f"reconstruction {len(m5.events) / dt:.0f} events/s"
    evs = sorted(list(s3.events) + list(p4.events) + list(m5.events), key=event_key)
    eng = MicroFeatureEngine("BTC", MicroFeatureConfig())
    t0 = time.perf_counter()
    for e in evs:
        eng.ingest(e)
    dt = time.perf_counter() - t0
    assert len(evs) / dt > 2000, f"feature ingest {len(evs) / dt:.0f} events/s"
    t0 = time.perf_counter()
    eng.features_at(evs[-1].receive_ts_ms)
    assert time.perf_counter() - t0 < 2.0


def test_previous_stages():
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 21):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-600:], p.stderr[-600:])


TESTS = [
    ("baselines", "1 Step-1 strategy (47 fixtures, legacy + extended), settlement / market-data / perp-data fingerprints", test_baselines_untouched),
    ("veto", "2 EXISTING perp veto chain byte-identical, INACTIVE, never imports microstructure", test_existing_perp_veto_untouched),
    ("isolation", "3 production isolation: nothing production / earlier-layer imports microstructure; read-only; LIVE refused", test_production_isolation),
    ("audit", "4 audit vocabulary; PRICE-LEVEL depth vs ORDER-LEVEL queue (no queue position anywhere)", test_audit_and_price_level_vs_queue),
    ("book", "5 local book: snapshot, insert / modify / delete, rank, crossed, relative sizes, truncation", test_local_book),
    ("states", "6 book states: NO_BOOK / AWAITING / WARMING_UP / READY / STALE / INVALID; reset + resnapshot", test_book_states_and_resnapshot),
    ("sequence", "7 sequence policies: Coinbase / Kalshi chains, Binance U/u/pu alignment, OKX prevSeqId, Kraken checksum, Bybit", test_sequence_policies),
    ("kalshi", "8 Kalshi YES / NO bid-ask helpers with explicit units; adapter + engine in YES terms", test_kalshi_conventions),
    ("adapters", "9 native message parsing for all six venues; failures recorded, never guessed", test_adapters_parse_native_messages),
    ("collector", "10 collector: raw first, connection numbers, resnapshot on gap, BOOK_RESET, GET-only snapshots, overload recorded", test_collector_resnapshot_and_raw_first),
    ("faults", "11 injected faults per venue detected + recovered; replay == live; renormalize identical", test_fault_injection_and_recovery),
    ("replay", "12 deterministic replay (digests) + RAW -> normalization reproducible", test_replay_deterministic),
    ("registry", "13 feature registry: ten families, controlled count, estimates / removed-depth / VPIN wording", test_feature_registry),
    ("features", "14 book features: mid, spread, microprice, depth, imbalance, gaps, concentration, slope, liquidity", test_book_features_math),
    ("ofi", "15 OFI (L1, 5-level), depth added / removed, net change, cancel ESTIMATE", test_ofi_and_depth_deltas),
    ("intensity", "16 trade intensity + causal large-trade threshold", test_trade_intensity_and_large_trades),
    ("sweeps", "17 evidence-based sweeps + REPLENISHMENT_PATTERN", test_sweeps_and_replenishment),
    ("toxicity", "18 VPIN-style readiness, trade-sign autocorrelation, causal adverse move, liquidity vacuum", test_vacuum_toxicity_adverse),
    ("missing", "19 missing book -> None + MISSING (never 0), warm-up, stale, OKX unverified units", test_missing_and_warmup),
    ("causality", "20 receive-time causality; streaming == batch; guards", test_receive_time_causality),
    ("retro", "21 no retroactive repair after a resnapshot", test_no_retroactive_repair),
    ("leakage", "22 LEAKAGE: future events / canaries invisible; labels only after T", test_no_leakage),
    ("labels", "23 post-event price-impact labels (250 ms / 500 ms / 1 s / 5 s), separate from features", test_post_event_labels),
    ("leadlag", "24 offline lead-lag: best lag chosen on discovery only", test_lead_lag_offline),
    ("grid", "25 sub-second grids: slow sources excluded, never forward-filled; causal", test_subsecond_grid),
    ("dataset", "26 joint Step 2-5 dataset == batch paths; earlier features unchanged; labels separate", test_joint_dataset),
    ("cli", "27 dry run (no network / no writes / no secrets), flags, combined run with fake transports", test_collector_cli),
    ("storage", "28 storage controls: compression level, rotation, nothing dropped, retention, estimate", test_storage_controls),
    ("fingerprint", "29 separate microstructure fingerprint; detects module changes; refuses silent rewrites", test_microstructure_fingerprint),
    ("docs", "30 docs + benchmark / mutation outputs", test_docs_and_outputs),
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
        print("\nAll Stage 21 tests passed.")
    else:
        print(f"\nSelected Stage 21 tests passed: {','.join(sorted(only))}")
