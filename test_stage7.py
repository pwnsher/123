#!/usr/bin/env python3
"""Stage 7 tests: PASSIVE perp telemetry (Step 1). No network. Run:  py test_stage7.py

Every patch made here is restored afterwards, so this file can run in the same
process as the other stage tests."""
import csv
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import types
from contextlib import contextmanager

import requests

import kalshi_dashboard as k
import perp_telemetry as pt

CFG = {"series": "KXTEST", "product": "TEST-USD", "sl_pct": 0.75}
FIXED = dt.datetime(2026, 9, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
STRATEGY_FIELDS = ("signal", "fav", "conf", "p_up", "raw_edge", "net_edge", "side_ask", "rec_stop", "reason")


# ───────────────────────── helpers ─────────────────────────
class _FrozenDT(dt.datetime):
    @classmethod
    def now(cls, tz=None):
        return FIXED if tz else FIXED.replace(tzinfo=None)


FROZEN_DT = types.SimpleNamespace(datetime=_FrozenDT, timezone=dt.timezone, timedelta=dt.timedelta)


@contextmanager
def patched(obj, **attrs):
    old = {a: getattr(obj, a) for a in attrs}
    try:
        for a, v in attrs.items():
            setattr(obj, a, v)
        yield
    finally:
        for a, v in old.items():
            setattr(obj, a, v)


def _market(strike, up_ask, dn_ask, mins):
    ct = (FIXED + dt.timedelta(minutes=mins)).isoformat().replace("+00:00", "Z")
    return {"ticker": "KXGOLD-1", "close_time": ct, "floor_strike": strike,
            "yes_ask_dollars": f"{up_ask/100:.4f}", "yes_bid_dollars": f"{(up_ask-2)/100:.4f}",
            "no_ask_dollars": f"{dn_ask/100:.4f}", "no_bid_dollars": f"{(dn_ask-2)/100:.4f}"}


CALM = [100.0] * 200 + [100.30 + (0.01 if i % 2 else 0.0) for i in range(62)]
NOISY = [100.0 * (1 + 0.0012 * ((i * 7) % 5 - 2) / 2) for i in range(262)]


@contextmanager
def binary_inputs(market, px, closes):
    with patched(k, dt=FROZEN_DT, current_market=lambda s: market, spot=lambda p: px,
                 candles=lambda p: {"close": closes, "high": [c * 1.0005 for c in closes],
                                    "low": [c * 0.9995 for c in closes]},
                 DIRECTION="BOTH", ACTIVE_COINS={"BTC": True, "ETH": True, "SOL": True, "XRP": True}):
        yield


def _snap(coin="BTC", ts=1000.0, mid=100.0, idx=100.0, funding=0.0001, status=pt.STATUS_FRESH, idx_ts=None):
    return pt.PerpSnapshot(ts=ts, coin=coin, perp_symbol=f"X{coin}PERP", perp_bid=None if mid is None else mid - 0.01,
                           perp_ask=None if mid is None else mid + 0.01, perp_mid=mid, perp_last=mid, perp_mark=mid,
                           index_price=idx, index_ts_ms=int((idx_ts if idx_ts is not None else ts) * 1000),
                           funding_rate=funding, source_status=status)


class StaticProvider(pt.PerpProvider):
    def __init__(self, fn): self.fn = fn
    def snapshot(self, coin): return self.fn(coin)


class RaisingProvider(pt.PerpProvider):
    def snapshots(self, coins): raise requests.RequestException("simulated Kalshi outage")
    def snapshot(self, coin): raise requests.RequestException("simulated Kalshi outage")


def _clock(t):
    return lambda: t[0]


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except AssertionError as e:
        print(f"FAIL  {name}: {e}"); raise


# ───────────────────────── 1: premium ─────────────────────────
def test_premium():
    assert abs(pt.premium_bps(100100, 100000) - 10.0) < 1e-9
    assert abs(pt.premium_bps(99900, 100000) - (-10.0)) < 1e-9
    assert pt.premium_bps(100000, 100000) == 0.0


# ───────────────────────── 2: 30/60/180 returns by timestamp ─────────────────────────
def test_returns_use_timestamps():
    h = pt.PriceHistory(maxlen=1000, max_age_s=1000)
    # irregular cadence (3..6 s jitter + one 9 s dropout); price = 100 + 0.01 * t
    t, times = 0.0, []
    gaps = [3, 5, 4, 6, 9, 3, 4, 5]
    i = 0
    while t <= 400:
        times.append(t); h.append(t, 100 + 0.01 * t)
        t += gaps[i % len(gaps)]; i += 1
    now_t, now_px = times[-1], 100 + 0.01 * times[-1]
    for hz in (30, 60, 180):
        target = now_t - hz
        exp_t = max(x for x in times if x <= target)          # newest point at/before target
        exp = (now_px / (100 + 0.01 * exp_t) - 1) * 1e4
        got = h.return_bps(hz, tolerance_s=10)
        assert got is not None and abs(got - exp) < 1e-9, (hz, got, exp)
    # the sample-count shortcut would be wrong here: history[-N] is NOT N*4 seconds ago
    naive = (now_px / h._obs[-16][1] - 1) * 1e4
    assert abs(naive - h.return_bps(60, 10)) > 1e-6


# ───────────────────────── 3: lead = perp - spot ─────────────────────────
def test_lead():
    t = [1000.0]
    tel = pt.PerpTelemetry(StaticProvider(lambda c: None), ["BTC"], clock=_clock(t))
    rows = None
    for step in range(0, 200, 4):
        now = 1000.0 + step
        t[0] = now
        perp = 100.0 * (1 + 0.0002 * step)        # perp rises faster than spot
        spot = 50.0 * (1 + 0.00005 * step)
        rows = tel.record_cycle({"BTC": {"status": "ok", "spot_raw": spot}}, {"BTC": now},
                                snapshots={"BTC": _snap(ts=now, mid=perp, idx=perp)}, now=now)
    r = rows["BTC"]
    for hz in (30, 60, 180):
        p, s, l = r[f"perp_ret_{hz}s_bps"], r[f"spot_ret_{hz}s_bps"], r[f"lead_{hz}s_bps"]
        assert None not in (p, s, l), (hz, p, s, l)
        assert abs(l - round(p - s, 4)) <= 2e-4, (hz, p, s, l)
        assert p > s > 0


# ───────────────────────── 4: no future leakage ─────────────────────────
def test_no_future_leakage():
    h = pt.PriceHistory(1000, 1000)
    for ts, px in ((60.0, 1.0), (69.0, 2.0), (71.0, 999.0), (100.0, 3.0)):
        h.append(ts, px)
    # target 70: 71 is equally close but in the FUTURE of the target -> must pick 69
    assert h.at_or_before(70.0, 10) == (69.0, 2.0)
    assert abs(h.return_bps(30, 10) - (3.0 / 2.0 - 1) * 1e4) < 1e-9
    h2 = pt.PriceHistory(1000, 1000)
    for ts, px in ((70.0, 5.0), (70.5, 999.0), (100.0, 10.0)):
        h2.append(ts, px)
    assert h2.at_or_before(70.0, 10) == (70.0, 5.0)          # exactly-at-target is allowed
    # streaming vs. hindsight: a value computed at step i never changes when later data arrives
    obs = [(float(i * 4 + (i % 3)), 100 + ((i * 37) % 11)) for i in range(120)]
    live, streamed = pt.PriceHistory(1000, 1000), []
    for ts, px in obs:
        live.append(ts, px); streamed.append(live.return_bps(60, 10))
    for i in range(len(obs)):
        replay = pt.PriceHistory(1000, 1000)
        for ts, px in obs[:i + 1]:
            replay.append(ts, px)
        assert replay.return_bps(60, 10) == streamed[i], i


# ───────────────────────── 5: incomplete history ─────────────────────────
def test_incomplete_history():
    t = [5000.0]
    tel = pt.PerpTelemetry(StaticProvider(lambda c: None), ["BTC"], clock=_clock(t))
    row = None
    for s in range(0, 11, 2):                      # only 10 s of history
        now = 5000.0 + s; t[0] = now
        row = tel.record_cycle({"BTC": {"status": "ok", "spot_raw": 100 + s}}, {"BTC": now},
                               snapshots={"BTC": _snap(ts=now, mid=200 + s, idx=200)}, now=now)["BTC"]
    for hz in (30, 60, 180):
        for kind in ("perp_ret", "spot_ret", "lead"):
            assert row[f"{kind}_{hz}s_bps"] is None, (kind, hz, row[f"{kind}_{hz}s_bps"])
    # a gap wider than the tolerance is also None, not a fabricated long-horizon return
    h = pt.PriceHistory(100, 1000)
    h.append(0.0, 100.0); h.append(100.0, 110.0)
    assert h.return_bps(60, 10) is None


# ───────────────────────── 6: missing index ─────────────────────────
def test_missing_index():
    assert pt.premium_bps(100.0, None) is None
    assert pt.premium_bps(100.0, 0.0) is None
    assert pt.premium_bps(None, 100.0) is None
    tel = pt.PerpTelemetry(StaticProvider(lambda c: None), ["BTC"], clock=lambda: 10.0)
    row = tel.record_cycle({"BTC": {}}, {}, snapshots={"BTC": _snap(ts=10.0, idx=None)}, now=10.0)["BTC"]
    assert row["premium_bps"] is None and row["index_price"] is None
    # parser: zero / garbage index -> None, not 0
    assert pt._price("0") is None and pt._price("abc") is None and pt._price("nan") is None


# ───────────────────────── 7: provider failure can't break evaluation ─────────────────────────
def test_provider_failure_isolated():
    with binary_inputs(_market(100.0, 88, 12, 6.0), 100.305, CALM):
        tel = pt.PerpTelemetry(RaisingProvider(), list(k.COINS), clock=time.time)
        r = k.evaluate("BTC", CFG)
        rows = tel.record_cycle({"BTC": r}, {"BTC": time.time()})     # must not raise
        assert rows["BTC"]["source_status"] == pt.STATUS_ERROR, rows["BTC"]
        assert "outage" in rows["BTC"]["source_error"]
        r2 = k.evaluate("BTC", CFG)
        assert r2["status"] == "ok" and r2["signal"] is True and r2["reason"] == "ENTER", r2
        # the real KalshiPerpProvider surfaces transport errors as status, never raises
        def boom(*a, **kw): raise requests.Timeout("timed out")
        prov = pt.KalshiPerpProvider(http_get=boom, clock=time.time)
        snaps = prov.snapshots(["BTC", "ETH", "SOL", "XRP"])
        assert all(s.source_status == pt.STATUS_ERROR for s in snaps.values())
    # full poller cycle with an exploding provider: binary results are still published
    out = _one_poller_cycle(RaisingProvider())
    assert out["coins"]["BTC"]["signal"] is True and out["coins"]["BTC"]["reason"] == "ENTER"
    assert out["perps"]["BTC"]["source_status"] == pt.STATUS_ERROR


# ───────────────────────── 8: telemetry does not change strategy ─────────────────────────
def _bull(coin, ts): return _snap(coin, ts=ts, mid=150.0, idx=100.0, funding=0.02)     # +5000bp prem
def _bear(coin, ts): return _snap(coin, ts=ts, mid=50.0, idx=100.0, funding=-0.02)     # -5000bp prem


def _extreme_telemetry(kind):
    t = [2000.0]
    tel = pt.PerpTelemetry(StaticProvider(lambda c: None), list(k.COINS), clock=_clock(t))
    for s in range(0, 200, 4):                      # extreme perp trend history
        t[0] = 2000.0 + s
        f = (1 + 0.01 * s) if kind == "bull" else (1 - 0.004 * s)
        tel.record_cycle({c: {} for c in k.COINS}, {},
                         snapshots={c: _snap(c, ts=t[0], mid=100 * f, idx=100) for c in k.COINS}, now=t[0])
    return tel, t


def test_telemetry_does_not_change_strategy():
    cases = [(_market(100.0, 88, 12, 6.0), 100.305, CALM), (_market(100.0, 99, 3, 6.0), 100.305, CALM),
             (_market(100.6, 14, 88, 5.0), 100.305, CALM), (_market(100.0, 80, 22, 6.0), 100.12, NOISY),
             (_market(100.2, 30, 72, 4.0), 100.05, NOISY)]
    for m, px, closes in cases:
        with binary_inputs(m, px, closes):
            with patched(k, PERP_TELEMETRY_ENABLED=False):
                base = k.evaluate("BTC", CFG)
            outs = []
            for kind in ("bull", "bear"):
                tel, t = _extreme_telemetry(kind)
                r = k.evaluate("BTC", CFG)
                before = json.dumps(r, sort_keys=True, default=str)
                snap = (_bull if kind == "bull" else _bear)("BTC", t[0] + 4)
                row = tel.record_cycle({"BTC": r}, {"BTC": t[0] + 4},
                                       snapshots={"BTC": snap}, now=t[0] + 4)["BTC"]
                assert abs(abs(row["premium_bps"]) - 5000) < 1e-6, row["premium_bps"]
                assert json.dumps(r, sort_keys=True, default=str) == before, "telemetry mutated the result"
                with k.LOCK:
                    saved_perps = k.STATE.get("perps")
                    k.STATE["perps"] = {c: dict(row, coin=c) for c in k.COINS}   # extreme values visible in STATE
                try:
                    r_after = k.evaluate("BTC", CFG)
                finally:
                    with k.LOCK:
                        k.STATE["perps"] = saved_perps
                outs += [r, r_after]
            for o in outs:
                for f in STRATEGY_FIELDS + ("verdict", "edge"):
                    assert o[f] == base[f], (f, o[f], base[f])
    # and through the REAL poller loop, disabled vs bullish vs bearish
    for fx in POLLER_FIXTURES:
        _poller_modes_identical(fx)


def _poller_modes_identical(fx):
    # order matters: each run leaves its STATE["perps"] behind for the next run's evaluate()
    res = {}
    for mode, prov in (("disabled", None), ("bull", StaticProvider(lambda c: _bull(c, time.time()))),
                       ("bear", StaticProvider(lambda c: _bear(c, time.time()))),
                       ("bull2", StaticProvider(lambda c: _bull(c, time.time())))):
        res[mode] = _one_poller_cycle(prov, enabled=(mode != "disabled"), fixture=fx)
    for c in k.COINS:
        ref = {f: res["disabled"]["coins"][c][f] for f in STRATEGY_FIELDS}
        for mode in ("bull", "bear", "bull2"):
            got = {f: res[mode]["coins"][c][f] for f in STRATEGY_FIELDS}
            assert got == ref, (c, mode, got, ref)
    for mode in ("bull", "bear", "bull2"):
        assert res[mode]["alerted"] == res["disabled"]["alerted"], mode
        assert res[mode]["paper_rows"] == res["disabled"]["paper_rows"], mode
    assert res["disabled"]["perps"]["BTC"]["source_status"] == "disabled"
    assert res["bull"]["perps"]["BTC"]["premium_bps"] > 4999


# ───────────────────────── 9: stale telemetry ─────────────────────────
def test_stale():
    tel = pt.PerpTelemetry(StaticProvider(lambda c: None), ["BTC"], stale_seconds=15, clock=lambda: 1000.0)
    row = tel.record_cycle({"BTC": {}}, {}, snapshots={"BTC": _snap(ts=940.0)}, now=1000.0)["BTC"]
    assert row["source_status"] == pt.STATUS_STALE and row["perp_data_age_ms"] == 60000, row
    assert row["premium_bps"] is None and len(tel.perp_hist["BTC"]) == 0   # stale data is not fed into history
    row2 = tel.record_cycle({"BTC": {}}, {}, snapshots={"BTC": _snap(ts=999.0, idx_ts=900.0)}, now=1000.0)["BTC"]
    assert row2["source_status"] == pt.STATUS_STALE and row2["index_age_ms"] == 100000, row2
    row3 = tel.record_cycle({"BTC": {}}, {}, snapshots={"BTC": _snap(ts=999.0)}, now=1000.0)["BTC"]
    assert row3["source_status"] == pt.STATUS_FRESH
    with binary_inputs(_market(100.0, 88, 12, 6.0), 100.305, CALM):
        r = k.evaluate("BTC", CFG)
        assert r["signal"] is True and r["reason"] == "ENTER"
    out = _one_poller_cycle(StaticProvider(lambda c: _snap(c, ts=time.time() - 120)))
    assert out["perps"]["BTC"]["source_status"] == pt.STATUS_STALE
    assert out["coins"]["BTC"]["signal"] is True


# ───────────────────────── 10: CSV schema ─────────────────────────
def test_csv():
    d = tempfile.mkdtemp(prefix="_t7csv")
    try:
        path = os.path.join(d, "perp.csv")
        t = [100.0]
        tel = pt.PerpTelemetry(StaticProvider(lambda c: None), list(k.COINS), log_path=path, clock=_clock(t))
        for i in range(3):
            t[0] = 100.0 + 4 * i
            tel.record_cycle({c: {"status": "ok", "spot_raw": 10.0, "p_up": 83.0, "signal": False} for c in k.COINS},
                             {c: t[0] for c in k.COINS},
                             snapshots={c: _snap(c, ts=t[0], idx=None, funding=None) for c in k.COINS}, now=t[0])
        text = open(path).read()
        lines = text.strip().splitlines()
        assert lines[0].split(",") == pt.CSV_COLUMNS
        assert text.count("ts_utc,") == 1, "duplicate header"
        rows = list(csv.DictReader(open(path)))
        assert len(rows) == 3 * len(k.COINS)
        required = {"ts_utc", "ts_epoch_ms", "coin", "binary_ticker", "binary_close_time", "minutes_left",
                    "spot_price", "strike", "base_p_up", "base_conf", "fav", "side_ask", "raw_edge", "net_edge",
                    "binary_signal", "binary_reason", "perp_symbol", "perp_bid", "perp_ask", "perp_mid",
                    "perp_mark", "index_price", "premium_bps", "funding_rate",
                    "perp_ret_30s_bps", "perp_ret_60s_bps", "perp_ret_180s_bps",
                    "spot_ret_30s_bps", "spot_ret_60s_bps", "spot_ret_180s_bps",
                    "lead_30s_bps", "lead_60s_bps", "lead_180s_bps",
                    "perp_data_age_ms", "source_status", "source_error"}
        assert required <= set(rows[0]), required - set(rows[0])
        assert rows[0]["funding_rate"] == "" and rows[0]["premium_bps"] == "" and rows[0]["index_price"] == ""
        assert "None" not in text
        # funding of exactly zero is DATA, and must survive as 0.0 (not blank)
        tel.record_cycle({"BTC": {}}, {}, snapshots={"BTC": _snap(ts=t[0] + 1, funding=0.0)}, now=t[0] + 1)
        last = [r for r in csv.DictReader(open(path)) if r["coin"] == "BTC"][-1]
        assert last["funding_rate"] == "0.0", last["funding_rate"]
        # a file with a different (old) header is rotated aside, never appended to
        with open(path, "w") as f:
            f.write("old,header\n1,2\n")
        pt.TelemetryCSVLogger(path).write_rows([{"coin": "BTC"}])
        assert open(path).readline().strip().split(",") == pt.CSV_COLUMNS
        assert any(n.startswith("perp.csv.schema-") for n in os.listdir(d))
    finally:
        shutil.rmtree(d, ignore_errors=True)


# ───────────────────────── 11: bounded history ─────────────────────────
def test_bounded():
    h = pt.PriceHistory(maxlen=500, max_age_s=10 ** 9)
    for i in range(20000):
        h.append(float(i), 100.0 + i)
    assert len(h) == 500 and h.maxlen == 500
    h2 = pt.PriceHistory(maxlen=10 ** 6, max_age_s=250)
    for i in range(20000):
        h2.append(float(i), 1.0)
    assert len(h2) <= 251, len(h2)
    tel = pt.PerpTelemetry(StaticProvider(lambda c: None), ["BTC"], history_maxlen=300)
    for i in range(5000):
        tel.record_cycle({"BTC": {"spot_raw": 1.0}}, {"BTC": float(i)},
                         snapshots={"BTC": _snap(ts=float(i))}, now=float(i))
    assert len(tel.perp_hist["BTC"]) <= 300 and len(tel.spot_hist["BTC"]) <= 300
    assert tel._q.maxsize == 1        # hand-off queue is bounded too


# ───────────────────────── extra: p_up is the SAME model probability ─────────────────────────
def test_p_up_consistent():
    for m, px, closes in ((_market(100.0, 80, 22, 6.0), 100.12, NOISY), (_market(100.2, 30, 72, 4.0), 100.05, NOISY),
                          (_market(100.0, 55, 47, 7.0), 100.01, NOISY), (_market(100.0, 88, 12, 6.0), 100.305, CALM)):
        with binary_inputs(m, px, closes):
            r = k.evaluate("BTC", CFG)
        assert 0.0 <= r["p_up"] <= 100.0
        assert r["fav"] == ("UP" if r["p_up"] >= 50 else "DOWN")
        assert abs(r["conf"] - round(max(r["p_up"], 100 - r["p_up"]), 1)) <= 0.051, r


# ───────────────────────── extra: golden fixture from the ORIGINAL (pre-Step-1) code ─────────────────────────
# Captured by running the unmodified kalshi_dashboard.evaluate() with frozen time.
GOLDEN = {
    "up_enter": {"signal": True, "fav": "UP", "conf": 100.0, "raw_edge": 12.0, "net_edge": 10.0, "side_ask": 88.0, "rec_stop": 66, "reason": "ENTER", "verdict": "ENTER UP", "remain": 6.0, "spot": 100.31, "strike": 100.0, "edge": 10.0},
    "up_no_edge": {"signal": False, "fav": "UP", "conf": 100.0, "raw_edge": 1.0, "net_edge": -1.0, "side_ask": 99.0, "rec_stop": 74, "reason": "WAIT_NO_EDGE", "verdict": "Wait UP \u00b7 net edge -1.0\u00a2", "remain": 6.0, "spot": 100.31, "strike": 100.0, "edge": -1.0},
    "down": {"signal": True, "fav": "DOWN", "conf": 100.0, "raw_edge": 12.0, "net_edge": 10.0, "side_ask": 88.0, "rec_stop": 66, "reason": "ENTER", "verdict": "ENTER DOWN", "remain": 5.0, "spot": 100.31, "strike": 100.6, "edge": 10.0},
    "too_early": {"signal": False, "fav": "UP", "conf": 100.0, "raw_edge": 12.0, "net_edge": 10.0, "side_ask": 88.0, "rec_stop": 66, "reason": "WAIT_TOO_EARLY", "verdict": "Wait UP \u00b7 too early", "remain": 12.0, "spot": 100.31, "strike": 100.0, "edge": 10.0},
    "mid_up": {"signal": False, "fav": "UP", "conf": 61.3, "raw_edge": -18.7, "net_edge": -20.7, "side_ask": 80.0, "rec_stop": 60, "reason": "BLOCK_CHOP", "verdict": "Sit out \u00b7 choppy", "remain": 6.0, "spot": 100.12, "strike": 100.0, "edge": -20.7},
    "mid_down": {"signal": False, "fav": "DOWN", "conf": 67.0, "raw_edge": -5.0, "net_edge": -7.0, "side_ask": 72.0, "rec_stop": 54, "reason": "WAIT_LOW_CONFIDENCE", "verdict": "Wait DOWN \u00b7 67% (needs 80%)", "remain": 4.0, "spot": 100.05, "strike": 100.2, "edge": -7.0},
    "near_strike": {"signal": False, "fav": "UP", "conf": 50.9, "raw_edge": -4.1, "net_edge": -6.1, "side_ask": 55.00000000000001, "rec_stop": 41, "reason": "BLOCK_CHOP", "verdict": "Sit out \u00b7 choppy", "remain": 7.0, "spot": 100.01, "strike": 100.0, "edge": -6.1},
}
GOLDEN_INPUTS = {
    "up_enter": ((100.0, 88, 12, 6.0), 100.305, CALM), "up_no_edge": ((100.0, 99, 3, 6.0), 100.305, CALM),
    "down": ((100.6, 14, 88, 5.0), 100.305, CALM), "too_early": ((100.0, 88, 12, 12.0), 100.305, CALM),
    "mid_up": ((100.0, 80, 22, 6.0), 100.12, NOISY), "mid_down": ((100.2, 30, 72, 4.0), 100.05, NOISY),
    "near_strike": ((100.0, 55, 47, 7.0), 100.01, NOISY),
}


def _golden_hash(d):
    return hashlib.sha256(json.dumps(d, sort_keys=True).encode()).hexdigest()


def test_golden_unchanged():
    got = {}
    for name, (margs, px, closes) in GOLDEN_INPUTS.items():
        with binary_inputs(_market(*margs), px, closes):
            r = k.evaluate("BTC", CFG)
        got[name] = {f: r[f] for f in GOLDEN[name]}
    for name in GOLDEN:
        assert got[name] == GOLDEN[name], (name, got[name], GOLDEN[name])
    assert _golden_hash(got) == _golden_hash(GOLDEN)


# ───────────────────────── extra: real provider vs. documented schema (no network) ─────────────────────────
class _Resp:
    def __init__(self, code, body): self.status_code, self._b = code, body
    def raise_for_status(self):
        if self.status_code >= 400: raise requests.HTTPError(f"{self.status_code} error")
    def json(self):
        if isinstance(self._b, Exception): raise self._b
        return self._b


def _doc_market(ticker, bid, ask, ref, ts_ms, status="active"):
    # field names/shapes exactly as in perps_openapi.yaml MarginMarket
    return {"ticker": ticker, "title": ticker, "status": status, "contract_size": "0.000100",
            "underlying_multiplier": "1", "tick_size": "0.01", "fractional_trading_enabled": True,
            "schedule": None, "exchange_index": 0, "price": f"{(bid + ask) / 2:.4f}",
            "bid": f"{bid:.4f}", "ask": f"{ask:.4f}",
            "settlement_mark_price": {"price": f"{ref:.4f}", "ts_ms": ts_ms},
            "reference_price": {"price": f"{ref:.4f}", "ts_ms": ts_ms}}


def test_kalshi_provider_parsing_and_rate():
    now = [1_800_000_000.0]
    calls = []
    markets = [_doc_market("KXBTCPERP", 10.00, 10.02, 10.00, int(now[0] * 1000)),
               _doc_market("KXETHPERP", 0.30, 0.3002, 0.30, int(now[0] * 1000)),
               _doc_market("KXXRPPERP", 0.0002, 0.0002, 0.0002, int(now[0] * 1000))]
    funding = {"KXBTCPERP": {"market_ticker": "KXBTCPERP", "funding_rate": 0.0,
                             "next_funding_time": "2027-01-01T00:00:00Z", "computed_time": "x"},
               "KXETHPERP": {"market_ticker": "KXETHPERP", "next_funding_time": "2027-01-01T00:00:00Z"}}

    def fake_get(url, params=None, timeout=None):
        calls.append((url, dict(params or {})))
        if url.endswith("/margin/markets"):
            return _Resp(200, {"markets": markets})
        if url.endswith("/margin/funding_rates/estimate"):
            b = funding.get(params["ticker"])
            return _Resp(200, b) if b else _Resp(400, {"code": "bad"})
        return _Resp(404, {})

    p = pt.KalshiPerpProvider(http_get=fake_get, clock=_clock(now), sample_seconds=4, funding_refresh_seconds=60)
    s = p.snapshots(["BTC", "ETH", "SOL", "XRP"])
    assert s["BTC"].source_status == pt.STATUS_FRESH and s["BTC"].perp_symbol == "KXBTCPERP"
    assert abs(s["BTC"].perp_mid - 10.01) < 1e-12 and s["BTC"].index_price == 10.0
    assert s["BTC"].funding_rate == 0.0, "a documented zero funding rate must stay 0.0"
    assert s["ETH"].funding_rate is None and "funding_rate missing" in s["ETH"].source_error
    assert s["SOL"].source_status == pt.STATUS_UNAVAILABLE and "no perp market" in s["SOL"].source_error
    assert s["BTC"].contract_size == 0.0001
    assert sum(1 for u, _ in calls if u.endswith("/margin/markets")) == 1, "one markets call for all coins"
    n = len(calls)
    now[0] += 2; p.snapshots(["BTC", "ETH", "SOL", "XRP"])
    assert len(calls) == n, "within sample_seconds everything is cached"
    now[0] += 3; p.snapshots(["BTC", "ETH", "SOL", "XRP"])
    new = calls[n:]
    assert [u for u, _ in new if u.endswith("/markets")] and not [u for u, _ in new if "funding" in u
                                                                   and _[ "ticker"] == "KXBTCPERP"], new
    for u, _ in calls:
        assert u.startswith("https://external-api.kalshi.com/trade-api/v2/margin/"), u
    # HTTP 401 / malformed body / crossed book -> status, not exception, not fake numbers
    p401 = pt.KalshiPerpProvider(http_get=lambda *a, **kw: _Resp(401, {}), clock=_clock(now))
    assert p401.snapshot("BTC").source_status == pt.STATUS_ERROR
    pbad = pt.KalshiPerpProvider(http_get=lambda *a, **kw: _Resp(200, {"oops": 1}), clock=_clock(now))
    assert "malformed" in pbad.snapshot("BTC").source_error
    pjson = pt.KalshiPerpProvider(http_get=lambda *a, **kw: _Resp(200, ValueError("bad json")), clock=_clock(now))
    assert pjson.snapshot("BTC").source_status == pt.STATUS_ERROR
    crossed = [_doc_market("KXBTCPERP", 10.05, 10.00, 10.0, 1)]
    pc = pt.KalshiPerpProvider(http_get=lambda u, **kw: _Resp(200, {"markets": crossed} if u.endswith("markets")
                                                              else {"funding_rate": 1e-4}), clock=_clock(now))
    sc = pc.snapshot("BTC")
    assert sc.perp_mid is None and "no two-sided book" in sc.source_error
    # ambiguous tickers are refused rather than guessed; overrides resolve them
    amb = [_doc_market("KXBTCPERP", 1, 1.1, 1, 1), _doc_market("KXBTCPERP2", 1, 1.1, 1, 1)]
    pa = pt.KalshiPerpProvider(http_get=lambda u, **kw: _Resp(200, {"markets": amb}), clock=_clock(now))
    assert "ambiguous" in pa.snapshot("BTC").source_error
    po = pt.KalshiPerpProvider(ticker_overrides={"BTC": "KXBTCPERP2"},
                               http_get=lambda u, **kw: _Resp(200, {"markets": amb} if u.endswith("markets")
                                                              else {"funding_rate": 1e-4}), clock=_clock(now))
    assert po.snapshot("BTC").perp_symbol == "KXBTCPERP2"


def test_backoff_limits_requests():
    now = [0.0]; count = [0]
    def fail(*a, **kw):
        count[0] += 1; raise requests.ConnectionError("down")
    p = pt.KalshiPerpProvider(http_get=fail, clock=_clock(now), error_backoff_seconds=15)
    for i in range(20):                      # 20 poll cycles over 76 s
        now[0] = i * 4.0; p.snapshots(["BTC", "ETH", "SOL", "XRP"])
    assert count[0] <= 6, count[0]


def test_read_only_static():
    import ast
    tree = ast.parse(open(pt.__file__).read())
    docstrings = {id(n.body[0].value) for n in ast.walk(tree)
                  if isinstance(n, (ast.Module, ast.ClassDef, ast.FunctionDef)) and n.body
                  and isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Constant)}
    literals = [n.value for n in ast.walk(tree)
                if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in docstrings]
    paths = [x for x in literals if x.startswith("/") and len(x) > 1]
    assert set(paths) <= {"/margin/markets", "/margin/funding_rates/estimate"}, paths
    for x in literals:
        assert not re.search(r"order|transfer|portfolio|leverage|subaccount", x, re.I) or "set PERP_TICKERS" in x, x
    called = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert not called & {"post", "put", "delete", "patch", "request"}, called & {"post", "put", "delete", "patch"}
    assert not re.search(r"(api[_-]?key|secret|password)\s*=\s*['\"][^'\"]+['\"]", open(pt.__file__).read(), re.I)
    # evaluate() and sizing do not reference telemetry at all
    import inspect
    for fn in (k.evaluate, k._size_fraction, k._contracts, k.log_call, k.build_call_embed, k.settle_calls):
        assert "perp" not in inspect.getsource(fn).lower(), fn.__name__
    bt = open(os.path.join(os.path.dirname(k.__file__), "kalshi_backtest.py")).read()
    assert "perp" not in bt.lower()


def test_submit_never_blocks_poller():
    class Slow(pt.PerpProvider):
        def snapshot(self, coin):
            time.sleep(1.5); return _snap(coin, ts=time.time())
    tel = pt.PerpTelemetry(Slow(), ["BTC"])
    t0 = time.time()
    for _ in range(5):
        tel.submit({"BTC": {"status": "ok"}}, {"BTC": time.time()})
    assert time.time() - t0 < 0.2, "submit must be non-blocking"


def test_contract_rescale_resets_history():
    tel = pt.PerpTelemetry(StaticProvider(lambda c: None), ["BTC"])
    for i in range(10):
        s = _snap(ts=float(i * 4)); s.contract_size, s.underlying_multiplier = 0.0001, 1.0
        tel.record_cycle({"BTC": {}}, {}, snapshots={"BTC": s}, now=float(i * 4))
    assert len(tel.perp_hist["BTC"]) == 10
    s = _snap(ts=40.0); s.contract_size, s.underlying_multiplier = 0.001, 1.0
    tel.record_cycle({"BTC": {}}, {}, snapshots={"BTC": s}, now=40.0)
    assert len(tel.perp_hist["BTC"]) == 1


# ───────────────────────── one real poller() cycle, fully sandboxed ─────────────────────────
class _StopPoller(BaseException):
    pass


POLLER_FIXTURES = [((100.0, 88, 12, 6.0), 100.305, CALM),      # saturated, ENTER
                   ((100.0, 80, 22, 6.0), 100.12, NOISY),      # mid confidence (~61%)
                   ((100.2, 30, 72, 4.0), 100.05, NOISY)]      # mid confidence DOWN (~67%)


def _one_poller_cycle(provider, enabled=True, fixture=None):
    margs, px, closes = fixture or POLLER_FIXTURES[0]
    d = tempfile.mkdtemp(prefix="_t7poll")
    done = threading.Event()
    published = {}
    def publish(rows):
        published.update(rows)
        with k.LOCK:
            k.STATE["perps"] = rows
        done.set()
    def stop_sleep(_s): raise _StopPoller()
    tel = pt.PerpTelemetry(provider, list(k.COINS), log_path=os.path.join(d, "perp.csv"),
                           on_rows=publish) if provider is not None else None
    fake_time = types.SimpleNamespace(time=time.time, sleep=stop_sleep)
    saved_sets = (set(k._alerted), dict(k._pending), dict(k._call_pending))
    k._alerted.clear(); k._pending.clear(); k._call_pending.clear()
    try:
        with binary_inputs(_market(*margs), px, closes), \
             patched(k, time=fake_time, _perp=tel, PERP_TELEMETRY_ENABLED=enabled, ON_CALL=None, POST_EMBED=None,
                     TELEGRAM_BOT_TOKEN="", RESULTS_FILE=os.path.join(d, "r.json"),
                     CALLS_FILE=os.path.join(d, "c.json"), TRADES_CSV=os.path.join(d, "t.csv"),
                     PAPER_ORDERS_CSV=os.path.join(d, "p.csv"), EXPLAIN_MARK_FILE=os.path.join(d, "e"),
                     PERP_LOG_FILE=os.path.join(d, "perp_default.csv")):
            k.RUNNING.set()
            try:
                k.poller()
            except _StopPoller:
                pass
            if enabled and tel is not None:
                assert done.wait(5), "telemetry worker did not publish"
            with k.LOCK:
                out = {"coins": json.loads(json.dumps(k.STATE["coins"])),
                       "perps": json.loads(json.dumps(k.STATE.get("perps", {})))}
            out["alerted"] = sorted(k._alerted)
            p = os.path.join(d, "p.csv")
            out["paper_rows"] = [{c: r[c] for c in ("ticker", "coin", "side", "limit_price", "contracts", "stop")}
                                 for r in csv.DictReader(open(p))] if os.path.exists(p) else []
            return out
    finally:
        k._alerted.clear(); k._alerted.update(saved_sets[0])
        k._pending.clear(); k._pending.update(saved_sets[1])
        k._call_pending.clear(); k._call_pending.update(saved_sets[2])
        shutil.rmtree(d, ignore_errors=True)


# ───────────────────────── 12: all earlier stage suites ─────────────────────────
def test_previous_stages():
    here = os.path.dirname(os.path.abspath(__file__))
    for i in range(1, 7):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=here, capture_output=True, text=True)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout, p.stderr)


if __name__ == "__main__":
    run("1  premium +/- bps", test_premium)
    run("2  30/60/180s returns select by timestamp", test_returns_use_timestamps)
    run("3  lead = perp return - spot return", test_lead)
    run("4  no future leakage", test_no_future_leakage)
    run("5  incomplete history -> None", test_incomplete_history)
    run("6  missing index -> premium None", test_missing_index)
    run("7  provider failure cannot break evaluation/poller", test_provider_failure_isolated)
    run("8  telemetry does not change strategy (evaluate + poller)", test_telemetry_does_not_change_strategy)
    run("9  stale telemetry recognised, strategy unaffected", test_stale)
    run("10 CSV schema, single header, None-safe", test_csv)
    run("11 bounded history", test_bounded)
    run("+  p_up is the same model probability", test_p_up_consistent)
    run("+  golden fixture from original code unchanged", test_golden_unchanged)
    run("+  Kalshi provider: documented schema, 1 request/cycle", test_kalshi_provider_parsing_and_rate)
    run("+  outage backoff limits requests", test_backoff_limits_requests)
    run("+  read-only / isolation static checks", test_read_only_static)
    run("+  submit never blocks the poller", test_submit_never_blocks_poller)
    run("+  contract rescale resets perp history", test_contract_rescale_resets_history)
    run("12 all previous stage suites", test_previous_stages)
    print("\nAll Stage 7 tests passed.")
