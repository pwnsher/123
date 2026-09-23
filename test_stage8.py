#!/usr/bin/env python3
"""Stage 8 tests: CAUSAL perp features + data quality (Step 2). No network.
Run:  py test_stage8.py        Every patch is restored afterwards."""
import csv
import io
import json
import math
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import types
from contextlib import redirect_stdout

import requests

import kalshi_dashboard as k
import perp_telemetry as pt
import analyze_perp_quality as aq
import test_stage7 as t7
from test_stage7 import (patched, binary_inputs, _market, _snap, StaticProvider, RaisingProvider,
                         GOLDEN, GOLDEN_INPUTS, POLLER_FIXTURES, CFG, STRATEGY_FIELDS, _StopPoller)

GOLDEN_FIELDS = ("signal", "fav", "conf", "raw_edge", "net_edge", "edge", "side_ask", "rec_stop", "reason", "verdict")
ALL_STRATEGY = STRATEGY_FIELDS + ("edge", "verdict")
OK = {"status": "ok"}


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:                            # any exception is a failure, reported as such
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


class FakeTime:
    """Deterministic clock usable as clock, monotonic and wait for PerpSampler."""
    def __init__(self, t0=1000.0): self.t = float(t0)
    def __call__(self): return self.t
    def wait(self, secs): self.t += secs; return False


def _tel(max_lag=8.0, coins=("BTC",), **kw):
    store = pt.SnapshotStore(list(coins), kw.pop("maxlen", 600), kw.pop("max_age", 1800.0))
    smp = pt.PerpSampler(StaticProvider(lambda c: None), list(coins), store)
    return pt.PerpTelemetry(StaticProvider(lambda c: None), list(coins), sampler=smp, max_lag_s=max_lag,
                            session_id="TEST", clock=kw.pop("clock", lambda: 0.0), **kw)


def _add(tel, coin, ts, mid=100.0, idx=100.0, **kw):
    s = _snap(coin, ts=ts, mid=mid, idx=idx, funding=kw.pop("funding", 0.0001), status=kw.pop("status", pt.STATUS_FRESH),
              idx_ts=kw.pop("idx_ts", None))
    for a, v in kw.items():
        setattr(s, a, v)
    return tel.store.add(s)


def _row(tel, t_spot, spot=100.0, coin="BTC", r=None):
    res = dict(r if r is not None else OK)
    res.setdefault("spot_raw", spot)
    return tel.record_cycle({coin: res}, {coin: t_spot}, now=t_spot + 0.5)[coin]


def _flags(row):
    return set(filter(None, (row["quality_flags"] or "").split(";")))


# ───────────── 1: background sampler records bounded snapshots ─────────────
def test_sampler_records():
    ft = FakeTime(1000.0)
    calls = []
    def mk(c):
        calls.append((c, ft.t)); return _snap(c, ts=ft.t, mid=100 + ft.t / 1000)
    store = pt.SnapshotStore(["BTC", "ETH", "SOL", "XRP"], maxlen=8, max_age_s=1800)
    smp = pt.PerpSampler(StaticProvider(mk), ["BTC", "ETH", "SOL", "XRP"], store, interval_s=4.0,
                         clock=ft, monotonic=ft, wait=ft.wait)
    smp.run(max_iterations=20)
    for c in ("BTC", "ETH", "SOL", "XRP"):
        snaps = store.upto(c, 1e12)
        assert len(snaps) == 8, (c, len(snaps))                         # bounded by maxlen
        gaps = [b.avail - a.avail for a, b in zip(snaps, snaps[1:])]
        assert all(abs(g - 4.0) < 1e-9 for g in gaps), gaps               # monotonic 4 s schedule
        assert all(s.available_ts is not None and s.available_ts >= s.ts for s in snaps)
    assert smp.samples_total == 20
    agestore = pt.SnapshotStore(["BTC"], maxlen=10_000, max_age_s=60)     # bounded by age too
    for i in range(500):
        agestore.add(_snap("BTC", ts=float(i * 4)))
    assert agestore.count("BTC") <= 16, agestore.count("BTC")
    # error backoff: a failing provider is not hammered
    ft2 = FakeTime(0.0); n = [0]
    def boom(c): n[0] += 1; raise requests.ConnectionError("down")
    smp2 = pt.PerpSampler(StaticProvider(boom), ["BTC"], None, interval_s=4, error_backoff_s=15,
                          clock=ft2, monotonic=ft2, wait=ft2.wait)
    smp2.run(max_iterations=5)
    assert abs(ft2.t - 60.0) < 1e-9 and smp2.sample_errors == 5, (ft2.t, smp2.sample_errors)
    # a real thread starts, survives provider exceptions, and stops
    flaky = [0]
    def sometimes(c):
        flaky[0] += 1
        if flaky[0] % 3 == 0: raise RuntimeError("boom")
        return _snap(c, ts=time.time())
    live = pt.PerpSampler(StaticProvider(sometimes), ["BTC"], None, interval_s=0.02, error_backoff_s=0.02)
    live.start(); time.sleep(0.4)
    assert live.alive and live.samples_total >= 5, live.samples_total
    live.stop(); time.sleep(0.1)
    assert not live.alive


# ───────────── 2–5: causal selection and lag ─────────────
def test_selection_is_causal():
    tel = _tel()
    for t in (96.0, 100.0, 104.0):
        _add(tel, "BTC", t, mid=100 + t)
    assert tel.store.latest_at_or_before("BTC", 102.0).ts == 100.0
    row = _row(tel, 102.0)
    assert row["perp_snapshot_ts_epoch_ms"] == 100000, row["perp_snapshot_ts_epoch_ms"]
    assert row["perp_mid"] == 200.0 and row["perp_lag_to_spot_ms"] == 2000
    assert tel.store.latest_at_or_before("BTC", 100.0).ts == 100.0       # equal time is allowed
    tel2 = _tel()
    for t in (100.0, 102.001):                                          # 1 ms in the future
        _add(tel2, "BTC", t, mid=100 + t)
    assert tel2.store.latest_at_or_before("BTC", 102.0).ts == 100.0
    assert _row(tel2, 102.0)["perp_snapshot_ts_epoch_ms"] == 100000


def test_no_prior_snapshot():
    tel = _tel()
    _add(tel, "BTC", 104.0, mid=999.0, idx=100.0)
    row = _row(tel, 102.0)
    assert Q_in(row, "NO_PRIOR_PERP")
    assert row["perp_snapshot_ts_epoch_ms"] is None and row["perp_lag_to_spot_ms"] is None
    assert row["causal_pair_ok"] is False and row["analysis_ready"] is False
    for c in ("causal_premium_bps", "perp_spread_bps", "mark_index_premium_bps", "causal_perp_ret_30s_bps",
              "premium_z_5m", "perp_rv_60s_bps", "funding_available"):
        assert row[c] is None, (c, row[c])
    assert row["perp_mid"] is None and row["premium_bps"] is None        # legacy columns don't use t=104 either


def Q_in(row, code):
    return code in _flags(row)


def test_excessive_lag():
    tel = _tel(max_lag=8.0)
    _add(tel, "BTC", 100.0)
    row = _row(tel, 120.0)
    assert row["perp_lag_to_spot_ms"] == 20000
    assert row["causal_pair_ok"] is False and row["analysis_ready"] is False
    assert Q_in(row, "PERP_TOO_OLD") and row["causal_premium_bps"] is None


def test_valid_causal_pair():
    tel = _tel(max_lag=8.0)
    _add(tel, "BTC", 101.0, mid=100.1, idx=100.0)
    row = _row(tel, 105.0)
    assert row["perp_lag_to_spot_ms"] == 4000
    assert row["causal_pair_ok"] is True and row["analysis_ready"] is True, row["quality_flags"]
    assert abs(row["causal_premium_bps"] - 10.0) < 1e-6


# ───────────── 6: spot timestamp captured at the spot request ─────────────
def test_spot_timestamp_at_request():
    seq = iter([5000.0, 5000.25])
    called_at = []
    fake_time = types.SimpleNamespace(time=lambda: next(seq), sleep=time.sleep)
    margs, px, closes = GOLDEN_INPUTS["up_enter"]
    with binary_inputs(_market(*margs), px, closes):
        def spot(p):
            called_at.append("spot"); return px
        with patched(k, time=fake_time, spot=spot):
            r = k.evaluate("BTC", CFG)
    assert called_at == ["spot"]
    assert r["spot_observed_ts"] == 5000.25 and r["spot_request_latency_ms"] == 250.0, r
    for f in GOLDEN_FIELDS:
        assert r[f] == GOLDEN["up_enter"][f], (f, r[f], GOLDEN["up_enter"][f])
    # stale-data path exposes it too
    seq2 = iter([7.0, 7.5])
    with binary_inputs(_market(100.0, 88, 12, 6.0), 100.3, [100.0] * 5), \
         patched(k, time=types.SimpleNamespace(time=lambda: next(seq2), sleep=time.sleep)):
        r2 = k.evaluate("BTC", CFG)
    assert r2["status"] == "stale" and r2["spot_observed_ts"] == 7.5


# ───────────── poller helper (sampler mode) ─────────────
def _poller_cycle(tel, enabled=True, fixture=None):
    margs, px, closes = fixture or POLLER_FIXTURES[0]
    d = tempfile.mkdtemp(prefix="_t8poll")
    done = threading.Event()
    if tel is not None:
        user_cb = tel.on_rows
        def publish(rows):
            with k.LOCK:
                k.STATE["perps"] = rows
            if user_cb: user_cb(rows)
            done.set()
        tel.on_rows = publish
    def stop_sleep(_s): raise _StopPoller()
    fake_time = types.SimpleNamespace(time=time.time, sleep=stop_sleep)
    saved = (set(k._alerted), dict(k._pending), dict(k._call_pending))
    k._alerted.clear(); k._pending.clear(); k._call_pending.clear()
    try:
        with binary_inputs(_market(*margs), px, closes), \
             patched(k, time=fake_time, _perp=tel, PERP_TELEMETRY_ENABLED=enabled, ON_CALL=None, POST_EMBED=None,
                     TELEGRAM_BOT_TOKEN="", RESULTS_FILE=os.path.join(d, "r.json"),
                     CALLS_FILE=os.path.join(d, "c.json"), TRADES_CSV=os.path.join(d, "t.csv"),
                     PAPER_ORDERS_CSV=os.path.join(d, "p.csv"), EXPLAIN_MARK_FILE=os.path.join(d, "e"),
                     PERP_LOG_FILE=os.path.join(d, "perp.csv")):
            k.RUNNING.set()
            t0 = time.time()
            try:
                k.poller()
            except _StopPoller:
                pass
            elapsed = time.time() - t0
            if enabled and tel is not None:
                assert done.wait(5), "telemetry worker did not publish"
            with k.LOCK:
                out = {"coins": json.loads(json.dumps(k.STATE["coins"])),
                       "perps": json.loads(json.dumps(k.STATE.get("perps", {}), default=str))}
            out["elapsed"] = elapsed
            out["alerted"] = sorted(k._alerted)
            p = os.path.join(d, "p.csv")
            out["paper_rows"] = list(csv.DictReader(open(p))) if os.path.exists(p) else []
            for r in out["paper_rows"]:
                r.pop("ts", None)
            return out
    finally:
        k._alerted.clear(); k._alerted.update(saved[0])
        k._pending.clear(); k._pending.update(saved[1])
        k._call_pending.clear(); k._call_pending.update(saved[2])
        shutil.rmtree(d, ignore_errors=True)


def _sampler_tel(fn=None, prefill=None, max_lag=8.0):
    store = pt.SnapshotStore(list(k.COINS))
    smp = pt.PerpSampler(StaticProvider(fn or (lambda c: _snap(c, ts=time.time()))), list(k.COINS), store,
                         interval_s=0.05, error_backoff_s=0.05)
    tel = pt.PerpTelemetry(smp.provider, list(k.COINS), sampler=smp, max_lag_s=max_lag, session_id="T8")
    if prefill:
        now = time.time()
        for c in k.COINS:
            for back in range(60, 0, -4):
                s = prefill(c, now - back - 1)
                store.add(s)
    return tel


# ───────────── 7: sampler cannot block the watcher ─────────────
def test_sampler_cannot_block():
    def slow(c):
        time.sleep(3.0); return _snap(c, ts=time.time())
    tel = _sampler_tel(slow)
    tel.start_sampler()
    time.sleep(0.05)                                   # sampler is now stuck inside a 3 s request
    t0 = time.time()
    for _ in range(20):
        tel.submit({c: {"status": "ok", "spot_raw": 1.0} for c in k.COINS}, {c: time.time() for c in k.COINS})
    assert time.time() - t0 < 0.2, "submit must be non-blocking"
    out = _poller_cycle(tel)
    assert out["elapsed"] < 1.5, f"poller cycle took {out['elapsed']:.2f}s with a hung sampler"
    assert out["coins"]["BTC"]["signal"] is True
    assert "NO_PRIOR_PERP" in out["perps"]["BTC"]["quality_flags"]   # first cycle: honest, not blocked
    tel.sampler.stop()
    tel2 = _sampler_tel(lambda c: (_ for _ in ()).throw(requests.ConnectionError("down")))
    tel2.start_sampler()
    out2 = _poller_cycle(tel2)
    assert out2["elapsed"] < 1.5 and out2["coins"]["BTC"]["signal"] is True
    tel2.sampler.stop()


# ───────────── 8: queue drops are counted ─────────────
def test_queue_drops():
    entered, release = threading.Event(), threading.Event()
    def slow_publish(rows):
        entered.set(); release.wait(5)
    tel = _tel(on_rows=slow_publish)
    _add(tel, "BTC", 99.0)
    tel.submit({"BTC": dict(OK, spot_raw=1.0)}, {"BTC": 100.0})          # cycle 1: taken by worker
    assert entered.wait(3)
    entered.clear()
    for i in range(4):                                                  # 2 queued; 3,4,5 each drop one
        tel.submit({"BTC": dict(OK, spot_raw=1.0)}, {"BTC": 101.0 + i})
    assert tel.submitted_cycles_total == 5 and tel.dropped_cycles_total == 3, \
        (tel.submitted_cycles_total, tel.dropped_cycles_total)
    release.set()
    deadline = time.time() + 3
    while tel.processed_cycles_total < 2 and time.time() < deadline:
        time.sleep(0.01)
    time.sleep(0.05)
    assert tel.processed_cycles_total == 2
    row = tel.latest_rows["BTC"]
    assert row["cycle_id"] == 5 and row["dropped_cycles_total"] == 3 and row["drops_since_previous_row"] == 3
    assert Q_in(row, "QUEUE_DROPS_SINCE_PREV")
    assert tel.health()["dropped_cycles_total"] == 3


# ───────────── 9–10: spread and basis ─────────────
def test_spread():
    assert abs(pt.spread_bps(99, 101) - 200.0) < 1e-9
    assert pt.spread_bps(101, 99) is None and pt.spread_bps(None, 101) is None and pt.spread_bps(0, 1) is None
    tel = _tel(); _add(tel, "BTC", 10.0, mid=100.0)
    tel.store.latest("BTC").perp_bid, tel.store.latest("BTC").perp_ask = 99.0, 101.0
    assert abs(_row(tel, 11.0)["perp_spread_bps"] - 200.0) < 1e-6


def test_basis():
    assert abs(pt.premium_bps(100.2, 100.0) - 20.0) < 1e-9 and abs(pt.premium_bps(99.8, 100.0) + 20.0) < 1e-9
    tel = _tel()
    _add(tel, "BTC", 10.0, mid=100.10, idx=100.0, perp_mark=100.05, perp_last=99.90)
    r = _row(tel, 11.0)
    assert abs(r["causal_premium_bps"] - 10.0) < 1e-4
    assert abs(r["mark_index_premium_bps"] - 5.0) < 1e-4
    assert abs(r["last_index_premium_bps"] - (-10.0)) < 1e-4
    assert abs(r["mid_mark_basis_bps"] - (100.10 / 100.05 - 1) * 1e4) < 1e-4
    tel2 = _tel()
    _add(tel2, "BTC", 10.0, mid=100.0, idx=100.0, perp_mark=None, perp_last=None)
    r2 = _row(tel2, 11.0)
    assert r2["mark_index_premium_bps"] is None and r2["last_index_premium_bps"] is None and r2["mid_mark_basis_bps"] is None


# ───────────── 11: premium changes ─────────────
def _prem_mid(p_bps, idx=100.0):
    return idx * (1 + p_bps / 1e4)


def _expected_at_or_before(pts, target, tol=10.0):
    cands = [(t, v) for t, v in pts if t <= target]
    if not cands: return None
    t, v = cands[-1]
    return v if target - t <= tol else None


IRREG = [0, 4, 9, 13, 18, 21, 26, 30, 35, 38, 43, 47, 52, 55, 60, 64, 69, 72, 77, 81]


def _irregular(n):
    out, t, i = [], 0.0, 0
    while len(out) < n:
        out.append(t); t += (4, 5, 4, 5, 3, 5)[i % 6]; i += 1
    return out


def test_premium_changes():
    tel = _tel()
    times = _irregular(80)
    prem = {t: 0.1 * t + (2.0 if int(t) % 3 == 0 else 0.0) for t in times}
    for t in times:
        _add(tel, "BTC", 1000 + t, mid=_prem_mid(prem[t]))
    end = 1000 + times[-1] + 1.0
    row = _row(tel, end)
    pts = [(1000 + t, prem[t]) for t in times]
    cur_t, cur = pts[-1]
    for h in (30, 60, 180):
        past = _expected_at_or_before(pts, cur_t - h)
        exp = None if past is None else cur - past
        got = row[f"premium_change_{h}s_bps"]
        assert (exp is None and got is None) or abs(got - exp) < 1e-3, (h, got, exp)
    assert row["premium_change_30s_bps"] is not None and row["premium_change_180s_bps"] is not None


# ───────────── 12: premium z-score ─────────────
def test_premium_zscore():
    series = [(float(i * 4), math.sin(i / 3.0) * 5 + i * 0.01) for i in range(90)]      # 356 s
    z, n, why = pt.trailing_zscore(series, 300.0, 20, 240.0)
    win = [v for t, v in series if series[-1][0] - 300 < t <= series[-1][0]]
    exp = (win[-1] - statistics.mean(win)) / statistics.stdev(win)
    assert why is None and n == len(win) == 75 and abs(z - exp) < 1e-9, (z, exp, n)
    assert pt.trailing_zscore(series[:15], 300.0, 20, 240.0)[0] is None                    # too few samples
    dense = [(i * 1.0, float(i % 7)) for i in range(40)]                                  # 40 pts over 39 s
    z2, n2, why2 = pt.trailing_zscore(dense, 300.0, 20, 240.0)
    assert z2 is None and n2 == 40 and why2 == "insufficient"                             # span too short
    flat = [(i * 4.0, 7.0) for i in range(90)]
    assert pt.trailing_zscore(flat, 300.0, 20, 240.0) == (None, 75, "zero_variance")      # no div-by-zero
    # through a row: the 15m z needs 12 minutes; the 5m one is populated
    tel = _tel()
    for i in range(100):
        _add(tel, "BTC", 1000 + i * 4.0, mid=_prem_mid(math.sin(i / 3.0) * 5))
    r = _row(tel, 1000 + 99 * 4 + 1)
    assert r["premium_z_5m"] is not None and r["premium_z_5m_n"] == 75
    assert r["premium_z_15m"] is None and Q_in(r, "INSUFFICIENT_15M_HISTORY") and r["analysis_ready"] is True
    # no future data: adding snapshots AFTER the spot time changes nothing
    _add(tel, "BTC", 1000 + 99 * 4 + 1.5, mid=_prem_mid(500.0))
    tel2 = _tel()
    for i in range(100):
        _add(tel2, "BTC", 1000 + i * 4.0, mid=_prem_mid(math.sin(i / 3.0) * 5))
    _add(tel2, "BTC", 1000 + 99 * 4 + 1.5, mid=_prem_mid(500.0))
    r2 = _row(tel2, 1000 + 99 * 4 + 1)
    assert r2["premium_z_5m"] == r["premium_z_5m"] and r2["causal_premium_bps"] == r["causal_premium_bps"]


# ───────────── 13–16: causal returns, momentum gap, irregular times, no leakage ─────────────
def _feed_both(tel, times, perp_fn, spot_fn, offset=1000.0, spot_delay=1.0):
    """Perp snapshot at each t; spot observation at t + spot_delay (so perp is prior)."""
    rows = []
    for t in times:
        _add(tel, "BTC", offset + t, mid=perp_fn(t), idx=100.0)
        rows.append(_row(tel, offset + t + spot_delay, spot=spot_fn(t)))
    return rows


def test_causal_returns_and_gap():
    tel = _tel()
    times = _irregular(70)
    pf = lambda t: 100.0 * math.exp(0.00002 * t) + 0.01 * math.sin(t)
    sf = lambda t: 50.0 * math.exp(0.00001 * t) + 0.003 * math.cos(t)
    row = _feed_both(tel, times, pf, sf)[-1]
    pp = [(1000 + t, pf(t)) for t in times]
    sp = [(1001 + t, sf(t)) for t in times]
    for h in (30, 60, 180):
        pe = _expected_at_or_before(pp, pp[-1][0] - h); se = _expected_at_or_before(sp, sp[-1][0] - h)
        ep = (pp[-1][1] / pe - 1) * 1e4; es = (sp[-1][1] / se - 1) * 1e4
        assert abs(row[f"causal_perp_ret_{h}s_bps"] - ep) < 1e-3, (h, row[f"causal_perp_ret_{h}s_bps"], ep)
        assert abs(row[f"causal_spot_ret_{h}s_bps"] - es) < 1e-3
        # 14: momentum gap
        assert abs(row[f"momentum_gap_{h}s_bps"] - (row[f"causal_perp_ret_{h}s_bps"] - row[f"causal_spot_ret_{h}s_bps"])) <= 2e-4


def test_irregular_timestamps():
    tel = _tel()
    tail, t = [], 81.0
    for i in range(40):                                           # spacing 3,5,6,4,5 (mean 4.6 s, never a fixed 4)
        t += (3, 5, 6, 4, 5)[i % 5]; tail.append(t)
    times = IRREG + tail
    rows = _feed_both(tel, times, lambda t: 100 + 0.05 * t, lambda t: 100 + 0.02 * t)
    r = rows[-1]
    pp = [(1000 + t, 100 + 0.05 * t) for t in times]
    for h in (30, 60, 180):
        past = _expected_at_or_before(pp, pp[-1][0] - h)
        assert abs(r[f"causal_perp_ret_{h}s_bps"] - (pp[-1][1] / past - 1) * 1e4) < 1e-3
    # a naive sample-count shortcut disagrees with the timestamp answer
    naive = (pp[-1][1] / pp[-16][1] - 1) * 1e4
    assert abs(naive - r["causal_perp_ret_60s_bps"]) > 1e-3


def test_no_future_leakage_irregular():
    base = [float(t) for t in IRREG + [85, 90, 94, 99, 103, 108, 112, 117, 121, 126]]
    end_t = 126.0
    def build(extra):
        tel = _tel()
        pts = sorted(set(base) | set(extra))
        for t in pts:
            mid = 1e6 if t in extra else 100 + 0.1 * t
            _add(tel, "BTC", 1000 + t, mid=mid)
        tel.spot_long["BTC"].append(1000 + end_t - 30 + 0.1, 99999.0) if extra else None
        return tel
    clean = build([])
    r_clean = _row(clean, 1000 + end_t + 0.5, spot=100.0)
    # attractive points: just AFTER the 30 s target time, and AFTER the feature end time
    poisoned = build([end_t - 30 + 0.2, end_t + 0.6, end_t + 3.0])
    r_poison = _row(poisoned, 1000 + end_t + 0.5, spot=100.0)
    target = 1000 + end_t - 30
    assert r_clean["causal_perp_ret_30s_bps"] is not None
    # the return anchor looks back to <= target; the point at target+0.2 is not used
    exp = ((100 + 0.1 * end_t) / (100 + 0.1 * 94) - 1) * 1e4                 # 94 is the last base point <= 96
    assert abs(r_poison["causal_perp_ret_30s_bps"] - exp) < 1e-3, (r_poison["causal_perp_ret_30s_bps"], exp)
    assert r_poison["perp_snapshot_ts_epoch_ms"] == int((1000 + end_t) * 1000)
    for c in ("causal_premium_bps", "perp_spread_bps", "premium_change_30s_bps", "causal_perp_ret_60s_bps",
              "causal_perp_ret_180s_bps"):
        assert r_poison[c] == r_clean[c], (c, r_poison[c], r_clean[c])
    # and row timestamps obey the contract: nothing newer than feature_end
    assert r_poison["perp_snapshot_ts_epoch_ms"] <= r_poison["feature_end_ts_epoch_ms"]


# ───────────── 17–19: realized volatility ─────────────
def test_rv_math():
    pts = [(0, 100.0), (5, 101.0), (11, 100.5), (15, 100.0), (20, 102.0), (26, 101.0),
           (30, 101.5), (36, 100.8), (40, 101.2), (45, 100.9), (50, 101.7), (55, 101.1), (60, 101.3)]
    pts = [(float(t), p) for t, p in pts]
    rv = sum(math.log(b / a) ** 2 for (_, a), (_, b) in zip(pts, pts[1:]))
    exp = math.sqrt(rv * 60.0 / 60.0) * 1e4
    got = pt.realized_vol_bps(pts, 60.0, 6, 45.0)
    assert abs(got - exp) < 1e-9, (got, exp)
    half = [(t / 2, p) for t, p in pts]                          # same returns, half the span
    assert abs(pt.realized_vol_bps(half, 60.0, 6, 25.0) - math.sqrt(rv * 60.0 / 30.0) * 1e4) < 1e-9
    const = [(float(t), 100.0) for t in range(0, 61, 5)]
    assert pt.realized_vol_bps(const, 60.0, 6, 45.0) == 0.0


def test_rv_sparse_none():
    good = [(float(t), 100.0 + (t % 3)) for t in range(0, 61, 5)]
    assert pt.realized_vol_bps(good, 60.0, 6, 45.0) is not None
    gap = [p for p in good if not 15 < p[0] < 50]                # 35 s hole
    assert pt.realized_vol_bps(gap, 60.0, 4, 30.0) is None       # internal gap -> None, not interpolated
    assert pt.realized_vol_bps(good[-4:], 60.0, 6, 45.0) is None  # too few points
    assert pt.realized_vol_bps(good[-8:], 60.0, 6, 45.0) is None  # span 35 s < 45 s
    tel = _tel()
    for t in list(range(0, 200, 4)) + list(range(260, 324, 4)):   # 64 s outage inside the 300 s window
        _add(tel, "BTC", 1000 + t, mid=100 + (t % 7) * 0.01)
    r = _row(tel, 1000 + 320 + 0.5)                                  # 60 s of clean data after the outage
    assert r["perp_rv_60s_bps"] is not None and r["perp_rv_300s_bps"] is None and r["perp_rv_900s_bps"] is None
    assert r["perp_vol_shock_60v300"] is None and Q_in(r, "RV_INSUFFICIENT")


def test_vol_shock():
    assert pt.ratio(12.0, 4.0) == 3.0
    assert pt.ratio(12.0, 0.0) is None and pt.ratio(None, 4.0) is None and pt.ratio(3.0, None) is None
    tel = _tel()
    for i in range(240):                                          # 956 s of 4 s samples
        t = i * 4.0
        wiggle = 0.02 if i >= 225 else 0.002                      # volatility jumps in the last minute
        _add(tel, "BTC", 1000 + t, mid=100 + (wiggle if i % 2 else -wiggle))
    r = _row(tel, 1000 + 239 * 4 + 0.5)
    for a, b in (("60s", "300s"), ("60s", "900s")):
        exp = r[f"perp_rv_{a}_bps"] / r[f"perp_rv_{b}_bps"]
        assert abs(r[f"perp_vol_shock_60v{b[:-1]}"] - exp) < 1e-3, (a, b)
    assert r["perp_vol_shock_60v300"] > 2.0


# ───────────── 20: contract scale change ─────────────
def test_contract_scale_change():
    tel = _tel()
    for i in range(60):
        _add(tel, "BTC", 1000 + i * 4.0, mid=100 + i * 0.01, contract_size=0.0001, underlying_multiplier=1.0)
    assert tel.store.count("BTC") == 60
    for i in range(60, 64):                                        # new scale: 10x price level
        _add(tel, "BTC", 1000 + i * 4.0, mid=1000 + i * 0.1, idx=1000.0, contract_size=0.001, underlying_multiplier=1.0)
    assert tel.store.count("BTC") == 4                             # old levels discarded
    r = _row(tel, 1000 + 63 * 4 + 0.5)
    assert Q_in(r, "CONTRACT_SCALE_CHANGED")
    for c in ("causal_perp_ret_30s_bps", "causal_perp_ret_60s_bps", "causal_perp_ret_180s_bps",
              "premium_change_30s_bps", "premium_change_60s_bps", "perp_rv_60s_bps"):
        assert r[c] is None, (c, r[c])                             # nothing bridges the re-scale
    assert r["premium_z_5m_n"] == 4
    assert r["causal_premium_bps"] is not None and r["analysis_ready"] is True


# ───────────── 21–22: quality flags and analysis_ready ─────────────
KNOWN_FLAGS = {v for k2, v in vars(pt).items() if k2.startswith("Q_") and isinstance(v, str)} | \
              set(pt.Q_INSUFF.values()) | set(pt.Q_INSUFF_Z.values())


def test_quality_flags():
    t = _tel(); r = _row(t, 50.0)
    assert Q_in(r, "NO_PRIOR_PERP")
    t = _tel(); _add(t, "BTC", 49.0, idx_ts=10.0); r = _row(t, 50.0)
    assert Q_in(r, "PERP_STALE") and Q_in(r, "INDEX_STALE") and not r["analysis_ready"]
    t = _tel(); _add(t, "BTC", 49.0, idx=None); r = _row(t, 50.0)
    assert Q_in(r, "MISSING_INDEX") and not r["analysis_ready"]
    t = _tel(); _add(t, "BTC", 49.0, mid=None); r = _row(t, 50.0)
    assert Q_in(r, "NO_TWO_SIDED_BOOK") and not r["analysis_ready"]
    t = _tel(); _add(t, "BTC", 10.0); r = _row(t, 50.0)
    assert Q_in(r, "PERP_TOO_OLD")
    t = _tel(); _add(t, "BTC", 49.0, status=pt.STATUS_ERROR); r = _row(t, 50.0)
    assert Q_in(r, "PERP_SOURCE_ERROR") and not r["analysis_ready"]
    t = _tel(); _add(t, "BTC", 49.0); r = _row(t, 50.0)
    assert {"INSUFFICIENT_30S_HISTORY", "INSUFFICIENT_5M_HISTORY", "INSUFFICIENT_15M_HISTORY"} <= _flags(r)
    assert r["analysis_ready"] is True                             # warm-up does not block the base row
    t = _tel(); _add(t, "BTC", 49.0); r = t.record_cycle({"BTC": {"status": "ok"}}, {"BTC": None}, now=51.0)["BTC"]
    assert {"MISSING_SPOT", "SPOT_TIMESTAMP_MISSING"} <= _flags(r)
    t = _tel(); _add(t, "BTC", 49.0); r = _row(t, 50.0, r={"status": "stale"})
    assert Q_in(r, "BINARY_NOT_OK") and not r["analysis_ready"]
    t = _tel(); _add(t, "BTC", 49.0, funding=None); r = _row(t, 50.0)
    assert Q_in(r, "FUNDING_MISSING") and r["funding_available"] is False and r["analysis_ready"] is True
    # codes are stable identifiers, ';'-delimited, no prose
    for row in (r,):
        for fl in _flags(row):
            assert re.fullmatch(r"[A-Z0-9_]+", fl) and fl in KNOWN_FLAGS, fl


def test_analysis_ready():
    def ready_row(extra):
        t = _tel(); _add(t, "BTC", 49.0, mid=100.2, idx=100.0)
        return _row(t, 50.0, r=dict(OK, **extra))
    base = ready_row({})
    assert base["analysis_ready"] is True and base["causal_pair_ok"] is True
    # eventual outcome fields (if they were ever present) cannot change readiness or any feature
    for outcome in ({"result": "yes"}, {"result": "no"}, {"settle_result": "yes", "win": True},
                    {"settle_result": "no", "win": False}):
        r = ready_row(outcome)
        for c in pt.STEP2_COLUMNS:
            if c not in ("submitted_cycles_total", "processed_cycles_total"):
                assert r[c] == base[c], (outcome, c)
    import ast, inspect, textwrap
    tree = ast.parse(textwrap.dedent(inspect.getsource(pt.PerpTelemetry._causal)))
    tokens = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | \
             {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | \
             {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)}
    hits = {t for t in tokens if re.search(r"(result|settle|\bwin\b|outcome|pnl)", str(t), re.I)}
    assert not hits, f"readiness code references outcome fields: {hits}"
    t = _tel(); _add(t, "BTC", 49.0); r = _row(t, 60.0)
    assert r["analysis_ready"] is False


# ───────────── 23: CSV versioning ─────────────
def test_csv_versioning():
    d = tempfile.mkdtemp(prefix="_t8csv")
    try:
        path = os.path.join(d, "perp.csv")
        with open(path, "w", newline="") as f:                     # a genuine Step 1 file
            w = csv.writer(f); w.writerow(pt.STEP1_COLUMNS); w.writerow(["x"] * len(pt.STEP1_COLUMNS))
        tel = _tel(log_path=path)
        _add(tel, "BTC", 49.0)
        _row(tel, 50.0); _row(tel, 54.0)
        rows = list(csv.DictReader(open(path)))
        assert open(path).readline().strip().split(",") == pt.CSV_COLUMNS
        assert len(rows) == 2 and all(r["telemetry_schema_version"] == "2" and r["feature_version"] == "step2_v1" for r in rows)
        assert all(r["telemetry_session_id"] == "TEST" for r in rows) and [r["cycle_id"] for r in rows] == ["1", "2"]
        baks = [n for n in os.listdir(d) if n.startswith("perp.csv.schema-")]
        assert len(baks) == 1
        old = list(csv.reader(open(os.path.join(d, baks[0]))))
        assert old[0] == pt.STEP1_COLUMNS and len(old) == 2           # Step 1 data preserved, not mixed
        assert set(pt.STEP1_COLUMNS) <= set(pt.CSV_COLUMNS)           # every Step 1 column still present
        assert len(pt.CSV_COLUMNS) == len(set(pt.CSV_COLUMNS))
        for bad in ("perp_score", "bull_score", "bear_score", "confidence_modifier", "trade_score",
                    "confirmation_score", "order_book_imbalance", "buy_pressure", "sell_pressure", "depth_imbalance"):
            assert bad not in pt.CSV_COLUMNS and bad not in open(pt.__file__).read()
    finally:
        shutil.rmtree(d, ignore_errors=True)


# ───────────── 24: quality analyzer ─────────────
SAMPLE_REPORT = {}


def _synthetic_csv(path):
    tel = _tel(coins=("BTC", "ETH"), log_path=path)
    for i in range(60):                                            # ETH: perp only for the first half
        t = 1000 + i * 4.0
        _add(tel, "BTC", t - 1.0, mid=100 + 0.01 * i)
        if i < 30:
            _add(tel, "ETH", t - 3.0, mid=10 + 0.001 * i, idx=10.0)
        tel.record_cycle({c: dict(OK, spot_raw=50.0 + 0.001 * i, ticker=f"KX{c}-{i // 30}") for c in ("BTC", "ETH")},
                         {c: t for c in ("BTC", "ETH")}, now=t + 0.2)
    rows = list(csv.DictReader(open(path)))
    bad = []
    dup = dict(rows[5]); bad.append(dup)                           # exact duplicate row
    neg = dict(rows[7]); neg["perp_lag_to_spot_ms"] = "-500"; neg["ts_epoch_ms"] = str(int(neg["ts_epoch_ms"]) + 1); bad.append(neg)
    nan = dict(rows[9]); nan["causal_premium_bps"] = "nan"; nan["ts_epoch_ms"] = str(int(nan["ts_epoch_ms"]) + 2); bad.append(nan)
    crossed = dict(rows[11]); crossed["perp_bid"], crossed["perp_ask"] = "101", "99"; crossed["ts_epoch_ms"] = str(int(crossed["ts_epoch_ms"]) + 3); bad.append(crossed)
    zero = dict(rows[13]); zero["index_price"] = "0"; zero["ts_epoch_ms"] = str(int(zero["ts_epoch_ms"]) + 4); bad.append(zero)
    ooo = dict(rows[2]); ooo["ts_epoch_ms"] = str(int(ooo["ts_epoch_ms"]) - 1); ooo["cycle_id"] = "999"; bad.append(ooo)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=pt.CSV_COLUMNS)
        for b in bad:
            w.writerow(b)
    return rows, bad


def test_quality_analyzer():
    d = tempfile.mkdtemp(prefix="_t8aq")
    try:
        path = os.path.join(d, "perp.csv")
        rows, bad = _synthetic_csv(path)
        rep = aq.analyze(path)
        o, btc, eth = rep["overall"], rep["by_coin"]["BTC"], rep["by_coin"]["ETH"]
        assert o["rows"] == 120 + len(bad) and btc["rows"] == 60 + sum(b["coin"] == "BTC" for b in bad)
        assert rep["by_coin"]["SOL"]["rows"] == 0
        assert o["sessions"] == 1 and btc["unique_binary_tickers"] == 2
        # alignment: BTC lag is exactly 1000 ms in every clean row
        clean_btc = [r for r in rows if r["coin"] == "BTC"]
        assert all(r["perp_lag_to_spot_ms"] == "1000" for r in clean_btc)
        lags = sorted(float(r["perp_lag_to_spot_ms"]) for r in rows + bad
                      if r["coin"] == "BTC" and r["perp_lag_to_spot_ms"] != "")
        assert btc["perp_lag_to_spot_ms"]["median"] == statistics.median(lags)
        assert btc["perp_lag_to_spot_ms"]["max"] == max(lags)
        assert eth["perp_lag_to_spot_ms"]["max"] > 8000                          # ETH went too old
        exp_ready = 100.0 * sum(r["analysis_ready"] == "True" for r in rows + bad if r["coin"] == "ETH") / eth["rows"]
        assert abs(eth["analysis_ready_pct"] - round(exp_ready, 2)) < 1e-9
        assert 0 < eth["analysis_ready_pct"] < 60 and btc["analysis_ready_pct"] == 100.0
        assert eth["quality_flags"].get("PERP_TOO_OLD", 0) > 0
        assert o["feature_coverage_pct"]["causal_perp_ret_30s_bps"] > 0
        assert o["feature_coverage_pct"]["premium_z_15m"] == 0.0                   # 4 minutes of data
        s = rep["structural"]
        assert s["duplicate_rows"] == 1 and s["negative_causal_lag"] == 1 and s["crossed_books"] == 1
        assert s["non_finite_values"] == {"causal_premium_bps": 1} and s["impossible_prices"] == {"index_price": 1}
        assert s["out_of_order_rows"] >= 1 and s["duplicate_timestamps_per_coin"] >= 1
        assert s["schema_versions_present"] == {"2": 126} and s["feature_versions_present"] == {"step2_v1": 126}
        assert o["queue"]["processed"] == 60
        # cadence is per-coin stream, never interleaved across coins (BTC/ETH offsets differ by 2 s)
        assert o["cadence_perp_snapshots"]["median_s"] == 4.0, o["cadence_perp_snapshots"]
        assert btc["cadence_perp_snapshots"]["median_s"] == 4.0
        # CLI + JSON output
        jp = os.path.join(d, "q.json")
        p = subprocess.run([sys.executable, "analyze_perp_quality.py", "--file", path, "--json", jp],
                           cwd=os.path.dirname(os.path.abspath(__file__)), capture_output=True, text=True)
        assert p.returncode == 0 and "STRUCTURAL CHECKS" in p.stdout, p.stderr
        assert json.load(open(jp))["overall"]["rows"] == 126
        SAMPLE_REPORT["text"] = p.stdout
        # the analyzer computes no predictive/outcome metrics
        import ast
        tree = ast.parse(open(aq.__file__).read())
        doc_ids = {id(n.body[0].value) for n in ast.walk(tree)
                   if isinstance(n, (ast.Module, ast.FunctionDef)) and n.body and isinstance(n.body[0], ast.Expr)}
        code = " ".join(
            [n.id for n in ast.walk(tree) if isinstance(n, ast.Name)] +
            [n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)] +
            [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)
             and id(n) not in doc_ids and not n.value.startswith("Integrity only")]).lower()
        for term in ("win_rate", "winrate", "correl", "auc", "brier", "sharpe", "pnl", "logistic",
                     "regression", "settle_result", "profit"):
            assert term not in code, term
        imported = {a.name for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom)) for a in n.names}
        assert not imported & {"requests", "urllib", "socket", "http"}, imported
        # an old Step 1 file still analyses, with a warning
        old = os.path.join(d, "old.csv")
        with open(old, "w", newline="") as f:
            w = csv.writer(f); w.writerow(pt.STEP1_COLUMNS)
        assert "warning" in aq.analyze(old)
    finally:
        shutil.rmtree(d, ignore_errors=True)


# ───────────── 25: strategy identical under every perp condition ─────────────
MODES = {
    "normal": lambda c, t: _snap(c, ts=t, mid=100.05, idx=100.0),
    "bull": lambda c, t: _snap(c, ts=t, mid=150.0, idx=100.0, funding=0.02),
    "bear": lambda c, t: _snap(c, ts=t, mid=50.0, idx=100.0, funding=-0.02),
    "stale": lambda c, t: _snap(c, ts=t, mid=100.0, idx=100.0, idx_ts=t - 3600),
}
FIXTURE_NAMES = {0: "up_enter", 1: "mid_up", 2: "mid_down"}


def test_strategy_identical():
    # evaluate level: extreme telemetry visible in STATE must not move any field
    for name, (margs, px, closes) in GOLDEN_INPUTS.items():
        with binary_inputs(_market(*margs), px, closes):
            with patched(k, PERP_TELEMETRY_ENABLED=False):
                ref = k.evaluate("BTC", CFG)
            for mode in ("bull", "bear", "stale", "missing"):
                with k.LOCK:
                    saved = k.STATE.get("perps")
                    k.STATE["perps"] = {c: ({} if mode == "missing" else
                                            {"causal_premium_bps": 5000 if mode == "bull" else -5000,
                                             "premium_z_5m": 9.9, "momentum_gap_30s_bps": 800, "analysis_ready": True})
                                        for c in k.COINS}
                try:
                    r = k.evaluate("BTC", CFG)
                finally:
                    with k.LOCK:
                        k.STATE["perps"] = saved
                for f in ALL_STRATEGY:
                    assert r[f] == ref[f], (name, mode, f)
            for f in GOLDEN_FIELDS:
                assert ref[f] == GOLDEN[name][f], (name, f, ref[f], GOLDEN[name][f])
    # full poller loop, sampler mode, every perp condition
    for idx, fx in enumerate(POLLER_FIXTURES):
        gname = FIXTURE_NAMES[idx]
        res = {"disabled": _poller_cycle(None, enabled=False, fixture=fx),
               "missing": _poller_cycle(_sampler_tel(), fixture=fx)}          # empty store, sampler not started
        for mode, mk in MODES.items():
            res[mode] = _poller_cycle(_sampler_tel(prefill=mk), fixture=fx)
        ref = res["disabled"]
        for mode, out in res.items():
            for c in k.COINS:
                got = {f: out["coins"][c][f] for f in ALL_STRATEGY}
                exp = {f: ref["coins"][c][f] for f in ALL_STRATEGY}
                assert got == exp, (gname, mode, c, got, exp)
                # the golden fixture was captured with CFG sl_pct=0.75; rec_stop is only
                # comparable for coins whose real config shares it (BTC, SOL). Every field
                # of every coin is still compared ACROSS modes above.
                for f in GOLDEN_FIELDS:
                    if f == "rec_stop" and k.COINS[c]["sl_pct"] != CFG["sl_pct"]:
                        continue
                    assert out["coins"][c][f] == GOLDEN[gname][f], (gname, mode, c, f)
            assert out["alerted"] == ref["alerted"] and out["paper_rows"] == ref["paper_rows"], (gname, mode)
        assert res["normal"]["perps"]["BTC"]["analysis_ready"] is True, res["normal"]["perps"]["BTC"]["quality_flags"]
        assert res["bull"]["perps"]["BTC"]["causal_premium_bps"] > 4999
        assert "PERP_STALE" in res["stale"]["perps"]["BTC"]["quality_flags"]
        assert "NO_PRIOR_PERP" in res["missing"]["perps"]["BTC"]["quality_flags"]
        assert res["disabled"]["perps"]["BTC"]["source_status"] == "disabled"


# ───────────── extras ─────────────
def test_first_cycle_and_health():
    tel = _sampler_tel()
    t0 = time.time()
    rows = tel.record_cycle({c: dict(OK, spot_raw=1.0) for c in k.COINS}, {c: time.time() for c in k.COINS})
    assert time.time() - t0 < 0.5
    for c in k.COINS:
        assert rows[c]["analysis_ready"] is False and "NO_PRIOR_PERP" in rows[c]["quality_flags"]
    h = tel.health()
    assert h["sampler_alive"] is False and h["processed_cycles_total"] == 1 and set(h["coins"]) == set(k.COINS)
    json.dumps(h)


def test_memory_bounds():
    tel = _tel(maxlen=600, max_age=1800.0)
    for i in range(3000):
        _add(tel, "BTC", i * 4.0)
        tel.spot_long["BTC"].append(i * 4.0 + 1, 100.0)
    assert tel.store.count("BTC") <= 600 and len(tel.spot_long["BTC"]) <= 600
    assert tel.store.maxlen == 600 and tel._q.maxsize == 1


def test_static_read_only_and_no_scores():
    t7.test_read_only_static()                                      # reuse the Stage 7 AST check
    src = open(pt.__file__).read()
    for word in ("perp_score", "bull_score", "bear_score", "confidence_modifier", "trade_score", "imbalance"):
        assert word not in src, word
    import inspect
    for fn in (k.evaluate, k._size_fraction, k._contracts, k.log_call, k.build_call_embed, k.settle_calls):
        assert "perp" not in inspect.getsource(fn).lower(), fn.__name__
    bt = open(os.path.join(os.path.dirname(k.__file__), "kalshi_backtest.py")).read()
    assert "perp" not in bt.lower()


# ───────────── 26: earlier suites ─────────────
def test_previous_stages():
    here = os.path.dirname(os.path.abspath(__file__))
    for i in range(1, 8):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=here, capture_output=True, text=True)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-800:], p.stderr[-800:])


if __name__ == "__main__":
    run("1  background sampler records bounded snapshots", test_sampler_records)
    run("2  snapshot selection is causal (96/100/104 @102 -> 100)", test_selection_is_causal)
    run("3  no prior snapshot -> no causal perp feature", test_no_prior_snapshot)
    run("4  excessive lag -> not causal, not ready", test_excessive_lag)
    run("5  valid causal pair (lag 4000 ms)", test_valid_causal_pair)
    run("6  spot timestamp captured at the spot request", test_spot_timestamp_at_request)
    run("7  sampler cannot block the watcher", test_sampler_cannot_block)
    run("8  queue drops are counted", test_queue_drops)
    run("9  spread 99/101 -> 200 bp", test_spread)
    run("10 basis measures (+/-, missing)", test_basis)
    run("11 premium changes 30/60/180", test_premium_changes)
    run("12 premium z-score (math, min n, min span, zero var, no future)", test_premium_zscore)
    run("13 causal returns + 14 momentum gap", test_causal_returns_and_gap)
    run("15 irregular timestamps", test_irregular_timestamps)
    run("16 no future leakage under irregular timestamps", test_no_future_leakage_irregular)
    run("17 realized volatility math", test_rv_math)
    run("18 volatility None on sparse data / gaps", test_rv_sparse_none)
    run("19 volatility shock ratios", test_vol_shock)
    run("20 contract scale change resets histories", test_contract_scale_change)
    run("21 quality flags", test_quality_flags)
    run("22 analysis_ready (true/false, outcome-independent)", test_analysis_ready)
    run("23 CSV versioning + Step 1 rotation", test_csv_versioning)
    run("24 quality analyzer", test_quality_analyzer)
    run("25 strategy identical: disabled/normal/bull/bear/stale/missing", test_strategy_identical)
    run("+  first cycle + health", test_first_cycle_and_health)
    run("+  memory bounds", test_memory_bounds)
    run("+  static read-only / no scores", test_static_read_only_and_no_scores)
    run("26 all previous stage suites", test_previous_stages)
    print("\nAll Stage 8 tests passed.")
    if "--show-report" in sys.argv:
        print(SAMPLE_REPORT.get("text", ""))
