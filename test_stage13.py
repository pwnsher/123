#!/usr/bin/env python3
"""Stage 13 — PERFORMANCE-ONLY REGRESSION. Proves the optimisation pass changed speed, not
behaviour: every optimised path is compared EXACTLY against the pre-optimisation reference
implementation (embedded below). No wall-clock thresholds, no network.
Run:  py test_stage13.py"""
import ast
import bisect
import builtins
import csv
import hashlib
import inspect
import json
import math
import os
import random
import shutil
import subprocess
import sys
import tempfile
import textwrap
import threading
import types

import analyze_perp_shadow as ash
import build_perp_shadow_policy as bp
import check_perp_deployment as cd
import http_session
import kalshi_dashboard as k
import label_binary_outcomes as lb
import perp_live as pl
import perp_probability as pp
import perp_shadow as ps
import perp_telemetry as pt
import strategy_fingerprint as sf

HERE = os.path.dirname(os.path.abspath(__file__))
TRUSTED_V2 = "8d94f241e8fc8edadc76058e1f12f430b6e4f499f4c0a30fba1cb5cf07dad82a"
MASTER = os.environ.get("KALSHI_MASTER_TEST_RUN") == "1"


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


# ═══════════════════ pre-optimisation REFERENCE implementations (verbatim logic) ═══════════════════
def ref_point_at_or_before(series, target_ts, tolerance_s):
    i = bisect.bisect_right([p[0] for p in series], target_ts) - 1
    if i < 0:
        return None
    ts, v = series[i]
    return (ts, v) if target_ts - ts <= tolerance_s else None


def ref_trailing_zscore(series, window_s, min_n, min_span_s):
    if not series:
        return None, 0, "empty"
    end = series[-1][0]
    pts = [v for ts, v in series if end - window_s < ts <= end]
    tss = [ts for ts, v in series if end - window_s < ts <= end]
    n = len(pts)
    if n < min_n or (tss[-1] - tss[0]) < min_span_s:
        return None, n, "insufficient"
    mean = sum(pts) / n
    sd = math.sqrt(sum((x - mean) ** 2 for x in pts) / (n - 1))
    if not math.isfinite(sd) or sd <= pt.ZERO_VAR_EPS:
        return None, n, "zero_variance"
    return (pts[-1] - mean) / sd, n, None


def ref_realized_vol_bps(series, window_s, min_n, min_span_s, max_gap_s=pt.RV_MAX_GAP_S):
    if not series:
        return None
    end = series[-1][0]
    pts = [(ts, v) for ts, v in series if end - window_s <= ts <= end]
    if len(pts) < min_n:
        return None
    span = pts[-1][0] - pts[0][0]
    if span < min_span_s or span <= 0:
        return None
    rv = 0.0
    for (t0, p0), (t1, p1) in zip(pts, pts[1:]):
        if t1 - t0 > max_gap_s or p0 <= 0 or p1 <= 0:
            return None
        rv += math.log(p1 / p0) ** 2
    return math.sqrt(rv * 60.0 / span) * 10000.0


def ref_shadow_bootstrap(rows, stat, reps, seed):
    by = {}
    for r in rows:
        by.setdefault(r["ticker"], []).append(r)
    keys = sorted(by)
    if not keys:
        return []
    rng = random.Random(seed)
    out = []
    for _ in range(reps):
        sample = [r for j in rng.choices(range(len(keys)), k=len(keys)) for r in by[keys[j]]]
        out.append(stat(sample))
    return out


def _series(rng, n, dup=False):
    t, out = rng.uniform(0, 100), []
    for _ in range(n):
        t += rng.choice((0.0,) if dup and rng.random() < 0.1 else (rng.uniform(0.5, 30.0),))
        out.append((t, rng.uniform(50, 150)))
    return out


# ═══════════════════ 1-2 test runner ═══════════════════
def test_master_runner_runs_each_stage_once():
    d = tempfile.mkdtemp()
    shutil.copy(os.path.join(HERE, "run_all_tests.py"), d)
    log = os.path.join(d, "calls.log")
    last = 14                                        # run_all_tests.py runs stages 1..14 (stage 14: schema 3)
    for i in range(1, last + 1):
        body = (f"import os, subprocess, sys\nopen({log!r}, 'a').write('stage{i}\\n')\n"
                f"if os.environ.get('KALSHI_MASTER_TEST_RUN') != '1':\n"
                f"    for j in range(1, {i}):\n"
                f"        subprocess.run([sys.executable, f'test_stage{{j}}.py'])\n"
                f"print('All Stage {i} tests passed.')\n")
        open(os.path.join(d, f"test_stage{i}.py"), "w").write(body)
    p = subprocess.run([sys.executable, "run_all_tests.py"], cwd=d, capture_output=True, text=True,
                       env={kk: v for kk, v in os.environ.items() if kk != "KALSHI_MASTER_TEST_RUN"})
    calls = open(log).read().split()
    assert p.returncode == 0 and calls == [f"stage{i}" for i in range(1, last + 1)], calls    # 1: exactly once each
    assert "ALL SUITES PASSED" in p.stdout
    for f in ("test_stage9.py", "test_stage10.py", "test_stage11.py", "test_stage12.py", "test_stage14.py"):
        src = open(os.path.join(HERE, f)).read()
        fn = src[src.index("def test_previous_stages"):].split("\ndef ")[0]
        assert 'os.environ.get("KALSHI_MASTER_TEST_RUN") == "1"' in fn and 'KALSHI_MASTER_TEST_RUN="1"' in fn, f


def test_standalone_stage12_regression():
    import test_stage12 as t12
    calls = []
    def fake_run(args, **kw):
        calls.append((args[-1], (kw.get("env") or {}).get("KALSHI_MASTER_TEST_RUN")))
        return types.SimpleNamespace(returncode=0, stdout=f"All Stage {args[-1][10:-3]} tests passed.", stderr="")
    saved_env = os.environ.pop("KALSHI_MASTER_TEST_RUN", None)
    try:
        orig = t12.subprocess.run
        t12.subprocess.run = fake_run
        t12.test_previous_stages()                                         # standalone: stages 1..11, once each
        assert [c[0] for c in calls] == [f"test_stage{i}.py" for i in range(1, 12)], calls
        assert all(c[1] == "1" for c in calls)                             # children do not re-nest
        calls.clear()
        os.environ["KALSHI_MASTER_TEST_RUN"] = "1"
        t12.test_previous_stages()                                         # master mode: nothing re-run
        assert calls == []
    finally:
        t12.subprocess.run = orig
        os.environ.pop("KALSHI_MASTER_TEST_RUN", None)
        if saved_env is not None:
            os.environ["KALSHI_MASTER_TEST_RUN"] = saved_env


# ═══════════════════ 3-4 historical lookup ═══════════════════
def test_point_lookup_parity_and_no_allocation():
    rng = random.Random(11)
    cases = 0
    for trial in range(2500):
        s = [] if trial % 50 == 0 else _series(rng, rng.choice((1, 2, 5, 40, 600)), dup=trial % 3 == 0)
        pts_ = [t for t, _ in s]
        targets = [rng.uniform(-50, (pts_[-1] if pts_ else 100) + 50) for _ in range(6)] + pts_[:3] + pts_[-2:]
        for tgt in targets:
            for tol in (0.0, 4.0, 10.0, 1e9):
                assert pt.point_at_or_before(s, tgt, tol) == ref_point_at_or_before(s, tgt, tol), (trial, tgt, tol)
                cases += 1
    assert cases > 20000
    for fn in (pt.point_at_or_before, pt._count_le, pt._count_lt):          # 4: no timestamp list per lookup
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
        assert not any(isinstance(n, (ast.ListComp, ast.GeneratorExp)) for n in ast.walk(tree)), fn.__name__
    s = [(float(i), 1.0) for i in range(1000)]
    import tracemalloc
    tracemalloc.start()
    for i in range(200):
        pt.point_at_or_before(s, 500.5, 10.0)
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    assert peak < 4000, peak                                               # a 1000-element list would be ~8 KB


# ═══════════════════ 5-7 causal feature engine ═══════════════════
def test_causal_feature_parity():
    rng = random.Random(5)
    for trial in range(10000):                                              # helper-level parity, exact
        s = _series(rng, rng.choice((0, 1, 3, 25, 80, 240)))
        for w, mn, span in ((300.0, 20, 240.0), (900.0, 60, 720.0)):
            assert pt.trailing_zscore(s, w, mn, span) == ref_trailing_zscore(s, w, mn, span), trial
        for w, (mn, span) in pt.RV_WINDOWS.items():
            assert pt.realized_vol_bps(s, w, mn, span) == ref_realized_vol_bps(s, w, mn, span), (trial, w)
    # end-to-end: 10,000 feature cycles, optimised module vs the same module with reference helpers
    def cycles(use_ref):
        saved = (pt.point_at_or_before, pt.trailing_zscore, pt.realized_vol_bps)
        if use_ref:
            pt.point_at_or_before, pt.trailing_zscore, pt.realized_vol_bps = \
                ref_point_at_or_before, ref_trailing_zscore, ref_realized_vol_bps
        try:
            r2 = random.Random(9)
            coins = ["BTC", "ETH", "SOL", "XRP"]
            tel = pt.PerpTelemetry(None, coins, sampler=None, session_id="P13")
            out, now, mid = [], 1.9e9, {c: 100.0 for c in coins}
            for step in range(2500):
                now += r2.uniform(3.0, 7.0)
                for c in coins:
                    mid[c] *= math.exp(r2.gauss(0, 5e-4))
                    if r2.random() > 0.05:                                  # occasional missing perp sample
                        ts = now - r2.uniform(0.2, 3.0)
                        tel.store.add(pt.PerpSnapshot(ts=ts, coin=c, perp_bid=mid[c] - .01, perp_ask=mid[c] + .01,
                                                      perp_mid=mid[c], perp_last=mid[c], perp_mark=mid[c],
                                                      index_price=mid[c] * (1 + r2.gauss(0, 3e-4)),
                                                      index_ts_ms=int(ts * 1000), funding_rate=1e-4,
                                                      source_status=pt.STATUS_FRESH))
                res = {c: {"status": "ok", "spot_raw": mid[c] * 0.5 * (1 + r2.gauss(0, 2e-4)), "p_up": 60.0}
                       for c in coins}
                rows = tel.record_cycle(res, {c: now for c in coins}, now=now + 0.1)
                out.append([[rows[c][col] for col in pt.STEP2_COLUMNS if not col.endswith("_total")
                             and col != "drops_since_previous_row"] for c in coins])
            return out
        finally:
            pt.point_at_or_before, pt.trailing_zscore, pt.realized_vol_bps = saved
    new, ref = cycles(False), cycles(True)
    assert len(new) * 4 == 10000 and new == ref                             # exact equality, every field
    ready = sum(1 for cyc in new for row in cyc if row[pt.STEP2_COLUMNS.index("analysis_ready")])
    assert ready > 5000                                                      # the fixture exercises real features


def test_causality_and_preview_parity():
    import test_stage12 as t12
    t12.test_preview_parity_and_causality()          # 6 + 7: no future snapshot; preview == record; irregular
    t12.test_preview_is_read_only()


# ═══════════════════ 8-9 CSV ═══════════════════
def test_csv_header_io():
    d = tempfile.mkdtemp()
    path = os.path.join(d, "t.csv")
    lg = pt.TelemetryCSVLogger(path)
    reads = {"n": 0}
    orig = lg._header_ok
    def counting():
        reads["n"] += 1; return orig()
    lg._header_ok = counting
    rows = [{c: 1 for c in pt.CSV_COLUMNS}]
    opens = {"n": 0}
    real_open = builtins.open
    def counting_open(*a, **kw):
        opens["n"] += 1; return real_open(*a, **kw)
    builtins.open = counting_open
    try:
        for _ in range(200):
            lg.write_rows(rows)
    finally:
        builtins.open = real_open
    assert reads["n"] == 0 and opens["n"] == 200                    # 8: new file -> no header reads; 1 open/write
    lines = open(path).read().splitlines()
    assert lines.count(",".join(pt.CSV_COLUMNS)) == 1 and len(lines) == 201
    lg2 = pt.TelemetryCSVLogger(path)                                # restart: existing file validated once
    lg2._header_ok = lambda o=lg2._header_ok: (reads.__setitem__("n", reads["n"] + 1), o())[1]
    for _ in range(50):
        lg2.write_rows(rows)
    assert reads["n"] == 1 and open(path).read().count(",".join(pt.CSV_COLUMNS)) == 1
    with open(path + ".tmp", "w") as f:                              # 9: external schema change -> rotate
        f.write("old,header\n1,2\n")
    os.replace(path + ".tmp", path)
    lg2.write_rows(rows)
    assert open(path).readline().strip().split(",") == pt.CSV_COLUMNS
    assert any(n.startswith("t.csv.schema-") for n in os.listdir(d))
    import test_stage7 as t7
    t7.test_csv()                                                    # the original Stage 7 CSV contract


# ═══════════════════ 10-12 HTTP sessions ═══════════════════
def test_http_sessions():
    s1, s2 = http_session.session(), http_session.session()
    assert s1 is s2                                                                # 10: reused within a thread
    other = {}
    th = threading.Thread(target=lambda: other.setdefault("s", http_session.session()))
    th.start(); th.join()
    assert other["s"] is not s1                                                    # 11: never shared across threads
    import http.cookiejar
    assert isinstance(s1.cookies._policy, http.cookiejar.DefaultCookiePolicy) and s1.cookies._policy.allowed_domains() == ()
    seen = []
    class FakeResp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"ok": 1}
    def fake_get(self, url, **kw):
        seen.append((url, kw)); return FakeResp()
    import requests
    orig = requests.Session.get
    requests.Session.get = fake_get
    try:
        assert k._get("https://example.invalid/x", {"a": 1}) == {"ok": 1}
        prov = pt.KalshiPerpProvider(base_url="https://example.invalid/v2", clock=lambda: 1000.0)
        assert prov._http_get is http_session.get
        prov._get_json("/margin/markets")
        f = lb.OutcomeFetcher(sleep=lambda s: None, min_interval=0)
        assert f.http_get is http_session.get
    finally:
        requests.Session.get = orig
    assert seen[0] == ("https://example.invalid/x", {"params": {"a": 1}, "timeout": 15})          # 12: unchanged
    assert seen[1] == ("https://example.invalid/v2/margin/markets", {"params": None, "timeout": 5.0})
    src = inspect.getsource(k._get)
    assert "timeout=15" in src and "params=params" in src


# ═══════════════════ 13-19 candles + settlement (optimisations deferred: prove unchanged) ═══════════════════
def test_candles_and_settlement_unchanged():
    import test_stage9 as t9
    for fn in ("candles", "spot", "current_market", "settle_pending"):                # 13-15 + 16: untouched
        assert hashlib.sha256(inspect.getsource(getattr(k, fn)).encode()).hexdigest() == t9.STRATEGY_FUNCTION_HASHES[fn], fn
    calls = []
    closes = [[1_700_000_000 + 60 * i, 1.0, 2.0, 1.5, 1.6 + i * 0.01, 10.0] for i in range(300)][::-1]
    def fake_get(url, params=None):
        calls.append((url, params)); return closes
    k._candle_cache.pop("TEST-USD", None)
    orig = k._get
    k._get = fake_get
    try:
        c1 = k.candles("TEST-USD")
        c2 = k.candles("TEST-USD")                                                     # within existing TTL
    finally:
        k._get = orig
    assert c1 == c2 and len(calls) == 1                                                # existing candle cache still works
    import test_stage10 as t10
    t10.fixture()
    t10.test_legacy_rows_and_golden_settlement()          # 16-18: winner/loser/stop/gap/maker-unfilled golden rows
    t10.test_trades_csv_migration()
    assert hashlib.sha256(inspect.getsource(k.kalshi_fee_cents).encode()).hexdigest() == \
        t9.STRATEGY_FUNCTION_HASHES["kalshi_fee_cents"]                                  # 19: fee function untouched


# ═══════════════════ 20-22 bootstrap ═══════════════════
def test_bootstrap_parity_and_memory():
    rng = random.Random(21)
    for n, seed in ((1, 1), (7, 2), (300, 3), (2000, 4)):
        rows = [{"ticker": f"T{rng.randint(0, n * 3)}{i}", "decision": ps.WOULD_BLOCK if rng.random() < 0.15 else ps.ALLOW,
                 "win": rng.random() > 0.3, "pc": round(rng.uniform(-95, 12), 1)} for i in range(n)]
        for stat in (ash._enrichment_stat, ash._pc_improvement_stat):
            assert ash._bootstrap(rows, stat, 500, seed) == ref_shadow_bootstrap(rows, stat, 500, seed), (n, stat)   # 20
    assert ash._bootstrap([], ash._enrichment_stat, 10, 1) == []
    only_allow = [{"ticker": f"A{i}", "decision": ps.ALLOW, "win": True, "pc": 5.0} for i in range(20)]
    assert ash._bootstrap(only_allow, ash._pc_improvement_stat, 50, 1) == ref_shadow_bootstrap(only_allow, ash._pc_improvement_stat, 50, 1)
    # 21 (behavioural): the cluster unit is still the ticker, keyed in sorted order, so results are
    # invariant to row order and multi-row tickers resample as one unit exactly like the reference.
    multi = []
    for i in range(400):
        tk = f"M{i % 150}"                                            # ~2.7 rows per ticker
        multi.append({"ticker": tk, "decision": ps.WOULD_BLOCK if rng.random() < 0.2 else ps.ALLOW,
                      "win": rng.random() > 0.35, "pc": round(rng.uniform(-95, 12), 1)})
    shuffled = list(multi)
    random.Random(99).shuffle(shuffled)
    for stat in (ash._enrichment_stat, ash._pc_improvement_stat):
        assert ash._bootstrap(multi, stat, 300, 8) == ref_shadow_bootstrap(multi, stat, 300, 8)
        by_tk = {}
        for r in shuffled:
            by_tk.setdefault(r["ticker"], []).append(r)
        regrouped = [r for tk in sorted(by_tk) for r in by_tk[tk]]
        assert ash._bootstrap(regrouped, stat, 300, 8) == ref_shadow_bootstrap(regrouped, stat, 300, 8)
        if stat is ash._enrichment_stat:                              # integer statistic: order-invariant
            assert ash._bootstrap(shuffled, stat, 300, 8) == ash._bootstrap(multi, stat, 300, 8)
    calls = {"n": 0}
    orig = ash.settlement_metrics
    ash.settlement_metrics = lambda rows: (calls.__setitem__("n", calls["n"] + 1), orig(rows))[1]
    try:
        big = [{"ticker": f"T{i}", "decision": ps.ALLOW if i % 7 else ps.WOULD_BLOCK, "win": i % 3 > 0, "pc": 1.0}
               for i in range(500)]
        ash._bootstrap(big, ash._enrichment_stat, 200, 1)
    finally:
        ash.settlement_metrics = orig
    assert calls["n"] == 0                                                           # 22: no per-replicate row lists


# ═══════════════════ 23-28 strategy / safety invariants ═══════════════════
def test_strategy_and_safety_invariants():
    m = json.load(open(os.path.join(HERE, "step5_baseline_manifest.json")))
    assert m["legacy_strategy_fingerprint"] == TRUSTED_V2 == sf.current_fingerprint(os.path.join(HERE, "kalshi_dashboard.py"))[0]  # 23
    d = tempfile.mkdtemp()
    s = cd.status(os.path.join(d, "none.json"), os.path.join(d, "p.json"), os.path.join(HERE, "step5_baseline_manifest.json"),
                  os.path.join(HERE, "kalshi_dashboard.py"), os.path.join(d, "c.json"), env_enabled=False)
    assert s["strategy_fingerprint_valid"] is True and s["final_status"] == "INACTIVE"                                  # 24
    assert not any("strategy drift" in x for x in s["problems"])
    vals, _ = sf.extract(os.path.join(HERE, "kalshi_dashboard.py"))
    assert vals == m["strategy_config_values"] and sf.strategy_config_hash(vals) == m["strategy_config_hash"]          # 27
    assert pp.ALPHA_GRID == (0.25, 0.50, 0.75, 1.00) and pp.MAX_ABS_PERP_DELTA == 0.05                                 # 28
    assert bp.THRESHOLD_QUANTILES == (5.0, 10.0, 15.0, 20.0, 25.0)
    assert bp.DEFAULT_GATES == {"min_threshold_total": 500, "min_threshold_blocked": 50, "min_threshold_kept": 300,
                                "min_block_fraction": 0.05, "max_block_fraction": 0.25, "min_loss_enrichment_pp": 5.0,
                                "q_max": 0.05}
    assert (pl.MAX_CONSECUTIVE_GATE_ERRORS, pl.LIVE_VETO_MAX_BLOCK_FRACTION, pl.LIVE_VETO_MAX_FAIL_OPEN_FRACTION,
            pl.MAX_GATE_LATENCY_MS, pl.MAX_EXPIRY_DAYS) == (3, 0.35, 0.50, 25.0, 30)
    import test_stage12 as t12
    t12.test_live_math()                                                                                              # 25
    t12.test_fail_open()                                                                                              # 26
    t12.test_gate_cannot_upgrade()


def test_memory_bounds():
    store = pt.SnapshotStore(["BTC"], 600, 1800.0)
    for i in range(5000):
        store.add(pt.PerpSnapshot(ts=float(i * 4), coin="BTC", source_status=pt.STATUS_FRESH))
    assert store.count("BTC") <= 600
    h = pt.PriceHistory(600, 1800.0)
    for i in range(5000):
        h.append(float(i * 4), 1.0)
    assert len(h) <= 600
    lg = pt.TelemetryCSVLogger(os.path.join(tempfile.mkdtemp(), "x.csv"))
    assert set(vars(lg)) == {"path", "columns", "_lock", "_validated_id"}       # the only new state is one tuple
    assert set(vars(http_session._local)) <= {"session"}                         # one session per thread, no growth


# ═══════════════════ 29 earlier stages ═══════════════════
def test_previous_stages():
    if MASTER:
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 13):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-500:], p.stderr[-500:])


if __name__ == "__main__":
    import test_stage10 as _t10
    try:
        run("1  master runner executes each stage exactly once", test_master_runner_runs_each_stage_once)
        run("2  standalone Stage 12 still re-verifies earlier stages (each once)", test_standalone_stage12_regression)
        run("3+4 point lookup parity (20k+ cases), no timestamp-list allocation", test_point_lookup_parity_and_no_allocation)
        run("5  causal feature engine parity (10k helper cases + 10k end-to-end cycles)", test_causal_feature_parity)
        run("6+7 no future snapshot; preview/record parity; preview read-only", test_causality_and_preview_parity)
        run("8+9 CSV: no per-cycle header reread; schema change still rotates", test_csv_header_io)
        run("10-12 thread-local HTTP sessions; request semantics unchanged", test_http_sessions)
        run("13-19 candles/settlement/fees unchanged (optimisations deferred)", test_candles_and_settlement_unchanged)
        run("20-22 bootstrap: exact parity, ticker clustering, no per-replicate row lists", test_bootstrap_parity_and_memory)
        run("23-28 fingerprint, deployment, constants, Step 4/5/6 parameters, veto math", test_strategy_and_safety_invariants)
        run("+  new caches bounded", test_memory_bounds)
        run("29 all Stage 1-12 suites", test_previous_stages)
    finally:
        if _t10.FX.get("dir"):
            shutil.rmtree(_t10.FX["dir"], ignore_errors=True)
    print("\nAll Stage 13 tests passed.")
