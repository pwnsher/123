#!/usr/bin/env python3
"""Stage 9 tests: outcome labeling + offline predictive analysis (Step 3). No network.
Run:  py test_stage9.py"""
import ast
import csv
import datetime as dt
import hashlib
import json
import math
import os
import random
import shutil
import subprocess
import sys
import tempfile

import requests

import analyze_perp_predictive as ap
import label_binary_outcomes as lb

HERE = os.path.dirname(os.path.abspath(__file__))
FAST = {"min_analysis_markets": 100, "min_candidate_markets": 300, "min_class_count": 50,
        "min_valid_folds": 3, "min_leadlag_anchors": 100}
REPS = 200


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


def _iso(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).isoformat().replace("+00:00", "Z")


# ═══════════════════ labeler helpers ═══════════════════
class FakeHTTP:
    def __init__(self, results):
        self.results, self.calls = results, []

    def __call__(self, url, timeout=None, **kw):
        self.calls.append(url)
        tk = url.rsplit("/", 1)[-1]
        res = self.results.get(tk)
        if isinstance(res, Exception):
            raise res
        class R:
            status_code = 200 if not isinstance(res, int) else res
            def json(s): return {"market": {"ticker": tk, "result": res}}
        return R()


def _tele_csv(path, tickers):
    """Minimal telemetry for the labeler: (ticker, coin, close_ts)."""
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts_utc", "coin", "binary_ticker", "binary_close_time"])
        for tk, coin, close in tickers:
            w.writerow([_iso(close - 300), coin, tk, _iso(close)])


def _fetcher(http):
    return lb.OutcomeFetcher(http_get=http, sleep=lambda s: None, min_interval=0)


# 1
def test_label_mapping():
    assert lb.map_result("yes") == 1 and lb.map_result("no") == 0 and lb.map_result(" YES ") == 1
    for bad in ("", None, "void", "all_no", "scalar", 1, "maybe"):
        assert lb.map_result(bad) is None, bad
    e = lb.apply_result({"binary_ticker": "X"}, "void", None, 1000.0)
    assert e["label_status"] == lb.ST_UNEXPECTED and e["outcome_up"] == ""
    e = lb.apply_result({"binary_ticker": "X"}, "", None, 1000.0)
    assert e["label_status"] == lb.ST_PENDING


# 2 + 3
def test_no_open_or_grace_queries():
    d = tempfile.mkdtemp()
    try:
        now = 10_000.0
        _tele_csv(os.path.join(d, "t.csv"), [("OPEN", "BTC", now + 300), ("GRACE", "ETH", now - 10),
                                              ("READY", "SOL", now - 200)])
        http = FakeHTTP({"OPEN": "yes", "GRACE": "yes", "READY": "no"})
        lb.run_labeler(os.path.join(d, "t.csv"), os.path.join(d, "o.csv"), now=now, grace=90,
                       fetcher=_fetcher(http), log=None)
        assert [u.rsplit("/", 1)[-1] for u in http.calls] == ["READY"], http.calls
        rows = {r["binary_ticker"]: r for r in csv.DictReader(open(os.path.join(d, "o.csv")))}
        assert rows["OPEN"]["label_status"] == "pending" and rows["GRACE"]["label_status"] == "pending"
        assert rows["READY"]["outcome_up"] == "0" and rows["READY"]["label_status"] == "final"
        assert all(u.startswith("https://external-api.kalshi.com/trade-api/v2/markets/") for u in http.calls)
    finally:
        shutil.rmtree(d)


# 4
def test_cache_idempotent():
    d = tempfile.mkdtemp()
    try:
        now = 10_000.0
        _tele_csv(os.path.join(d, "t.csv"), [("A", "BTC", now - 500), ("B", "ETH", now - 500)])
        http = FakeHTTP({"A": "yes", "B": ""})
        out = os.path.join(d, "o.csv")
        lb.run_labeler(os.path.join(d, "t.csv"), out, now=now, fetcher=_fetcher(http), log=None)
        http.calls.clear()
        lb.run_labeler(os.path.join(d, "t.csv"), out, now=now + 1000, fetcher=_fetcher(http), log=None)
        assert [u.rsplit("/", 1)[-1] for u in http.calls] == ["B"], http.calls      # final A not refetched
        http.calls.clear()
        lb.run_labeler(os.path.join(d, "t.csv"), out, now=now + 1010, fetcher=_fetcher(http), log=None)
        assert http.calls == [], "unsettled ticker re-polled too soon"
        before = open(os.path.join(d, "t.csv"), "rb").read()
        assert open(os.path.join(d, "t.csv"), "rb").read() == before
    finally:
        shutil.rmtree(d)


# 5
def test_conflicting_settlement():
    d = tempfile.mkdtemp()
    try:
        now = 10_000.0
        _tele_csv(os.path.join(d, "t.csv"), [("A", "BTC", now - 500)])
        out = os.path.join(d, "o.csv")
        lb.run_labeler(os.path.join(d, "t.csv"), out, now=now, fetcher=_fetcher(FakeHTTP({"A": "yes"})), log=None)
        c = lb.run_labeler(os.path.join(d, "t.csv"), out, now=now + 10, verify_final=True,
                           fetcher=_fetcher(FakeHTTP({"A": "no"})), log=None)
        r = next(csv.DictReader(open(out)))
        assert r["label_status"] == "conflict" and r["quality_flags"] == "CONFLICTING_SETTLEMENT_RESULT"
        assert r["settle_result"] == "yes" and r["conflicting_result"] == "no"      # both facts kept
        assert r["outcome_up"] == "" and "cached 'yes' vs API 'no'" in r["source_error"] and c["conflicts"] == ["A"]
        labels, _ = ap.load_labels(out)
        assert "A" not in labels                                                   # never analysed
        lb.run_labeler(os.path.join(d, "t.csv"), out, now=now + 20, verify_final=True,
                       fetcher=_fetcher(FakeHTTP({"A": "yes"})), log=None)
        assert next(csv.DictReader(open(out)))["label_status"] == "conflict"       # never auto-resolved
    finally:
        shutil.rmtree(d)


# 6
def test_labeler_get_only():
    tree = ast.parse(open(lb.__file__).read())
    called = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert not called & {"post", "put", "patch", "delete", "request", "Session"}, called
    lits = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
    for s in lits:
        assert not any(k in s.lower() for k in ("/portfolio", "/orders", "/order", "cancel", "/transfer")), s
    # retry/backoff + timeout behaviour on a flaky endpoint
    n = [0]
    def flaky(url, timeout=None):
        n[0] += 1
        assert timeout == lb.REQUEST_TIMEOUT_SECONDS
        if n[0] < 3:
            raise requests.Timeout("slow")
        class R:
            status_code = 200
            def json(s): return {"market": {"result": "yes"}}
        return R()
    slept = []
    f = lb.OutcomeFetcher(http_get=flaky, sleep=slept.append, min_interval=0)
    assert f.fetch("X") == ("yes", None) and n[0] == 3 and slept[:2] == [1.0, 2.0]
    f404 = lb.OutcomeFetcher(http_get=lambda u, timeout=None: type("R", (), {"status_code": 404})(),
                             sleep=lambda s: None, min_interval=0)
    assert f404.fetch("X") == (None, "HTTP 404") and f404.requests_made == 1


# ═══════════════════ selection helpers (internal row format) ═══════════════════
def mkrow(ts, ticker="T1", coin="BTC", close=1000.0, ready=True, signal=False, p=0.6, status="ok",
          session="S", spot=100.0, f=None, i=[0]):
    i[0] += 1
    return {"i": i[0], "session": session, "coin": coin, "ticker": ticker, "close_raw": "", "close": close,
            "status": status, "minutes_left": (close - ts) / 60.0, "spot": spot, "p": p, "fav": "UP",
            "signal": signal, "ready": ready, "causal_ok": ready, "spot_ms": ts * 1000,
            "perp_ms": ts * 1000 - 1000, "fe_ms": ts * 1000, "lag_ms": 1000.0, "ts": ts,
            "f": dict({c: None for c in ap.FEATURE_ALLOWLIST}, **(f or {}))}


# 7 + 10
def test_horizon_selector_one_per_horizon():
    close = 10_000.0
    rows = [mkrow(t, close=close) for t in range(int(close) - 600, int(close), 3)]
    by = ap.index_by_ticker(rows)
    obs = ap.build_horizon_observations(by, {"T1": {"y": 1}}, {})
    for h in (8, 6, 4, 2):
        assert len(obs[h]) == 1, (h, len(obs[h]))
        o = obs[h][0]
        assert o["ts"] <= close - 60 * h and 0 <= o["horizon_early_ms"] <= 10_000
    rows2 = rows + [mkrow(t, ticker="T2", close=close) for t in range(int(close) - 600, int(close), 1)]
    obs2 = ap.build_horizon_observations(ap.index_by_ticker(rows2), {"T1": {"y": 1}, "T2": {"y": 0}}, {})
    for h in (8, 6, 4, 2):
        assert sorted(o["ticker"] for o in obs2[h]) == ["T1", "T2"]     # one per ticker, never per row


# 8
def test_selector_never_future():
    rows = [mkrow(t) for t in (96.0, 99.0, 101.0)]
    ts = [r["ts"] for r in rows]
    assert ap.select_horizon_row(rows, ts, 100.0)["ts"] == 99.0
    assert ap.select_horizon_row(rows, ts, 99.0)["ts"] == 99.0        # exactly-at-target allowed


# 9
def test_selector_too_early():
    rows = [mkrow(t) for t in (60.0, 85.0, 130.0)]
    assert ap.select_horizon_row(rows, [r["ts"] for r in rows], 100.0) is None     # 15 s early > 10 s


# 11
def test_first_signal():
    close = 5000.0
    rows = [mkrow(close - 700, close=close, signal=True),                  # 11.7 min left: outside window
            mkrow(close - 450, close=close, signal=False),
            mkrow(close - 400, close=close, signal=True, p=0.81),          # first valid signal
            mkrow(close - 300, close=close, signal=True, p=0.9),
            mkrow(close - 200, close=close, signal=True, p=0.95)]
    obs = ap.build_first_signal_cohort(ap.index_by_ticker(rows), {"T1": {"y": 1}}, {})
    assert len(obs) == 1 and obs[0]["ts"] == close - 400 and obs[0]["p"] == 0.81
    assert obs[0]["favored_correct"] is True and abs(obs[0]["base_error"] - 0.19) < 1e-12


# 12
def test_analysis_ready_filtering():
    close = 5000.0
    target = close - 240
    rows = [mkrow(target - 8, close=close, p=0.3),
            mkrow(target - 1, close=close, ready=False, p=0.9),           # closer but not ready
            mkrow(target - 0.5, close=close, status="stale", p=0.8)]      # binary not ok
    rows[2]["ready"] = True
    obs = ap.build_horizon_observations(ap.index_by_ticker(rows), {"T1": {"y": 0}}, {}, horizons=(4,))
    assert len(obs[4]) == 1 and obs[4][0]["p"] == 0.3


# ═══════════════════ synthetic CSV end-to-end fixture ═══════════════════
T0 = 1_780_000_000.0


def write_e2e(d, n_times=120, leak_decoys=True, signal_feature="causal_premium_bps", seed=7, shuffle=False,
              bad_row=None):
    """Telemetry + labels. One real row per (ticker, horizon) at target-2s, plus (optionally)
    a 'leak decoy' row at target+3s whose features ENCODE THE OUTCOME. A causal selector
    must never pick it."""
    import perp_telemetry as pt
    rng = random.Random(seed)
    # schema-v3 research features get their own stream, so adding them leaves the original
    # (step2_v1) synthetic realization — labels, base probabilities, planted signal — bit-identical
    v3 = sorted(set(ap.FEATURE_ALLOWLIST) & set(pt.VOLATILITY_COLUMNS))
    xrng = random.Random(seed + 1)
    tpath, lpath = os.path.join(d, "tele.csv"), os.path.join(d, "labels.csv")
    rows, labels = [], []
    for j in range(n_times):
        close = T0 + j * 900.0
        for coin in ap.COINS:
            tk = f"KX{coin}15M-{j:04d}"
            b, s, z = rng.gauss(0, 1.2), rng.gauss(0, 1), rng.gauss(0, 1)
            y = 1 if rng.random() < ap.sigmoid(b + 0.4 * s + 0.9 * z) else 0
            labels.append((tk, coin, close, y))
            p = round(ap.sigmoid(b) * 100, 4)
            feats = {c: round(rng.gauss(0, 1), 6) for c in ap.FEATURE_ALLOWLIST if c not in v3}
            feats.update({c: round(xrng.gauss(0, 1), 6) for c in v3})
            for c in ("causal_spot_ret_30s_bps", "causal_spot_ret_60s_bps", "causal_spot_ret_180s_bps"):
                feats[c] = round(s, 6)
            for c in ("spot_vol_shock_60v300", "spot_vol_shock_60v900", "perp_vol_shock_60v300",
                      "perp_vol_shock_60v900", "perp_spread_bps"):
                feats[c] = round(abs(rng.gauss(1, 0.3)), 6)
            feats[signal_feature] = round(3 * z, 6)
            for h in (8, 6, 4, 2):
                target = close - 60 * h
                rows.append((target - 2, tk, coin, close, p, feats, h == 6))
                if leak_decoys:
                    leak = dict(feats, **{signal_feature: 1000.0 * (2 * y - 1), "causal_spot_ret_60s_bps": 0.0})
                    rows.append((target + 3, tk, coin, close, p, leak, False))
    with open(tpath, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=pt.CSV_COLUMNS)
        w.writeheader()
        for k, (ts, tk, coin, close, p, feats, sig) in enumerate(sorted(rows)):
            spot_ms = int(ts * 1000)
            row = {c: "" for c in pt.CSV_COLUMNS}
            row.update(telemetry_schema_version=pt.TELEMETRY_SCHEMA_VERSION, feature_version=pt.FEATURE_VERSION,
                       telemetry_session_id="SESS",
                       cycle_id=k, ts_utc=_iso(ts), ts_epoch_ms=spot_ms + 200, coin=coin, binary_status="ok",
                       binary_ticker=tk, binary_close_time=_iso(close), minutes_left=round((close - ts) / 60, 1),
                       spot_price=100.0, base_p_up=p, fav="UP" if p >= 50 else "DOWN", binary_signal=sig,
                       spot_observed_ts_epoch_ms=spot_ms, perp_snapshot_ts_epoch_ms=spot_ms - 1500,
                       perp_lag_to_spot_ms=1500, feature_end_ts_epoch_ms=spot_ms, causal_pair_ok=True,
                       analysis_ready=True, **feats)
            if bad_row is not None and k == bad_row:
                row["perp_snapshot_ts_epoch_ms"] = spot_ms + 2000        # a FUTURE perp snapshot
                row["perp_lag_to_spot_ms"] = -2000
            w.writerow(row)
    ys = [l[3] for l in labels]
    if shuffle:
        random.Random(99).shuffle(ys)
    with open(lpath, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(lb.OUT_COLUMNS)
        for (tk, coin, close, _), y in zip(labels, ys):
            w.writerow([tk, coin, _iso(close), "yes" if y else "no", y, "final", "", lb.SOURCE, "", "", "", 1, "", ""])
    return tpath, lpath


E2E = {}


def _e2e():
    if not E2E:
        d = tempfile.mkdtemp(prefix="_t9e2e")
        t, l = write_e2e(d)
        sha = hashlib.sha256(open(t, "rb").read()).hexdigest()
        rep = ap.run_analysis(t, l, os.path.join(d, "out"), reps=REPS, thresholds=FAST, log=None)
        E2E.update(dir=d, tele=t, labels=l, rep=rep, sha=sha)
    return E2E


# 13
def test_causal_invariant_failure():
    d = tempfile.mkdtemp()
    try:
        t, l = write_e2e(d, n_times=8, bad_row=5)
        try:
            ap.run_analysis(t, l, os.path.join(d, "out"), reps=10, thresholds=FAST, log=None)
            raise AssertionError("analysis should have failed")
        except ap.CausalInvariantError as e:
            assert "perp_snapshot_ts > spot_observed_ts" in str(e)
        p = subprocess.run([sys.executable, "analyze_perp_predictive.py", "--telemetry", t, "--labels", l,
                            "--outdir", os.path.join(d, "o2")], cwd=HERE, capture_output=True, text=True)
        assert p.returncode == 3 and "ANALYSIS FAILED" in p.stderr
        assert not os.path.exists(os.path.join(d, "o2", "perp_candidate_manifest.json"))
    finally:
        shutil.rmtree(d)


# ═══════════════════ synthetic observation-level generator ═══════════════════
def synth(n=1500, seed=1, feat="causal_perp_ret_60s_bps", kind="positive", coins=ap.COINS):
    rng = random.Random(seed)
    obs = []
    for i in range(n):
        base, spot, sig = rng.gauss(0, 1.2), rng.gauss(0, 1), rng.gauss(0, 1)
        if kind == "positive":
            true, fv = base + 0.4 * spot + 0.8 * sig, sig
        elif kind == "confound":
            true, fv = base + 0.8 * spot, spot + rng.gauss(0, 0.3)
        else:
            true, fv = base + 0.4 * spot, rng.gauss(0, 1)
        y = 1 if rng.random() < ap.sigmoid(true) else 0
        p = ap.sigmoid(base)
        c = coins[i % len(coins)]
        f = {k: None for k in ap.FEATURE_ALLOWLIST}
        for ctl in ap.spot_controls_for(feat):
            f[ctl] = spot if "ret" in ctl else abs(rng.gauss(1, .2))
        f[feat] = fv
        obs.append({"ticker": f"T{i:05d}{c}", "coin": c, "session": "s", "close": 1e9 + (i // len(coins)) * 900.0,
                    "ts": 0.0, "horizon": 4, "y": y, "p": p, "base_logit": ap.logit(p),
                    "fav": "UP" if p >= .5 else "DOWN", "f": f, "ready": True, "row_index": i,
                    "horizon_early_ms": 0, "favored_correct": None, "base_error": 0})
    return obs


def screen(obs, feat="causal_perp_ret_60s_bps", th=None, **kw):
    r = ap.screen_feature(obs, feat, group=kw.pop("group", "ALL"), horizon=4, reps=kw.pop("reps", REPS),
                          thresholds=th or FAST, **kw)
    ap.apply_bh_and_classify([r], th or FAST)
    return r


# 14 + 15
def test_walk_forward_split():
    obs = synth(400)
    folds = ap.walk_forward_folds([o["close"] for o in obs])
    assert len(folds) == 4
    for fno, tr, te in folds:
        trk, tek = {obs[i]["ticker"] for i in tr}, {obs[i]["ticker"] for i in te}
        assert not trk & tek
        assert max(obs[i]["close"] for i in tr) < min(obs[i]["close"] for i in te)     # strictly earlier
        assert not {obs[i]["close"] for i in tr} & {obs[i]["close"] for i in te}     # same-time markets together
    sizes = [len(te) for _, _, te in folds]
    assert abs(len(folds[0][1]) - 160) <= 4 and all(abs(s - 60) <= 4 for s in sizes), sizes
    shuffled = list(range(len(obs)))
    random.Random(3).shuffle(shuffled)
    keys = [obs[i]["close"] for i in shuffled]                    # input order must not matter
    for _, tr, te in ap.walk_forward_folds(keys):
        assert max(keys[i] for i in tr) < min(keys[i] for i in te)


# 16 + 34
def test_train_only_preprocessing_and_quintiles():
    obs = synth(800, kind="positive", feat="causal_premium_bps")
    r1 = screen(obs, "causal_premium_bps")
    last = r1["folds"][-1]
    poisoned = [dict(o, f=dict(o["f"])) for o in obs]
    for o in poisoned:
        if o["close"] >= last["test_close_min"]:               # only the final holdout block
            o["f"]["causal_premium_bps"] = 1e6
            o["f"]["causal_spot_ret_60s_bps"] = -1e6
    r2 = screen(poisoned, "causal_premium_bps")
    for a, b in zip(r1["folds"], r2["folds"]):
        assert a["scalers"] == b["scalers"], a["fold"]                           # 16
        assert a["quintile_bounds"] == b["quintile_bounds"], a["fold"]            # 34
    # the scaler itself: winsorised to train percentiles, extreme holdout clipped
    s = ap.Scaler([float(i) for i in range(101)])
    assert s.lo == 1.0 and s.hi == 99.0 and s(1e9) == s(99.0)


# 17-20
def test_metrics():
    p, y = [0.9, 0.2, 0.6, 0.4], [1, 0, 0, 1]
    assert abs(ap.brier(p, y) - (0.01 + 0.04 + 0.36 + 0.36) / 4) < 1e-12
    exp = -(math.log(0.9) + math.log(0.8) + math.log(0.4) + math.log(0.4)) / 4
    assert abs(ap.logloss(p, y) - exp) < 1e-12
    assert abs(ap.logloss([1.0, 0.0], [0, 1]) + math.log(1e-6)) < 1e-9          # clamped, finite
    assert ap.auc([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]) == 0.75
    assert ap.auc([0.5, 0.5], [0, 1]) == 0.5 and ap.auc([0.2, 0.3], [1, 1]) is None
    pe, ye = [0.05, 0.15, 0.95, 0.95], [0, 1, 1, 0]
    # bins: [0,.1): p.05 y0 -> |.05-0|; [.1,.2): |.15-1|; [.9,1]: mean .95 freq .5 -> .45
    assert abs(ap.ece(pe, ye) - (0.25 * 0.05 + 0.25 * 0.85 + 0.5 * 0.45)) < 1e-12
    cb = ap.calibration_bins([1.0], [1])
    assert cb[-1]["n"] == 1 and cb[-1]["bin"].endswith("]")


# 21-22
def test_logistic_solver():
    rng = random.Random(5)
    X, y = [], []
    for _ in range(4000):
        a, b = rng.gauss(0, 1), rng.gauss(0, 1)
        X.append([a, b]); y.append(1 if rng.random() < ap.sigmoid(0.3 + 1.5 * a - 0.7 * b) else 0)
    fit = ap.fit_logistic(X, y)
    b0, b1, b2 = fit["beta"]
    assert fit["converged"] and abs(b0 - 0.3) < 0.15 and abs(b1 - 1.5) < 0.2 and abs(b2 + 0.7) < 0.15, fit
    p = ap.predict_logistic(fit["beta"], [[3, 0], [-3, 0]])
    assert p[0] > 0.95 and p[1] < 0.05
    assert ap.fit_logistic(X, y) == fit                                                    # deterministic


def test_logistic_stability():
    X = [[ap.logit(1.0), 1e6], [ap.logit(0.0), -1e6], [13.8, 5.0], [-13.8, -5.0]] * 20
    y = [1, 0, 1, 0] * 20                                                                   # separable
    fit = ap.fit_logistic(X, y)
    assert all(math.isfinite(b) for b in fit["beta"])
    for pr in ap.predict_logistic(fit["beta"], X + [[1e9, -1e9]]):
        assert math.isfinite(pr) and 0.0 <= pr <= 1.0
    assert all(math.isfinite(ap.logloss([pr], [yy])) for pr, yy in zip(ap.predict_logistic(fit["beta"], X), y))
    try:
        ap.solve_linear([[1, 2], [2, 4]], [1, 2]); raise AssertionError("singular not detected")
    except ap.SingularMatrixError:
        pass


# 23
def test_same_rows_for_b_and_c():
    obs = synth(900, kind="positive")
    rng = random.Random(11)
    for o in obs:
        if rng.random() < 0.3:
            o["f"]["causal_perp_ret_60s_bps"] = None             # control present, feature missing
    r = ap.screen_feature(obs, "causal_perp_ret_60s_bps", group="ALL", horizon=4, reps=50, thresholds=FAST,
                          keep_oof=True)
    have = {o["ticker"] for o in obs if o["f"]["causal_perp_ret_60s_bps"] is not None}
    oof = r["_oof"]
    assert oof and {x["ticker"] for x in oof} <= have
    assert all(x["pa"] is not None and x["pb"] is not None and x["pc"] is not None for x in oof)
    assert r["n_available"] == len(have) and abs(r["coverage_pct"] - 100 * len(have) / 900) < 0.01
    assert r["oof_brier_b"] == ap.brier([x["pb"] for x in oof], [x["y"] for x in oof])


# 24-27
def test_positive_control():
    r = screen(synth(1500, seed=1, kind="positive"))
    assert r["brier_improvement_vs_b"] > 0 and r["logloss_improvement_vs_b"] > 0 and r["brier_ci_low"] > 0
    assert r["status"] == ap.ST_CANDIDATE, (r["status"], r["status_reasons"])
    assert r["coefficient_sign"] == "+"


def test_spot_confound():
    vs_a, cands, dbs = 0, 0, []
    for seed in range(1, 6):
        r = screen(synth(1500, seed=seed, kind="confound"))
        vs_a += r["brier_improvement_vs_a"] > 0.005
        cands += r["status"] == ap.ST_CANDIDATE
        dbs.append(r["brier_improvement_vs_b"])
    assert vs_a >= 4, "confound should look useful against MODEL A"
    assert cands == 0 and max(dbs) < 0.003, dbs


def test_noise_feature():
    cands = sum(screen(synth(1500, seed=s, kind="noise"))["status"] == ap.ST_CANDIDATE for s in range(1, 6))
    assert cands == 0


def test_shuffled_labels():
    obs = synth(1500, seed=1, kind="positive")
    ys = [o["y"] for o in obs]
    random.Random(42).shuffle(ys)
    for o, yy in zip(obs, ys):
        o["y"] = yy
    r = screen(obs)
    assert r["status"] != ap.ST_CANDIDATE and r["brier_ci_low"] <= 0, (r["status"], r["brier_ci_low"])


# 28-30
def test_bootstrap_ci():
    rng = random.Random(1)
    pos = [0.01 + rng.gauss(0, 0.002) for _ in range(300)]
    b = ap.cluster_bootstrap([f"t{i}" for i in range(300)], {"d": pos}, 500, 7)["d"]
    lo, hi = ap.ci95(b)
    m = sum(pos) / len(pos)
    assert 0 < lo < m < hi and ap.bootstrap_p_improvement(b) == 1 / 501, (lo, m, hi)
    neg = [-v for v in pos]
    lo2, hi2 = ap.ci95(ap.cluster_bootstrap([f"t{i}" for i in range(300)], {"d": neg}, 500, 7)["d"])
    assert hi2 < 0
    # clustering: 2 rows per ticker are resampled together
    cl = [f"t{i // 2}" for i in range(20)]
    reps = ap.cluster_bootstrap(cl, {"d": [float(i // 2) for i in range(20)]}, 50, 3)["d"]
    assert all(abs(v * 20 - round(v * 20)) < 1e-9 for v in reps)


def test_bootstrap_deterministic():
    rng = random.Random(2)
    d = [rng.gauss(0, 1) for _ in range(200)]
    c = [f"t{i}" for i in range(200)]
    a1 = ap.ci95(ap.cluster_bootstrap(c, {"d": d}, 300, 11)["d"])
    a2 = ap.ci95(ap.cluster_bootstrap(c, {"d": d}, 300, 11)["d"])
    a3 = ap.ci95(ap.cluster_bootstrap(c, {"d": d}, 300, 12)["d"])
    assert a1 == a2 and a1 != a3


def test_bh():
    q = ap.bh_qvalues([0.01, 0.04, 0.03, 0.005])
    assert [round(x, 10) for x in q] == [0.02, 0.04, 0.04, 0.02], q
    assert ap.bh_qvalues([0.5, None, 0.01]) == [0.5, None, 0.02]
    assert ap.bh_qvalues([0.9, 0.95]) == [0.95, 0.95]
    # family: 7 primaries at p=0.02 each -> q = 0.02 (not inflated) ; one at 0.02 among six 0.9s -> 0.14
    q2 = ap.bh_qvalues([0.02] + [0.9] * 6)
    assert abs(q2[0] - 0.14) < 1e-12
    # wiring: the classifier really applies BH across the 7 primaries of one family
    fam = [{"cohort": "primary", "group": "BTC", "horizon_min": 4, "primary_or_secondary": "primary",
            "feature_family": "directional", "feature_name": f, "p_value": p, "n_available": 1000,
            "valid_folds": 4, "brier_improvement_vs_b": 0.01, "logloss_improvement_vs_b": 0.01,
            "brier_ci_low": 0.001, "sign_consistency_pct": 100.0, "yes_count": 500, "no_count": 500}
           for f, p in zip(ap.PRIMARY_FEATURES, [0.02] + [0.9] * 6)]
    ap.apply_bh_and_classify(fam, FAST)
    assert abs(fam[0]["q_value"] - 0.14) < 1e-12 and fam[0]["status"] != ap.ST_CANDIDATE
    other = dict(fam[0], group="ETH", p_value=0.02)                  # separate family -> not diluted
    ap.apply_bh_and_classify([other], FAST)
    assert other["q_value"] == 0.02 and other["status"] == ap.ST_CANDIDATE


# 31-33
def test_sample_size_gate():
    rng = random.Random(1)
    obs = synth(150, seed=3, kind="positive")
    for o in obs:
        o["y"] = 1 if o["f"]["causal_perp_ret_60s_bps"] > 0 else 0         # perfectly predictable
    r = screen(obs, th=dict(ap.DEFAULT_THRESHOLDS))
    assert r["status"] == ap.ST_INSUFFICIENT
    # the classifier gate on its own (defence in depth): perfect metrics, 150 markets
    perfect = {"n_available": 150, "valid_folds": 4, "primary_or_secondary": "primary",
               "feature_family": "directional", "brier_improvement_vs_b": 0.2, "logloss_improvement_vs_b": 0.5,
               "brier_ci_low": 0.1, "q_value": 1e-4, "sign_consistency_pct": 100.0, "yes_count": 75, "no_count": 75}
    assert ap.classify(perfect, ap.DEFAULT_THRESHOLDS)[0] == ap.ST_INSUFFICIENT
    assert ap.classify(dict(perfect, n_available=450, yes_count=225, no_count=225),
                       ap.DEFAULT_THRESHOLDS)[0] == ap.ST_EXPLORATORY          # 200-499: shown, never a candidate
    r2 = screen(synth(400, seed=1, kind="positive"), th=dict(ap.DEFAULT_THRESHOLDS))
    assert r2["status"] in (ap.ST_EXPLORATORY, ap.ST_NO_VALUE, ap.ST_UNSTABLE) and r2["status"] != ap.ST_CANDIDATE
    assert "below candidate sample thresholds" in " ".join(r2["status_reasons"]) or r2["status"] != ap.ST_EXPLORATORY


def test_secondary_never_candidate():
    for feat in ("causal_perp_ret_30s_bps", "premium_z_15m", "funding_rate"):
        r = screen(synth(1500, seed=1, kind="positive", feat=feat), feat)
        assert r["brier_improvement_vs_b"] > 0 and r["brier_ci_low"] > 0, feat
        assert r["status"] == ap.ST_EXPLORATORY, (feat, r["status"])
    assert ap.feature_family("funding_rate") == "EXPLORATORY_FUNDING"


def test_sign_stability():
    good = {"n_available": 1000, "valid_folds": 4, "primary_or_secondary": "primary", "feature_family": "directional",
            "brier_improvement_vs_b": 0.01, "logloss_improvement_vs_b": 0.02, "brier_ci_low": 0.002,
            "q_value": 0.01, "yes_count": 500, "no_count": 500}
    assert ap.classify(dict(good, sign_consistency_pct=100.0), FAST)[0] == ap.ST_CANDIDATE
    assert ap.classify(dict(good, sign_consistency_pct=50.0), FAST)[0] == ap.ST_UNSTABLE
    ss = ap.sign_stats([0.5, -0.4, 0.6, -0.2], 4)
    assert ss["positive_folds"] == 2 and ss["negative_folds"] == 2 and ss["sign_consistency_pct"] == 50.0
    assert ap.sign_stats([0.5, 0.4, 0.1], 4)["zero_or_failed_folds"] == 1
    # data-generated reversal: the effect flips sign halfway through time
    obs = synth(1600, seed=4, kind="positive")
    for o in obs[len(obs) * 55 // 100:]:
        v = o["f"]["causal_perp_ret_60s_bps"]
        o["y"] = 1 if random.Random(o["row_index"]).random() < ap.sigmoid(o["base_logit"] - 2.5 * v) else 0
    r = screen(obs)
    assert r["status"] != ap.ST_CANDIDATE and r["gamma_positive_folds"] >= 1 and r["gamma_negative_folds"] >= 1


# ═══════════════════ lead / lag ═══════════════════
def test_future_spot_return():
    rows = [mkrow(float(t), spot=100.0 + t / 10.0, ticker="L") for t in range(0, 300, 4)]
    for H in (10, 30, 60, 120):
        anchors = ap.build_leadlag_anchors(rows, H, tol=8.0, spacing=1)
        a = next(x for x in anchors if x["ts"] == 0.0)
        cands = [t for t in range(0, 300, 4) if abs(t - H) <= 8]
        tf = min(cands, key=lambda t: abs(t - H))
        assert a["label_ts"] == tf and abs(a["y"] - ((100 + tf / 10) / 100 - 1) * 1e4) < 1e-9, (H, a)
        assert all(x["label_ts"] > x["ts"] for x in anchors)
        assert "y" not in a["f"] and not any("future" in k for k in a["f"])        # label never a feature
    thin = ap.build_leadlag_anchors(rows, 60)
    assert all(b["ts"] - a["ts"] >= 60 for a, b in zip(thin, thin[1:]))            # overlap reduction


def test_future_label_same_session_and_coin():
    rows = [mkrow(0.0, session="A", spot=100.0), mkrow(30.0, session="B", spot=200.0),
            mkrow(30.0, session="A", coin="ETH", spot=300.0)]
    assert ap.build_leadlag_anchors(rows, 30) == []
    rows += [mkrow(15.0, session="A", spot=100.5, ready=False), mkrow(31.0, session="A", spot=101.0, ready=False)]
    a = ap.build_leadlag_anchors(rows, 30)
    assert len(a) == 1 and a[0]["ts"] == 0.0 and abs(a[0]["y"] - 100.0) < 1e-9, a     # same session+coin only


def test_future_label_tolerance_and_gaps():
    rows = [mkrow(0.0, spot=100.0), mkrow(45.0, spot=101.0)]                        # nearest to +30 is 15 s off
    assert ap.build_leadlag_anchors(rows, 30, tol=8.0) == []
    gap = [mkrow(0.0, spot=100.0), mkrow(4.0, spot=100.0), mkrow(118.0, spot=103.0), mkrow(122.0, spot=103.0)]
    assert [a for a in ap.build_leadlag_anchors(gap, 120, spacing=1) if a["ts"] == 0.0] == []   # 114 s hole


def test_ols():
    rng = random.Random(3)
    X = [[rng.gauss(0, 1), rng.gauss(0, 1)] for _ in range(500)]
    y = [2.0 + 3.0 * a - 1.5 * b for a, b in X]
    beta = ap.fit_ols(X, y)
    assert all(abs(u - v) < 1e-8 for u, v in zip(beta, [2.0, 3.0, -1.5]))
    try:
        ap.fit_ols([[a, 2 * a] for a, _ in X], y); raise AssertionError("collinearity not detected")
    except ap.SingularMatrixError:
        pass


def _lead_path(n=2400, lead=True, seed=5):
    """Rows every 4 s. Perp moves first; spot copies the perp move 32 s later (lead=True)
    or at the same time (lead=False: identical information)."""
    rng = random.Random(seed)
    e = [rng.gauss(0, 3) for _ in range(n)]
    lagk = 8 if lead else 0
    perp, spot = [0.0], [0.0]
    for i in range(1, n):
        perp.append(perp[-1] + e[i])
        spot.append(spot[-1] + (e[i - lagk] if i - lagk >= 0 else 0.0) + rng.gauss(0, 0.5))
    rows = []
    for i in range(n):
        f = {}
        for h, k in ((30, 8), (60, 15), (180, 45)):
            if i >= k:
                f[f"causal_perp_ret_{h}s_bps"] = perp[i] - perp[i - k]
                f[f"causal_spot_ret_{h}s_bps"] = spot[i] - spot[i - k]
                f[f"momentum_gap_{h}s_bps"] = f[f"causal_perp_ret_{h}s_bps"] - f[f"causal_spot_ret_{h}s_bps"]
        rows.append(mkrow(float(4 * i), spot=100.0 * math.exp(spot[i] / 1e4), ticker=f"M{i // 225}", f=f))
    return rows


def test_genuine_lead():
    anchors = ap.build_leadlag_anchors(_lead_path(lead=True), 30)
    r = ap.screen_leadlag(anchors, "causal_perp_ret_30s_bps", 30, group="BTC", reps=REPS, thresholds=FAST)
    r["q_value"] = ap.bh_qvalues([r["p_value"]])[0]
    assert r["mse_improvement"] > 0 and r["mse_ci_low"] > 0 and r["r2_oos_aug"] > r["r2_oos_base"] + 0.2, r
    assert ap.classify_leadlag(r, FAST) == ap.LL_SIGNAL and r["gamma_dominant_sign"] == "+"


def test_no_incremental_lead():
    anchors = ap.build_leadlag_anchors(_lead_path(lead=False), 30)
    r = ap.screen_leadlag(anchors, "causal_perp_ret_30s_bps", 30, group="BTC", reps=REPS, thresholds=FAST)
    r["q_value"] = ap.bh_qvalues([r["p_value"]])[0]
    assert abs(r.get("mse_improvement") or 0) < 0.02 * (r.get("mse_base") or 1), r
    assert ap.classify_leadlag(r, FAST) != ap.LL_SIGNAL
    for a in anchors:                                                   # exact duplicate information
        a["f"]["causal_perp_ret_30s_bps"] = a["f"].get("causal_spot_ret_30s_bps")
    r2 = ap.screen_leadlag(anchors, "causal_perp_ret_30s_bps", 30, group="BTC", reps=REPS, thresholds=FAST)
    assert r2["valid_folds"] == 0 and ap.classify_leadlag(dict(r2, q_value=None), FAST) != ap.LL_SIGNAL


# ═══════════════════ static / end-to-end / reproducibility ═══════════════════
STDLIB_OK = {"argparse", "array", "bisect", "csv", "datetime", "hashlib", "json", "math", "os", "random", "sys", "collections"}


def test_analyzer_offline():
    tree = ast.parse(open(ap.__file__).read())
    mods = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
           {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    assert mods <= STDLIB_OK, mods - STDLIB_OK
    # runtime: sockets disabled, numpy/pandas/requests unavailable -> still runs
    d = tempfile.mkdtemp()
    try:
        t, l = write_e2e(d, n_times=30)
        code = ("import sys, socket\n"
                "for m in ('numpy','pandas','scipy','sklearn','statsmodels','requests','urllib3'): sys.modules[m]=None\n"
                "def boom(*a,**k): raise RuntimeError('network used')\n"
                "socket.socket=boom; socket.create_connection=boom\n"
                "import analyze_perp_predictive as ap\n"
                f"ap.run_analysis({t!r},{l!r},{os.path.join(d, 'o')!r},reps=20,log=None)\n"
                "print('OK')")
        p = subprocess.run([sys.executable, "-c", code], cwd=HERE, capture_output=True, text=True)
        assert p.returncode == 0 and "OK" in p.stdout, p.stderr[-800:]
    finally:
        shutil.rmtree(d)


def test_allowlist():
    for bad in ("outcome_up", "settle_result", "win", "pnl", "exit_reason", "future_spot_return_30",
                "binary_signal", "base_p_up", "net_edge", "spot_price"):
        try:
            ap.assert_allowed_predictor(bad); raise AssertionError(f"{bad} accepted")
        except ValueError:
            pass
        try:
            ap.screen_feature(synth(50), bad); raise AssertionError(f"screen accepted {bad}")
        except ValueError:
            pass
    for c in ap.SPOT_CONTROL_FEATURES:
        try:
            ap.screen_feature(synth(50), c); raise AssertionError("spot control screened as candidate")
        except ValueError:
            pass
    assert set(ap.PRIMARY_FEATURES) <= set(ap.CANDIDATE_FEATURES) and len(ap.PRIMARY_FEATURES) == 8      # step3_v2 added exactly one: perp_momentum_z_60s


def test_e2e_leak_decoys_and_outputs():
    e = _e2e()
    rep, out = e["rep"], os.path.join(e["dir"], "out")
    m = rep["dataset_manifest"]
    assert m["unique_tickers_labeled"] == 480 and m["unique_tickers_analyzed"] == 480 and m["unlabeled_tickers"] == 0
    assert m["telemetry_schema_version"] == "3" and m["feature_version"] == "step2_v2" and m["bootstrap_seed"] == ap.SEED
    b = rep["baseline"]["ALL|4"]
    assert b["unique_markets"] == 480 and b["yes_count"] + b["no_count"] == 480 and len(b["calibration"]) == 10
    # the leak decoys (after target, outcome encoded) must never be selected
    scr = {(r["cohort"], r["group"], r["horizon_min"], r["feature_name"]): r for r in rep["feature_screen"]}
    prem = scr[("primary", "ALL", 4, "causal_premium_bps")]
    assert prem["auc_c"] < 0.9, "a future (leak decoy) row was selected"
    assert prem["brier_improvement_vs_b"] > 0 and prem["status"] == ap.ST_CANDIDATE, prem["status_reasons"]
    for fname in ("perp_predictive_report.json", "perp_feature_screen.csv", "perp_leadlag_report.csv",
                  "perp_candidate_manifest.json", "perp_oof_predictions.csv"):
        assert os.path.exists(os.path.join(out, fname)), fname
    man = json.load(open(os.path.join(out, "perp_candidate_manifest.json")))
    assert man and all(c["feature"] in ap.PRIMARY_FEATURES for c in man)
    forbidden = {"threshold", "veto", "position_size", "confidence_modifier", "trade", "instruction"}
    assert not any(k for c in man for k in c if any(f in k for f in forbidden))
    hdr = next(csv.reader(open(os.path.join(out, "perp_feature_screen.csv"))))
    for c in ("oof_brier_a", "oof_brier_b", "oof_brier_c", "brier_improvement_vs_b", "q_value",
              "sign_consistency_pct", "interaction_sign", "status", "coverage_pct"):
        assert c in hdr, c
    assert {r["cohort"] for r in rep["feature_screen"]} == {"primary", "first_signal"}
    assert rep["first_signal_cohort"]["ALL"]["calls"] == 480
    assert hashlib.sha256(open(e["tele"], "rb").read()).hexdigest() == e["sha"]    # raw telemetry untouched
    # default (conservative) thresholds on the same small data -> nothing promoted
    rep2 = ap.run_analysis(e["tele"], e["labels"], os.path.join(e["dir"], "out_default"), reps=50, log=None)
    assert rep2["candidates"] == [] and "No feature currently meets" in ap.render(rep2)
    assert all(r["status"] in (ap.ST_INSUFFICIENT, ap.ST_EXPLORATORY) for r in rep2["feature_screen"])


def test_reproducible():
    e = _e2e()
    out2 = os.path.join(e["dir"], "out_repeat")
    ap.run_analysis(e["tele"], e["labels"], out2, reps=REPS, thresholds=FAST, log=None)
    def load(d):
        j = json.load(open(os.path.join(d, "perp_predictive_report.json")))
        j.pop("generated_at_utc")
        return j
    assert load(os.path.join(e["dir"], "out")) == load(out2)
    for f in ("perp_feature_screen.csv", "perp_leadlag_report.csv", "perp_candidate_manifest.json"):
        assert open(os.path.join(e["dir"], "out", f)).read() == open(os.path.join(out2, f)).read(), f


def test_schema_and_metadata_validation():
    import perp_telemetry as pt
    assert ap.EXPECTED_SCHEMA_VERSION == str(pt.TELEMETRY_SCHEMA_VERSION) and ap.EXPECTED_FEATURE_VERSION == pt.FEATURE_VERSION
    d = tempfile.mkdtemp()
    try:
        t, l = write_e2e(d, n_times=6)
        lines = open(t).read().splitlines()
        lines[3] = lines[3].replace(",step2_v2,", ",step2_v1,", 1)          # one OLD-schema row
        open(t, "w").write("\n".join(lines) + "\n")
        try:
            ap.load_telemetry(t); raise AssertionError("mixed schema accepted")
        except ap.SchemaError as e:
            assert "mixed" in str(e)
        with open(t, "w", newline="") as f:
            csv.writer(f).writerow(pt.STEP1_COLUMNS)
        try:
            ap.load_telemetry(t); raise AssertionError("step 1 file accepted")
        except ap.SchemaError:
            pass
    finally:
        shutil.rmtree(d)
    rows = [mkrow(1.0, ticker="X", coin="BTC", close=900.0), mkrow(2.0, ticker="X", coin="ETH", close=900.0),
            mkrow(3.0, ticker="Y", close=900.0), mkrow(4.0, ticker="Y", close=1800.0), mkrow(5.0, ticker="Z", close=900.0)]
    ex = ap.ticker_metadata(rows, {"Z": {"y": 1, "coin": "SOL", "close": 900.0}})
    assert set(ex) == {"X", "Y", "Z"}


def test_streaming_loader_equivalence():
    """The memory-saving loader must select EXACTLY what a naive full-row load selects."""
    e = _e2e()
    kept, meta = ap.load_telemetry(e["tele"])
    with open(e["tele"], newline="") as f:
        full = [ap._row_from_csv(i, r) for i, r in enumerate(csv.DictReader(f))]
    assert len(kept) < len(full) / 1.5 and meta["rows"] == len(full)
    labels, _ = ap.load_labels(e["labels"])
    ex_full = ap.ticker_metadata(full, labels)
    ex_kept = ap.ticker_metadata(kept, labels, meta["ticker_coins"], meta["ticker_closes"])
    assert ex_full == ex_kept
    key = lambda obs: sorted((o["ticker"], o["horizon"], o["row_index"]) for o in obs)
    a = ap.build_horizon_observations(ap.index_by_ticker(full), labels, ex_full)
    b = ap.build_horizon_observations(ap.index_by_ticker(kept), labels, ex_kept)
    for h in ap.HORIZONS_MIN:
        assert key(a[h]) == key(b[h]) and len(a[h]) == 480, h
    assert key(ap.build_first_signal_cohort(ap.index_by_ticker(full), labels, ex_full)) == \
        key(ap.build_first_signal_cohort(ap.index_by_ticker(kept), labels, ex_kept))
    for H in (10, 30):
        x = ap.build_leadlag_anchors(full, H)
        y = ap.build_leadlag_anchors(None, H, streams=meta["streams"])
        assert [(q["ticker"], q["ts"], q["y"]) for q in x] == [(q["ticker"], q["ts"], q["y"]) for q in y]


# Step 4 note: kalshi_dashboard.py was changed by Step 4 (call-journal ticker/signal_ts
# provenance and a passive shadow hook, both required by the Step 4 spec). Its whole-file
# hash is therefore replaced by per-function hashes: every STRATEGY function must still be
# byte-identical to Step 3. (log_call/settle_calls gained provenance fields; their
# behaviour is pinned by golden settlement rows in test_stage10.)
STRATEGY_FUNCTION_HASHES = {
    "evaluate": "97ace1f8e00ac2529ae406e6b4a6b9f522e65bf974c0c4c3ab3b236c4e24266c",
    "_size_fraction": "f634a2cf0a0106a00d3d4c24e5ec582720b9731254b1cbda3965b2af288c7dfd",
    "_contracts": "0d9de825476d3ceff0abd58c9a5264c996b0366b18011fc7dbdb6ea2ae11b36e",
    "_limit_price": "9ed3bc9cc928e0422237f663af42c10f085b0d067c5dfd8ca456a049bb44e4e7",
    "kalshi_fee_cents": "4e12ca7d542bb3e5a43f49d07265eb9b7d42248a79ee98b2c6daeda21108b38d",
    "build_call_embed": "292f926807fc867bdcc3ab7df852ab2197f8eaf6bd74621e8f8c60cf9a351947",
    "current_market": "de72f7fe61bcfb4423b09daeea12e098ca067b7c2aa81222316a3c93cda00945",
    "spot": "fd995080ca05a65c4f9c9c4054b30d5b566e2ef468c94d3d4c5c5f4313d28db4",
    "candles": "018a20b49ede364e5c4b04cc4db334c000de9f9c215271700c50428b26b48b7f",
    "norm_cdf": "a1d002bc89a5873d8253039dabcf9cfe4df9d918eaf3a5c17caf2edb335b1c61",
    "call_record": "c1d17e8e5ac49a9540f90ba94dac75a56f600277a7b8596ae10baeec51d3d973",
    "compute_stats": "7c660fa3a41d016e308ca3a50c805b7025788a89dde8ff8386d7d6daf37fa63b",
    "settle_pending": "4c1746d8d4b6ea007816463fb8ba925320394ff3c42c4db6af3fb6f6b8c7adba",
    "paper_enter": "1b4dc530fba074e558f351645221b184cad9d2921da65bb30dda13f1f8f3195f",
    "_append_paper": "296dc188c1c985729f96aed47f52fd30953f69d909b33b847a3d9b1a8d967731",
}

# Optimization-pass note: perp_telemetry.py, kalshi_dashboard.py (_get only) and
# analyze_perp_shadow.py changed for performance with proven output parity (test_stage13).
# Step 6 note: kalshi_dashboard.py (live-veto plumbing in poller/config/health) and
# perp_telemetry.py (read-only preview_row) changed by design in Step 6. Their LEGACY
# behaviour is pinned by the per-function strategy hashes below, by the AST strategy
# fingerprint (test_stage12) and by the unchanged Stage 1-11 behavioural tests.
# Step 2 v2 note (volatility-regime research telemetry, schema 3 / step2_v2): perp_telemetry.py
# (new causal columns) and kalshi_dashboard.py (display-only perp-vol line in _perp_publish/PAGE)
# changed by design. Legacy behaviour is still pinned by the per-function strategy hashes, the
# AST strategy fingerprint (test_stage12/14) and the unchanged behavioural tests; test_stage14
# additionally proves poller outputs are identical with stressed, normal and disabled telemetry.
LIVE_HASHES = {   # Step 2 output, byte-identical in Steps 3 and 4
    "perp_telemetry.py": "5e1de6d6553032bd2902d0b9766086b826876c632bb41cfcfe52234c90b4d99e",
    "kalshi_bot.py": "9e064548c21144b00226920e21d114407ff306386fd740272cbd889d7d896d53",
    "kalshi_backtest.py": "718968348cacd89e07740414ac9567db6a22ba3b3dcd7d8d70cd4c0a0ef631f8",
    "kalshi_api_learn.py": "7b7bc2dd095cabe391549208a849c612a5465aac9139e8f8e841216cc96a26f6",
}


def test_live_files_unchanged():
    for f, h in LIVE_HASHES.items():
        assert hashlib.sha256(open(os.path.join(HERE, f), "rb").read()).hexdigest() == h, f
    import inspect
    import kalshi_dashboard as k
    for fn, h in STRATEGY_FUNCTION_HASHES.items():
        assert hashlib.sha256(inspect.getsource(getattr(k, fn)).encode()).hexdigest() == h, f"strategy function changed: {fn}"
    for f in ("analyze_perp_predictive.py", "label_binary_outcomes.py"):
        src = open(os.path.join(HERE, f)).read()
        for word in ("BLOCK_PERP_CONFLICT", "PERP_CONFIRM", "PERP_SCORE", "import kalshi_dashboard"):
            assert word not in src, (f, word)


def test_previous_stages():
    # Master mode (run_all_tests.py): every stage is already run exactly once by the runner.
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    # Standalone: re-verify earlier stages, each EXACTLY once (children run in master mode).
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 9):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-600:], p.stderr[-600:])


if __name__ == "__main__":
    try:
        run("1  outcome label mapping", test_label_mapping)
        run("2+3 no open-market / in-grace queries", test_no_open_or_grace_queries)
        run("4  settled cache is idempotent", test_cache_idempotent)
        run("5  contradictory settlement flagged, never overwritten", test_conflicting_settlement)
        run("6  labeler GET-only, timeout, retry/backoff", test_labeler_get_only)
        run("7+10 one row per ticker per horizon", test_horizon_selector_one_per_horizon)
        run("8  selector never uses a future row", test_selector_never_future)
        run("9  row too early -> no observation", test_selector_too_early)
        run("11 first-signal cohort", test_first_signal)
        run("12 analysis-ready filtering", test_analysis_ready_filtering)
        run("13 causal invariant hard failure", test_causal_invariant_failure)
        run("14+15 walk-forward: no overlap, chronological", test_walk_forward_split)
        run("16+34 train-only preprocessing & quintiles", test_train_only_preprocessing_and_quintiles)
        run("17-20 Brier / logloss / AUC / ECE", test_metrics)
        run("21 logistic solver recovery", test_logistic_solver)
        run("22 logistic numerical stability", test_logistic_stability)
        run("23 MODEL B/C scored on identical rows", test_same_rows_for_b_and_c)
        run("24 positive synthetic perp signal -> candidate", test_positive_control)
        run("25 spot-only confound rejected", test_spot_confound)
        run("26 noise feature rejected", test_noise_feature)
        run("27 shuffled labels -> signal disappears", test_shuffled_labels)
        run("28 bootstrap CI behaviour", test_bootstrap_ci)
        run("29 deterministic bootstrap", test_bootstrap_deterministic)
        run("30 Benjamini-Hochberg", test_bh)
        run("31 sample-size gate", test_sample_size_gate)
        run("32 secondary/funding never candidate", test_secondary_never_candidate)
        run("33 sign stability -> UNSTABLE", test_sign_stability)
        run("35 future spot return labels", test_future_spot_return)
        run("36 future label never crosses session/coin", test_future_label_same_session_and_coin)
        run("37 future label tolerance / gaps", test_future_label_tolerance_and_gaps)
        run("38 OLS solver", test_ols)
        run("39 genuine synthetic perp lead", test_genuine_lead)
        run("40 no incremental lead", test_no_incremental_lead)
        run("41 analyzer offline (static + runtime, no numpy)", test_analyzer_offline)
        run("+  feature allowlist / outcome never a feature", test_allowlist)
        run("+  e2e: leak decoys never selected, outputs, conservative defaults", test_e2e_leak_decoys_and_outputs)
        run("+  reproducible output", test_reproducible)
        run("+  schema / metadata validation", test_schema_and_metadata_validation)
        run("+  streaming loader == naive full load", test_streaming_loader_equivalence)
        run("42 live strategy files byte-identical", test_live_files_unchanged)
        run("43 all previous stage suites", test_previous_stages)
    finally:
        if E2E.get("dir"):
            shutil.rmtree(E2E["dir"], ignore_errors=True)
    print("\nAll Stage 9 tests passed.")
