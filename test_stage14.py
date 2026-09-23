#!/usr/bin/env python3
"""Stage 14 tests: volatility-regime RESEARCH telemetry (schema v3 / feature_version step2_v2).
No network. Run:  py test_stage14.py        Every patch is restored afterwards.

  A  volatility normalisation (exact values, zero / missing / non-finite / warm-up)
  B  causality (enormous FUTURE perp snapshots and spot values cannot move any new feature)
  C  volatility regime + stability rule (exact boundaries, UNKNOWN)
  D  spread stress (normal, widening, missing bid/ask, crossed, insufficient, re-scale)
  E  normalised perp-vs-spot gap (exact values, missing inputs)
  F  dashboard (unavailable telemetry never breaks the view or the page renderer)
  G  schema/version (header, versions, old files rotate, old artifacts fail safely)
  H  legacy strategy isolation (identical outputs; nothing new can become a call/candidate)
  +  Step 3 families: spot-controlled normalised momentum, research-only interactions
  +  quality report coverage / regime distribution / warm-up reasons / pathological values
"""
import csv
import json
import math
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time

import kalshi_dashboard as k
import perp_telemetry as pt
import analyze_perp_predictive as ap
import analyze_perp_quality as aq
import perp_shadow as ps
import perp_live as pl
import strategy_fingerprint as sf
import build_perp_shadow_policy as bp
import build_perp_integration_experiment as bx
import promote_perp_integration as promote
import test_stage7 as t7
import test_stage8 as t8

HERE = os.path.dirname(os.path.abspath(__file__))
TRUSTED_V2 = "8d94f241e8fc8edadc76058e1f12f430b6e4f499f4c0a30fba1cb5cf07dad82a"   # == test_stage12/13
OK = {"status": "ok"}


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


# ─────────── fixtures ───────────
def _tel(coins=("BTC",), max_lag=8.0):
    store = pt.SnapshotStore(list(coins), 600, 1800.0)
    smp = pt.PerpSampler(t7.StaticProvider(lambda c: None), list(coins), store)
    return pt.PerpTelemetry(t7.StaticProvider(lambda c: None), list(coins), sampler=smp, max_lag_s=max_lag,
                            session_id="T14", clock=lambda: 0.0)


def _snap(ts, mid, coin="BTC", half_spread=0.01, bid=..., ask=..., idx=None, scale=(None, None)):
    b = mid - half_spread if bid is ... else bid
    a = mid + half_spread if ask is ... else ask
    m = pt._mid(b, a)
    return pt.PerpSnapshot(ts=ts, coin=coin, perp_symbol="XPERP", perp_bid=b, perp_ask=a, perp_mid=m, perp_last=mid,
                           perp_mark=mid, index_price=idx if idx is not None else mid * 0.9999,
                           index_ts_ms=int(ts * 1000), funding_rate=1e-4, source_status=pt.STATUS_FRESH,
                           contract_size=scale[0], underlying_multiplier=scale[1])


def _history(tel, t_end, n=240, step=4.0, seed=3, coin="BTC", vol=4e-4, spot_vol=3e-4, half_spread=0.01):
    """n perp snapshots and n spot observations ending at t_end (both 4 s apart)."""
    rng = random.Random(seed)
    mid, spot = 100.0, 50.0
    for i in range(n):
        ts = t_end - (n - 1 - i) * step
        mid *= math.exp(rng.gauss(0, vol))
        spot *= math.exp(rng.gauss(0, spot_vol))
        tel.store.add(_snap(ts - 0.5, mid, coin, half_spread=half_spread, idx=mid * (1 - rng.gauss(0, 2e-4))))
        if i < n - 1:
            tel.spot_long[coin].append(ts, spot)
    return spot


def _ready_row(tel, t_end, spot, coin="BTC"):
    return tel.record_cycle({coin: dict(OK, spot_raw=spot)}, {coin: t_end}, now=t_end + 0.5)[coin]


def _flags(row):
    return set(filter(None, (row["quality_flags"] or "").split(";")))


# ═══════════════════ A: normalisation ═══════════════════
def test_normalization_math():
    for h in (30, 60, 180):
        sig = 7.5 * math.sqrt(h / 60.0)
        assert pt.horizon_sigma_bps(7.5, h) == sig
        assert pt.normalized_return(12.0, 7.5, h) == 12.0 / sig
        assert pt.normalized_return(-12.0, 7.5, h) == -12.0 / sig
    assert pt.normalized_return(12.0, 7.5, 60) == 1.6                    # 12 / (7.5 * 1)
    assert pt.normalized_return(0.0, 7.5, 60) == 0.0                     # a genuine zero stays zero
    for bad_vol in (0.0, -1.0, None, float("nan"), float("inf"), True, "5", pt.NORM_MIN_VOL_BPS / 2):
        assert pt.normalized_return(12.0, bad_vol, 60) is None, bad_vol   # zero/missing/non-finite -> None
    for bad_ret in (None, float("nan"), float("inf"), float("-inf"), False):
        assert pt.normalized_return(bad_ret, 7.5, 60) is None, bad_ret
    assert pt.normalized_return(5.0, pt.NORM_MIN_VOL_BPS, 60) == 5.0 / pt.NORM_MIN_VOL_BPS   # floor inclusive
    assert pt.horizon_sigma_bps(7.5, 0) is None
    z = pt.normalized_return(1e300, pt.NORM_MIN_VOL_BPS, 60)             # overflow -> None, never inf
    assert z is None or math.isfinite(z)
    assert pt.normalized_return(1e308, pt.NORM_MIN_VOL_BPS, 60) is None


def test_normalization_in_rows():
    tel, t = _tel(), 10_000.0
    spot = _history(tel, t)
    row = _ready_row(tel, t, spot)
    assert row["analysis_ready"] is True
    for side in ("perp", "spot"):
        rv = row[f"{side}_rv_300s_bps"]
        assert rv is not None and rv > 0
        for h in (30, 60, 180):
            ret = row[f"causal_{side}_ret_{h}s_bps"]
            assert ret is not None
            assert row[f"{side}_momentum_z_{h}s"] == round(ret / (rv * math.sqrt(h / 60.0)), 4), (side, h)
    for c in pt.VOLATILITY_COLUMNS:                         # no inf / nan anywhere in the new columns
        v = row[c]
        assert not (isinstance(v, float) and not math.isfinite(v)), c


def test_warmup_and_zero_vol():
    # 2 minutes of history: 60 s return exists, 300 s RV does not -> z None, row still analysis_ready
    tel, t = _tel(), 10_000.0
    spot = _history(tel, t, n=31)
    row = _ready_row(tel, t, spot)
    assert row["analysis_ready"] is True and row["causal_perp_ret_60s_bps"] is not None
    assert row["perp_rv_300s_bps"] is None and row["perp_momentum_z_60s"] is None
    assert row["momentum_gap_z_60s"] is None and row["spot_momentum_z_60s"] is None
    assert "RV_INSUFFICIENT" in _flags(row) and row["vol_regime"] == pt.VR_UNKNOWN
    assert row["perp_stability_state"] == pt.ST_UNKNOWN            # inputs missing, nothing triggered
    # perfectly flat prices: RV == 0 -> no normalisation (None, flagged), never inf/nan, never 0-substituted
    tel2 = _tel()
    for i in range(240):
        tel2.store.add(_snap(t - (239 - i) * 4 - 0.5, 100.0))
        if i < 239:
            tel2.spot_long["BTC"].append(t - (239 - i) * 4, 50.0)
    r2 = _ready_row(tel2, t, 50.0)
    assert r2["perp_rv_300s_bps"] == 0.0 and r2["causal_perp_ret_60s_bps"] == 0.0
    for h in (30, 60, 180):
        assert r2[f"perp_momentum_z_{h}s"] is None and r2[f"spot_momentum_z_{h}s"] is None
        assert r2[f"momentum_gap_z_{h}s"] is None
    assert pt.Q_NORM_FLOOR in _flags(r2) and r2["analysis_ready"] is True
    # no spot history at all: spot-side None, perp-side still normalised
    tel3 = _tel()
    rng = random.Random(1)
    mid = 100.0
    for i in range(240):
        mid *= math.exp(rng.gauss(0, 4e-4))
        tel3.store.add(_snap(t - (239 - i) * 4 - 0.5, mid))
    r3 = _ready_row(tel3, t, 50.0)
    assert r3["perp_momentum_z_60s"] is not None and r3["spot_momentum_z_60s"] is None
    assert r3["momentum_gap_z_60s"] is None


# ═══════════════════ B: causality ═══════════════════
def test_future_values_cannot_leak():
    tel, t = _tel(), 10_000.0
    spot = _history(tel, t)
    r = dict(OK, spot_raw=spot)
    before = tel.preview_row("BTC", r, t)
    rec = tel.record_cycle({"BTC": r}, {"BTC": t}, now=t + 0.5)["BTC"]
    for c in pt.VOLATILITY_COLUMNS:
        assert before[c] == rec[c], c                             # preview == recorded row
    # enormous FUTURE perp snapshots (price, spread, index) and FUTURE spot observations
    for j, dtf in enumerate((0.001, 1.0, 5.0, 30.0, 400.0)):
        tel.store.add(_snap(t + dtf, 1e6 * (j + 2), half_spread=5e5, idx=1.0))
    tel.spot_long["BTC"].append(t + 1.0, 1e9)
    tel.spot_long["BTC"].append(t + 50.0, 1e-9)
    after = tel.preview_row("BTC", r, t)
    for c in pt.VOLATILITY_COLUMNS + ["perp_spread_bps", "perp_rv_300s_bps", "spot_rv_300s_bps"]:
        assert before[c] == after[c], (c, before[c], after[c])
    assert before["perp_snapshot_ts_epoch_ms"] <= before["feature_end_ts_epoch_ms"]
    # the anchor snapshot itself is excluded from its own spread baseline
    assert pt.trailing_median_before([(1.0, 1.0), (2.0, 1.0), (3.0, 99.0)], 3.0, 10.0, 1, 0.0) == (1.0, 2)
    # static: the new code reads no outcome-like field
    import ast, inspect, textwrap
    for fn in (pt.normalized_return, pt.normalized_gap, pt.trailing_median_before, pt.spread_ratio,
               pt.vol_regime, pt.stability_state, pt.stability_levels, pt.dashboard_view):
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
        doc = {id(n.body[0].value) for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.body
               and isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Constant)}
        toks = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | \
               {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | \
               {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)
                and id(n) not in doc}
        assert not {x for x in toks if re.search(r"(result|settle|\bwin\b|outcome|pnl)", str(x), re.I)}, fn


def test_outcome_fields_cannot_change_new_features():
    tel, t = _tel(), 10_000.0
    spot = _history(tel, t)
    base = tel.preview_row("BTC", dict(OK, spot_raw=spot), t)
    for extra in ({"result": "yes"}, {"settle_result": "no", "win": True}, {"outcome_up": 1, "pnl": 99}):
        got = tel.preview_row("BTC", dict(OK, spot_raw=spot, **extra), t)
        for c in pt.VOLATILITY_COLUMNS:
            assert got[c] == base[c], (extra, c)


# ═══════════════════ C: regime + stability ═══════════════════
def test_vol_regime_boundaries():
    V = pt.vol_regime
    assert pt.VOL_REGIME_LOW_BELOW == 0.5 and pt.VOL_REGIME_HIGH_AT == 1.5 and pt.VOL_REGIME_EXTREME_AT == 2.5
    for x, want in ((0.0, "LOW"), (0.4999, "LOW"), (0.5, "NORMAL"), (1.0, "NORMAL"), (1.4999, "NORMAL"),
                    (1.5, "HIGH"), (2.4999, "HIGH"), (2.5, "EXTREME"), (40.0, "EXTREME")):
        assert V(x) == want, (x, V(x), want)
    for bad in (None, float("nan"), float("inf"), -0.1, True, "1.0"):
        assert V(bad) == "UNKNOWN", bad
    assert V(None, 3.0, 3.0, 3.0) == "UNKNOWN"                       # perp 60v300 is required
    assert V(1.0, None, 2.6, None) == "EXTREME"                      # max of the available ratios
    assert V(0.3, 0.6) == "NORMAL" and V(0.3, 0.4, 0.2, 0.1) == "LOW"
    assert V(1.0, float("nan"), None, float("inf")) == "NORMAL"      # non-finite optional ratios ignored
    assert set(pt.VOL_REGIMES) == set(ap.VOL_REGIME_CATEGORIES) == set(aq.VOL_REGIMES)
    assert set(pt.STABILITY_STATES) == set(ap.STABILITY_CATEGORIES) == set(aq.STABILITY_STATES)


def test_stability_rule():
    S = pt.stability_state
    assert S(True, "NORMAL", 0.0, 0.0, 1.0) == ("STABLE", [])
    assert S(True, "LOW", -0.9999, 2.4999, 1.9999) == ("STABLE", [])
    assert S(True, "HIGH", 0.0, 0.0, 1.0) == ("CAUTION", ["VOL_HIGH"])
    assert S(True, "NORMAL", -1.0, 0.0, 1.0) == ("CAUTION", ["SPOT_PERP_DIVERGING"])       # |gap| boundary
    assert S(True, "NORMAL", 0.0, 2.5, 1.0) == ("CAUTION", ["PREMIUM_ELEVATED"])
    assert S(True, "NORMAL", 0.0, 0.0, 2.0) == ("CAUTION", ["SPREAD_WIDENING"])
    assert S(True, "HIGH", 0.0, 0.0, 2.0) == ("UNSTABLE", ["VOL_HIGH", "SPREAD_WIDENING"])  # two cautions
    assert S(True, "EXTREME", 0.0, 0.0, 1.0) == ("UNSTABLE", ["VOL_EXTREME"])
    assert S(True, "NORMAL", 2.0, 0.0, 1.0) == ("UNSTABLE", ["SPOT_PERP_DISAGREE"])
    assert S(True, "NORMAL", 0.0, 4.0, 1.0) == ("UNSTABLE", ["PREMIUM_EXTREME"])
    assert S(True, "NORMAL", 0.0, 0.0, 4.0) == ("UNSTABLE", ["SPREAD_BLOWOUT"])
    assert S(True, "UNKNOWN", 0.0, 0.0, 1.0) == ("UNKNOWN", ["VOL_UNKNOWN"])
    assert S(True, "NORMAL", None, 0.0, 1.0) == ("UNKNOWN", ["AGREEMENT_UNKNOWN"])
    assert S(True, "NORMAL", None, 2.5, None)[0] == "CAUTION"            # lower bound with missing inputs
    assert S(True, "UNKNOWN", 5.0, None, None)[0] == "UNSTABLE"
    assert S(False, "EXTREME", 5.0, 9.0, 9.0) == ("UNKNOWN", ["NOT_ANALYSIS_READY"])
    assert S(None, "NORMAL", 0.0, 0.0, 1.0)[0] == "UNKNOWN"
    assert S(True, "NORMAL", float("nan"), 0.0, 1.0)[0] == "UNKNOWN"
    # the SAME raw +40 bp perp move reads differently in a calm vs a stressed context
    calm = pt.normalized_return(40.0, 40.0, 60)
    wild = pt.normalized_return(40.0, 400.0, 60)
    assert calm == 1.0 and wild == 0.1
    assert S(True, pt.vol_regime(1.0), 0.2, 0.5, 1.0)[0] == "STABLE"
    assert S(True, pt.vol_regime(3.0), 2.5, 4.5, 3.0)[0] == "UNSTABLE"


def test_regime_and_stability_logged():
    tel, t = _tel(), 10_000.0
    spot = _history(tel, t)
    row = _ready_row(tel, t, spot)
    assert row["vol_regime"] == pt.vol_regime(row["perp_vol_shock_60v300"], row["perp_vol_shock_60v900"],
                                              row["spot_vol_shock_60v300"], row["spot_vol_shock_60v900"])
    st, why = pt.stability_state(row["analysis_ready"], row["vol_regime"], row["momentum_gap_z_60s"],
                                 row["premium_stress_5m"], row["perp_spread_ratio_5m"])
    assert row["perp_stability_state"] == st and row["perp_stability_reasons"] == ";".join(why)
    assert row["premium_stress_5m"] == abs(row["premium_z_5m"])
    # an unusable row is UNKNOWN, with its reason
    t2 = _tel()
    r2 = t2.record_cycle({"BTC": dict(OK, spot_raw=50.0)}, {"BTC": t}, now=t + 0.5)["BTC"]
    assert r2["analysis_ready"] is False and r2["vol_regime"] == "UNKNOWN"
    assert r2["perp_stability_state"] == "UNKNOWN" and r2["perp_stability_reasons"] == "NOT_ANALYSIS_READY"


# ═══════════════════ D: spread stress ═══════════════════
def _spread_tel(prior, current, t=10_000.0, step=4.0):
    """prior: list of snapshot kwargs (oldest first) ending one step before `current`."""
    tel = _tel()
    n = len(prior)
    for i, kw in enumerate(prior):
        tel.store.add(_snap(t - 0.5 - (n - i) * step, **kw))
    tel.store.add(_snap(t - 0.5, **current))
    return tel, tel.preview_row("BTC", dict(OK, spot_raw=50.0), t)


def test_spread_stress():
    normal = [{"mid": 100.0, "half_spread": 0.01}] * 70                 # 2 bps, 276 s of baseline
    tel, r = _spread_tel(normal, {"mid": 100.0, "half_spread": 0.01})
    assert r["perp_spread_bps"] == 2.0 and r["perp_spread_median_5m_bps"] == 2.0
    assert r["perp_spread_ratio_5m"] == 1.0 and r["perp_spread_baseline_n"] == 70
    tel, r = _spread_tel(normal, {"mid": 100.0, "half_spread": 0.03})   # widening 2 -> 6 bps
    assert r["perp_spread_ratio_5m"] == 3.0 and r["analysis_ready"] is True
    assert "SPREAD_WIDENING" in (r["perp_stability_reasons"] or "")
    tel, r = _spread_tel(normal, {"mid": 100.0, "half_spread": 0.05})   # 10 bps -> ratio 5
    assert r["perp_spread_ratio_5m"] == 5.0 and "SPREAD_BLOWOUT" in r["perp_stability_reasons"]
    assert r["perp_stability_state"] == "UNSTABLE"
    # the baseline is a MEDIAN: a few extreme prior spreads do not move it
    spiky = [{"mid": 100.0, "half_spread": 0.01}] * 65 + [{"mid": 100.0, "half_spread": 5.0}] * 5
    tel, r = _spread_tel(spiky, {"mid": 100.0, "half_spread": 0.01})
    assert r["perp_spread_median_5m_bps"] == 2.0 and r["perp_spread_ratio_5m"] == 1.0
    # missing bid / missing ask / crossed book in the CURRENT snapshot -> no spread, no ratio, not ready
    for cur in ({"mid": 100.0, "bid": None}, {"mid": 100.0, "ask": None}, {"mid": 100.0, "bid": 100.05, "ask": 99.95}):
        tel, r = _spread_tel(normal, cur)
        assert r["perp_spread_bps"] is None and r["perp_spread_ratio_5m"] is None, cur
        assert r["perp_spread_median_5m_bps"] == 2.0 and r["analysis_ready"] is False, cur
    # missing bid / ask / crossed IN THE HISTORY: those snapshots are excluded, never faked
    holes = [{"mid": 100.0, "bid": None}, {"mid": 100.0, "ask": None}, {"mid": 100.0, "bid": 100.05, "ask": 99.95}]
    mixed = [dict(holes[i % 3]) if i % 4 == 0 else {"mid": 100.0, "half_spread": 0.01} for i in range(70)]
    tel, r = _spread_tel(mixed, {"mid": 100.0, "half_spread": 0.02})
    assert r["perp_spread_baseline_n"] == 52 and r["perp_spread_median_5m_bps"] == 2.0     # 18 holes skipped
    assert r["perp_spread_ratio_5m"] == 2.0
    # insufficient trailing history: too few points, or enough points but too short a span
    tel, r = _spread_tel(normal[:60], {"mid": 100.0, "half_spread": 0.01})                # 60 pts, 236 s < 240 s
    assert r["perp_spread_baseline_n"] == 60 and r["perp_spread_median_5m_bps"] is None
    tel, r = _spread_tel(normal[:10], {"mid": 100.0, "half_spread": 0.01})
    assert r["perp_spread_median_5m_bps"] is None and r["perp_spread_ratio_5m"] is None
    assert r["perp_spread_baseline_n"] == 10 and pt.Q_SPREAD_BASE_INSUFF in _flags(r)
    tel, r = _spread_tel(normal[:30], {"mid": 100.0, "half_spread": 0.01}, step=2.0)     # 30 pts over 58 s
    assert r["perp_spread_baseline_n"] == 30 and r["perp_spread_median_5m_bps"] is None
    # points older than the 5-minute window are not part of the baseline
    tel, r = _spread_tel(normal, {"mid": 100.0, "half_spread": 0.01}, step=10.0)          # 600 s of history
    assert r["perp_spread_baseline_n"] == 30
    # zero-width (locked) baseline -> ratio None, never infinite
    assert pt.spread_ratio(2.0, 0.0) is None and pt.spread_ratio(None, 2.0) is None
    assert pt.spread_ratio(2.0, float("nan")) is None and pt.spread_ratio(3.0, 2.0) == 1.5


def test_spread_scale_change():
    t = 10_000.0
    tel = _tel()
    for i in range(60):
        tel.store.add(_snap(t - 0.5 - (60 - i) * 4, 100.0, half_spread=0.01, scale=(1.0, 1.0)))
    tel.store.add(_snap(t - 0.5, 50.0, half_spread=0.03, scale=(0.5, 1.0)))              # re-scaled contract
    r = tel.preview_row("BTC", dict(OK, spot_raw=50.0), t)
    assert r["perp_spread_bps"] == 12.0 and r["perp_spread_median_5m_bps"] is None       # nothing bridged
    assert r["perp_spread_ratio_5m"] is None and pt.Q_SCALE in _flags(r)
    # defence in depth: even if an old-scale snapshot survived in the store it is skipped
    tel2 = _tel()
    for i in range(60):
        tel2.store.add(_snap(t - 0.5 - (60 - i) * 4, 100.0, half_spread=0.5, scale=(1.0, 1.0)))
    for s in list(tel2.store._d["BTC"]):
        s.contract_size = 2.0                                                            # fake survivor
    tel2.store._scale["BTC"] = (1.0, 1.0)
    tel2.store.add(_snap(t - 0.5, 100.0, half_spread=0.01, scale=(1.0, 1.0)))
    r2 = tel2.preview_row("BTC", dict(OK, spot_raw=50.0), t)
    assert r2["perp_spread_median_5m_bps"] is None and r2["perp_spread_baseline_n"] == 0


def test_trailing_median_helper():
    s = [(float(i), float(v)) for i, v in enumerate([5, 1, 3, 2, 4, 100])]
    assert pt.trailing_median_before(s, 5.0, 10.0, 1, 0.0) == (3.0, 5)          # odd n, anchor excluded
    assert pt.trailing_median_before(s, 4.0, 10.0, 1, 0.0) == (2.5, 4)          # even n
    assert pt.trailing_median_before(s, 5.0, 2.0, 1, 0.0) == (3.0, 2)           # window [3, 5): values 2, 4
    assert pt.trailing_median_before(s, 5.0, 10.0, 6, 0.0) == (None, 5)
    assert pt.trailing_median_before(s, 5.0, 10.0, 1, 4.5) == (None, 5)         # span 4 < 4.5
    assert pt.trailing_median_before([], 5.0, 10.0, 0, 0.0) == (None, 0)


# ═══════════════════ E: normalised gap ═══════════════════
def test_normalized_gap():
    G = pt.normalized_gap
    assert G(10.0, 3.0, 4.0, 60) == 2.0                           # 10 / sqrt(3^2 + 4^2)
    assert G(10.0, 3.0, 4.0, 240) == 1.0                          # sigmas double at 4x the horizon
    assert G(-10.0, 3.0, 4.0, 60) == -2.0 and G(0.0, 3.0, 4.0, 60) == 0.0
    assert abs(G(6.0, 3.0, 4.0, 30) - 6.0 / (5.0 * math.sqrt(0.5))) < 1e-12
    for args in ((None, 3.0, 4.0), (10.0, None, 4.0), (10.0, 3.0, None), (10.0, 0.0, 4.0), (10.0, 3.0, 0.0),
                 (float("nan"), 3.0, 4.0), (10.0, float("inf"), 4.0)):
        assert G(*args, 60) is None, args
    tel, t = _tel(), 10_000.0
    spot = _history(tel, t)
    row = _ready_row(tel, t, spot)
    prv, srv = row["perp_rv_300s_bps"], row["spot_rv_300s_bps"]
    for h in (30, 60, 180):
        gap = row[f"momentum_gap_{h}s_bps"]
        want = gap / math.hypot(prv * math.sqrt(h / 60.0), srv * math.sqrt(h / 60.0))
        assert row[f"momentum_gap_z_{h}s"] == round(want, 4), h


# ═══════════════════ F: dashboard ═══════════════════
def test_dashboard_view_never_breaks():
    for row in (None, {}, [], "x", {"coin": "BTC", "source_status": "disabled"},
                {c: None for c in pt.CSV_COLUMNS}, {c: "garbage" for c in pt.CSV_COLUMNS},
                {"perp_momentum_z_60s": float("nan"), "vol_regime": 7, "perp_stability_state": "WEIRD",
                 "analysis_ready": "yes", "perp_lag_to_spot_ms": float("inf")}):
        v = pt.dashboard_view(row)
        assert v["stability"] == "UNKNOWN" and v["vol_regime"] == "UNKNOWN" and v.get("direction") is None, row
        json.dumps(v, allow_nan=False)                           # the /data endpoint can always serialise it
    tel, t = _tel(), 10_000.0
    spot = _history(tel, t)
    row = _ready_row(tel, t, spot)
    v = pt.dashboard_view(row)
    assert v["vol_regime"] == row["vol_regime"] and v["stability"] == row["perp_stability_state"]
    assert v["momentum_z_60s"] == row["perp_momentum_z_60s"] and v["analysis_ready"] is True
    assert v["direction"] in ("UP", "DOWN", "FLAT")
    for z, d in ((1.0, "UP"), (0.9999, "FLAT"), (-1.0, "DOWN"), (-0.5, "FLAT")):
        assert pt.dashboard_view({"perp_momentum_z_60s": z})["direction"] == d
    assert pt.dashboard_view({"perp_spread_ratio_5m": 2.0})["spread_stress"] == "WIDENING"
    assert pt.dashboard_view({"momentum_gap_z_60s": -2.5})["agreement"] == "DISAGREE"
    assert pt.dashboard_view({"premium_stress_5m": 0.3})["premium_stress"] == "NORMAL"


def test_dashboard_publish_and_page():
    views = k._perp_vol_views({"BTC": None, "ETH": {}, "SOL": {"coin": "SOL", "source_status": "disabled"}})
    assert set(views) == {"BTC", "ETH", "SOL"} and all(v["stability"] == "UNKNOWN" for v in views.values())
    assert all(v["live_veto_feature"] is None for v in views.values())       # no gate -> observational label
    assert k._perp_vol_views(None) == {}
    with k.LOCK:
        saved = dict(k.STATE)
    try:
        with t7.patched(k, _shadow_process=lambda rows: None, _perp=None):
            k._perp_publish({"BTC": {"coin": "BTC", "source_status": "error"}})
        with k.LOCK:
            assert k.STATE["perp_vol"]["BTC"]["stability"] == "UNKNOWN"
            json.dumps(k.STATE["perp_vol"], allow_nan=False)
    finally:
        with k.LOCK:
            k.STATE.clear(); k.STATE.update(saved)
    # the page renders the line defensively and labels it as observation-only telemetry
    assert 'id="pv_${c}"' in k.PAGE and "renderPerpVol(c,(d.perp_vol||{})[c])" in k.PAGE
    assert "(telemetry, observe only)" in k.PAGE
    node = shutil.which("node")
    if not node:
        print("  (node not found: JS renderer smoke test skipped; Python view checks above still ran)")
        return
    js = k.PAGE[k.PAGE.index("function renderPerpVol"):k.PAGE.index("let candleTick")]
    harness = ("const els={};const document={getElementById:id=>(els[id]=els[id]||{textContent:'',className:'',title:''})};\n"
               + js + "\nconst out=[];\n"
               "for(const v of [undefined,null,{},{stability:'CAUTION'},{direction:'UP',momentum_z_60s:1.84,vol_regime:'HIGH',"
               "vol_shock_60v300:1.7,agreement:'AGREE',spread_stress:'NORMAL',premium_stress:'ELEVATED',stability:'UNSTABLE',"
               "live_veto_feature:'perp_momentum_z_60s'},{momentum_z_60s:NaN,vol_shock_60v300:Infinity}]){"
               "renderPerpVol('BTC',v);out.push([els.pv_BTC.textContent,els.pv_BTC.className,(els.pvk_BTC||{}).textContent]);}\n"
               "console.log(JSON.stringify(out));")
    p = subprocess.run([node, "-e", harness], capture_output=True, text=True, timeout=30)
    assert p.returncode == 0, p.stderr[-600:]
    out = json.loads(p.stdout)
    assert out[0][0] == "–" and out[1][0] == "–"                                   # unavailable -> dash
    assert "UNKNOWN" in out[2][0] and out[2][1] == "dim"
    assert out[3][1] == "amb"
    assert out[4][0].startswith("UP +1.8σ60 · vol HIGH 1.70x") and out[4][1] == "red"
    assert "LIVE veto uses perp_momentum_z_60s" in out[4][2]
    assert "– " in out[5][0] or "–σ60" in out[5][0]


# ═══════════════════ G: schema / versions / stale artifacts ═══════════════════
def test_schema_header_and_versions():
    assert pt.TELEMETRY_SCHEMA_VERSION == 3 and pt.FEATURE_VERSION == "step2_v2"
    assert ap.EXPECTED_SCHEMA_VERSION == "3" and ap.EXPECTED_FEATURE_VERSION == "step2_v2"
    assert ap.ANALYSIS_CODE_VERSION == "step3_v2"
    assert len(pt.CSV_COLUMNS) == len(set(pt.CSV_COLUMNS))
    assert all(pt.CSV_COLUMNS.count(c) == 1 for c in pt.VOLATILITY_COLUMNS)
    assert set(pt.VOLATILITY_COLUMNS) <= set(pt.STEP2_COLUMNS)
    for c in ("open_interest", "liquidation", "liquidations", "aggressor", "depth", "funding_pressure"):
        assert not any(c in col for col in pt.CSV_COLUMNS), c                  # nothing fabricated
    d = tempfile.mkdtemp(prefix="_t14csv")
    try:
        path = os.path.join(d, "perp.csv")
        v2_header = [c for c in pt.CSV_COLUMNS if c not in pt.VOLATILITY_COLUMNS]   # the schema-v2 header
        with open(path, "w", newline="") as f:
            w = csv.writer(f); w.writerow(v2_header); w.writerow(["2" if c == "telemetry_schema_version" else "x"
                                                                 for c in v2_header])
        tel = pt.PerpTelemetry(t7.StaticProvider(lambda c: None), ["BTC"], log_path=path, session_id="T14",
                               sampler=pt.PerpSampler(t7.StaticProvider(lambda c: None), ["BTC"]))
        spot = _history(tel, 10_000.0)
        tel.record_cycle({"BTC": dict(OK, spot_raw=spot)}, {"BTC": 10_000.0}, now=10_000.5)
        tel.record_cycle({"BTC": dict(OK, spot_raw=spot)}, {"BTC": 10_004.0}, now=10_004.5)
        lines = open(path).read().splitlines()
        assert lines[0].split(",") == pt.CSV_COLUMNS and lines.count(lines[0]) == 1 and len(lines) == 3
        rows = list(csv.DictReader(open(path)))
        assert all(r["telemetry_schema_version"] == "3" and r["feature_version"] == "step2_v2" for r in rows)
        assert rows[0]["vol_regime"] in pt.VOL_REGIMES and rows[0]["perp_stability_state"] in pt.STABILITY_STATES
        baks = [n for n in os.listdir(d) if n.startswith("perp.csv.schema-")]
        assert len(baks) == 1                                                      # rotated, not appended
        old = list(csv.reader(open(os.path.join(d, baks[0]))))
        assert old[0] == v2_header and len(old) == 2                               # v2 data preserved intact
        # Step 3 refuses a v2 file (missing v3 columns) and refuses v2 rows under a v3 header
        try:
            ap.load_telemetry(os.path.join(d, baks[0])); raise AssertionError("v2 file accepted")
        except ap.SchemaError:
            pass
        mixed = os.path.join(d, "mixed.csv")
        body = open(path).read()
        assert "\n3,step2_v2," in body
        body = body.replace("\n3,step2_v2,", "\n2,step2_v1,", 1)            # one OLD-version data row
        open(mixed, "w").write(body)
        try:
            ap.load_telemetry(mixed); raise AssertionError("old-version rows accepted")
        except ap.SchemaError as e:
            assert "mixed" in str(e) or "incompatible" in str(e)
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_old_step3_and_step4_artifacts_fail_safely():
    # a Step 3 report produced by step3_v1 on schema 2 cannot feed the Step 4 builder
    rep = {"dataset_manifest": {"telemetry_schema_version": "2", "feature_version": "step2_v1",
                                "analysis_code_version": "step3_v1", "telemetry_sha256": "t", "labels_sha256": "l"},
           "candidates": []}
    errs = bp.verify_artifacts(rep, [], {"telemetry": "t", "labels": "l"})
    assert any("schema" in e for e in errs) and any("feature version" in e for e in errs)
    assert any("Step 3 analysis version" in e for e in errs)
    # a frozen Step 4 policy from schema 2 is refused everywhere (never migrated)
    import test_stage10 as t10
    good = t10.make_policy()
    assert ps.validate_policy(good, allow_synthetic=True) == []
    z = t10.make_policy(feature="perp_momentum_z_60s", controls=("spot_momentum_z_60s",))
    assert ps.validate_policy(z, allow_synthetic=True) == []         # the new primary is representable
    old = dict(good, telemetry_schema_version="2", feature_version="step2_v1")
    old["policy_hash"] = ps.compute_policy_hash(old)
    errs = ps.validate_policy(old, allow_synthetic=True)
    assert "telemetry schema mismatch" in errs and "feature version mismatch" in errs
    try:
        ps.FrozenPolicy(old, allow_synthetic=True); raise AssertionError("old policy accepted")
    except ValueError:
        pass
    d = tempfile.mkdtemp(prefix="_t14pol")
    try:
        reg = os.path.join(d, "p.json")
        json.dump({"policy_schema_version": ps.POLICY_SCHEMA_VERSION, "policies": [old]}, open(reg, "w"))
        pols, perrs, status = ps.load_policy_registry(reg, allow_synthetic=True)
        assert pols == [] and perrs and status == "error"
        ev = ps.ShadowEvaluator.from_files(reg, os.path.join(d, "j.csv"), allow_synthetic=True)
        ev.process_rows({"BTC": {"coin": "BTC", "binary_signal": True, "binary_ticker": "T", "analysis_ready": True}})
        assert ev.health()["status"] == "error" and ev.counts["shadow_rows"] == 0   # nothing scored
        assert any("telemetry schema mismatch" in e for e in bx.verify_pair(old, {}))  # Step 5 refuses too
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_old_promotion_chain_fails_closed():
    """A complete, valid synthetic Step 4->5->6 chain, then the code's expected telemetry schema
    moves on (as it did from v2 to v3): promotion is refused and the live gate refuses to activate,
    so the legacy call path runs untouched."""
    import test_stage12 as t12
    f = t12.chain()
    t12.the_promotion()                                     # built while the versions still match
    d = tempfile.mkdtemp(prefix="_t14promo")
    try:
        with t7.patched(ap, EXPECTED_SCHEMA_VERSION="4", EXPECTED_FEATURE_VERSION="step2_v3"):
            try:
                t12.promote_ok(output=os.path.join(d, "promo.json"))
                raise AssertionError("promotion of a stale-schema policy was accepted")
            except promote.PromotionRefused as e:
                assert "telemetry schema mismatch" in str(e) and "feature version mismatch" in str(e)
            assert not os.path.exists(os.path.join(d, "promo.json"))
            g = pl.LiveVetoGate.from_files(f["promo"], f["reg"], t12.BASELINE, t12.DASH, os.path.join(d, "v.csv"),
                                           env_enabled=True, allow_synthetic=True)
            assert g.status == "REFUSED" and g.active is False
            assert any("schema" in p or "policy registry unusable" in p for p in g.problems), g.problems
            res = g.evaluate("BTC", {"signal": True, "ticker": "T", "fav": "UP"}, {"analysis_ready": True})
            assert res["decision"] == pl.INACTIVE and res["legacy_signal"] is True    # legacy call proceeds
        g2 = pl.LiveVetoGate.from_files(f["promo"], f["reg"], t12.BASELINE, t12.DASH, os.path.join(d, "v2.csv"),
                                        env_enabled=True, allow_synthetic=True)
        assert g2.status == "ACTIVE"                          # control: same chain, matching versions
    finally:
        shutil.rmtree(d, ignore_errors=True)


# ═══════════════════ H: legacy strategy isolation ═══════════════════
def test_fingerprint_and_static_isolation():
    man = json.load(open(os.path.join(HERE, "step5_baseline_manifest.json")))
    ok, why = sf.verify(os.path.join(HERE, "kalshi_dashboard.py"), man)
    assert ok, why
    assert sf.current_fingerprint(os.path.join(HERE, "kalshi_dashboard.py"))[0] == TRUSTED_V2
    import inspect
    new_names = set(pt.VOLATILITY_COLUMNS) | {"dashboard_view", "perp_vol", "stability", "vol_regime", "momentum_z"}
    for fn in sf.STRATEGY_FUNCTIONS + ("build_call_embed", "_gate_decision"):
        src = inspect.getsource(getattr(k, fn))
        assert not any(n in src for n in new_names), fn
    # nothing outside telemetry / analysis / quality / display code references the new columns
    readers = {"perp_telemetry.py", "analyze_perp_predictive.py", "analyze_perp_quality.py", "kalshi_dashboard.py"}
    for name in os.listdir(HERE):
        if name.endswith(".py") and not name.startswith("test_") and name not in readers and name != "bench_perf.py":
            src = open(os.path.join(HERE, name)).read()
            hits = [c for c in pt.VOLATILITY_COLUMNS if c in src]
            assert not hits, (name, hits)
    dash = open(os.path.join(HERE, "kalshi_dashboard.py")).read()
    for c in pt.VOLATILITY_COLUMNS:                 # dashboard PYTHON never reads the columns; only the page JS
        assert dash.count(c) == k.PAGE.count(c), c   # displays fields of pt.dashboard_view(...)
    assert pp_constants_unchanged()


def pp_constants_unchanged():
    import perp_probability as pp
    return pp.ALPHA_GRID == (0.25, 0.50, 0.75, 1.00) and pp.MAX_ABS_PERP_DELTA == 0.05 \
        and pl.MODE == "LIVE_VETO_ONLY" and pl.REQUIRED_SCOPE["new_calls"] is False


def _stressed_tel():
    """Sampler-backed telemetry whose latest state is EXTREME / UNSTABLE: vol burst, blown-out
    spread, premium jump, perp/spot disagreement."""
    tel = t8._sampler_tel()
    now = time.time()
    rng = random.Random(4)
    for c in k.COINS:
        mid = 100.0
        for back in range(1200, 0, -4):
            ts = now - back
            burst = back <= 60
            mid *= math.exp(rng.gauss(0, 3e-3 if burst else 2e-4) + (4e-3 if burst else 0.0))
            tel.store.add(_snap(ts - 0.5, mid, coin=c, half_spread=0.5 if burst else 0.01,
                                idx=mid * (0.99 if burst else 0.9999)))
            tel.spot_long[c].append(ts, 100.3)
    return tel, now


def test_poller_identical_with_stressed_telemetry():
    import test_stage12 as t12
    base = t12._poller(None)                                         # normal telemetry, no promotion
    with t7.patched(t12, _tel_with_history=lambda minutes=6: _stressed_tel()):
        stressed = t12._poller(None)
    with t7.patched(k, PERP_TELEMETRY_ENABLED=False):
        off = t12._poller(None)
    for c in k.COINS:
        for f in t7.STRATEGY_FIELDS + ("edge", "verdict"):
            assert stressed["coins"][c][f] == base["coins"][c][f] == off["coins"][c][f], (c, f)
    for key in ("on_call", "log_call", "paper", "alerted"):
        assert stressed[key] == base[key] == off[key], key
    for tk, cp in stressed["pending"].items():
        for x in ("contracts", "stop", "entry", "entry_mode", "side", "maker_filled"):
            assert cp[x] == base["pending"][tk][x] == off["pending"][tk][x], (tk, x)
    tel, now = _stressed_tel()
    row = tel.preview_row("BTC", dict(t12._r(now), spot_raw=100.3), now)
    assert row["perp_stability_state"] == "UNSTABLE" and row["vol_regime"] in ("HIGH", "EXTREME"), \
        (row["perp_stability_state"], row["vol_regime"], row["perp_stability_reasons"])
    assert base["health"]["active"] is False and stressed["health"]["active"] is False


def test_nothing_new_can_become_a_candidate():
    # interactions: the best possible status is research-only evidence, never a Step 3 candidate
    perfect = {"n_available": 5000, "valid_folds": 4, "brier_improvement": 0.1, "logloss_improvement": 0.1,
               "brier_ci_low": 0.05, "q_value": 0.001, "sign_consistency_pct": 100.0, "yes_count": 2500,
               "no_count": 2500}
    st, _ = ap.classify_interaction(perfect)
    assert st == ap.IX_EVIDENCE and st != ap.ST_CANDIDATE
    # secondary new features are screened but can never be candidates
    for feat in ("momentum_gap_z_60s", "perp_momentum_z_30s", "perp_spread_ratio_5m", "premium_stress_5m"):
        assert feat not in ap.PRIMARY_FEATURES
        st, why = ap.classify(dict(perfect, primary_or_secondary="secondary", feature_family=ap.feature_family(feat),
                                   brier_improvement_vs_b=0.1, logloss_improvement_vs_b=0.1))
        assert st == ap.ST_EXPLORATORY, (feat, st)
    assert [f for f in ap.PRIMARY_FEATURES if f in pt.VOLATILITY_COLUMNS] == ["perp_momentum_z_60s"]
    for f in pt.VOLATILITY_COLUMNS:                            # categorical / count columns are never predictors
        if f in ("vol_regime", "perp_stability_state", "perp_stability_reasons", "perp_spread_baseline_n",
                 "perp_spread_median_5m_bps"):
            try:
                ap.assert_allowed_predictor(f); raise AssertionError(f"{f} accepted as a predictor")
            except ValueError:
                pass
    # control mapping: normalised perp momentum is judged against EQUALLY normalised spot momentum
    for h in ("30s", "60s", "180s"):
        assert ap.spot_controls_for(f"perp_momentum_z_{h}") == [f"spot_momentum_z_{h}"]
        assert ap.spot_controls_for(f"momentum_gap_z_{h}") == [f"spot_momentum_z_{h}"]
        assert f"spot_momentum_z_{h}" in ap.SPOT_CONTROL_FEATURES
    assert ap.spot_controls_for("perp_spread_ratio_5m") == ["spot_vol_shock_60v300"]
    assert ap.feature_family("perp_momentum_z_60s") == "directional"
    assert ap.feature_family("momentum_gap_z_60s") == "directional"
    assert ap.feature_family("perp_spread_ratio_5m") == ap.feature_family("premium_stress_5m") == "reliability"
    try:
        ap.screen_feature([], "spot_momentum_z_60s"); raise AssertionError("spot control screened")
    except ValueError:
        pass
    try:
        ap.screen_interaction([], "perp_momentum_z_60s", "premium_stress_5m"); raise AssertionError("undeclared")
    except ValueError:
        pass


# ═══════════════════ Step 3 behaviour on planted synthetic data ═══════════════════
FAST = {"min_analysis_markets": 100, "min_candidate_markets": 300, "min_class_count": 50,
        "min_valid_folds": 3, "min_leadlag_anchors": 100}


def _obs(n=900, seed=11, mode="perp"):
    """Synthetic observations. mode 'perp': outcome depends on perp_momentum_z_60s beyond spot.
    'spot': outcome depends on spot momentum only; perp_z = spot_z + noise (should NOT count).
    'ix': perp_z matters only when perp_vol_shock_60v300 is low (interaction).  'none': no effect."""
    rng = random.Random(seed)
    out = []
    for i in range(n):
        b, s = rng.gauss(0, 1.0), rng.gauss(0, 1)
        e = rng.gauss(0, 1)
        vs = abs(rng.gauss(1.0, 0.5))
        perp = s + 0.8 * e if mode == "spot" else e
        if mode == "perp":
            eta = b + 0.3 * s + 1.0 * perp
        elif mode == "spot":
            eta = b + 1.2 * s
        elif mode == "ix":
            eta = b + 0.3 * s + 1.6 * perp * (1.5 - vs)
        else:
            eta = b + 0.3 * s
        y = 1 if rng.random() < ap.sigmoid(eta) else 0
        p = ap.sigmoid(b)
        coin = ap.COINS[i % 4]
        f = {c: None for c in ap.FEATURE_ALLOWLIST}
        f.update(perp_momentum_z_60s=perp, spot_momentum_z_60s=s, perp_vol_shock_60v300=vs,
                 momentum_gap_z_60s=(perp - s) / math.sqrt(2), perp_spread_ratio_5m=abs(rng.gauss(1, 0.3)))
        out.append({"ticker": f"T{i}", "coin": coin, "close": 1e9 + (i // 4) * 900.0, "y": y, "p": p,
                    "base_logit": ap.logit(p), "fav": "UP" if p >= 0.5 else "DOWN", "f": f, "cat": {},
                    "favored_correct": (y == 1) if p >= 0.5 else (y == 0)})
    return out


def test_step3_spot_controlled_momentum():
    r = ap.screen_feature(_obs(mode="perp"), "perp_momentum_z_60s", group="ALL", horizon=4, reps=200,
                          thresholds=FAST)
    assert r["controls"] == ["spot_momentum_z_60s"] and r["primary_or_secondary"] == "primary"
    assert r["brier_improvement_vs_b"] > 0 and r["brier_ci_low"] > 0, r
    ap.apply_bh_and_classify([r], FAST)
    assert r["status"] == ap.ST_CANDIDATE, r["status_reasons"]
    r2 = ap.screen_feature(_obs(mode="spot"), "perp_momentum_z_60s", group="ALL", horizon=4, reps=200,
                           thresholds=FAST)
    ap.apply_bh_and_classify([r2], FAST)
    assert r2["status"] != ap.ST_CANDIDATE, "spot-driven outcome credited to perp momentum"
    assert r2["brier_improvement_vs_b"] < 0.002


def test_step3_interactions():
    ix = ap.screen_interaction(_obs(mode="ix", n=1600), "perp_momentum_z_60s", "perp_vol_shock_60v300",
                               group="ALL", horizon=4, reps=200, thresholds=FAST)
    assert ix["valid_folds"] >= 3 and ix["brier_improvement"] > 0 and ix["brier_ci_low"] > 0, ix
    assert ix["interaction_sign"] == "-"                          # value FALLS as the vol shock rises
    ap.apply_bh_interactions([ix], FAST)
    assert ix["status"] == ap.IX_EVIDENCE, ix["status_reasons"]
    no = ap.screen_interaction(_obs(mode="perp", n=1600), "perp_momentum_z_60s", "perp_vol_shock_60v300",
                               group="ALL", horizon=4, reps=200, thresholds=FAST)
    ap.apply_bh_interactions([no], FAST)
    assert no["status"] == ap.IX_NONE, (no["status"], no.get("brier_improvement"), no.get("q_value"))
    few = ap.screen_interaction(_obs(n=60), "perp_momentum_z_60s", "perp_vol_shock_60v300", thresholds=FAST)
    assert ap.classify_interaction(few, FAST)[0] == ap.IX_INSUFFICIENT
    # deterministic
    again = ap.screen_interaction(_obs(mode="ix", n=1600), "perp_momentum_z_60s", "perp_vol_shock_60v300",
                                  group="ALL", horizon=4, reps=200, thresholds=FAST)
    assert {k_: v for k_, v in again.items() if k_ not in ("q_value", "status", "status_reasons")} == \
        {k_: v for k_, v in ix.items() if k_ not in ("q_value", "status", "status_reasons")}


def test_step3_report_sections():
    import test_stage9 as t9
    e = t9._e2e()
    rep = e["rep"]
    ix = rep["interaction_screen"]
    assert len(ix) == len(ap.INTERACTIONS) * (len(ap.HORIZONS_MIN) + 1) * len(ap.GROUPS)
    assert {r["status"] for r in ix} <= {ap.IX_INSUFFICIENT, ap.IX_NONE, ap.IX_EVIDENCE}
    assert all(c["feature"] in ap.PRIMARY_FEATURES for c in rep["candidates"])
    vr = rep["volatility_regime_descriptive"]["ALL|4"]
    assert set(vr["vol_regime"]) == set(ap.VOL_REGIME_CATEGORIES)
    assert sum(v["n"] for v in vr["vol_regime"].values()) == rep["baseline"]["ALL|4"]["unique_markets"]
    assert rep["config"]["interactions"]["members"] == [list(x) for x in ap.INTERACTIONS]
    out = os.path.join(e["dir"], "out")
    hdr = next(csv.reader(open(os.path.join(out, "perp_interaction_screen.csv"))))
    assert hdr == ap.INTERACTION_CSV_COLUMNS
    assert "INTERACTIONS (research only" in ap.render(rep)
    scr = {(r["cohort"], r["group"], r["horizon_min"], r["feature_name"]): r for r in rep["feature_screen"]}
    assert scr[("primary", "ALL", 4, "perp_momentum_z_60s")]["controls"] == ["spot_momentum_z_60s"]
    assert scr[("primary", "ALL", 4, "perp_spread_ratio_5m")]["primary_or_secondary"] == "secondary"


# ═══════════════════ quality report ═══════════════════
def test_quality_report_volatility_section():
    d = tempfile.mkdtemp(prefix="_t14q")
    try:
        path = os.path.join(d, "perp.csv")
        tel = pt.PerpTelemetry(t7.StaticProvider(lambda c: None), ["BTC", "ETH"], log_path=path, session_id="T14",
                               sampler=pt.PerpSampler(t7.StaticProvider(lambda c: None), ["BTC", "ETH"]))
        t = 10_000.0
        spot = _history(tel, t, coin="BTC")
        _history(tel, t, n=20, coin="ETH")                    # ETH still warming up
        for j in range(3):
            tel.record_cycle({"BTC": dict(OK, spot_raw=spot), "ETH": dict(OK, spot_raw=50.0)},
                             {"BTC": t + 2 * j, "ETH": t + 2 * j}, now=t + 2 * j + 0.5)   # within max lag
        rows = list(csv.DictReader(open(path)))
        rows[0]["perp_momentum_z_60s"] = "123.0"             # plant pathological values (reported, not clipped)
        rows[1]["perp_spread_ratio_5m"] = "inf"
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=pt.CSV_COLUMNS); w.writeheader(); w.writerows(rows)
        rep = aq.analyze(path)
        o, btc, eth = rep["overall"], rep["by_coin"]["BTC"], rep["by_coin"]["ETH"]
        for f in aq.VOL_FEATURES:
            assert f in o["feature_coverage_pct"] and f in o["feature_coverage_pct_when_analysis_ready"], f
        assert btc["feature_coverage_pct"]["perp_momentum_z_60s"] == 100.0
        assert eth["feature_coverage_pct"]["perp_momentum_z_60s"] == 0.0
        v = eth["volatility"]
        assert v["vol_regime"]["UNKNOWN"]["count"] == 3 and v["vol_regime_unknown_pct"] == 100.0
        assert v["missing_reasons"]["perp_momentum_z_60s"] == {"PERP_RV300_WARMUP_OR_GAP": 3}
        assert v["missing_reasons"]["perp_spread_median_5m_bps"] == {"SPREAD_BASELINE_INSUFFICIENT": 3}
        assert sum(x["count"] for x in btc["volatility"]["stability_state"].values()
                   if isinstance(x, dict) and "count" in x) == 3
        e = btc["volatility"]["extremes"]["perp_momentum_z_60s"]
        assert e["max"] == 123.0 and e["n"] == 3                                         # not clipped
        pv = rep["structural"]["pathological_values"]
        assert pv["perp_momentum_z_60s"]["count"] == 1 and pv["perp_momentum_z_60s"]["examples"][0]["value"] == "123.0"
        assert pv["perp_spread_ratio_5m"]["count"] == 1
        assert rep["structural"]["non_finite_values"].get("perp_spread_ratio_5m") == 1
        assert "vol_regime" not in rep["structural"]["non_finite_values"]
        txt = aq.render(rep)
        assert "volatility regime (telemetry, observational)" in txt and "blank-value reasons" in txt
        json.dumps(rep)
        assert rows == list(csv.DictReader(open(path)))                                  # file untouched
    finally:
        shutil.rmtree(d, ignore_errors=True)


def test_static_read_only():
    t7.test_read_only_static()
    t8.test_static_read_only_and_no_scores()
    src = open(pt.__file__).read()
    assert "stability_score" not in src and "weighted" not in src.lower()
    for word in ("liquidation", "open_interest"):
        for line in src.splitlines():
            if word in line.lower():
                assert "no " in line.lower() or "not" in line.lower() or "nothing" in line.lower(), line


def test_previous_stages():
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 14):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-600:], p.stderr[-600:])


if __name__ == "__main__":
    run("A1 normalisation math (30/60/180, zero/missing/non-finite)", test_normalization_math)
    run("A2 normalisation in telemetry rows (exact)", test_normalization_in_rows)
    run("A3 warm-up / zero volatility / missing spot", test_warmup_and_zero_vol)
    run("B1 enormous future perp/spot values cannot move any new feature", test_future_values_cannot_leak)
    run("B2 outcome-like fields cannot change new features", test_outcome_fields_cannot_change_new_features)
    run("C1 volatility regime exact boundaries + UNKNOWN", test_vol_regime_boundaries)
    run("C2 stability rule (transparent, boundaries, lower bounds)", test_stability_rule)
    run("C3 regime + stability logged consistently", test_regime_and_stability_logged)
    run("D1 spread stress (normal/widening/missing bid/ask/crossed/insufficient)", test_spread_stress)
    run("D2 spread stress never bridges a contract re-scale", test_spread_scale_change)
    run("D3 trailing median helper", test_trailing_median_helper)
    run("E  normalised perp-vs-spot gap", test_normalized_gap)
    run("F1 dashboard view never breaks on unavailable telemetry", test_dashboard_view_never_breaks)
    run("F2 dashboard publish + page renderer", test_dashboard_publish_and_page)
    run("G1 schema v3 header, versions, v2 file rotated not appended", test_schema_header_and_versions)
    run("G2 old Step 3 / Step 4 artifacts fail safely", test_old_step3_and_step4_artifacts_fail_safely)
    run("G3 old-schema promotion chain refused; gate stays inactive", test_old_promotion_chain_fails_closed)
    run("H1 strategy fingerprint unchanged; static isolation", test_fingerprint_and_static_isolation)
    run("H2 poller outputs identical with stressed / normal / no telemetry", test_poller_identical_with_stressed_telemetry)
    run("H3 nothing new can become a call or a candidate", test_nothing_new_can_become_a_candidate)
    run("+  Step 3: normalised perp momentum judged against normalised spot", test_step3_spot_controlled_momentum)
    run("+  Step 3: research-only interactions", test_step3_interactions)
    run("+  Step 3: report sections / outputs", test_step3_report_sections)
    run("+  quality report: coverage, regimes, reasons, pathological values", test_quality_report_volatility_section)
    run("+  read-only / no scores / no fabricated inputs", test_static_read_only)
    run("I  all previous stage suites", test_previous_stages)
    print("\nAll Stage 14 tests passed.")
