#!/usr/bin/env python3
"""Stage 10 tests: Step 4 candidate filters + SHADOW-ONLY validation. No network.
Run:  py test_stage10.py"""
import ast
import copy
import csv
import hashlib
import json
import math
import os
import random
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import types

import analyze_perp_predictive as ap
import analyze_perp_shadow as ash
import build_perp_shadow_policy as bp
import kalshi_dashboard as k
import perp_shadow as ps
import perp_telemetry as pt
import test_stage7 as t7
import test_stage8 as t8
import test_stage9 as t9

HERE = os.path.dirname(os.path.abspath(__file__))
TEST_GATES = {"min_threshold_total": 200, "min_threshold_blocked": 20, "min_threshold_kept": 100}
FX = {}


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


# ═══════════════════ shared Phase A fixture (synthetic; tests only) ═══════════════════
def fixture():
    if not FX:
        d = tempfile.mkdtemp(prefix="_t10")
        t, l = t9.write_e2e(d, n_times=150)
        ap.run_analysis(t, l, os.path.join(d, "out"), reps=200, thresholds=t9.FAST, log=None)
        rep, man = os.path.join(d, "out", "perp_predictive_report.json"), os.path.join(d, "out", "perp_candidate_manifest.json")
        pol_path = os.path.join(d, "policies.json")
        summ = bp.build(t, l, rep, man, pol_path, allow_synthetic=True, gates=TEST_GATES, reps=300, log=None, now=1.9e9)
        FX.update(dir=d, tele=t, labels=l, report=rep, manifest=man, policies=pol_path, summary=summ,
                  policy=json.load(open(pol_path))["policies"][0])
    return FX


def _copy_artifacts(report_edit=None, manifest=None):
    """Write an edited-but-consistent copy of the Step 3 report + manifest."""
    f = fixture()
    d = tempfile.mkdtemp(prefix="_t10a", dir=f["dir"])
    rep = json.load(open(f["report"]))
    if report_edit:
        report_edit(rep)
    if manifest is not None:
        rep["candidates"] = manifest
    rp, mp = os.path.join(d, "r.json"), os.path.join(d, "m.json")
    json.dump(rep, open(rp, "w")); json.dump(rep["candidates"], open(mp, "w"))
    return rp, mp, os.path.join(d, "pol.json")


# 1
def test_empty_manifest():
    f = fixture()
    rp, mp, out = _copy_artifacts(manifest=[])
    lines = []
    s = bp.build(f["tele"], f["labels"], rp, mp, out, log=lines.append)
    assert s["policies_created"] == 0 and s["status"] == bp.ST_NO_REAL
    assert json.load(open(out)) == {"policy_schema_version": 1, "policies": []}
    text = "\n".join(lines)
    assert "No eligible real Step 3 first-signal candidates." in text and "No shadow filter policy created." in text
    p = subprocess.run([sys.executable, "build_perp_shadow_policy.py", "--telemetry", f["tele"], "--labels", f["labels"],
                        "--report", rp, "--candidates", mp, "--output", out], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "Policies created: 0" in p.stdout, p.stderr


def _entry_as_candidate(status):
    f = fixture()
    rep = json.load(open(f["report"]))
    e = next(r for r in rep["feature_screen"] if r["cohort"] == "first_signal" and r["group"] == "BTC"
             and r["feature_name"] in ap.PRIMARY_FEATURES and r.get("brier_improvement_vs_b") is not None)
    def edit(r):
        tgt = next(x for x in r["feature_screen"] if x["cohort"] == "first_signal" and x["group"] == "BTC"
                   and x["feature_name"] == e["feature_name"])
        tgt["status"] = status
    cand = {"cohort": "first_signal", "coin": "BTC", "horizon": "first_signal", "feature": e["feature_name"],
            "feature_family": e["feature_family"], "coefficient_sign": e["coefficient_sign"],
            "interaction_sign": e.get("interaction_sign"), "sample_size": e["n_available"],
            "oof_brier_improvement_vs_b": e["brier_improvement_vs_b"],
            "oof_logloss_improvement_vs_b": e["logloss_improvement_vs_b"],
            "brier_ci95": [e["brier_ci_low"], e["brier_ci_high"]], "q_value": e["q_value"],
            "sign_consistency_pct": e["sign_consistency_pct"]}
    return edit, cand


# 2 + 3
def test_non_candidates_rejected():
    f = fixture()
    for status in (ap.ST_NO_VALUE, ap.ST_EXPLORATORY, ap.ST_UNSTABLE):
        edit, cand = _entry_as_candidate(status)
        rp, mp, out = _copy_artifacts(report_edit=edit, manifest=[cand])
        try:
            bp.build(f["tele"], f["labels"], rp, mp, out, allow_synthetic=True, gates=TEST_GATES, reps=50, log=None)
            raise AssertionError(f"{status} produced a policy")
        except bp.BuildError as e:
            assert "not PROMISING_CANDIDATE" in str(e), e
        assert not os.path.exists(out)                                   # fail closed: nothing written


# 4 + 5
def test_fixed_horizon_and_first_signal():
    s = fixture()["summary"]
    by = {(x["candidate"]["cohort"], x["candidate"]["horizon"]): x["status"] for x in s["candidates"]}
    for h in (8, 6, 4, 2):
        assert by[("primary", h)] == bp.ST_NOT_CALL_FILTER, by
    assert by[("first_signal", "first_signal")] == bp.ST_READY and s["policies_created"] == 1
    p = fixture()["policy"]
    assert p["cohort"] == "first_signal" and p["mode"] == "SHADOW_ONLY" and p["synthetic"] is True
    assert len(p["threshold_grid_evaluation"]) == 5


# 6 + 71
def test_synthetic_production_guard():
    f = fixture()
    out = os.path.join(f["dir"], "prod_guard.json")
    s = bp.build(f["tele"], f["labels"], f["report"], f["manifest"], out, log=None)
    assert s["policies_created"] == 0 and s["real_step3_candidates"] == 0
    assert any(x["status"] == bp.ST_SYNTHETIC for x in s["candidates"])
    assert json.load(open(out))["policies"] == []
    try:
        bp.build(f["tele"], f["labels"], f["report"], f["manifest"], out, gates=TEST_GATES, log=None)
        raise AssertionError("gate override accepted without the synthetic flag")
    except bp.BuildError:
        pass
    assert bp.LABEL_SOURCE == __import__("label_binary_outcomes").SOURCE


# 7 + 35
def test_synthetic_live_load_guard():
    f = fixture()
    pols, errs, status = ps.load_policy_registry(f["policies"])
    assert pols == [] and status == "error" and "synthetic" in errs[0]
    d = tempfile.mkdtemp(dir=f["dir"])
    with t7.patched(k, _shadow=None, PERP_SHADOW_POLICY_FILE=f["policies"], PERP_SHADOW_JOURNAL=os.path.join(d, "j.csv")):
        ev = k._shadow_evaluator()
        h = ev.health()
        assert h["valid_policies"] == 0 and h["invalid_policies"] == 1 and h["status"] == "error"


# 8
def test_deterministic_policy_id():
    f = fixture()
    p = f["policy"]
    a = bp.make_policy_id(p["feature_name"], p["coin_or_group"], p["cohort"], p["discovery_cutoff_utc"],
                          p["source_hashes"], p["threshold_gates"])
    assert a == p["policy_id"] and len(a) == 16
    h2 = dict(p["source_hashes"], telemetry="0" * 64)
    assert bp.make_policy_id(p["feature_name"], p["coin_or_group"], p["cohort"], p["discovery_cutoff_utc"],
                             h2, p["threshold_gates"]) != a
    s = bp.build(f["tele"], f["labels"], f["report"], f["manifest"], f["policies"], allow_synthetic=True,
                 gates=TEST_GATES, reps=300, log=None, now=1.95e9)
    assert any(x["status"] == bp.ST_ALREADY and x["policy_id"] == a for x in s["candidates"])
    reg = json.load(open(f["policies"]))
    assert len(reg["policies"]) == 1 and reg["policies"][0] == p                  # frozen policy untouched


def _discovery():
    f = fixture()
    obs, cutoff, meta = bp.discovery_data(f["tele"], f["labels"])
    return obs, cutoff


def _future_copies(obs, cutoff):
    fut = []
    for i, o in enumerate(obs[:120]):
        c = copy.deepcopy(o)
        c["ticker"] = f"FUTURE-{i}"
        c["close"] = cutoff + 900.0 * (i + 1)
        c["f"]["causal_premium_bps"] = 1e4 * (1 if c["y"] else -1)             # would dominate any fit
        fut.append(c)
    return fut


# 9 + 25
def test_discovery_cutoff():
    obs, cutoff = _discovery()
    p = fixture()["policy"]
    assert abs(p["discovery_cutoff_epoch"] - cutoff) < 1e-6
    assert all(o["close"] <= cutoff for o in obs)
    mixed = obs + _future_copies(obs, cutoff)
    coh = bp.discovery_cohort(mixed, "ALL", cutoff)
    assert not any(o["ticker"].startswith("FUTURE") for o in coh)
    ctl = p["controls"]
    a = bp.final_fit(bp.discovery_cohort(obs, "ALL", cutoff), "causal_premium_bps", "ALL", ctl)
    b = bp.final_fit(coh, "causal_premium_bps", "ALL", ctl)
    assert a["beta_b"] == b["beta_b"] and a["beta_c"] == b["beta_c"] and a["scalers"] == b["scalers"]
    assert p["model_c_coefficients"] == a["beta_c"] and p["scalers"] == a["scalers"]   # frozen = discovery-only fit
    ra, _ = bp.oof_conflicts(bp.discovery_cohort(obs, "ALL", cutoff), "causal_premium_bps", "ALL", t9.FAST, ap.SEED)
    rb, _ = bp.oof_conflicts(coh, "causal_premium_bps", "ALL", t9.FAST, ap.SEED)
    assert ra == rb


# 10
def test_oof_conflict_scores():
    obs, cutoff = _discovery()
    coh = bp.discovery_cohort(obs, "ALL", cutoff)
    rows, scr = bp.oof_conflicts(coh, "causal_premium_bps", "ALL", t9.FAST, ap.SEED)
    ref = ap.screen_feature(coh, "causal_premium_bps", group="ALL", horizon="first_signal", cohort="first_signal",
                            reps=10, thresholds=t9.FAST, keep_oof=True)["_oof"]
    by = {r["ticker"]: r for r in ref}
    assert len(rows) == len(ref) < len(coh)                            # first training block never scored
    for r in rows:
        o = by[r["ticker"]]
        exp = ps.p_favored(o["pc"], o["fav"]) - ps.p_favored(o["pb"], o["fav"])
        assert abs(r["conflict"] - exp) < 1e-15
    fit = bp.final_fit(coh, "causal_premium_bps", "ALL", scr["controls"])
    pol = ps.FrozenPolicy(dict(fixture()["policy"]), allow_synthetic=True)
    insample = []
    for r in rows:
        o = next(x for x in coh if x["ticker"] == r["ticker"])
        row = {"coin": o["coin"], "analysis_ready": True, "base_p_up": o["p"] * 100, "fav": o["fav"],
               **{c: o["f"][c] for c in scr["controls"] + ["causal_premium_bps"]}}
        insample.append(pol.score(row)["conflict"])
    assert any(abs(a - r["conflict"]) > 1e-6 for a, r in zip(insample, rows)), "OOF must differ from in-sample"


# ═══════════════════ hand-made frozen policies (tests only) ═══════════════════
def make_policy(gamma=-3.0, threshold=-0.1, feature="causal_premium_bps", controls=("causal_spot_ret_60s_bps",),
                group="ALL", cutoff=1.0e9, synthetic=True, **extra):
    fam = ap.feature_family(feature)
    rel = fam == "reliability"
    keys = list(ap.COINS) if group == "ALL" else ["*"]
    st = [-1e6, 1e6, 0.0, 1.0]
    p = {"policy_id": "TESTPOLICY" + str(abs(hash((gamma, threshold, feature, group)) % 10**6)).zfill(6),
         "policy_schema_version": 1, "policy_version": bp.POLICY_VERSION, "mode": "SHADOW_ONLY",
         "synthetic": synthetic, "created_at_utc": "2026-09-01T00:00:00+00:00", "feature_name": feature,
         "feature_family": fam, "reliability_interaction": rel, "cohort": "first_signal", "coin_or_group": group,
         "by_coin_scaling": group == "ALL",
         "discovery_cutoff_utc": "2001-09-09T01:46:40+00:00", "discovery_cutoff_epoch": cutoff,
         "controls": list(controls), "scalers": {kk: {c: st for c in list(controls) + [feature]} for kk in keys},
         "model_b_coefficients": [0.0, 1.0] + [0.0] * len(controls),
         "model_c_coefficients": [0.0, 1.0] + [0.0] * len(controls) + [gamma] + ([0.0] if rel else []),
         "conflict_threshold": threshold, "expected_training_block_fraction": 0.10,
         "telemetry_schema_version": "2", "feature_version": "step2_v1",
         "source_hashes": {"telemetry": "t", "labels": "l"},
         "training_feature_summary": {"n": 500, "mean": 0.0, "std": 3.0, "p05": -5, "p25": -2, "p50": 0, "p75": 2, "p95": 5}}
    p.update(extra)
    p["policy_hash"] = ps.compute_policy_hash(p)
    return p


def trow(**kw):
    r = {"coin": "BTC", "analysis_ready": True, "base_p_up": 70.0, "fav": "UP", "causal_premium_bps": 2.0,
         "causal_spot_ret_60s_bps": 0.5, "binary_signal": True, "binary_ticker": "TK1",
         "spot_observed_ts_epoch_ms": 2_000_000_000_000, "telemetry_session_id": "S", "cycle_id": 1}
    r.update(kw)
    return r


# 11-14
def test_favored_side_and_sign():
    assert ps.p_favored(0.7, "UP") == 0.7 and abs(ps.p_favored(0.7, "DOWN") - 0.3) < 1e-15
    pol = ps.FrozenPolicy(make_policy(gamma=-1.0), allow_synthetic=True)
    up = pol.score(trow(fav="UP", base_p_up=70.0, causal_premium_bps=2.0))
    assert abs(up["p_b_fav"] - up["p_b_up"]) < 1e-15 and abs(up["p_b_up"] - 0.7) < 1e-9          # 11
    dn = pol.score(trow(fav="DOWN", base_p_up=30.0, causal_premium_bps=2.0))
    assert abs(dn["p_b_fav"] - (1 - dn["p_b_up"])) < 1e-15 and abs(dn["p_c_fav"] - (1 - dn["p_c_up"])) < 1e-15   # 12
    assert up["conflict"] < 0 and up["p_c_up"] < up["p_b_up"]                   # 13: perp lowers P(UP) vs fav UP
    assert dn["conflict"] > 0                                                   # same move SUPPORTS a DOWN call
    assert abs(up["conflict"] - (up["p_c_fav"] - up["p_b_fav"])) < 1e-15
    strong = ps.FrozenPolicy(make_policy(gamma=-3.0, threshold=-0.05), allow_synthetic=True)
    assert strong.score(trow(fav="UP", causal_premium_bps=5.0))["decision"] == ps.WOULD_BLOCK
    sup = strong.score(trow(fav="DOWN", base_p_up=30.0, causal_premium_bps=5.0))
    assert sup["conflict"] > 0 and sup["decision"] == ps.ALLOW                  # 14: support never blocked
    for thr in (0.0, 0.2):
        assert ps.validate_policy(make_policy(threshold=thr), allow_synthetic=True)   # non-negative refused


# 15
def test_threshold_grid_predeclared():
    rng = random.Random(1)
    rows = [{"ticker": f"t{i}", "conflict": rng.gauss(0, 0.1), "win": rng.random() < 0.7} for i in range(3000)]
    grid = bp.threshold_grid([r["conflict"] for r in rows])
    assert [q for q, _ in grid] == [5.0, 10.0, 15.0, 20.0, 25.0] == list(bp.THRESHOLD_QUANTILES)
    ev = bp.evaluate_thresholds(rows, TEST_GATES, reps=50)
    assert len(ev) == 5 and [m["source_quantile"] for m in ev] == list(bp.THRESHOLD_QUANTILES)
    src = open(bp.__file__).read()
    assert "THRESHOLD_QUANTILES = (5.0, 10.0, 15.0, 20.0, 25.0)" in src


# 16-20
def _good_metrics(**kw):
    m = {"threshold": -0.05, "n_total": 1000, "n_blocked": 100, "n_kept": 900, "block_fraction": 0.10,
         "loss_enrichment_pp": 12.0, "loss_enrichment_ci95_pp": [4.0, 20.0], "threshold_q": 0.01,
         "kept_win_rate": 0.75, "baseline_win_rate": 0.73}
    m.update(kw)
    return m


def test_threshold_gates():
    assert bp.eligibility_reasons(_good_metrics()) == []
    cases = {"block fraction outside allowed range": [dict(block_fraction=0.04), dict(block_fraction=0.26)],  # 16,17
             "blocked count below minimum": [dict(n_blocked=49)],                                            # 18
             "kept count below minimum": [dict(n_kept=299)],                                                 # 19
             "loss enrichment below minimum effect size": [dict(loss_enrichment_pp=4.9, loss_enrichment_ci95_pp=[1.0, 8.0],
                                                                threshold_q=1e-6)],                          # 20
             "total below minimum": [dict(n_total=499)],
             "threshold not negative (would block confirming calls)": [dict(threshold=0.0), dict(threshold=0.01)],
             "bootstrap CI for loss enrichment includes 0": [dict(loss_enrichment_ci95_pp=[0.0, 9.0])],
             "BH q above limit": [dict(threshold_q=0.051)],
             "kept win rate worse than baseline": [dict(kept_win_rate=0.72)]}
    for reason, edits in cases.items():
        for e in edits:
            got = bp.eligibility_reasons(_good_metrics(**e))
            assert reason in got, (reason, e, got)
    # data-level: heavy ties put 40% of calls at the threshold -> blocks too many -> rejected
    rows = [{"ticker": f"t{i}", "conflict": -0.5 if i < 400 else 0.1 + i * 1e-5, "win": i >= 200} for i in range(1000)]
    ev = bp.evaluate_thresholds(rows, reps=50)
    assert all(abs(m["block_fraction"] - 0.4) < 1e-9 and "block fraction outside allowed range" in m["ineligible_reasons"]
               for m in ev)


# 21 + 22
def test_threshold_bootstrap_and_bh():
    rng = random.Random(3)
    rows = []
    for i in range(1200):
        c = rng.gauss(0, 0.1)
        bad = c < -0.1
        rows.append({"ticker": f"t{i}", "conflict": c, "win": rng.random() < (0.35 if bad else 0.8)})
    ev = bp.evaluate_thresholds(rows, reps=400, seed=5)
    for m in ev:
        assert m["loss_enrichment_ci95_pp"][0] > 0, m                              # 21
    ps_ = [m["threshold_p"] for m in ev]
    assert [m["threshold_q"] for m in ev] == ap.bh_qvalues(ps_)                   # 22
    q = ap.bh_qvalues([0.01, 0.04, 0.03, 0.005, 0.2])
    assert [round(x, 10) for x in q] == [0.025, 0.05, 0.05, 0.025, 0.2]
    assert bp.evaluate_thresholds(rows, reps=400, seed=5) == ev                   # deterministic


# 23 + 24
def test_conservative_selection_and_no_threshold():
    ev = [dict(_good_metrics(block_fraction=0.05, loss_enrichment_pp=6.0), source_quantile=5.0, eligible=True),
          dict(_good_metrics(block_fraction=0.10, loss_enrichment_pp=20.0), source_quantile=10.0, eligible=True),
          dict(_good_metrics(block_fraction=0.15), source_quantile=15.0, eligible=False),
          dict(_good_metrics(block_fraction=0.20, loss_enrichment_pp=40.0), source_quantile=20.0, eligible=True)]
    assert bp.select_threshold(ev)["source_quantile"] == 5.0                     # not the best-looking one
    assert bp.select_threshold([dict(e, eligible=False) for e in ev]) is None
    f = fixture()
    out = os.path.join(f["dir"], "strict.json")
    s = bp.build(f["tele"], f["labels"], f["report"], f["manifest"], out, allow_synthetic=True,
                 gates=dict(TEST_GATES, min_loss_enrichment_pp=99.0), reps=100, log=None)
    assert s["policies_created"] == 0 and s["status"] == bp.ST_NO_THRESHOLD
    assert json.load(open(out))["policies"] == []


# 26 + 27
def test_policy_serialization_and_hash():
    p = fixture()["policy"]
    a = ps.FrozenPolicy(p, allow_synthetic=True)
    b = ps.FrozenPolicy(json.loads(json.dumps(p)), allow_synthetic=True)
    rng = random.Random(9)
    for _ in range(50):
        row = trow(coin=rng.choice(ap.COINS), fav=rng.choice(("UP", "DOWN")), base_p_up=rng.uniform(5, 95),
                   causal_premium_bps=rng.gauss(0, 3), causal_spot_ret_60s_bps=rng.gauss(0, 1),
                   spot_vol_shock_60v300=abs(rng.gauss(1, .3)))
        assert a.score(row) == b.score(row)
    assert ps.validate_policy(p, allow_synthetic=True) == []
    for edit in (("conflict_threshold", p["conflict_threshold"] * 1.01),
                 ("model_c_coefficients", [x + 1e-9 for x in p["model_c_coefficients"]]),
                 ("controls", p["controls"][:1])):
        bad = dict(p, **{edit[0]: edit[1]})
        errs = ps.validate_policy(bad, allow_synthetic=True)
        assert any("policy_hash" in e for e in errs), (edit[0], errs)
        assert ps.compute_policy_hash(bad) != p["policy_hash"]
    for key in ("enabled_for_trading", "live_filter"):
        bad = dict(p, **{key: True}); bad["policy_hash"] = ps.compute_policy_hash(bad)
        assert any("execution-like" in e for e in ps.validate_policy(bad, allow_synthetic=True))


# ═══════════════════ evaluator (first signal, restart, unavailable) ═══════════════════
def _evaluator(policy, lookup=None, journal=None):
    d = tempfile.mkdtemp(dir=fixture()["dir"])
    j = ps.ShadowJournal(journal or os.path.join(d, "shadow.csv"))
    return ps.ShadowEvaluator([ps.FrozenPolicy(policy, allow_synthetic=True)], j, lookup), j


def _journal_rows(j):
    return list(csv.DictReader(open(j.path))) if os.path.exists(j.path) else []


# 28
def test_first_signal_only():
    ev, j = _evaluator(make_policy())
    for n in range(3):
        ev.process_rows({"BTC": trow(spot_observed_ts_epoch_ms=2e12 + n * 4000, causal_premium_bps=float(n))})
    rows = _journal_rows(j)
    assert len(rows) == 1 and rows[0]["signal_ts_epoch_ms"] == str(int(2e12))
    ev.process_rows({"BTC": trow(binary_signal=False, binary_ticker="TK2")})
    assert len(_journal_rows(j)) == 1                                             # non-signal rows ignored


# 29 + 36 + 37
def test_first_signal_unavailable_not_replaced():
    ev, j = _evaluator(make_policy())
    ev.process_rows({"BTC": trow(analysis_ready=False)})
    ev.process_rows({"BTC": trow(spot_observed_ts_epoch_ms=2e12 + 4000)})         # later, fully valid
    rows = _journal_rows(j)
    assert len(rows) == 1 and rows[0]["shadow_decision"] == ps.UNAVAILABLE
    assert rows[0]["shadow_reason"] == ps.R_NOT_READY
    pol = ps.FrozenPolicy(make_policy(), allow_synthetic=True)
    assert pol.score(trow(causal_premium_bps=None))["reason"] == ps.R_FEATURE          # 36
    assert pol.score(trow(causal_spot_ret_60s_bps=None))["reason"] == ps.R_CONTROL     # 37
    assert pol.score(trow(base_p_up=None))["reason"] == ps.R_BASE
    one = ps.FrozenPolicy(make_policy(group="ETH"), allow_synthetic=True)
    assert one.score(trow(coin="BTC"))["decision"] == ps.NOT_APPLICABLE
    # the real first call happened on a cycle whose telemetry row was dropped -> UNAVAILABLE, not a later row
    ev2, j2 = _evaluator(make_policy(), lookup=lambda tk: {"signal_ts_epoch_ms": 1.9e12, "rec_stop": 66})
    ev2.process_rows({"BTC": trow(spot_observed_ts_epoch_ms=2e12)})
    r2 = _journal_rows(j2)[0]
    assert r2["shadow_decision"] == ps.UNAVAILABLE and r2["shadow_reason"] == ps.R_MISSED
    ev3, j3 = _evaluator(make_policy(), lookup=lambda tk: {"signal_ts_epoch_ms": 2e12, "rec_stop": 66})
    ev3.process_rows({"BTC": trow(spot_observed_ts_epoch_ms=2e12)})
    r3 = _journal_rows(j3)[0]
    assert r3["first_signal_verified"] == "True" and r3["rec_stop"] == "66"


# 30
def test_restart_dedup():
    ev, j = _evaluator(make_policy())
    ev.process_rows({"BTC": trow()})
    ev2 = ps.ShadowEvaluator([ps.FrozenPolicy(make_policy(), allow_synthetic=True)], ps.ShadowJournal(j.path))
    ev2.process_rows({"BTC": trow(spot_observed_ts_epoch_ms=2e12 + 60000)})
    assert len(_journal_rows(j)) == 1
    # a schema-rotated journal still counts for de-duplication
    old = j.path + ".schema-1.bak"
    shutil.copy(j.path, old)
    with open(j.path, "w") as f:
        f.write("some,other,header\n")
    ev3 = ps.ShadowEvaluator([ps.FrozenPolicy(make_policy(), allow_synthetic=True)], ps.ShadowJournal(j.path))
    ev3.process_rows({"BTC": trow()})
    assert not os.path.exists(j.path) or len(_journal_rows(ps.ShadowJournal(j.path))) == 0


# ═══════════════════ poller integration (the most important tests) ═══════════════════
def _poller(shadow, fixture_idx=0, on_call=None, prefill_spot=True):
    """One REAL poller() cycle: sampler-mode telemetry wired to the real k._perp_publish (which
    runs the shadow hook), a distinct ticker per coin, optional ON_CALL recorder. Returns STATE
    coins, the pending calls, ON_CALL invocations, paper rows and shadow health."""
    tel = t8._sampler_tel(prefill=t8.MODES["normal"])
    if prefill_spot:
        now = time.time()
        for c in k.COINS:
            for back in range(300, 0, -4):
                tel.spot_long[c].append(now - back, 100.3)
    done = threading.Event()
    def publish(rows):
        k._perp_publish(rows)
        done.set()
    tel.on_rows = publish
    calls = []
    cb = (lambda coin, r, crec: calls.append((coin, r["ticker"], r["fav"]))) if on_call else None
    margs, px, closes = t8.POLLER_FIXTURES[fixture_idx]
    mkt = t7._market(*margs)
    d = tempfile.mkdtemp(dir=fixture()["dir"])
    def stop_sleep(_s): raise t7._StopPoller()
    saved = (set(k._alerted), dict(k._pending), dict(k._call_pending), dict(k.STATE))
    k._alerted.clear(); k._pending.clear(); k._call_pending.clear()
    try:
        with t7.binary_inputs(mkt, px, closes), \
             t7.patched(k, current_market=lambda series: dict(mkt, ticker=f"{series}-T1"),
                        time=types.SimpleNamespace(time=time.time, sleep=stop_sleep), _perp=tel, _shadow=shadow,
                        PERP_TELEMETRY_ENABLED=True, PERP_SHADOW_ENABLED=True, ON_CALL=cb, POST_EMBED=None,
                        TELEGRAM_BOT_TOKEN="", RESULTS_FILE=os.path.join(d, "r.json"),
                        CALLS_FILE=os.path.join(d, "c.json"), TRADES_CSV=os.path.join(d, "t.csv"),
                        PAPER_ORDERS_CSV=os.path.join(d, "p.csv"), EXPLAIN_MARK_FILE=os.path.join(d, "e")):
            k.RUNNING.set()
            try:
                k.poller()
            except t7._StopPoller:
                pass
            assert done.wait(5), "telemetry worker did not publish"
            with k.LOCK:
                out = {"coins": json.loads(json.dumps(k.STATE["coins"])),
                       "shadow_state": json.loads(json.dumps(k.STATE.get("perp_shadow"), default=str))}
            out["pending"] = {tk: dict(v) for tk, v in k._call_pending.items()}
            out["alerted"] = sorted(k._alerted)
            pp = os.path.join(d, "p.csv")
            out["paper_rows"] = [{kk: vv for kk, vv in r.items() if kk != "ts"} for r in csv.DictReader(open(pp))] \
                if os.path.exists(pp) else []
            out["on_call"] = calls
            return out
    finally:
        k._alerted.clear(); k._alerted.update(saved[0]); k._pending.clear(); k._pending.update(saved[1])
        k._call_pending.clear(); k._call_pending.update(saved[2])
        with k.LOCK:
            k.STATE.clear(); k.STATE.update(saved[3])


def _shadow_for(mode):
    d = tempfile.mkdtemp(dir=fixture()["dir"])
    jp = os.path.join(d, "shadow.csv")
    if mode == "none":
        return None, jp
    if mode == "error":
        bad = os.path.join(d, "bad.json")
        open(bad, "w").write("{ this is not json")
        return ps.ShadowEvaluator.from_files(bad, jp, first_signal_lookup=k._shadow_first_signal), jp
    gamma = {"block": -3.0, "allow": 3.0, "unavailable": -3.0}[mode]
    pol = make_policy(gamma=gamma, threshold=-0.1,
                      feature="causal_premium_bps" if mode != "unavailable" else "premium_z_5m")
    return ps.ShadowEvaluator([ps.FrozenPolicy(pol, allow_synthetic=True)], ps.ShadowJournal(jp),
                              first_signal_lookup=k._shadow_first_signal), jp


# 31 + 32
def test_would_block_cannot_block_call():
    ref = _poller(None, on_call=True)
    ev, jp = _shadow_for("block")
    out = _poller(ev, on_call=True)
    rows = list(csv.DictReader(open(jp)))
    assert rows and all(r["shadow_decision"] == ps.WOULD_BLOCK for r in rows), rows[:1]
    assert all(r["first_signal_verified"] == "True" for r in rows)
    assert out["coins"]["BTC"]["signal"] is True
    assert sorted(out["pending"]) == sorted(ref["pending"]) and len(out["pending"]) == 4       # calls logged
    assert out["alerted"] == ref["alerted"] and out["paper_rows"] == ref["paper_rows"]
    assert out["on_call"] == ref["on_call"] and len(out["on_call"]) == 4                        # 32: callbacks fired
    assert {r["binary_ticker"] for r in rows} == set(out["pending"])
    assert out["shadow_state"]["would_block"] == 4
    # normal settlement still happens, with exact provenance
    d = tempfile.mkdtemp(dir=fixture()["dir"])
    with t7.patched(k, CALLS_FILE=os.path.join(d, "c.json"), TRADES_CSV=os.path.join(d, "t.csv"),
                    market_result=lambda tk: "yes", bot_post=lambda *a, **kw: None):
        k._call_pending.clear()
        for tk, v in out["pending"].items():
            k._call_pending[tk] = dict(v, close="2020-01-01T00:00:00Z")
        k.settle_calls()
        settled = json.load(open(k.CALLS_FILE))
        k._call_pending.clear()
    assert len(settled) == 4 and {s["ticker"] for s in settled} == {r["binary_ticker"] for r in rows}


# 33 + 34 + 35
def test_strategy_identical_across_shadow_modes():
    fields = t7.STRATEGY_FIELDS + ("edge", "verdict")
    res = {}
    for mode in ("none", "allow", "block", "unavailable", "error"):
        ev, jp = _shadow_for(mode)
        res[mode] = (_poller(ev, on_call=True), jp)
    base = res["none"][0]
    for mode, (out, jp) in res.items():
        for c in k.COINS:
            assert {f: out["coins"][c][f] for f in fields} == {f: base["coins"][c][f] for f in fields}, (mode, c)
        assert out["alerted"] == base["alerted"] and out["paper_rows"] == base["paper_rows"], mode
        assert out["on_call"] == base["on_call"], mode
        strip = lambda p: {tk: {kk: vv for kk, vv in v.items() if kk not in ("signal_ts", "signal_ts_epoch_ms")}
                           for tk, v in p.items()}
        assert strip(out["pending"]) == strip(base["pending"]), mode
    dec = lambda m: {r["shadow_decision"] for r in csv.DictReader(open(res[m][1]))}
    assert dec("allow") == {ps.ALLOW} and dec("block") == {ps.WOULD_BLOCK}
    assert dec("unavailable") == {ps.UNAVAILABLE}                                   # premium_z_5m not warmed up
    assert res["error"][0]["shadow_state"]["status"] == "error"                     # 34: corrupt policy, watcher fine
    assert not os.path.exists(res["error"][1])


# ═══════════════════ call journal provenance (38-43) ═══════════════════
GOLD = json.loads('{"stop_gap": {"coin": "BTC", "side": "UP", "entry": 90, "stop": 68, "contracts": 5, "entry_mode": "taker", "low": 90, "high": 95, "bid_lo": 60, "exit_price": 60, "exit_reason": "stop", "settle_result": "yes", "win": false, "per_contract": -33.0, "pnl": -165.0}, "settle_win": {"coin": "ETH", "side": "UP", "entry": 88, "stop": 66, "contracts": 3, "entry_mode": "taker", "low": 85, "high": 97, "bid_lo": 80, "exit_price": 100.0, "exit_reason": "settle", "settle_result": "yes", "win": true, "per_contract": 10.0, "pnl": 30.0}, "settle_loss": {"coin": "SOL", "side": "DOWN", "entry": 85, "stop": 60, "contracts": 4, "entry_mode": "taker", "low": 80, "high": 90, "bid_lo": 70, "exit_price": 0.0, "exit_reason": "settle", "settle_result": "yes", "win": false, "per_contract": -87.0, "pnl": -348.0}, "maker_unfilled": {"coin": "XRP", "side": "DOWN", "entry": 86, "stop": 64, "contracts": 0, "entry_mode": "maker", "low": 88, "high": 92, "bid_lo": 85, "exit_price": 0.0, "exit_reason": "unfilled", "settle_result": "no", "win": null, "per_contract": 0.0, "pnl": 0.0}, "maker_filled": {"coin": "BTC", "side": "DOWN", "entry": 86, "stop": 64, "contracts": 2, "entry_mode": "maker", "low": 86, "high": 92, "bid_lo": 84, "exit_price": 100.0, "exit_reason": "settle", "settle_result": "no", "win": true, "per_contract": 12.0, "pnl": 24.0}}')
SCEN = {
    "stop_gap": ("yes", {"coin": "BTC", "side": "UP", "entry": 90, "stop": 68, "contracts": 5, "lo": 90, "hi": 95, "bid_lo": 60, "maker_filled": True, "entry_mode": "taker"}),
    "settle_win": ("yes", {"coin": "ETH", "side": "UP", "entry": 88, "stop": 66, "contracts": 3, "lo": 85, "hi": 97, "bid_lo": 80, "maker_filled": True, "entry_mode": "taker"}),
    "settle_loss": ("yes", {"coin": "SOL", "side": "DOWN", "entry": 85, "stop": 60, "contracts": 4, "lo": 80, "hi": 90, "bid_lo": 70, "maker_filled": True, "entry_mode": "taker"}),
    "maker_unfilled": ("no", {"coin": "XRP", "side": "DOWN", "entry": 86, "entry_limit": 86, "stop": 64, "contracts": 2, "lo": 88, "hi": 92, "bid_lo": 85, "maker_filled": False, "entry_mode": "maker"}),
    "maker_filled": ("no", {"coin": "BTC", "side": "DOWN", "entry": 86, "entry_limit": 86, "stop": 64, "contracts": 2, "lo": 86, "hi": 92, "bid_lo": 84, "maker_filled": True, "entry_mode": "maker"}),
}


def _settle_env():
    d = tempfile.mkdtemp(dir=fixture()["dir"])
    return t7.patched(k, CALLS_FILE=os.path.join(d, "c.json"), TRADES_CSV=os.path.join(d, "t.csv"),
                      bot_post=lambda *a, **kw: None), d


# 38 + 39
def test_call_journal_provenance():
    env, d = _settle_env()
    margs, px, closes = t7.GOLDEN_INPUTS["up_enter"]
    with env, t7.binary_inputs(t7._market(*margs), px, closes):
        r = k.evaluate("BTC", t7.CFG)
        k._call_pending.clear()
        k.log_call("BTC", r)
        cp = k._call_pending[r["ticker"]]
        assert cp["ticker"] == r["ticker"] and cp["signal_ts_epoch_ms"] == int(round(r["spot_observed_ts"] * 1000))
        cp["close"] = "2020-01-01T00:00:00Z"
        with t7.patched(k, market_result=lambda tk: "yes"):
            k.settle_calls()
        row = json.load(open(k.CALLS_FILE))[-1]
        k._call_pending.clear()
    assert row["ticker"] == r["ticker"]                                                         # 38
    assert row["signal_ts_epoch_ms"] == int(round(r["spot_observed_ts"] * 1000))                # 39
    assert abs(ap._parse_iso(row["signal_ts"]) - r["spot_observed_ts"]) < 1e-3
    crow = list(csv.DictReader(open(os.path.join(d, "t.csv"))))[-1]
    assert crow["ticker"] == r["ticker"] and crow["signal_ts"] == row["signal_ts"]


# 40-42
def test_legacy_rows_and_golden_settlement():
    env, d = _settle_env()
    with env:
        legacy = [dict(GOLD["settle_win"], ts="2026-01-01T00:00:00+00:00")]              # old row: no ticker
        json.dump(legacy, open(k.CALLS_FILE, "w"))
        assert k.call_record(legacy)["n"] == 1                                            # 40
        for name, (res, info) in SCEN.items():
            k._call_pending.clear()
            k._call_pending["TK-" + name] = dict(info, close="2020-01-01T00:00:00Z")
            with t7.patched(k, market_result=lambda tk, res=res: res):
                k.settle_calls()
            row = json.load(open(k.CALLS_FILE))[-1]
            row.pop("ts")
            assert row.pop("ticker") == "TK-" + name
            assert row.pop("signal_ts") is None and row.pop("signal_ts_epoch_ms") is None   # legacy pending entry
            assert row == GOLD[name], (name, row, GOLD[name])                                # 41 + 42
        calls_path = k.CALLS_FILE
        rows = json.load(open(calls_path))
        k._call_pending.clear()
    assert rows[0] == legacy[0] and len(rows) == 6                                          # history kept
    idx, n_legacy, n = ash.load_calls(calls_path)
    assert n_legacy == 1 and n == 6 and "TK-stop_gap" in idx


# 43
def test_trades_csv_migration():
    env, d = _settle_env()
    with env:
        old_hdr = ["ts", "coin", "side", "entry", "stop", "contracts", "low", "high", "bid_lo", "exit_price",
                   "exit_reason", "settle_result", "win", "per_contract", "pnl"]
        with open(k.TRADES_CSV, "w", newline="") as f:
            w = csv.writer(f); w.writerow(old_hdr); w.writerow(["2026-01-01", "BTC", "UP"] + ["1"] * 12)
        old_bytes = open(k.TRADES_CSV, "rb").read()
        k._append_csv(dict(GOLD["settle_win"], ts="x", ticker="T9", signal_ts="s"))
        k._append_csv(dict(GOLD["settle_loss"], ts="y", ticker="T10", signal_ts="s2"))
        cur = list(csv.reader(open(k.TRADES_CSV)))
        legacy = [n for n in os.listdir(d) if ".legacy-" in n]
    assert cur[0] == k.TRADES_CSV_COLUMNS and len(cur) == 3 and cur[1][-2] == "T9"
    assert sum(1 for line in cur if line == old_hdr) == 0                                # never mixed
    assert len(legacy) == 1 and open(os.path.join(d, legacy[0]), "rb").read() == old_bytes   # never deleted


# ═══════════════════ prospective analyzer (44-58) ═══════════════════
def gen_prospective(pol, n=600, days=10.0, block_frac=0.10, blocked_loss=0.6, allowed_loss=0.2, seed=1,
                    unavailable=0.02, pre_cutoff=0, feature_shift=0.0, direction_flip=False, unfilled=0.05,
                    contracts=3):
    """Synthetic prospective shadow journal + labels + paper calls (tests only)."""
    rng = random.Random(seed)
    thr, cut = pol["conflict_threshold"], pol["discovery_cutoff_epoch"]
    jrows, labels, calls = [], {}, []
    for i in range(n + pre_cutoff):
        pre = i < pre_cutoff
        ts = (cut - 3600 * (i + 1)) if pre else cut + 3600 + (i - pre_cutoff) * days * 86400.0 / n
        tk, coin, fav = f"PX-{i:05d}", ap.COINS[i % 4], ("UP" if (i // 4) % 2 == 0 else "DOWN")
        dec = ps.UNAVAILABLE if rng.random() < unavailable else (ps.WOULD_BLOCK if rng.random() < block_frac else ps.ALLOW)
        cs = None if dec == ps.UNAVAILABLE else (thr - 0.05 if dec == ps.WOULD_BLOCK else thr + 0.1)
        pl = blocked_loss if dec == ps.WOULD_BLOCK else allowed_loss
        if direction_flip and fav == "DOWN" and dec != ps.UNAVAILABLE:
            pl = allowed_loss if dec == ps.WOULD_BLOCK else blocked_loss
        win = rng.random() >= pl
        y = int(win) if fav == "UP" else int(not win)
        labels[tk] = {"y": y, "coin": coin, "close": ts + 300}
        jrows.append({"policy_id": pol["policy_id"], "policy_hash": pol["policy_hash"], "binary_ticker": tk,
                      "coin": coin, "fav": fav, "signal_ts_epoch_ms": str(int(ts * 1000)),
                      "shadow_decision": dec, "conflict_score": "" if cs is None else repr(cs),
                      "conflict_threshold": repr(thr),
                      "candidate_feature_value": repr(rng.gauss(feature_shift, 3.0))})
        if rng.random() < unfilled:
            calls.append({"ts": "x", "coin": coin, "ticker": tk, "signal_ts_epoch_ms": int(ts * 1000),
                          "exit_reason": "unfilled", "win": None, "per_contract": 0.0, "pnl": 0.0, "contracts": 0})
        else:
            pc = 10.0 if win else -80.0
            calls.append({"ts": "x", "coin": coin, "ticker": tk, "signal_ts_epoch_ms": int(ts * 1000),
                          "exit_reason": "settle", "win": win, "per_contract": pc, "pnl": pc * contracts,
                          "contracts": contracts})
    return jrows, labels, {c["ticker"]: c for c in calls}


def _analyze(pol, gen_kw=None, gates=None, reps=400, seed=ash.SEED, **kw):
    j, l, c = gen_prospective(pol, **(gen_kw or {}))
    return ash.analyze_policy(pol, j, l, c, gates, reps, seed, kw.get("now", pol["discovery_cutoff_epoch"] + 20 * 86400))


def _real_like_policy():
    """A frozen policy with synthetic=False-shaped content for analyzer maths (never loaded live)."""
    return make_policy(cutoff=1.9e9, discovery_cutoff_utc="2030-03-17T17:46:40+00:00")


# 44
def test_prospective_cutoff():
    pol = _real_like_policy()
    r = _analyze(pol, dict(n=300, pre_cutoff=25))
    assert r["integrity"]["pre_discovery_rows"] == 25 and "PRE_DISCOVERY_SHADOW_ROW" in r["flags"]
    assert r["shadow_total"] == 300 and r["prospective_first_ts"] > pol["discovery_cutoff_epoch"]


# 45
def test_exact_ticker_join():
    pol = _real_like_policy()
    j, l, c = gen_prospective(pol, n=40, unavailable=0.0, unfilled=0.0)
    calls = dict(c)
    near = dict(calls.pop("PX-00000"), ticker="PX-99999")          # same coin/time, different ticker
    calls["PX-99999"] = near
    r = ash.analyze_policy(pol, j, l, calls, None, 50, 1, pol["discovery_cutoff_epoch"] + 86400)
    assert r["calls_unmatched"] == 1 and r["calls_matched_filled"] == 39      # no coin/time fallback match
    d = tempfile.mkdtemp(dir=fixture()["dir"])
    p = os.path.join(d, "calls.json")
    json.dump([{"coin": "BTC", "pnl": 5, "per_contract": 5, "win": True}] + list(calls.values()), open(p, "w"))
    idx, legacy, n = ash.load_calls(p)
    assert legacy == 1 and "PX-00000" not in idx                                  # legacy never fuzzy-matched


# 46-48
def test_pnl_math_and_unfilled():
    pol = _real_like_policy()
    j = [{"policy_id": pol["policy_id"], "policy_hash": pol["policy_hash"], "binary_ticker": t, "coin": "BTC",
          "fav": "UP", "signal_ts_epoch_ms": str(int((pol["discovery_cutoff_epoch"] + 100 + i) * 1000)),
          "shadow_decision": d, "conflict_score": repr(-0.5 if d == ps.WOULD_BLOCK else 0.1),
          "conflict_threshold": repr(pol["conflict_threshold"]), "candidate_feature_value": "0"}
         for i, (t, d) in enumerate([("A", ps.ALLOW), ("B", ps.ALLOW), ("C", ps.WOULD_BLOCK), ("D", ps.WOULD_BLOCK),
                                     ("E", ps.ALLOW)])]
    labels = {t: {"y": y} for t, y in zip("ABCDE", (1, 1, 0, 0, 1))}
    def calls(mult):
        return {"A": {"ticker": "A", "exit_reason": "settle", "win": True, "per_contract": 10.0, "pnl": 10.0 * mult},
                "B": {"ticker": "B", "exit_reason": "stop", "win": False, "per_contract": -20.0, "pnl": -20.0 * mult},
                "C": {"ticker": "C", "exit_reason": "settle", "win": False, "per_contract": -80.0, "pnl": -80.0 * mult},
                "D": {"ticker": "D", "exit_reason": "stop", "win": False, "per_contract": -30.0, "pnl": -30.0 * mult},
                "E": {"ticker": "E", "exit_reason": "unfilled", "win": None, "per_contract": 0.0, "pnl": 0.0}}
    r = ash.analyze_policy(pol, j, labels, calls(1), None, 50, 1, pol["discovery_cutoff_epoch"] + 1000)
    pm = r["pnl"]
    assert r["calls_matched_unfilled_maker"] == 1 and pm["n_filled"] == 4                         # 46
    assert pm["baseline_total_pnl"] == -120.0 and pm["shadow_filtered_total_pnl"] == -10.0          # 47
    assert pm["shadow_pnl_difference"] == -10.0 - (-120.0) == 110.0
    assert pm["blocked_total_pnl"] == -110.0
    assert pm["baseline_mean_per_contract"] == -30.0 and pm["shadow_filtered_mean_per_contract"] == -2.5
    assert pm["per_contract_improvement"] == 27.5
    r5 = ash.analyze_policy(pol, j, labels, calls(5), None, 50, 1, pol["discovery_cutoff_epoch"] + 1000)
    for key in ("baseline_mean_per_contract", "shadow_filtered_mean_per_contract", "per_contract_improvement",
                "baseline_median_per_contract", "blocked_mean_per_contract"):
        assert r5["pnl"][key] == pm[key], key                                                        # 48
    assert r5["pnl"]["baseline_total_pnl"] == 5 * pm["baseline_total_pnl"]
    assert r["settlement"]["n"] == 5                                   # settlement uses all settled, incl. unfilled


# 49 + 50
def test_prospective_bootstrap_and_gate():
    pol = _real_like_policy()
    a = _analyze(pol, dict(n=300), reps=300, seed=7)
    b = _analyze(pol, dict(n=300), reps=300, seed=7)
    c = _analyze(pol, dict(n=300), reps=300, seed=8)
    assert a["loss_enrichment_ci95_pp"] == b["loss_enrichment_ci95_pp"]                        # 49
    assert a["per_contract_improvement_ci95"] == b["per_contract_improvement_ci95"]
    assert a["loss_enrichment_ci95_pp"] != c["loss_enrichment_ci95_pp"]
    small = _analyze(pol, dict(n=80, days=3))
    assert small["status"] == ash.S_COLLECTING                                                  # 50
    short = _analyze(pol, dict(n=600, days=5))
    assert short["status"] == ash.S_COLLECTING                                                  # < 7 days


# 51-54
def test_prospective_outcomes():
    pol = _real_like_policy()
    ok = _analyze(pol, dict(n=700, days=10, blocked_loss=0.65, allowed_loss=0.2))
    assert ok["status"] == ash.S_VALIDATED, (ok["status"], ok["status_reasons"], ok["flags"])       # 51
    assert ok["loss_enrichment_ci95_pp"][0] > 0 and ok["per_contract_improvement_ci95"][0] > 0
    gone = _analyze(pol, dict(n=700, days=10, blocked_loss=0.25, allowed_loss=0.25))
    assert gone["status"] == ash.S_FAILED, gone["status"]                                        # 52
    rev = _analyze(pol, dict(n=700, days=10, blocked_loss=0.05, allowed_loss=0.3))
    assert rev["status"] == ash.S_FAILED and rev["settlement"]["blocked_minus_allowed_loss_pp"] < 0   # 53
    shift = _analyze(pol, dict(n=700, days=10, block_frac=0.30))
    assert "BLOCK_RATE_SHIFT" in shift["flags"]                                                   # 54
    drift = _analyze(pol, dict(n=700, days=10, feature_shift=9.0))
    assert "FEATURE_DISTRIBUTION_SHIFT" in drift["flags"] and drift["status"] == ash.S_UNSTABLE
    stale = _analyze(pol, dict(n=100), now=pol["discovery_cutoff_epoch"] + 1e9)
    assert "POLICY_STALE" in stale["flags"]


# 55 + 56
def test_direction_and_coin_diagnostics():
    pol = _real_like_policy()
    j, l, c = gen_prospective(pol, n=800, days=10, blocked_loss=0.6, allowed_loss=0.15, direction_flip=True)
    r = ash.analyze_policy(pol, j, l, c, None, 200, 1, pol["discovery_cutoff_epoch"] + 86400 * 20)
    for side in ("UP", "DOWN"):
        rows = [x for x in j if x["fav"] == side and x["shadow_decision"] in (ps.ALLOW, ps.WOULD_BLOCK)]
        blk = [x for x in rows if x["shadow_decision"] == ps.WOULD_BLOCK]
        g = r["direction"][side]
        assert g["n_settled"] == len(rows) and g["n_blocked"] == len(blk)
        wins = sum(1 for x in rows if ash.favored_win(l[x["binary_ticker"]]["y"], side))
        assert abs(g["baseline_win_rate"] - wins / len(rows)) < 1e-12
    assert r["direction"]["UP"]["blocked_minus_allowed_loss_pp"] > 0 > r["direction"]["DOWN"]["blocked_minus_allowed_loss_pp"]
    assert "DIRECTION_INCONSISTENT" in r["flags"] and r["status"] in (ash.S_UNSTABLE, ash.S_FAILED, ash.S_COLLECTING)
    assert set(r["coins"]) == set(ap.COINS)                                                        # 56
    for coin in ap.COINS:
        n = sum(1 for x in j if x["coin"] == coin and x["shadow_decision"] in (ps.ALLOW, ps.WOULD_BLOCK))
        assert r["coins"][coin]["n_settled"] == n


# 57 + 58
def test_threshold_immutability_and_no_retuning():
    f = fixture()
    pol = f["policy"]
    j, l, c = gen_prospective(pol, n=200)
    d = tempfile.mkdtemp(dir=f["dir"])
    jp, lp, cp = os.path.join(d, "j.csv"), os.path.join(d, "l.csv"), os.path.join(d, "c.json")
    with open(jp, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=ps.JOURNAL_COLUMNS, extrasaction="ignore"); w.writeheader()
        for r in j:
            w.writerow(r)
    with open(lp, "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["binary_ticker", "coin", "binary_close_time", "outcome_up", "label_status"])
        for tk, v in l.items():
            w.writerow([tk, v["coin"], "", v["y"], "final"])
    json.dump(list(c.values()), open(cp, "w"))
    before = hashlib.sha256(open(f["policies"], "rb").read()).hexdigest()
    rep = ash.run(jp, lp, cp, f["policies"], os.path.join(d, "out"), allow_synthetic=True, reps=100, log=None)
    assert hashlib.sha256(open(f["policies"], "rb").read()).hexdigest() == before                 # 57
    r = rep["policies"][0]
    assert r["threshold_used"] == r["frozen_threshold"] == pol["conflict_threshold"]
    assert r["integrity"]["decision_mismatch"] == 0 and r["integrity"]["threshold_mismatch"] == 0
    tampered = [dict(x, conflict_threshold=repr(pol["conflict_threshold"] / 2)) if i == 3 else x for i, x in enumerate(j)]
    rt = ash.analyze_policy(pol, tampered, l, c, None, 50, 1, pol["discovery_cutoff_epoch"] + 86400)
    assert "POLICY_OR_DECISION_MUTATION" in rt["flags"] and rt["status"] == ash.S_FAILED
    assert os.path.exists(os.path.join(d, "out", "perp_shadow_report.json"))
    rep0 = ash.run(jp, lp, cp, f["policies"], os.path.join(d, "out2"), log=None)                   # live mode
    assert rep0["policies"] == [] and rep0["status"] == ash.S_NO_REAL                             # synthetic refused
    tree = ast.parse(open(ash.__file__).read())                                                  # 58
    mods = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
           {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    assert "build_perp_shadow_policy" not in mods
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | \
            {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    for forbidden in ("threshold_grid", "evaluate_thresholds", "select_threshold", "THRESHOLD_QUANTILES",
                      "fit_logistic", "fit_scalers", "final_fit", "screen_feature", "fit_ols"):
        assert forbidden not in names, forbidden
    for n in ast.walk(tree):
        if isinstance(n, (ast.Assign, ast.AugAssign)):
            for t in (n.targets if isinstance(n, ast.Assign) else [n.target]):
                if isinstance(t, ast.Subscript) and isinstance(t.slice, ast.Constant):
                    assert t.slice.value not in ("conflict_threshold", "model_c_coefficients"), "policy mutated"


# 59 + 60
def test_offline_and_no_trading_code():
    allowed = {"argparse", "csv", "datetime", "hashlib", "json", "math", "os", "random", "re", "sys", "tempfile",
               "time", "threading", "analyze_perp_predictive", "perp_shadow"}
    for mod in (bp, ash, ps):
        tree = ast.parse(open(mod.__file__).read())
        mods = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
               {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
        assert mods <= allowed, (mod.__name__, mods - allowed)
    f = fixture()
    d = tempfile.mkdtemp(dir=f["dir"])
    code = ("import sys, socket\n"
            "for m in ('requests','urllib3','numpy','pandas'): sys.modules[m]=None\n"
            "def boom(*a,**k): raise RuntimeError('network used')\n"
            "socket.socket=boom; socket.create_connection=boom\n"
            "import build_perp_shadow_policy as bp, analyze_perp_shadow as ash\n"
            f"s=bp.build({f['tele']!r},{f['labels']!r},{f['report']!r},{f['manifest']!r},{os.path.join(d, 'p.json')!r},"
            f"allow_synthetic=True,gates={TEST_GATES!r},reps=50,log=None)\n"
            f"ash.run({os.path.join(d, 'none.csv')!r},{f['labels']!r},{os.path.join(d, 'none.json')!r},"
            f"{os.path.join(d, 'p.json')!r},{os.path.join(d, 'o')!r},allow_synthetic=True,reps=20,log=None)\n"
            "print('OK', s['policies_created'])")
    p = subprocess.run([sys.executable, "-c", code], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "OK 1" in p.stdout, p.stderr[-600:]
    tree = ast.parse(open(ps.__file__).read())                                                   # 60
    called = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert not called & {"post", "put", "patch", "delete", "request", "get_json", "log_call", "paper_enter",
                         "send_alert", "bot_post", "post_embed"}, called
    src = open(ps.__file__).read()
    for word in ("kalshi_dashboard", "requests", "/orders", "/portfolio", "ON_CALL", "_call_pending"):
        assert word not in src, word
    for fn in (k.evaluate, k._size_fraction, k._contracts, k.log_call, k.settle_calls, k.build_call_embed):
        import inspect
        s = inspect.getsource(fn)
        assert "shadow" not in s.lower() and "perp" not in s.lower(), fn.__name__


def test_dashboard_diff_is_limited():
    """The only kalshi_dashboard.py changes are provenance fields, CSV migration, config and
    the passive shadow hook. Every strategy function hash is pinned in Stage 9 test 42."""
    import inspect
    src = inspect.getsource(k.poller)
    assert "_shadow" not in src and "shadow" not in src.lower()          # the poller never consults shadow
    pub = inspect.getsource(k._perp_publish)
    assert pub.strip().endswith("_shadow_process(rows)          # Step 4: observation only, on this worker thread")
    import textwrap
    tree = ast.parse(textwrap.dedent(inspect.getsource(k._shadow_process)))
    refs = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | \
           {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    for forbidden in ("log_call", "_call_pending", "_alerted", "ON_CALL", "bot_post", "evaluate", "paper_enter",
                      "COINS", "RUNNING", "ENTRY_MODE", "DIRECTION", "ACTIVE_COINS"):
        assert forbidden not in refs, forbidden
    lk = textwrap.dedent(inspect.getsource(k._shadow_first_signal))
    assert "_call_pending.get(" in lk and "=" not in lk.split("_call_pending.get(")[1].split(")")[0]   # read-only
    assert "pop(" not in lk and "[" + "ticker" + "] =" not in lk


def test_previous_stages():
    # Master mode (run_all_tests.py): every stage is already run exactly once by the runner.
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    # Standalone: re-verify earlier stages, each EXACTLY once (children run in master mode).
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 10):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-600:], p.stderr[-600:])


if __name__ == "__main__":
    try:
        run("1  empty real manifest -> 0 policies", test_empty_manifest)
        run("2+3 NO_INCREMENTAL/EXPLORATORY/UNSTABLE rejected (fail closed)", test_non_candidates_rejected)
        run("4+5 fixed-horizon NOT_CALL_FILTER_ELIGIBLE; first-signal accepted", test_fixed_horizon_and_first_signal)
        run("6+71 synthetic data refused by production builder", test_synthetic_production_guard)
        run("7+35 synthetic policy refused by live loader", test_synthetic_live_load_guard)
        run("8  deterministic policy id; frozen policy never rewritten", test_deterministic_policy_id)
        run("9+25 discovery cutoff: fit/scaler/threshold never see later data", test_discovery_cutoff)
        run("10 OOF conflict scores (not in-sample)", test_oof_conflict_scores)
        run("11-14 favored-side transform, sign, support never blocked", test_favored_side_and_sign)
        run("15 threshold grid predeclared", test_threshold_grid_predeclared)
        run("16-20 threshold gates", test_threshold_gates)
        run("21+22 threshold bootstrap CI + BH", test_threshold_bootstrap_and_bh)
        run("23+24 most-conservative selection; NO_TRAINING_THRESHOLD", test_conservative_selection_and_no_threshold)
        run("26+27 policy JSON round-trip; mutation breaks hash", test_policy_serialization_and_hash)
        run("28 first live signal only", test_first_signal_only)
        run("29+36+37 unavailable first signal never replaced", test_first_signal_unavailable_not_replaced)
        run("30 restart de-duplication", test_restart_dedup)
        run("31+32 WOULD_BLOCK cannot block the call / callback", test_would_block_cannot_block_call)
        run("33+34 strategy identical across shadow modes; corrupt policy fails open", test_strategy_identical_across_shadow_modes)
        run("38+39 call journal ticker + signal_ts", test_call_journal_provenance)
        run("40-42 legacy rows; stop/settle/maker golden unchanged", test_legacy_rows_and_golden_settlement)
        run("43 trades CSV schema migration", test_trades_csv_migration)
        run("44 prospective cutoff", test_prospective_cutoff)
        run("45 exact ticker join only", test_exact_ticker_join)
        run("46-48 unfilled maker, filtered P&L, per-contract", test_pnl_math_and_unfilled)
        run("49+50 prospective bootstrap determinism; sample gate", test_prospective_bootstrap_and_gate)
        run("51-54 validated / failed / reversed / shift", test_prospective_outcomes)
        run("55+56 direction and coin diagnostics", test_direction_and_coin_diagnostics)
        run("57+58 threshold immutability; no retuning code", test_threshold_immutability_and_no_retuning)
        run("59+60 offline tools; no trading code in perp_shadow", test_offline_and_no_trading_code)
        run("+  dashboard change is limited to provenance + passive hook", test_dashboard_diff_is_limited)
        run("61 all previous stage suites", test_previous_stages)
    finally:
        if FX.get("dir"):
            shutil.rmtree(FX["dir"], ignore_errors=True)
    print("\nAll Stage 10 tests passed.")
