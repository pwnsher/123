#!/usr/bin/env python3
"""Stage 11 tests: Step 5 validated perp probability overlay (offline, shadow-only). No network.
Run:  py test_stage11.py"""
import ast
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

import analyze_perp_integration as ai
import analyze_perp_predictive as ap
import analyze_perp_shadow as ash
import build_perp_integration_experiment as bx
import perp_probability as pp
import perp_shadow as ps
import step5_fixture as fx
import test_stage10 as t10

HERE = os.path.dirname(os.path.abspath(__file__))
K = {"MIN_CONF": 80.0, "MIN_PRICE": 85.0, "EDGE_THRESH": 0.0, "ENTRY_COST_CENTS": 2.0}
G = {"min_calendar_days": 7.0, "min_settled_total": 200, "min_tuning_settled": 100, "min_holdout_settled": 100,
     "min_holdout_filled": 80, "min_holdout_yes": 20, "min_holdout_no": 20}
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
STEP4_HASHES = {   # Step 4 output; Step 5 must leave these byte-identical
    "kalshi_dashboard.py": "3fcdea681d958fd751edcdc530114f2afc873a29941db5acf924c38c8cd51ca8",
    "perp_telemetry.py": "5e1de6d6553032bd2902d0b9766086b826876c632bb41cfcfe52234c90b4d99e",
    "perp_shadow.py": "ba3b3444e83fe384f5556050abc7d543d9c06fbc147a51319a314dddf12794c3",
    "kalshi_bot.py": "9e064548c21144b00226920e21d114407ff306386fd740272cbd889d7d896d53",
    "kalshi_backtest.py": "718968348cacd89e07740414ac9567db6a22ba3b3dcd7d8d70cd4c0a0ef631f8",
    "kalshi_api_learn.py": "7b7bc2dd095cabe391549208a849c612a5465aac9139e8f8e841216cc96a26f6",
    "build_perp_shadow_policy.py": "8003371e9a0dbf657c5b1db5888cbd6e285172a3fab45389385d062c03ea0e38",
    "analyze_perp_shadow.py": "5aa0ae764d417ed5e3118faff78dda747b492695936efeff3d81a1476534d87b",
}
CH = {}


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


# ═══════════════════ synthetic Step 4 -> Step 5 chain (tests only) ═══════════════════
def _write_journal(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ps.JOURNAL_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _write_labels(path, labels):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["binary_ticker", "coin", "binary_close_time", "outcome_up", "label_status"])
        for tk, v in labels.items():
            w.writerow([tk, v["coin"], "", v["y"], "final"])


def chain(step5_kw=None, seed=1):
    """Synthetic policy -> Step 4 journal -> real Step 4 analyzer (VALIDATED) -> Step 5 rows appended."""
    d = tempfile.mkdtemp(prefix="_t11")
    pol = t10.make_policy(cutoff=1.9e9)
    reg = os.path.join(d, "policies.json")
    json.dump({"policy_schema_version": 1, "policies": [pol]}, open(reg, "w"))
    j4, l4, c4 = t10.gen_prospective(pol, n=700, days=10, blocked_loss=0.65, allowed_loss=0.2, seed=seed)
    jp, lp, cp = os.path.join(d, "shadow.csv"), os.path.join(d, "labels.csv"), os.path.join(d, "calls.json")
    _write_journal(jp, j4); _write_labels(lp, l4); json.dump(list(c4.values()), open(cp, "w"))
    rep4 = ash.run(jp, lp, cp, reg, os.path.join(d, "out"), allow_synthetic=True, reps=200, log=None)
    res4 = rep4["policies"][0]
    assert res4["status"] == ash.S_VALIDATED, res4["status_reasons"]
    last = res4["prospective_last_ts"]
    kw5 = dict({"seed": seed}, **(step5_kw or {}))
    j5, l5, c5 = fx.gen_rows(pol, last, **kw5)
    _write_journal(jp, j4 + j5); _write_labels(lp, dict(l4, **l5)); json.dump(list(c4.values()) + list(c5.values()), open(cp, "w"))
    return {"dir": d, "pol": pol, "reg": reg, "journal": jp, "labels": lp, "calls": cp,
            "report": os.path.join(d, "out", "perp_shadow_report.json"), "res4": res4, "j4": j4, "j5": j5,
            "exp_path": os.path.join(d, "experiments.json"), "l5": l5, "c5": c5}


def base_chain():
    if not CH:
        CH.update(chain())
    return CH


def build(c, **kw):
    return bx.build(c["reg"], c["report"], c["journal"], os.path.join(HERE, "kalshi_dashboard.py"),
                    kw.pop("out", c["exp_path"]), log=kw.pop("log", None), **kw)


def the_experiment(c):
    if not os.path.exists(c["exp_path"]):
        build(c, allow_synthetic=True)
    return json.load(open(c["exp_path"]))["experiments"][0]


# ═══════════════════ 1-9 eligibility / builder ═══════════════════
def test_no_policies():
    d = tempfile.mkdtemp()
    lines = []
    s = bx.build(os.path.join(d, "none.json"), os.path.join(d, "none_report.json"), os.path.join(d, "j.csv"),
                 os.path.join(HERE, "kalshi_dashboard.py"), os.path.join(d, "e.json"), log=lines.append)
    assert s["experiments_created"] == 0 and json.load(open(os.path.join(d, "e.json")))["experiments"] == []
    txt = "\n".join(lines)
    assert "Step 4 validated candidates: 0" in txt and "No real SHADOW_VALIDATED_CANDIDATE policy exists." in txt
    json.dump({"policy_schema_version": 1, "policies": []}, open(os.path.join(d, "p.json"), "w"))
    p = subprocess.run([sys.executable, "build_perp_integration_experiment.py", "--policies", os.path.join(d, "p.json"),
                        "--shadow-report", os.path.join(d, "x.json"), "--output", os.path.join(d, "e2.json")],
                       cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "Experiments created: 0" in p.stdout, p.stderr


def _with_status(c, status):
    rep = json.load(open(c["report"]))
    rep["policies"][0]["status"] = status
    p = os.path.join(c["dir"], f"rep_{status}.json")
    json.dump(rep, open(p, "w"))
    return p


def test_nonvalidated_rejected():
    c = base_chain()
    for st in (ash.S_COLLECTING, ash.S_FAILED, ash.S_UNSTABLE):                 # 2, 3, 4
        out = os.path.join(c["dir"], f"e_{st}.json")
        s = bx.build(c["reg"], _with_status(c, st), c["journal"], os.path.join(HERE, "kalshi_dashboard.py"), out,
                     allow_synthetic=True, log=None)
        assert s["experiments_created"] == 0 and json.load(open(out))["experiments"] == [], st
        assert s["entries"][0]["status"] == bx.ST_NOT_VALIDATED


def test_validated_accepted_and_synthetic_refused():
    c = base_chain()
    out = os.path.join(c["dir"], "prod.json")
    s = bx.build(c["reg"], c["report"], c["journal"], os.path.join(HERE, "kalshi_dashboard.py"), out, log=None)
    assert s["experiments_created"] == 0 and s["entries"][0]["status"] == bx.ST_SYNTHETIC      # 6
    e = the_experiment(c)                                                                         # 5
    assert e["synthetic"] is True and e["source_policy_id"] == c["pol"]["policy_id"]
    assert e["strategy_constants"] == K and e["dashboard_sha256"] == STEP4_HASHES["kalshi_dashboard.py"]
    assert e["source_hashes"]["step4_policy_hash"] == c["pol"]["policy_hash"]
    assert not any("activation" in k or "enabled" in k for k in e)
    assert ai.validate_experiment(e) == ["synthetic/test experiment refused"]
    rep = ai.run(c["exp_path"], c["journal"], c["labels"], c["calls"], c["reg"], c["report"],
                 os.path.join(c["dir"], "o_prod"), log=None)
    assert rep["step5_experiments"] == 0 and rep["experiment_errors"]                        # production refuses


def test_mismatch_fails_closed():
    c = base_chain()
    dash = os.path.join(HERE, "kalshi_dashboard.py")
    rep = json.load(open(c["report"]))
    rep["policies"][0]["frozen_threshold"] = rep["policies"][0]["frozen_threshold"] / 2
    p = os.path.join(c["dir"], "rep_thr.json"); json.dump(rep, open(p, "w"))
    for bad_rep, bad_reg, bad_journal in ((p, c["reg"], c["journal"]), (c["report"], None, c["journal"]),
                                          (c["report"], c["reg"], None)):
        reg = c["reg"]
        if bad_reg is None:
            pol = dict(c["pol"], model_c_coefficients=[x + 0.1 for x in c["pol"]["model_c_coefficients"]])
            reg = os.path.join(c["dir"], "reg_tampered.json")
            json.dump({"policy_schema_version": 1, "policies": [pol]}, open(reg, "w"))
        jp = bad_journal
        if bad_journal is None:
            rows = list(csv.DictReader(open(c["journal"])))
            rows[3]["policy_hash"] = "0" * 64
            jp = os.path.join(c["dir"], "j_tampered.csv"); _write_journal(jp, rows)
        out = os.path.join(c["dir"], "never.json")
        try:
            bx.build(reg, bad_rep, jp, dash, out, allow_synthetic=True, log=None)
            raise AssertionError("mismatch accepted")
        except bx.BuildError:
            pass
        assert not os.path.exists(out)


def test_experiment_id_and_cutoff():
    c = base_chain()
    e = the_experiment(c)
    s = build(c, allow_synthetic=True)
    assert s["entries"][0]["status"] == bx.ST_ALREADY and s["entries"][0]["experiment_id"] == e["experiment_id"]
    assert bx.make_experiment_id(c["pol"], e["source_hashes"]["shadow_report"], e["step5_start_cutoff"]) == e["experiment_id"]
    assert bx.make_experiment_id(c["pol"], "f" * 64, e["step5_start_cutoff"]) != e["experiment_id"]        # 8
    last4 = max(int(r["signal_ts_epoch_ms"]) for r in c["j4"]) / 1000.0
    assert e["step5_start_cutoff"] == c["res4"]["prospective_last_ts"] == last4                           # 9
    assert bx.compute_experiment_hash(e) == e["experiment_hash"]
    assert ai.validate_experiment(dict(e, step5_start_cutoff=0.0), allow_synthetic=True)                  # tamper detected


def test_strategy_constants_extraction():
    assert bx.extract_strategy_constants(os.path.join(HERE, "kalshi_dashboard.py")) == K
    d = tempfile.mkdtemp()
    for src in ("MIN_CONF = 80.0\nMIN_PRICE = 85.0\nEDGE_THRESH = 0.0\n",                              # missing
                "MIN_CONF = float('80')\nMIN_PRICE = 85.0\nEDGE_THRESH = 0.0\nENTRY_COST_CENTS = 2.0\n",  # not literal
                "MIN_CONF = 80.0\nMIN_PRICE = 85.0\nEDGE_THRESH = 0.0\nENTRY_COST_CENTS = 2.0\n"
                "def f():\n    global MIN_CONF\n    MIN_CONF = 70\n",                                     # runtime change
                "MIN_CONF = 80.0\nMIN_CONF = 81.0\nMIN_PRICE = 85.0\nEDGE_THRESH = 0.0\nENTRY_COST_CENTS = 2.0\n"):
        p = os.path.join(d, "dash.py"); open(p, "w").write(src)
        try:
            bx.extract_strategy_constants(p); raise AssertionError(f"accepted: {src!r}")
        except bx.BuildError:
            pass


# ═══════════════════ 11-27 pure overlay math ═══════════════════
def test_overlay_math():
    assert abs(pp.perp_delta(0.64, 0.60) - 0.04) < 1e-15                                     # 11
    assert abs(pp.perp_delta(0.57, 0.60) + 0.03) < 1e-15                                     # 12
    assert pp.cap_delta(0.12) == 0.05 and pp.cap_delta(-0.13) == -0.05                      # 13, 14
    assert pp.cap_delta(0.03) == 0.03
    assert abs(pp.blend(0.60, 0.04, 0.25) - 0.61) < 1e-15                                    # 15
    assert abs(pp.blend(0.60, 0.04, 1.0) - 0.64) < 1e-15                                     # 16
    o = pp.overlay(0.60, 0.62, 0.66, 0.25, "UP", 50.0, K)
    assert abs(o["integrated_p_up"] - 0.61) < 1e-15 and o["delta_raw"] == 0.66 - 0.62        # base kept, only C-B added
    o2 = pp.overlay(0.60, 0.50, 0.70, 1.0, "UP", 50.0, K)
    assert abs(o2["integrated_p_up"] - 0.65) < 1e-15 and o2["cap_hit"] is True               # capped, not P_C
    for p in (pp.blend(0.999999, 0.05, 1.0), pp.blend(1e-9, -0.05, 1.0), pp.clamp_p(2.0), pp.clamp_p(-1.0)):
        assert 0.0 < p < 1.0                                                                 # 17
    assert pp.favored(0.7, "UP") == 0.7 and abs(pp.favored(0.7, "DOWN") - 0.3) < 1e-15       # 18, 19
    d = pp.decide(0.49, "UP", 85.0, K)
    assert d["decision"] == pp.BLOCK_FLIP and abs(d["integrated_conf"] - 49.0) < 1e-9        # 20: never 51 for DOWN
    d = pp.decide(0.50, "DOWN", 85.0, K)
    assert d["decision"] == pp.BLOCK_FLIP                                                    # 21 (legacy fav_up: p >= 50)
    assert pp.direction_flips(0.50, "UP") is False and pp.direction_flips(0.4999, "DOWN") is False
    d = pp.decide(0.93, "UP", 88.0, K)
    assert abs(d["integrated_conf"] - 93.0) < 1e-9                                           # 22
    assert abs(d["integrated_raw_edge"] - 5.0) < 1e-9                                        # 23: conf - ask
    assert abs(d["integrated_net_edge"] - 3.0) < 1e-9                                        # 24: - ENTRY_COST_CENTS
    d = pp.decide(0.08, "DOWN", 88.0, K)
    assert abs(d["integrated_conf"] - 92.0) < 1e-9 and d["decision"] == pp.WOULD_ALLOW       # 27 (DOWN side)
    assert pp.decide(0.79, "UP", 70.0, K)["decision"] == pp.BLOCK_LOW_CONF                   # 25
    assert pp.decide(0.90, "UP", 88.5, K)["decision"] == pp.BLOCK_NO_EDGE                    # 26 (net = -0.5)
    assert pp.decide(0.90, "UP", 88.0, K)["decision"] == pp.BLOCK_NO_EDGE                    # net == 0 is not > 0
    assert pp.decide(0.90, "UP", 87.9, K)["decision"] == pp.WOULD_ALLOW                      # 27
    k2 = dict(K, EDGE_THRESH=1.0)
    assert pp.decide(0.90, "UP", 87.5, k2)["decision"] == pp.BLOCK_NO_EDGE                   # net 0.5 < EDGE_THRESH


def test_matches_legacy_evaluate_formula():
    """The overlay at alpha=0 reproduces evaluate()'s own conf/raw/net edge exactly."""
    import test_stage7 as t7
    import kalshi_dashboard as k
    for name, (margs, px, closes) in t7.GOLDEN_INPUTS.items():
        with t7.binary_inputs(t7._market(*margs), px, closes):
            r = k.evaluate("BTC", t7.CFG)
        p_up = r["p_up"] / 100.0
        d = pp.decide(p_up, r["fav"], r["side_ask"], K)
        assert abs(round(d["integrated_conf"], 1) - r["conf"]) <= 0.05, name
        assert abs(round(d["integrated_raw_edge"], 1) - r["raw_edge"]) <= 0.051, name
        assert abs(round(d["integrated_net_edge"], 1) - r["net_edge"]) <= 0.051, name
        if r["signal"]:
            assert d["decision"] == pp.WOULD_ALLOW, name


def test_no_new_calls_and_grid():
    for p in (0.99, 0.5, 0.01):
        d = pp.decide(p, "UP", 50.0, K, legacy_signal=False)
        assert d["decision"] == pp.NO_NEW_CALL and d["integrated_p_up"] is None             # 62
        assert pp.overlay(0.6, 0.6, 0.9, 1.0, "UP", 50.0, K, legacy_signal=False)["decision"] == pp.NO_NEW_CALL
    assert pp.ALPHA_GRID == (0.25, 0.50, 0.75, 1.00)                                         # 37
    for bad in (0.1, 0.3, 1.25, 2.0, -0.25):
        try:
            pp.check_alpha(bad); raise AssertionError(f"alpha {bad} accepted")
        except ValueError:
            pass
    assert pp.check_alpha(0.0) == 0.0 and "WOULD_ENTER" not in open(pp.__file__).read()


# ═══════════════════ 28-36 universe, joins, split ═══════════════════
def _exp(pol, cutoff=2.0e9):
    return {"source_policy_id": pol["policy_id"], "source_policy_hash": pol["policy_hash"],
            "frozen_threshold": pol["conflict_threshold"], "step5_start_cutoff": cutoff, "strategy_constants": K,
            "coin_or_group": "ALL", "feature_name": "causal_premium_bps", "experiment_id": "TEST", "synthetic": True,
            "step5_start_cutoff_utc": "x"}


def test_universe_joins_and_split():
    pol = t10.make_policy(cutoff=1.9e9)
    rows, labels, calls = fx.gen_rows(pol, 2.0e9, n_groups=120, seed=4, step4_block_frac=0.1)
    pre, pl, pc = fx.gen_rows(pol, 2.0e9 - 5 * 86400, n_groups=10, days=2, seed=5)
    for r in pre:
        r["binary_ticker"] = "PRE-" + r["binary_ticker"]
    labels.update({"PRE-" + k2: v for k2, v in pl.items()})
    dup = dict(rows[7], signal_ts_epoch_ms=str(int(rows[7]["signal_ts_epoch_ms"]) + 30000))
    kept, counts, allrows = ai.prepare_rows(_exp(pol), pre + rows + [dup], labels, calls)
    tks = [r["ticker"] for r in kept]
    assert counts["pre_step5_cutoff"] == len(pre) and not any(t.startswith("PRE-") for t in tks)     # 10
    assert all(r["ts"] > 2.0e9 for r in kept)
    blocked = {r["binary_ticker"] for r in rows if r["shadow_decision"] == ps.WOULD_BLOCK}
    assert blocked and not blocked & set(tks) and counts["step4_would_block"] == len(blocked)          # 28
    allowed = {r["binary_ticker"] for r in rows if r["shadow_decision"] == ps.ALLOW}
    assert set(tks) == allowed and len(tks) == len(set(tks)) and counts["duplicate"] == 1              # 29, 30
    for r in kept:
        assert r["y"] == labels[r["ticker"]]["y"]                                                     # 31
    unf = {t for t, cc in calls.items() if cc["exit_reason"] == "unfilled"} & allowed
    assert unf and all(not r["filled"] for r in kept if r["ticker"] in unf)                            # 33
    assert counts["unfilled_maker"] == len(unf | ({t for t, cc in calls.items() if cc["exit_reason"] == "unfilled"} & blocked))
    near = dict(calls)
    victim = kept[0]["ticker"]
    moved = near.pop(victim)
    near["OTHER-" + victim] = dict(moved, ticker="OTHER-" + victim)                                  # same coin/time
    k2, c2, _ = ai.prepare_rows(_exp(pol), rows, labels, near)
    assert next(r for r in k2 if r["ticker"] == victim)["filled"] is False and c2["call_unmatched"] == 1   # 32
    tun, hold = ai.split_tuning_holdout(kept)
    assert max(r["close"] for r in tun) < min(r["close"] for r in hold)                               # 34
    assert not {r["close"] for r in tun} & {r["close"] for r in hold}                                 # 35
    assert not {r["ticker"] for r in tun} & {r["ticker"] for r in hold}                               # 36
    assert abs(len(tun) - len(hold)) <= 4


def test_integrity_checks():
    pol = t10.make_policy(cutoff=1.9e9)
    rows, labels, calls = fx.gen_rows(pol, 2.0e9, n_groups=30, seed=6)
    bad = [dict(r) for r in rows]
    victim = next(r for r in bad if r["shadow_decision"] == ps.ALLOW)
    victim["model_c_p_favored"] = repr(float(victim["model_c_p_favored"]) + 0.01)
    kept, counts, _ = ai.prepare_rows(_exp(pol), bad, labels, calls)
    assert counts["probability_integrity_error"] == 1 and victim["binary_ticker"] not in {r["ticker"] for r in kept}
    bad2 = [dict(r) for r in rows]
    bad2[0]["policy_hash"] = "0" * 64
    try:
        ai.prepare_rows(_exp(pol), bad2, labels, calls); raise AssertionError("policy mutation accepted")
    except ai.IntegrityError:
        pass
    r = ai.analyze_experiment(_exp(pol), bad2, labels, calls, G, 50, 1)
    assert r["status"] == ai.S_FAILED and "POLICY_INTEGRITY_FAILURE" in r["flags"]
    bad3 = [dict(x) for x in rows]
    v3 = next(x for x in bad3 if x["shadow_decision"] == ps.ALLOW)
    v3["net_edge"] = repr(float(v3["net_edge"]) + 1.0)
    assert ai.prepare_rows(_exp(pol), bad3, labels, calls)[1]["legacy_edge_mismatch"] == 1


# ═══════════════════ 38-45 alpha tuning ═══════════════════
def _m(**kw):
    m = {"brier_improvement": 0.001, "logloss_improvement": 0.002, "pc_improvement": 0.1, "retention_fraction": 0.95,
         "additional_block_fraction": 0.05, "numeric_ok": True}
    m.update(kw)
    return m


def test_alpha_gates_and_selection():
    ok = ai.tuning_reasons(_m(), 99.0, ai.DEFAULT_GATES)
    assert ok == []
    assert "Brier not improved" in ai.tuning_reasons(_m(brier_improvement=-1e-5), 99, ai.DEFAULT_GATES)        # 38
    assert "log loss not improved" in ai.tuning_reasons(_m(logloss_improvement=0.0), 99, ai.DEFAULT_GATES)     # 39
    assert "filtered per-contract P&L worse than legacy" in ai.tuning_reasons(_m(pc_improvement=-0.01), 99, ai.DEFAULT_GATES)  # 40
    assert ai.tuning_reasons(_m(pc_improvement=0.0), 99, ai.DEFAULT_GATES) == []                                 # >= legacy ok
    assert "additional block fraction above 10%" in ai.tuning_reasons(_m(additional_block_fraction=0.11), 99, ai.DEFAULT_GATES)  # 41
    assert "retention below 90%" in ai.tuning_reasons(_m(retention_fraction=0.89), 99, ai.DEFAULT_GATES)        # 42
    assert "coverage below minimum" in ai.tuning_reasons(_m(), 94.9, ai.DEFAULT_GATES)
    assert "impossible numeric values" in ai.tuning_reasons(_m(numeric_ok=False), 99, ai.DEFAULT_GATES)
    scr = {0.25: {"qualifies": True, "brier_improvement": 0.001}, 0.5: {"qualifies": True, "brier_improvement": 0.01},
           0.75: {"qualifies": False}, 1.0: {"qualifies": True, "brier_improvement": 0.05}}
    assert ai.select_alpha(scr) == 0.25                                                                          # 43
    assert ai.select_alpha({a: {"qualifies": a == 0.75} for a in pp.ALPHA_GRID}) == 0.75
    assert ai.select_alpha({a: {"qualifies": False} for a in pp.ALPHA_GRID}) is None
    pol = t10.make_policy(cutoff=1.9e9)
    rows, labels, calls = fx.gen_rows(pol, 2.0e9, seed=1, k=-3.0)
    r = ai.analyze_experiment(_exp(pol), rows, labels, calls, G, 100, 1)
    assert r["status"] == ai.S_NO_ALPHA and r["selected_alpha"] is None and "holdout" not in r                 # 44


def test_holdout_cannot_choose_alpha():
    pol = t10.make_policy(cutoff=1.9e9)
    rows, labels, calls = fx.gen_rows(pol, 2.0e9, seed=2, k_fn=lambda gi, c, f: -3.0 if gi < 225 else 12.0)
    r = ai.analyze_experiment(_exp(pol), rows, labels, calls, G, 100, 1)
    assert r["selected_alpha"] is None and r["status"] == ai.S_NO_ALPHA, (r["selected_alpha"], r["status"])   # 45
    assert set(r["screen"]) == set(pp.ALPHA_GRID) and all(s["n"] == r["tuning_n"] for s in r["screen"].values())


# ═══════════════════ 46-55 holdout validation ═══════════════════
def test_metrics_and_bootstrap():
    p, y = [0.9, 0.2, 0.6], [1, 0, 0]
    assert abs(ap.brier(p, y) - (0.01 + 0.04 + 0.36) / 3) < 1e-15                                               # 46
    assert abs(ap.logloss(p, y) + (math.log(0.9) + math.log(0.8) + math.log(0.4)) / 3) < 1e-15                   # 47
    pol = t10.make_policy(cutoff=1.9e9)
    rows, labels, calls = fx.gen_rows(pol, 2.0e9, seed=1)
    a = ai.analyze_experiment(_exp(pol), rows, labels, calls, G, 300, 11)
    b = ai.analyze_experiment(_exp(pol), rows, labels, calls, G, 300, 11)
    c = ai.analyze_experiment(_exp(pol), rows, labels, calls, G, 300, 12)
    for key in ("brier_ci95", "logloss_ci95", "pc_ci95"):
        assert a["holdout"][key] == b["holdout"][key] and a["holdout"][key] != c["holdout"][key]              # 48
    h = a["holdout"]
    assert h["brier_ci95"][0] > 0 and h["logloss_ci95"][0] > 0 and h["pc_ci95"][0] > 0                         # 49-51
    tun, hold = ai.split_tuning_holdout(ai.prepare_rows(_exp(pol), rows, labels, calls)[0])
    sc = ai.score_rows(hold, a["selected_alpha"], K)
    filled = [r for r in sc if r["filled"]]
    exp_pc = sum(r["pc_pnl"] for r in filled if r["o"]["decision"] == pp.WOULD_ALLOW) / len(filled) - \
        sum(r["pc_pnl"] for r in filled) / len(filled)
    assert abs(h["pc_improvement"] - exp_pc) < 1e-9 and h["n_filled"] == len(filled)
    few = ai.analyze_experiment(_exp(pol), rows[:200], labels, calls, G, 50, 1)
    assert few["status"] == ai.S_COLLECTING and "holdout" not in few                                            # 52


def test_controls():
    pol = t10.make_policy(cutoff=1.9e9)
    run_ = lambda **kw: ai.analyze_experiment(_exp(pol), *fx.gen_rows(pol, 2.0e9, **kw), G, 300, 3)
    pos = run_(seed=1)
    assert pos["status"] == ai.S_VALIDATED and pos["selected_alpha"] == 0.25, (pos["status"], pos.get("status_reasons"))  # 53
    gone = run_(seed=3, k_fn=lambda gi, c, f: 3.0 if gi < 225 else 0.0)
    assert gone["selected_alpha"] is not None and gone["status"] == ai.S_FAILED                                  # 54
    rev = run_(seed=1, k_fn=lambda gi, c, f: 3.0 if gi < 225 else -3.0)
    assert rev["status"] == ai.S_FAILED and rev["holdout"]["brier_improvement"] < 0                              # 55
    return pos


def test_diagnostics_flags():
    pol = t10.make_policy(cutoff=1.9e9)
    run_ = lambda fn, seed=1: ai.analyze_experiment(_exp(pol), *fx.gen_rows(pol, 2.0e9, seed=seed, k_fn=fn,
                                                                             n_groups=900, days=30), G, 100, 3)
    dr = run_(lambda gi, c, f: 3.0 if (gi < 450 or f == "UP") else -3.0)
    assert "DIRECTION_INCONSISTENT" in dr["flags"], dr["flags"]                                                  # 56
    assert set(dr["holdout"]["direction"]) == {"UP", "DOWN"}
    assert dr["holdout"]["direction"]["UP"]["brier_improvement"] > 0 > dr["holdout"]["direction"]["DOWN"]["brier_improvement"]
    co = run_(lambda gi, c, f: 3.0 if (gi < 450 or c != "XRP") else -6.0)
    assert "COIN_INCONSISTENT" in co["flags"] and set(co["holdout"]["coins"]) == {"BTC", "ETH", "SOL", "XRP"}     # 57
    tm = run_(lambda gi, c, f: 3.0 if gi < 675 else -3.0)
    assert "TIME_INCONSISTENT" in tm["flags"]                                                                    # 58
    assert tm["holdout"]["time_halves"]["first_half"]["brier_improvement"] > 0 > tm["holdout"]["time_halves"]["second_half"]["brier_improvement"]
    for r in (dr, co, tm):
        assert r["status"] != ai.S_VALIDATED


def test_delta_cap_hit_rate():
    pol = t10.make_policy(cutoff=1.9e9)
    rows, labels, calls = fx.gen_rows(pol, 2.0e9, seed=1)
    r = ai.analyze_experiment(_exp(pol), rows, labels, calls, G, 50, 3)
    hold = ai.split_tuning_holdout(ai.prepare_rows(_exp(pol), rows, labels, calls)[0])[1]
    exp_rate = sum(1 for x in hold if abs(x["pc"] - x["pb"]) > 0.05) / len(hold)
    assert abs(r["holdout"]["delta_cap_hit_rate"] - exp_rate) < 1e-12                                          # 59
    dd = r["holdout"]["delta_distribution"]
    assert dd["delta_capped"]["max"] <= 0.05 and dd["delta_capped"]["min"] >= -0.05
    assert abs(dd["applied_delta"]["max"] - 0.25 * dd["delta_capped"]["max"]) < 1e-12
    big = ai.analyze_experiment(_exp(pol), *fx.gen_rows(pol, 2.0e9, seed=1, delta_sd=0.2), G, 50, 3)
    if big.get("holdout"):
        assert "DELTA_CAP_FREQUENT" in big["flags"]


# ═══════════════════ 60-61 candidate artifact (end to end) ═══════════════════
def test_candidate_artifact():
    c = base_chain()
    the_experiment(c)
    out = os.path.join(c["dir"], "out5")
    rep = ai.run(c["exp_path"], c["journal"], c["labels"], c["calls"], c["reg"], c["report"], out,
                 allow_synthetic=True, gates=G, reps=300, log=None)
    r = rep["experiments"][0]
    assert r["status"] == ai.S_VALIDATED, (r["status"], r.get("status_reasons"))
    cand = json.load(open(os.path.join(out, "perp_integration_candidate.json")))["candidates"]
    assert len(cand) == 1 and cand[0]["activation_allowed"] is False                                          # 61
    assert cand[0]["mode"] == "VALIDATED_CANDIDATE_ONLY" and cand[0]["selected_alpha"] == 0.25
    assert cand[0]["synthetic"] is True and cand[0]["step5_start_cutoff"] == c["res4"]["prospective_last_ts"]
    hdr = next(csv.reader(open(os.path.join(out, "perp_integration_alpha_screen.csv"))))
    assert hdr == ai.SCREEN_COLUMNS
    assert json.load(open(os.path.join(out, "perp_integration_report.json")))["probability_overlay_candidates"] == 1
    c2 = chain(step5_kw={"seed": 3, "k_fn": lambda gi, cc, f: 3.0 if gi < 225 else 0.0}, seed=2)          # 60
    bx.build(c2["reg"], c2["report"], c2["journal"], os.path.join(HERE, "kalshi_dashboard.py"), c2["exp_path"],
             allow_synthetic=True, log=None)
    out2 = os.path.join(c2["dir"], "out5")
    rep2 = ai.run(c2["exp_path"], c2["journal"], c2["labels"], c2["calls"], c2["reg"], c2["report"], out2,
                  allow_synthetic=True, gates=G, reps=300, log=None)
    assert rep2["experiments"][0]["status"] != ai.S_VALIDATED
    assert json.load(open(os.path.join(out2, "perp_integration_candidate.json")))["candidates"] == []
    shutil.rmtree(c2["dir"], ignore_errors=True)
    lines = []
    ai.run(os.path.join(out, "nope.json"), c["journal"], c["labels"], c["calls"], c["reg"], c["report"],
           os.path.join(out, "empty"), log=lines.append)
    txt = "\n".join(lines)
    assert "Step 5 experiments: 0" in txt and "Step 5 integration remains inactive." in txt
    assert json.load(open(os.path.join(out, "empty", "perp_integration_candidate.json"))) == \
        {"candidate_schema_version": 1, "candidates": []}


# ═══════════════════ 63-66 static safety ═══════════════════
STEP5_MODULES = ("perp_probability.py", "build_perp_integration_experiment.py", "analyze_perp_integration.py")


def _tokens(path):
    tree = ast.parse(open(path).read())
    doc = {id(n.body[0].value) for n in ast.walk(tree)
           if isinstance(n, (ast.Module, ast.FunctionDef, ast.ClassDef)) and n.body and isinstance(n.body[0], ast.Expr)}
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | \
            {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | \
            {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)} | \
            {n.arg for n in ast.walk(tree) if isinstance(n, ast.arg)}
    lits = {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in doc}
    mods = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
           {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    return names, lits, mods


def test_static_safety():
    for f in STEP5_MODULES:
        names, lits, mods = _tokens(os.path.join(HERE, f))
        low = {x.lower() for x in names | lits}
        for bad in ("_size_fraction", "_contracts", "kelly", "bankroll", "contracts", "size_fraction",   # 63
                    "rec_stop", "stop", "sl_pct", "stop_price", "new_stop",                           # 64
                    "log_call", "settle_calls", "paper_enter", "bot_post", "on_call", "entry_mode", "would_enter"):
            assert not any(bad == t or t.startswith(bad + "_") or t.endswith("_" + bad) for t in low), (f, bad)
        assert mods <= {"argparse", "ast", "csv", "datetime", "hashlib", "json", "math", "os", "sys", "tempfile",
                        "time", "analyze_perp_predictive", "perp_probability", "perp_shadow",
                        "build_perp_integration_experiment"}, (f, mods)                               # 65 static
    _, _, mods = _tokens(os.path.join(HERE, "perp_probability.py"))
    assert mods == {"math"}                                                                          # pure module
    o = pp.overlay(0.9, 0.9, 0.95, 0.5, "UP", 88.0, K)
    assert set(o) == {"decision", "integrated_p_up", "integrated_p_favored", "integrated_conf", "integrated_raw_edge",
                      "integrated_net_edge", "alpha", "p_base_up", "delta_raw", "delta_capped", "applied_delta", "cap_hit"}


def test_offline_runtime():
    c = base_chain()
    the_experiment(c)
    d = tempfile.mkdtemp(dir=c["dir"])
    code = ("import sys, socket\n"
            "for m in ('requests','urllib3','numpy','pandas'): sys.modules[m]=None\n"
            "def boom(*a,**k): raise RuntimeError('network used')\n"
            "socket.socket=boom; socket.create_connection=boom\n"
            "import build_perp_integration_experiment as bx, analyze_perp_integration as ai\n"
            f"bx.build({c['reg']!r},{c['report']!r},{c['journal']!r},'kalshi_dashboard.py',{os.path.join(d, 'e.json')!r},"
            "allow_synthetic=True,log=None)\n"
            f"r=ai.run({os.path.join(d, 'e.json')!r},{c['journal']!r},{c['labels']!r},{c['calls']!r},{c['reg']!r},"
            f"{c['report']!r},{os.path.join(d, 'o')!r},allow_synthetic=True,gates={G!r},reps=50,log=None)\n"
            "print('OK', r['step5_experiments'])")
    p = subprocess.run([sys.executable, "-c", code], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "OK 1" in p.stdout, p.stderr[-600:]                              # 65 runtime


def test_live_files_unchanged():
    for f, h in STEP4_HASHES.items():
        assert hashlib.sha256(open(os.path.join(HERE, f), "rb").read()).hexdigest() == h, f     # 66


def test_previous_stages():
    # Master mode (run_all_tests.py): every stage is already run exactly once by the runner.
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    # Standalone: re-verify earlier stages, each EXACTLY once (children run in master mode).
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 11):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-600:], p.stderr[-600:])


if __name__ == "__main__":
    try:
        run("1  no Step 4 policies -> 0 experiments", test_no_policies)
        run("2-4 COLLECTING/FAILED/UNSTABLE rejected", test_nonvalidated_rejected)
        run("5+6 validated accepted (test flag); synthetic refused normally", test_validated_accepted_and_synthetic_refused)
        run("7  policy/report/journal mismatch fails closed", test_mismatch_fails_closed)
        run("8+9 deterministic experiment id; Step 5 cutoff", test_experiment_id_and_cutoff)
        run("+  strategy constants read safely (AST, fail closed)", test_strategy_constants_extraction)
        run("11-27 overlay math, flips, conf/edge, decisions", test_overlay_math)
        run("+  overlay reproduces evaluate() conf/edge exactly", test_matches_legacy_evaluate_formula)
        run("37+62 fixed alpha grid; no new calls", test_no_new_calls_and_grid)
        run("10+28-36 cutoff, universe, joins, split", test_universe_joins_and_split)
        run("+  policy/probability/legacy integrity", test_integrity_checks)
        run("38-44 alpha gates; smallest qualifying; none", test_alpha_gates_and_selection)
        run("45 holdout cannot choose alpha", test_holdout_cannot_choose_alpha)
        run("46-52 Brier/logloss, bootstrap, CIs, sample gate", test_metrics_and_bootstrap)
        run("53-55 positive / vanishing / reversed controls", test_controls)
        run("56-58 direction / coin / time inconsistency", test_diagnostics_flags)
        run("59 delta cap-hit rate + distribution", test_delta_cap_hit_rate)
        run("60+61 candidate only when validated; activation false", test_candidate_artifact)
        run("63-65 no sizing/stop/trade code; offline imports", test_static_safety)
        run("65 runtime offline (sockets disabled)", test_offline_runtime)
        run("66 live strategy files byte-identical to Step 4", test_live_files_unchanged)
        run("67 all previous stage suites", test_previous_stages)
    finally:
        if CH.get("dir"):
            shutil.rmtree(CH["dir"], ignore_errors=True)
    print("\nAll Stage 11 tests passed.")
