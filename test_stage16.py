#!/usr/bin/env python3
"""Stage 16 — STEP 1 BASELINE: behavioural regression contract + strategy fingerprint protection.

Run:  py test_stage16.py        (or py run_all_tests.py for stages 1-17)

  1-3   every stored regression case (regression/strategy_cases.json) reproduces EXACTLY with the
        current code: P(UP)/P(DOWN), direction, confidence, edges, call/no-call, stops, sizing,
        settlement, backtests, perp overlay, live veto default
  4     the fixtures cover every scenario class required by the Step 1 brief
  5     adapter invariant on every evaluate case: CALL <=> legacy signal, reasons mapped 1:1
  6     the harness restores every module attribute it patched and uses no network
  7-8   config/strategy_baseline.json matches the code; legacy fingerprint == step5 manifest
  9-12  fingerprint sensitivity: strategy edits are detected (legacy and/or extended), cosmetic
        edits are not; baseline tooling refuses accidental rewrites
  13    all previous stage suites
The fixture expectations were produced by the UNMODIFIED legacy code (regression/generate.py).
"""
import ast
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import kalshi_dashboard as k                       # noqa: E402
import strategy_fingerprint as sf                  # noqa: E402
from kalshi_core import baseline                   # noqa: E402
from regression import harness                     # noqa: E402

FIXTURES = os.path.join(HERE, "regression", "strategy_cases.json")
LEGACY_FP = "8d94f241e8fc8edadc76058e1f12f430b6e4f499f4c0a30fba1cb5cf07dad82a"


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


def _fixtures():
    with open(FIXTURES, encoding="utf-8") as f:
        return json.load(f)


# ═══════════════════ 1-6 regression contract ═══════════════════
def _check_kind(kinds):
    doc = _fixtures()
    assert doc["fixture_schema_version"] == harness.FIXTURE_SCHEMA_VERSION
    n = 0
    for case in doc["cases"]:
        if case["kind"] not in kinds:
            continue
        actual = harness.run_case(case)
        diffs = harness.compare(case["expected"], actual)
        assert not diffs, f"{case['id']} changed: " + "; ".join(diffs[:8])
        n += 1
    assert n > 0, kinds


def test_evaluate_cases():
    _check_kind({"evaluate"})


def test_poller_and_settlement_cases():
    _check_kind({"poller", "settle_calls", "settle_pending", "log_call", "fees"})


def test_backtest_and_overlay_cases():
    _check_kind({"backtest_coin", "backtest_full", "overlay", "gate_inactive"})


def test_scenario_coverage():
    doc = _fixtures()
    ev = {c["id"]: c for c in doc["cases"] if c["kind"] == "evaluate"}
    reasons = {c["expected"]["result"].get("reason") or ("RAISES" if "raises" in c["expected"]["result"] else None)
               for c in ev.values()}
    for r in ("ENTER", "WAIT_TOO_EARLY", "WAIT_LATE", "BLOCK_CHOP", "WAIT_LOW_CONFIDENCE", "WAIT_BAD_PRICE",
              "WAIT_NO_EDGE", "BLOCK_STALE_DATA", "BLOCK_COIN", "BLOCK_DIRECTION", "RAISES"):
        assert r in reasons, r
    sides = {c["expected"]["result"].get("fav") for c in ev.values() if c["expected"]["result"].get("signal")}
    assert sides == {"UP", "DOWN"}, sides                                            # above AND below strike
    remains = {c["expected"]["result"].get("remain") for c in ev.values() if c["expected"]["result"].get("signal")}
    assert 8.0 in remains and 2.0 in remains                                         # early + late eligible
    ids = {c["id"] for c in doc["cases"]}
    for need in ("E03_near_strike", "E04_high_volatility", "E05_low_volatility", "E14_choppy",
                 "E25_settlement_sensitive_exact_strike", "P01_cycle_gate_inactive", "P03_perp_veto_blocks",
                 "S01_settle_calls", "S02_settle_pending_stats", "B01_backtest_coin_btc", "B03_run_backtest_all",
                 "V01_perp_overlay_math", "V02_live_veto_inactive_default"):
        assert need in ids, need
    stops = {r["exit_reason"] for c in doc["cases"] if c["kind"] == "settle_calls" for r in c["expected"]["rows"]}
    assert stops == {"settle", "stop", "unfilled"}, stops


def test_adapter_invariants_on_fixtures():
    from kalshi_core.no_call import LEGACY_REASON_MAP
    for c in _fixtures()["cases"]:
        if c["kind"] != "evaluate":
            continue
        r, sd = c["expected"]["result"], c["expected"]["signal_decision"]
        assert (sd["decision"] == "CALL") == (r.get("signal") is True), c["id"]
        if r.get("reason") in LEGACY_REASON_MAP:
            assert sd["no_call_reasons"] == [LEGACY_REASON_MAP[r["reason"]].value], c["id"]
        if "p_up" in r:
            assert abs(sd["raw_probability_up"] - r["p_up"] / 100.0) < 1e-12
            assert abs(sd["raw_probability_up"] + sd["raw_probability_down"] - 1.0) < 1e-12
            assert sd["confidence_pct"] == r["conf"] and sd["side"] == r["fav"]
        assert sd["calibrated_probability_up"] is None and sd["calibration_method"] == "NONE"


def test_harness_restores_state_and_uses_no_network():
    before = {a: getattr(k, a) for a in ("dt", "time", "current_market", "spot", "candles", "market_result",
                                         "_get", "fetch_history", "CALLS_FILE", "ON_CALL", "POST_EMBED",
                                         "LOCAL_TZ", "PERP_TELEMETRY_ENABLED", "DIRECTION", "ENTRY_MODE")}
    coins_before = dict(k.ACTIVE_COINS)
    for c in _fixtures()["cases"][:6]:
        harness.run_case(c)
    for a, v in before.items():
        assert getattr(k, a) is v, f"harness did not restore {a}"
    assert k.ACTIVE_COINS == coins_before and not k._pending and not k._call_pending
    code = ("import sys, socket\n"
            "def boom(*a, **kw): raise RuntimeError('network used')\n"
            "socket.socket.connect = boom; socket.socket.connect_ex = boom; socket.create_connection = boom\n"
            "from regression import generate\n"
            "sys.exit(generate.main(['--check']))\n")
    p = subprocess.run([sys.executable, "-c", code], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "MATCH" in p.stdout, (p.stdout[-600:], p.stderr[-600:])


# ═══════════════════ 7-12 baseline manifest + fingerprints ═══════════════════
def test_baseline_manifest_matches():
    ok, problems = baseline.verify()
    assert ok, problems
    m = json.load(open(os.path.join(HERE, "config", "strategy_baseline.json"), encoding="utf-8"))
    assert m == baseline.build_manifest() == baseline.build_manifest()               # deterministic
    txt = json.dumps(m).lower()
    for bad in ("begin rsa", "private key-----", "bot_token\": \"", "api_key_id\": \""):
        assert bad not in txt, bad
    assert m["extracted"]["thresholds"]["MIN_CONF_pct"] == k.MIN_CONF == 80.0
    assert m["extracted"]["entry_window"] == {"max_minutes_left": 8.0, "min_minutes_left": 2.0,
                                              "inclusive_both_ends": True, "interval_minutes": 15,
                                              "source": m["extracted"]["entry_window"]["source"]}


def test_legacy_fingerprint_unchanged():
    legacy = json.load(open(os.path.join(HERE, "step5_baseline_manifest.json")))
    assert legacy["legacy_strategy_fingerprint"] == LEGACY_FP
    assert sf.current_fingerprint(os.path.join(HERE, "kalshi_dashboard.py"))[0] == LEGACY_FP
    m = baseline.build_manifest()
    assert m["fingerprints"]["legacy_strategy_fingerprint"] == LEGACY_FP
    comp = m["fingerprints"]["extended_components"]["kalshi_dashboard.py"]
    assert set(sf.STRATEGY_FUNCTIONS) <= set(comp["functions"]) and set(sf.STRATEGY_CONSTANTS) <= set(comp["constants"])


def _repo_copy():
    d = tempfile.mkdtemp(prefix="kalshi_fp_")
    for f in list(baseline.EXTENDED_SPEC) + ["step5_baseline_manifest.json"]:
        shutil.copy(os.path.join(HERE, f), d)
    os.makedirs(os.path.join(d, "config"))
    shutil.copy(os.path.join(HERE, "config", "strategy_baseline.json"), os.path.join(d, "config"))
    return d


def _mutate(root, fname, old, new):
    p = os.path.join(root, fname)
    s = open(p, encoding="utf-8").read()
    assert s.count(old) >= 1, (fname, old[:50])
    open(p, "w", encoding="utf-8").write(s.replace(old, new, 1))


def _fps(root):
    m = baseline.build_manifest(root)
    return m["fingerprints"]["legacy_strategy_fingerprint"], m["fingerprints"]["extended_strategy_fingerprint"]


def test_fingerprint_detects_strategy_changes():
    base_legacy, base_ext = _fps(HERE)
    #            file                   old                                         new                                     legacy changes?
    edits = [("kalshi_dashboard.py", "MIN_CONF      = 80.0", "MIN_CONF      = 79.0", True),
             ("kalshi_dashboard.py", "MAX_ENTRY_MIN = 8.0", "MAX_ENTRY_MIN = 9.0", True),
             ("kalshi_dashboard.py", "    conf = max(p_up, 100 - p_up)", "    conf = max(p_up, 100 - p_up) * 1.0", True),
             ("kalshi_dashboard.py", '"sl_pct": 0.75}', '"sl_pct": 0.70}', True),
             ("kalshi_dashboard.py", 'if ya is None and nb is not None: ya = 100 - nb',
              'if ya is None and nb is not None: ya = 99 - nb', False),                                   # book_of
             ("kalshi_dashboard.py", 'DIRECTION    = "BOTH"', 'DIRECTION    = "UP"', False),              # web filter
             ("kalshi_dashboard.py", 'if r.get("signal") and r.get("ticker") not in _alerted:',
              'if r.get("signal"):', False),                                                              # first-signal gate
             ("kalshi_dashboard.py", "POLL_SECONDS  = 4", "POLL_SECONDS  = 2", False),
             ("kalshi_dashboard.py", 'PERP_LIVE_VETO_ENABLED  = os.environ.get("PERP_LIVE_VETO_ENABLED", "0") == "1"',
              'PERP_LIVE_VETO_ENABLED  = os.environ.get("PERP_LIVE_VETO_ENABLED", "1") == "1"', False),
             ("kalshi_backtest.py", "MIN_CONF         = 80.0", "MIN_CONF         = 75.0", False),
             ("kalshi_backtest.py", "return sum((_pw(t) - (1 if t[\"win\"] else 0)) ** 2",
              "return sum((_pw(t) - (1 if t[\"win\"] else 0)) ** 3", False),                              # brier
             ("perp_probability.py", "MAX_ABS_PERP_DELTA = 0.05", "MAX_ABS_PERP_DELTA = 0.10", False),
             ("perp_live.py", "LIVE_VETO_MAX_BLOCK_FRACTION = 0.35", "LIVE_VETO_MAX_BLOCK_FRACTION = 0.50", False)]
    for fname, old, new, legacy_changes in edits:
        d = _repo_copy()
        try:
            _mutate(d, fname, old, new)
            leg, ext = _fps(d)
            assert ext != base_ext, (fname, old[:40])
            assert (leg != base_legacy) == legacy_changes, (fname, old[:40], "legacy")
            ok, problems = baseline.verify(d)
            assert not ok and any("baseline differs" in p for p in problems), (fname, old[:40])
        finally:
            shutil.rmtree(d, ignore_errors=True)


def test_fingerprint_ignores_cosmetic_changes():
    base = _fps(HERE)
    edits = [("kalshi_dashboard.py", "    conf = max(p_up, 100 - p_up)",
              "\n    # an explanatory comment\n\n    conf = max(p_up, 100 - p_up)"),
             ("kalshi_dashboard.py", '"""Quarter-Kelly risk fraction.', '"""Quarter Kelly risk fraction (reworded).'),
             ("kalshi_dashboard.py", "<b>Kalshi 15m Paper Desk</b>", "<b>Kalshi 15m Local Desk</b>"),
             ("kalshi_backtest.py", "# ─────────── math ───────────", "# ----- math -----"),
             ("perp_probability.py", "import math", "import math  # stdlib")]
    for fname, old, new in edits:
        d = _repo_copy()
        try:
            _mutate(d, fname, old, new)
            assert _fps(d) == base, (fname, old[:40])
            assert baseline.verify(d)[0], (fname, old[:40])
        finally:
            shutil.rmtree(d, ignore_errors=True)


def test_rewrite_guards():
    p = subprocess.run([sys.executable, "-m", "kalshi_core.baseline", "--write"], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 2 and "Refusing" in p.stderr
    p = subprocess.run([sys.executable, "-m", "regression.generate", "--write"], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 2 and "Refusing" in p.stderr
    p = subprocess.run([sys.executable, "-m", "kalshi_core.baseline", "--verify"], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "MATCHES" in p.stdout, p.stdout
    # the baseline tooling parses; it never imports the strategy module
    tree = ast.parse(open(os.path.join(HERE, "kalshi_core", "baseline.py"), encoding="utf-8").read())
    mods = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
           {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    assert not mods & {"kalshi_dashboard", "kalshi_backtest", "perp_live", "requests", "socket"}, mods


def test_previous_stages():
    # Master mode (run_all_tests.py): every stage is already run exactly once by the runner.
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    # Standalone: re-verify earlier stages, each EXACTLY once (children run in master mode).
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 16):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-600:], p.stderr[-600:])


if __name__ == "__main__":
    run("1 evaluate(): P(UP)/P(DOWN), side, conf, edges, gates, stops, sizing unchanged", test_evaluate_cases)
    run("2 poller cycle, settlement, stops, fees, call journal unchanged", test_poller_and_settlement_cases)
    run("3 backtests, calibration metrics, perp overlay, live-veto default unchanged", test_backtest_and_overlay_cases)
    run("4 fixtures cover every required scenario class", test_scenario_coverage)
    run("5 SignalDecision invariants on every evaluate fixture", test_adapter_invariants_on_fixtures)
    run("6 harness restores patched state; fixtures need no network", test_harness_restores_state_and_uses_no_network)
    run("7 config/strategy_baseline.json matches the code (deterministic, no secrets)", test_baseline_manifest_matches)
    run("8 legacy strategy fingerprint unchanged (8d94f241...)", test_legacy_fingerprint_unchanged)
    run("9-10 strategy edits change the fingerprint(s) and fail verification", test_fingerprint_detects_strategy_changes)
    run("11 comments/docstrings/UI text do not change the fingerprint", test_fingerprint_ignores_cosmetic_changes)
    run("12 baseline + fixture rewrites need an explicit flag", test_rewrite_guards)
    run("13 all previous stage suites", test_previous_stages)
    print("\nAll Stage 16 tests passed.")
