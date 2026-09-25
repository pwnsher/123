#!/usr/bin/env python3
"""Stage 17 — STEP 1 LOCAL-FIRST ARCHITECTURE: signal schema, NO_CALL codes, data health, Discord
isolation, execution safety boundary, logging, configuration, local startup.

Run:  py test_stage17.py        (or py run_all_tests.py for stages 1-17)

Nothing here changes or re-tunes the strategy: every check either reads the legacy output or
verifies that the new layer around it cannot alter it.
"""
import ast
import io
import json
import os
import re
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import kalshi_dashboard as k                                          # noqa: E402
from kalshi_core import adapter, config as kc, data_health as dh      # noqa: E402
from kalshi_core import execution as ex, interfaces as itf            # noqa: E402
from kalshi_core import logging_setup as klog                         # noqa: E402
from kalshi_core.no_call import NoCallReason, LEGACY_REASON_MAP, LEGACY_GATE_ORDER, reasons_for_evaluation  # noqa: E402
from kalshi_core.signal import SignalDecision, Decision, Side         # noqa: E402

CORE_MODULES = ("kalshi_core", "kalshi_core.no_call", "kalshi_core.data_health", "kalshi_core.signal",
                "kalshi_core.adapter", "kalshi_core.interfaces", "kalshi_core.execution", "kalshi_core.baseline",
                "kalshi_core.config", "kalshi_core.logging_setup")
PREDICTION_FILES = ("kalshi_dashboard.py", "kalshi_backtest.py", "perp_probability.py", "perp_live.py",
                    "perp_shadow.py", "perp_telemetry.py", "strategy_fingerprint.py", "analyze_perp_predictive.py",
                    "analyze_perp_integration.py", "analyze_perp_shadow.py", "build_perp_shadow_policy.py",
                    "build_perp_integration_experiment.py", "label_binary_outcomes.py", "run_local.py",
                    "kalshi_core/no_call.py", "kalshi_core/data_health.py", "kalshi_core/signal.py",
                    "kalshi_core/adapter.py", "kalshi_core/interfaces.py", "kalshi_core/baseline.py",
                    "regression/harness.py", "regression/cases.py", "regression/generate.py")


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


def _py_files():
    out = []
    for root, dirs, files in os.walk(HERE):
        dirs[:] = [d for d in dirs if d not in (".git", "__pycache__", ".venv", "venv", "env")]
        out += [os.path.relpath(os.path.join(root, f), HERE) for f in files if f.endswith(".py")]
    return sorted(out)


def _imports(path):
    tree = ast.parse(open(os.path.join(HERE, path), encoding="utf-8").read())
    mods = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    mods |= {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
    return mods


def _ok_result(**kw):
    r = {"coin": "BTC", "status": "ok", "ticker": "KXBTC15M-T", "remain": 5.0, "spot": 100.35, "strike": 100.0,
         "up_bid": 88.0, "up_ask": 90.0, "dn_bid": 9.0, "dn_ask": 11.0, "conf": 95.9, "fav": "UP", "p_up": 95.8587,
         "spot_raw": 100.35, "spot_observed_ts": 1790000000.25, "edge": 3.9, "raw_edge": 5.9, "net_edge": 3.9,
         "side_ask": 90.0, "rec_stop": 68, "sl_pct": 75, "verdict": "ENTER UP", "reason": "ENTER", "signal": True,
         "close": "2026-09-21T14:05:00Z", "stoch": 60, "stoch_arrow": "", "bb": "Inside", "atr": 0.1}
    r.update(kw)
    return r


# ═══════════════════ 1-4 signal schema, NO_CALL, data health ═══════════════════
def test_signal_schema_roundtrip():
    sd = adapter.from_evaluation(_ok_result(), coin="BTC", series="KXBTC15M", strategy_fingerprint="f" * 64)
    assert sd.decision == Decision.CALL and sd.side == Side.UP and sd.no_call_reasons == []
    assert sd.raw_probability_up == 0.958587 and abs(sd.raw_probability_down - 0.041413) < 1e-12
    assert sd.calibrated_probability_up is None and sd.calibration_method == "NONE" and sd.volatility_per_min is None
    assert sd.timestamp_epoch_ms == 1790000000250 and sd.stop_loss_fraction == 0.75
    for s in (sd, adapter.from_evaluation({"coin": "ETH", "status": "no market"})):
        j = s.to_json()
        back = SignalDecision.from_json(j)
        assert back == s and back.to_json() == j and json.loads(j)["schema_version"] == 1
    try:
        SignalDecision(asset="BTC", decision=Decision.CALL, no_call_reasons=[NoCallReason.DATA_STALE]); raise AssertionError
    except ValueError:
        pass
    try:
        SignalDecision(asset="BTC", decision=Decision.NO_CALL); raise AssertionError
    except ValueError:
        pass
    try:
        SignalDecision.from_dict(dict(sd.to_dict(), surprise=1)); raise AssertionError
    except ValueError:
        pass
    try:
        SignalDecision.from_dict(dict(sd.to_dict(), schema_version=2)); raise AssertionError
    except ValueError:
        pass


def test_no_call_codes_match_legacy_code():
    """Every reason code evaluate() can emit is mapped, in the same gate order, and nothing is invented."""
    src = open(os.path.join(HERE, "kalshi_dashboard.py"), encoding="utf-8").read()
    fn = next(n for n in ast.parse(src).body if isinstance(n, ast.FunctionDef) and n.name == "evaluate")
    emitted = []
    for n in ast.walk(fn):
        if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "reason" for t in n.targets) \
                and isinstance(n.value, ast.Constant):
            emitted.append((n.lineno, n.value.value))
    order = [v for _, v in sorted(emitted)]
    assert order == list(LEGACY_GATE_ORDER) + ["ENTER"], order
    lits = {n.value for n in ast.walk(fn) if isinstance(n, ast.Constant) and isinstance(n.value, str)
            and re.fullmatch(r"(BLOCK|WAIT)_[A-Z_]+", n.value)}
    assert lits == set(LEGACY_REASON_MAP), lits ^ set(LEGACY_REASON_MAP)
    assert len(set(LEGACY_REASON_MAP.values())) == len(LEGACY_REASON_MAP)             # 1:1
    assert reasons_for_evaluation({"status": "ok", "reason": "ENTER", "signal": True}) == []
    assert reasons_for_evaluation({"status": "net error"}) == [NoCallReason.DATA_UNAVAILABLE]
    assert reasons_for_evaluation({"status": "no market"}) == [NoCallReason.MARKET_UNAVAILABLE]
    assert reasons_for_evaluation({"status": "error: boom"}) == [NoCallReason.EVALUATION_ERROR]
    assert reasons_for_evaluation({"status": "stale", "reason": "BLOCK_STALE_DATA"}) == [NoCallReason.DATA_STALE]
    assert reasons_for_evaluation({"status": "ok", "reason": "SOMETHING_NEW"}) == [NoCallReason.EVALUATION_ERROR]
    # no invented gate: the only reasons outside the evaluate() map are the poller/status ones
    extra = {r for r in NoCallReason} - set(LEGACY_REASON_MAP.values())
    assert extra == {NoCallReason.MARKET_UNAVAILABLE, NoCallReason.DATA_UNAVAILABLE, NoCallReason.EVALUATION_ERROR,
                     NoCallReason.ALREADY_CALLED_THIS_MARKET, NoCallReason.PERP_VETO_SUPPRESSED,
                     NoCallReason.WATCHER_PAUSED}, extra
    poller = open(os.path.join(HERE, "kalshi_dashboard.py"), encoding="utf-8").read()
    for evidence in ('r.get("ticker") not in _alerted', '_g.get("decision") == "BLOCK"', "if not RUNNING.is_set()",
                     '"status": "net error"', '"status": f"error: {e}"', '"status": "no market"'):
        assert evidence in poller, evidence


def test_adapter_fails_closed():
    # inconsistent legacy dict (signal True but a WAIT reason, or ENTER without signal) -> never a CALL
    assert adapter.from_evaluation(_ok_result(reason="WAIT_NO_EDGE")).no_call_reasons == [NoCallReason.INSUFFICIENT_EDGE]
    bad = adapter.from_evaluation(_ok_result(signal=False))
    assert bad.decision == Decision.NO_CALL and bad.no_call_reasons == [NoCallReason.EVALUATION_ERROR]
    assert adapter.from_evaluation("garbage").no_call_reasons == [NoCallReason.EVALUATION_ERROR]
    assert adapter.from_evaluation(_ok_result(), already_called=True).no_call_reasons == \
        [NoCallReason.ALREADY_CALLED_THIS_MARKET]
    v = adapter.from_evaluation(_ok_result(), gate_result={"decision": "BLOCK", "reason": "BLOCK_STEP4_CONFLICT"})
    assert v.no_call_reasons == [NoCallReason.PERP_VETO_SUPPRESSED] and v.perp_overlay["reason"] == "BLOCK_STEP4_CONFLICT"
    assert adapter.from_evaluation(_ok_result(), gate_result={"decision": "INACTIVE"}).is_call
    # the adapter never mutates the legacy dict
    r = _ok_result(); snap = json.dumps(r, sort_keys=True)
    adapter.from_evaluation(r, gate_result={"decision": "BLOCK"})
    assert json.dumps(r, sort_keys=True) == snap


def test_data_health():
    ok = dh.health_from_evaluation(_ok_result())
    assert [h.status for h in ok] == [dh.FeedStatus.HEALTHY] * 2 and dh.overall(ok) == dh.FeedStatus.HEALTHY
    st = dh.health_from_evaluation({"status": "stale", "reason": "BLOCK_STALE_DATA", "verdict": "STALE · no volatility"})
    assert st[1].status == dh.FeedStatus.STALE and dh.overall(st) == dh.FeedStatus.STALE
    assert dh.health_from_evaluation({"status": "no market"})[0].status == dh.FeedStatus.UNAVAILABLE
    assert dh.overall(dh.health_from_evaluation({"status": "net error"})) == dh.FeedStatus.UNKNOWN
    for s, want in (("fresh", "HEALTHY"), ("stale", "STALE"), ("unavailable", "UNAVAILABLE"), ("error", "DISCONNECTED"),
                    ("disabled", "DISABLED"), ("???", "UNKNOWN")):
        assert dh.health_from_perp_row({"source_status": s}).status.value == want
    h = dh.FeedHealth("x", dh.FeedStatus.STALE, "d", 1.5)
    assert dh.FeedHealth.from_dict(h.to_dict()) == h
    # descriptive only: no strategy module reads the health layer
    for f in PREDICTION_FILES:
        if not f.startswith(("kalshi_core/", "regression/", "run_local")):
            assert not any(m.startswith("kalshi_core") for m in _imports(f)), f


# ═══════════════════ 5-8 Discord isolation, local startup ═══════════════════
def test_discord_confined_to_legacy_adapter():
    users = {f for f in _py_files() if any(m == "discord" or m.startswith("discord.") for m in _imports(f))}
    assert users == {"kalshi_bot.py", "check_channel.py"}, users
    bot_users = {f for f in _py_files() if "kalshi_bot" in _imports(f)}
    assert bot_users <= {"check_channel.py"}, bot_users
    code = ("import sys\nsys.modules['discord'] = None\n"
            f"import {', '.join(CORE_MODULES)}\n"
            "import kalshi_dashboard, kalshi_backtest, perp_live, perp_shadow, perp_probability, perp_telemetry\n"
            "import strategy_fingerprint, run_local\n"
            "from regression import harness\n"
            "assert sys.modules['discord'] is None\nprint('CORE_OK')\n")
    env = {kk: v for kk, v in os.environ.items() if not kk.startswith("DISCORD")}
    p = subprocess.run([sys.executable, "-c", code], cwd=HERE, capture_output=True, text=True, env=env)
    assert p.returncode == 0 and "CORE_OK" in p.stdout, p.stderr[-800:]


def test_kalshi_bot_is_optional():
    code = "import sys\nsys.modules['discord'] = None\nimport runpy\nrunpy.run_path('kalshi_bot.py', run_name='__main__')\n"
    p = subprocess.run([sys.executable, "-c", code], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 1 and "OPTIONAL legacy Discord adapter" in p.stderr and "run_local.py" in p.stderr, p.stderr
    assert "Traceback" not in p.stderr, p.stderr
    assert kc.discord_token_usable("abc.def.ghi") and not kc.discord_token_usable("PASTE_YOUR_BOT_TOKEN_HERE")
    assert not kc.discord_token_usable("") and not kc.discord_token_usable(None) and not kc.discord_token_usable("  ")
    src = open(os.path.join(HERE, "kalshi_bot.py"), encoding="utf-8").read()
    assert "discord_token_usable(BOT_TOKEN)" in src and 'os.environ.get("DISCORD_BOT_TOKEN")' in src


def test_local_check_without_credentials():
    env = {kk: v for kk, v in os.environ.items()
           if not kk.startswith(("DISCORD", "KALSHI_API", "KALSHI_PRIVATE", "TELEGRAM"))}
    env["DISCORD_BOT_TOKEN"] = "SENTINEL-NOT-A-REAL-TOKEN-4242"
    d = tempfile.mkdtemp()
    p = subprocess.run([sys.executable, os.path.join(HERE, "run_local.py"), "--check", "--env-file",
                        os.path.join(d, "none.env")], cwd=d, capture_output=True, text=True, env=env)
    out = p.stdout + p.stderr
    assert p.returncode == 0 and "LOCAL SELF-CHECK: OK" in p.stdout, out[-1500:]
    for ev in ("event=startup", "event=config_loaded", "event=strategy_fingerprint", "status=MATCHES"):
        assert ev in out, ev
    assert "SENTINEL-NOT-A-REAL-TOKEN-4242" not in out and '\\"DISCORD_BOT_TOKEN\\": \\"set\\"' in out
    assert os.listdir(d) == [], os.listdir(d)                                    # --check writes nothing


LOCAL_RUNTIME_SCRIPT = r'''
import io, json, os, sys, time, urllib.request
sys.path.insert(0, HERE)
import requests
def _blocked(*a, **kw): raise requests.ConnectionError("outbound network blocked in test")
requests.Session.request = _blocked; requests.get = _blocked; requests.post = _blocked
import run_local
from kalshi_core import config as kc, logging_setup as klog
import kalshi_dashboard as k
from regression import cases
import datetime as dt
def live(m, minutes):                       # this test runs on the REAL clock (not the fixtures' frozen one)
    m["close_time"] = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z")
    return m
mk = {"KXBTC15M": live(cases.market("KXBTC15M-RT", 5.0, 100.0, 88, 90, 9, 11), 5.0),
      "KXETH15M": live(cases.market("KXETH15M-RT", 10.0, 100.0, 88, 90, 9, 11), 10.0), "KXSOL15M": None, "KXXRP15M": None}
cl = cases.walk(80, 100.35, 0.0008, 11)
k.current_market = lambda s: mk.get(s)
k.spot = lambda p: 100.35
k.candles = lambda p: cases.candles(cl, 100.35)
k.market_result = lambda tk: ""
k.fetch_history = lambda p, d: {}
k.PERP_TELEMETRY_ENABLED = False
log = io.StringIO()
klog.configure("DEBUG", "text", "", stream=log, secrets=[])
journal = os.path.join(os.getcwd(), "decisions.jsonl")
cfg = kc.InfraConfig(log_level="DEBUG", decision_journal=journal, dashboard_port=0)
app = run_local.LocalApp(k, cfg, {"legacy": "L" * 64, "extended": "E" * 64, "model_id": "m"}).start(web=True)
port = app.server.server_address[1]
deadline = time.time() + 15
while time.time() < deadline and "event=no_call" not in log.getvalue():
    time.sleep(0.1)
data = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/data", timeout=5).read())
app.stop()
text = log.getvalue()
rows = [json.loads(x) for x in open(journal)]
print(json.dumps({"coins": {c: (r.get("reason") or r.get("status")) for c, r in data["coins"].items()},
                  "calls_logged": sorted(k._call_pending), "alerted": sorted(k._alerted),
                  "events": sorted(set(l.split("event=")[1].split()[0] for l in text.splitlines() if "event=" in l)),
                  "journal": [(r["asset"], r["decision"], r["no_call_reasons"]) for r in rows[:4]],
                  "paper_csv": os.path.exists(k.PAPER_ORDERS_CSV), "on_call_cleared": k.ON_CALL is None}))
'''


def test_local_runtime_end_to_end():
    """run_local.LocalApp starts the unmodified watcher + web server with no Discord and no network,
    logs the call and the NO_CALL reasons, journals SignalDecisions, serves /data, and stops cleanly."""
    d = tempfile.mkdtemp()
    code = f"HERE = {HERE!r}\n" + LOCAL_RUNTIME_SCRIPT
    env = {kk: v for kk, v in os.environ.items() if not kk.startswith("DISCORD")}
    p = subprocess.run([sys.executable, "-c", code], cwd=d, capture_output=True, text=True, env=env, timeout=120)
    assert p.returncode == 0, (p.stdout[-800:], p.stderr[-1500:])
    res = json.loads(p.stdout.strip().splitlines()[-1])
    assert res["coins"] == {"BTC": "ENTER", "ETH": "WAIT_TOO_EARLY", "SOL": "no market", "XRP": "no market"}, res
    assert res["calls_logged"] == ["KXBTC15M-RT"] and res["alerted"] == ["KXBTC15M-RT"], res
    for ev in ("model_initialized", "dashboard_started", "signal_generated", "no_call", "feed_status",
               "feed_degraded", "notification", "shutdown", "cycle"):
        assert ev in res["events"], (ev, res["events"])
    assert ("BTC", "CALL", []) in [tuple(x) for x in res["journal"]], res["journal"]
    assert ("ETH", "NO_CALL", ["OUTSIDE_ENTRY_WINDOW_EARLY"]) in [tuple(x) for x in res["journal"]]
    assert res["paper_csv"] is True and res["on_call_cleared"] is True
    # run_local starts exactly the legacy main()'s workers
    src = open(os.path.join(HERE, "run_local.py"), encoding="utf-8").read()
    for name in ("k._gate_banner()", "k.poller", "k.backtest_worker", "k.weekly_worker", "k.Handler",
                 '("127.0.0.1", self.cfg.dashboard_port)'):
        assert name in src, name


# ═══════════════════ 9 execution safety boundary ═══════════════════
def test_execution_boundary():
    for flag in ("LIVE_TRADING", "KALSHI_LIVE", "PERP_LIVE_VETO_ENABLED", "EXECUTION_MODE", "KALSHI_ENV"):
        os.environ[flag] = "1" if flag != "KALSHI_ENV" else "prod"
    try:
        for mode in ("LIVE", ex.ExecutionMode.LIVE):
            try:
                ex.get_execution_engine(mode); raise AssertionError("LIVE engine returned")
            except ex.LiveExecutionUnavailable:
                pass
        try:
            ex.get_execution_engine("PAPER"); raise AssertionError
        except ex.ExecutionDisabled:
            pass
        eng = ex.get_execution_engine()
        call = adapter.from_evaluation(_ok_result())
        intent = ex.OrderIntent(call, itf.RiskDecision(True, (), 5), 1, 90.0)
        try:
            eng.submit(intent); raise AssertionError
        except ex.ExecutionDisabled:
            pass
    finally:
        for flag in ("LIVE_TRADING", "KALSHI_LIVE", "PERP_LIVE_VETO_ENABLED", "EXECUTION_MODE", "KALSHI_ENV"):
            os.environ.pop(flag, None)
    assert ex.LIVE_EXECUTION_AVAILABLE is False
    nocall = adapter.from_evaluation({"coin": "BTC", "status": "no market"})
    for bad in ((nocall, itf.RiskDecision(True, (), 5), 1, 90.0), (call, itf.RiskDecision(False, ("x",), 5), 1, 90.0),
                (call, itf.RiskDecision(True, (), 5), 6, 90.0), (call, itf.RiskDecision(True, (), 5), 1, 100.0),
                (call, None, 1, 90.0), ({"decision": "CALL"}, itf.RiskDecision(True, (), 5), 1, 90.0)):
        try:
            ex.OrderIntent(*bad); raise AssertionError(bad)
        except ValueError:
            pass
    # static: prediction/signal/evaluation code never reaches an execution path
    for f in PREDICTION_FILES:
        mods = _imports(f)
        assert "kalshi_core.execution" not in mods and "kalshi_api_learn" not in mods, (f, mods)
        tree = ast.parse(open(os.path.join(HERE, f), encoding="utf-8").read())
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | \
                {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        assert not names & {"create_order", "place_order", "cancel_order", "execution"}, (f, names & {"create_order"})
    for f in _py_files():
        if f in ("kalshi_api_learn.py", "test_stage17.py") or f.startswith("test_stage"):
            continue
        assert "/portfolio/orders" not in open(os.path.join(HERE, f), encoding="utf-8").read(), f
    for m in ("kalshi_core/execution.py", "kalshi_core/signal.py", "kalshi_core/no_call.py", "kalshi_core/data_health.py",
              "kalshi_core/interfaces.py", "kalshi_core/adapter.py", "kalshi_core/config.py", "kalshi_core/logging_setup.py"):
        assert not _imports(m) & {"requests", "urllib", "urllib3", "http", "http.client", "socket", "httpx", "aiohttp"}, m
    # the one existing order-capable tool stays demo-locked + DRY_RUN
    src = open(os.path.join(HERE, "kalshi_api_learn.py"), encoding="utf-8").read()
    assert "DRY_RUN = True" in src and 'os.environ.get("KALSHI_ENV", "demo")' in src
    assert src.count("self._orders_allowed()") == 2 and 'if ENV != "demo":' in src


# ═══════════════════ 10-12 logging, configuration, hygiene ═══════════════════
def test_logging_structured_and_redacted():
    buf = io.StringIO()
    lg = klog.configure("INFO", "text", "", stream=buf, secrets=["SUPERSECRETVALUE99"])
    klog.log_event(klog.get_logger("t"), "signal_generated", coin="BTC", conf=91.23, note="has space",
                   token="SUPERSECRETVALUE99")
    klog.get_logger("t").info("leak -----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY----- and SUPERSECRETVALUE99")
    klog.get_logger("t").debug("hidden at INFO")
    fake_tok = "M" + "a" * 25 + "." + "b" * 6 + "." + "c" * 30
    klog.get_logger("t").warning("tok %s", fake_tok)
    out = buf.getvalue()
    assert "event=signal_generated coin=BTC conf=91.23" in out and 'note="has space"' in out
    assert "SUPERSECRETVALUE99" not in out and "BEGIN RSA" not in out and fake_tok not in out and "hidden" not in out
    buf2 = io.StringIO()
    klog.configure("INFO", "json", "", stream=buf2, secrets=["SUPERSECRETVALUE99"])
    klog.log_event(klog.get_logger("t"), "no_call", coin="ETH", reasons=["INSUFFICIENT_EDGE"], s="SUPERSECRETVALUE99")
    rec = json.loads(buf2.getvalue().strip())
    assert rec["event"] == "no_call" and rec["reasons"] == ["INSUFFICIENT_EDGE"] and rec["s"] == "***"
    assert lg.propagate is False


def test_configuration():
    env = {}
    d = tempfile.mkdtemp()
    p = os.path.join(d, ".env")
    open(p, "w").write("# c\nKALSHI_LOG_LEVEL=debug\nexport KALSHI_LOG_FORMAT='json'\nPERP_LIVE_VETO_ENABLED=1\n"
                       "DISCORD_BOT_TOKEN=\"abc123456\"\nbad line\nKALSHI_DASHBOARD_PORT=8123\n")
    env["KALSHI_DASHBOARD_PORT"] = "9000"                                   # the shell wins
    loaded, refused = kc.load_dotenv(p, env)
    assert refused == ["PERP_LIVE_VETO_ENABLED"] and "PERP_LIVE_VETO_ENABLED" not in env
    assert set(loaded) == {"KALSHI_LOG_LEVEL", "KALSHI_LOG_FORMAT", "DISCORD_BOT_TOKEN"} and env["KALSHI_DASHBOARD_PORT"] == "9000"
    c = kc.InfraConfig.from_env(env)
    assert c.log_level == "DEBUG" and c.log_format == "json" and c.dashboard_port == 9000
    assert kc.describe_secrets(env) == {"DISCORD_BOT_TOKEN": "set", "KALSHI_API_KEY_ID": "unset",
                                        "KALSHI_PRIVATE_KEY_PATH": "unset"}
    assert "abc123456" not in json.dumps(kc.describe_secrets(env)) + json.dumps(c.to_dict())
    assert kc.InfraConfig.from_env({"KALSHI_DASHBOARD_PORT": "x", "KALSHI_LOG_FORMAT": "xml"}) == kc.InfraConfig()
    snap = kc.strategy_config_snapshot(k)
    assert snap["MIN_CONF"] == 80.0 and snap["DIRECTION"] == "BOTH" and snap["COINS"]["BTC"]["sl_pct"] == 0.75
    assert kc.load_dotenv(os.path.join(d, "missing.env"), {}) == ([], [])


def test_repo_hygiene():
    gi = open(os.path.join(HERE, ".gitignore")).read().split()
    for pat in (".env", "*.pem", "*.zip", "__pycache__/"):
        assert pat in gi, pat
    tok = re.compile(r"\b[MNO][A-Za-z\d_-]{23,27}\.[A-Za-z\d_-]{6}\.[A-Za-z\d_-]{27,}\b")
    for root, dirs, files in os.walk(HERE):
        dirs[:] = [x for x in dirs if x not in (".git", "__pycache__", ".venv", "venv", "env")]
        for f in files:
            if f.endswith((".pyc", ".zip", ".pem")) or f == ".env":
                continue
            path = os.path.join(root, f)
            try:
                s = open(path, encoding="utf-8").read()
            except (UnicodeDecodeError, OSError):
                continue
            rel = os.path.relpath(path, HERE)
            if rel == "test_stage17.py":
                continue
            assert "PRIVATE KEY-----\n" not in s.replace("\r", "") or rel == "make_keys.py", rel
            assert not tok.search(s), rel
            assert not re.search(r"sk_live_[A-Za-z0-9]{8,}", s), rel
    ex_env = open(os.path.join(HERE, ".env.example")).read()
    for name in kc.SECRET_ENV_VARS:
        m = re.search(rf"^{name}=(.*)$", ex_env, re.M)
        assert m and m.group(1).strip() == "", name                       # placeholder only
    assert not re.search(r"^PERP_LIVE_VETO_ENABLED=", ex_env, re.M)


def test_interfaces_and_runner():
    assert isinstance(k, itf.LegacyStrategy)                               # the legacy module satisfies the protocol
    assert isinstance(ex.DisabledExecutionEngine(), object)
    for name in ("MarketDataProvider", "FeatureEngine", "ProbabilityModel", "Calibrator", "SignalEngine",
                 "RiskManager", "DecisionStore", "LegacyStrategy"):
        assert hasattr(itf, name)
    src = open(os.path.join(HERE, "run_all_tests.py")).read()
    assert "range(1, 22)" in src                                         # Step 2 added stage 18, Step 3 stage 19, Step 4 stage 20, Step 5 stage 21
    for f in ("test_stage16.py", "test_stage17.py"):
        s = open(os.path.join(HERE, f)).read()
        fn = s[s.index("def test_previous_stages"):].split("\ndef ")[0]
        assert 'os.environ.get("KALSHI_MASTER_TEST_RUN") == "1"' in fn and 'KALSHI_MASTER_TEST_RUN="1"' in fn, f


def test_previous_stages():
    # Master mode (run_all_tests.py): every stage is already run exactly once by the runner.
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    # Standalone: re-verify earlier stages, each EXACTLY once (children run in master mode).
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 17):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-600:], p.stderr[-600:])


if __name__ == "__main__":
    run("1 SignalDecision schema: fields, invariants, exact JSON round-trip", test_signal_schema_roundtrip)
    run("2 NO_CALL codes map 1:1 onto the legacy gates, same order, none invented", test_no_call_codes_match_legacy_code)
    run("3 adapter is read-only and fails closed", test_adapter_fails_closed)
    run("4 data-health representation (descriptive only)", test_data_health)
    run("5 discord is imported only by the legacy adapter; core imports without it", test_discord_confined_to_legacy_adapter)
    run("6 kalshi_bot.py exits cleanly without discord.py / without a token", test_kalshi_bot_is_optional)
    run("7 run_local.py --check works with no credentials and never prints secrets", test_local_check_without_credentials)
    run("8 local runtime end-to-end (no Discord, no network): call, NO_CALL, journal, web, shutdown",
        test_local_runtime_end_to_end)
    run("9 execution safety boundary: LIVE unavailable, prediction code cannot reach orders", test_execution_boundary)
    run("10 structured logging with secret redaction", test_logging_structured_and_redacted)
    run("11 configuration: env + .env (safety flags refused), secrets presence-only", test_configuration)
    run("12 repository hygiene: no secrets, .gitignore, .env.example placeholders", test_repo_hygiene)
    run("13 layer interfaces + runner covers stages 1-17", test_interfaces_and_runner)
    run("14 all previous stage suites", test_previous_stages)
    print("\nAll Stage 17 tests passed.")
