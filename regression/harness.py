"""
Deterministic regression harness for the LEGACY strategy (Step 1 behavioural contract).

It runs the UNMODIFIED production functions (kalshi_dashboard.evaluate, poller, settle_calls,
settle_pending, log_call, backtest_coin, run_backtest; kalshi_backtest.*; perp_probability.*;
perp_live.LiveVetoGate) against fixed inputs, with:

  * frozen time   - kalshi_dashboard's module attributes `dt` and `time` are swapped for shims
                    whose now()/time() return FROZEN_UTC; nothing in the strategy is edited;
  * no network    - the data functions (current_market, spot, candles, market_result,
                    fetch_history) are replaced by fixed inputs for the duration of a case;
  * no shared I/O - every file the legacy code writes goes to a fresh temporary directory;
  * LOCAL_TZ=None - time-of-day stats use the documented UTC fallback, so results do not depend
                    on whether the OS has IANA tzdata (Windows often does not).

Every patched attribute is restored after each case. Outputs are normalised through a JSON
round-trip so the stored expectations and fresh results compare like-for-like.
"""
import contextlib
import copy
import csv
import datetime as _datetime
import io
import json
import math
import os
import shutil
import sys
import tempfile
import types

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import kalshi_dashboard as k            # noqa: E402
import kalshi_backtest as kb            # noqa: E402
import perp_probability as pp           # noqa: E402
import perp_live as pl                  # noqa: E402
from kalshi_core import adapter         # noqa: E402

FIXTURE_SCHEMA_VERSION = 1
FROZEN_UTC = _datetime.datetime(2026, 9, 21, 14, 0, 0, tzinfo=_datetime.timezone.utc)   # a Monday
FROZEN_EPOCH = FROZEN_UTC.timestamp()
FLOAT_ABS_TOL = 1e-9
FLOAT_REL_TOL = 1e-12


class _FrozenDateTime(_datetime.datetime):
    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return cls.fromtimestamp(FROZEN_EPOCH, _datetime.timezone.utc).replace(tzinfo=None)
        return cls.fromtimestamp(FROZEN_EPOCH, tz)


class _StopLoop(BaseException):
    """Raised by the frozen sleep to end poller() after exactly one cycle."""


def _frozen_sleep(_s):
    raise _StopLoop()


def _no_network(*a, **kw):
    raise RuntimeError("network access is disabled in the regression harness")


@contextlib.contextmanager
def patched(obj, **attrs):
    missing = object()
    saved = {a: getattr(obj, a, missing) for a in attrs}
    try:
        for a, v in attrs.items():
            setattr(obj, a, v)
        yield
    finally:
        for a, v in saved.items():
            if v is missing:
                delattr(obj, a)
            else:
                setattr(obj, a, v)


@contextlib.contextmanager
def legacy_sandbox(extra=None):
    """Frozen clock, temp files, clean in-memory state, silenced stdout; everything restored."""
    d = tempfile.mkdtemp(prefix="kalshi_regr_")
    dt_shim = types.SimpleNamespace(datetime=_FrozenDateTime, timezone=_datetime.timezone,
                                    timedelta=_datetime.timedelta)
    time_shim = types.SimpleNamespace(time=lambda: FROZEN_EPOCH, sleep=_frozen_sleep)
    files = {n: os.path.join(d, n.lower()) for n in (
        "RESULTS_FILE", "CALLS_FILE", "TRADES_CSV", "PAPER_ORDERS_CSV", "EXPLAIN_MARK_FILE", "WEEKLY_MARK_FILE",
        "WATCHER_STATE_FILE", "BACKTEST_CACHE_FILE", "PERP_LIVE_PROMOTION_FILE", "PERP_LIVE_VETO_JOURNAL",
        "PERP_SHADOW_POLICY_FILE", "PERP_SHADOW_JOURNAL", "PERP_LOG_FILE")}
    files["PERP_LIVE_VETO_KILL_FILE"] = os.path.join(d, "kill")
    saved_state = {"_pending": dict(k._pending), "_call_pending": dict(k._call_pending),
                   "_alerted": set(k._alerted), "STATE": copy.deepcopy(k.STATE),
                   "running": k.RUNNING.is_set()}
    attrs = dict(files, dt=dt_shim, time=time_shim, LOCAL_TZ=None, ON_CALL=None, POST_EMBED=None,
                 _get=_no_network, market_result=lambda tk: "", fetch_history=lambda product, days: {},
                 _gate=None, _perp=None, _shadow=None, PERP_TELEMETRY_ENABLED=False,
                 PERP_LIVE_VETO_ENABLED=False, ACTIVE_COINS=dict(k.ACTIVE_COINS), DIRECTION=k.DIRECTION,
                 ENTRY_MODE=k.ENTRY_MODE, BANKROLL=k.BANKROLL)
    attrs.update(extra or {})
    k._pending.clear(); k._call_pending.clear(); k._alerted.clear(); k.RUNNING.set()
    try:
        with patched(k, **attrs), contextlib.redirect_stdout(io.StringIO()):
            yield d
    finally:
        k._pending.clear(); k._pending.update(saved_state["_pending"])
        k._call_pending.clear(); k._call_pending.update(saved_state["_call_pending"])
        k._alerted.clear(); k._alerted.update(saved_state["_alerted"])
        k.STATE.clear(); k.STATE.update(saved_state["STATE"])
        (k.RUNNING.set if saved_state["running"] else k.RUNNING.clear)()
        shutil.rmtree(d, ignore_errors=True)


def normalise(obj):
    return json.loads(json.dumps(obj, sort_keys=True, default=_json_default))


def _json_default(o):
    if isinstance(o, (set, frozenset)):
        return sorted(o)
    if hasattr(o, "value"):
        return o.value
    raise TypeError(f"not JSON serialisable: {type(o).__name__}")


def _exc(e):
    return {"raises": type(e).__name__, "message": str(e)}


# ═══════════════════ evaluate() ═══════════════════
def _patch_data(market_by_series, spot_by_product, candles_by_product):
    return dict(current_market=lambda series: copy.deepcopy(market_by_series.get(series)),
                spot=lambda product: spot_by_product[product],
                candles=lambda product: copy.deepcopy(candles_by_product[product]))


def _sizing(r):
    if not isinstance(r, dict) or r.get("status") != "ok" or r.get("side_ask") is None:
        return None
    return {"size_fraction": k._size_fraction(r), "contracts": k._contracts(r), "limit_price": k._limit_price(r),
            "call_margin": k._call_margin(r)}


def run_evaluate(case):
    inp = case["input"]
    cfg = inp["cfg"]
    with legacy_sandbox(inp.get("controls")):
        with patched(k, **_patch_data({cfg["series"]: inp["market"]}, {cfg["product"]: inp["spot"]},
                                      {cfg["product"]: inp["candles"]})):
            try:
                r = k.evaluate(inp["coin"], cfg)
            except Exception as e:                      # pinned: e.g. missing side ask
                r = _exc(e)
            pending = copy.deepcopy(k._pending)
            out = {"result": r, "pending": pending,
                   "p_down": (100.0 - r["p_up"]) if isinstance(r, dict) and "p_up" in r else None,
                   "sizing": _sizing(r)}
            sd_input = r if "raises" not in r else {"coin": inp["coin"], "status": "error: " + r["message"]}
            out["signal_decision"] = adapter.from_evaluation(sd_input, coin=inp["coin"], series=cfg["series"]).to_dict()
    return normalise(out)


# ═══════════════════ poller(): one full cycle ═══════════════════
def run_poller(case):
    inp = case["input"]
    events = []
    extra = dict(inp.get("controls") or {})
    extra["ON_CALL"] = lambda coin, r, crec: events.append(["on_call", coin, r.get("ticker"), r.get("fav")])
    extra["POST_EMBED"] = lambda dest, emb: events.append(["post_embed", dest, emb.get("title")])
    with legacy_sandbox(extra):
        with patched(k, **_patch_data(inp["markets"], inp["spots"], inp["candles"])):
            gate = inp.get("gate")
            gate_patch = {}
            if gate == "block":
                gate_patch["_gate_decision"] = lambda coin, r: {"decision": "BLOCK", "reason": "BLOCK_STEP4_CONFLICT"}
            if inp.get("paused"):
                k.RUNNING.clear()
            for t in inp.get("already_alerted", []):
                k._alerted.add(t)
            cycles = []
            with patched(k, **gate_patch):
                for _ in range(inp.get("cycles", 1)):
                    try:
                        k.poller()
                    except _StopLoop:
                        pass
                    cycles.append({"coins": copy.deepcopy(k.STATE.get("coins")),
                                   "calls": copy.deepcopy(k.STATE.get("calls")),
                                   "updated": k.STATE.get("updated")})
            paper = []
            if os.path.exists(k.PAPER_ORDERS_CSV):
                with open(k.PAPER_ORDERS_CSV, newline="") as f:
                    paper = list(csv.DictReader(f))
            out = {"cycles": cycles, "call_pending": copy.deepcopy(k._call_pending),
                   "alerted": sorted(k._alerted), "pending": copy.deepcopy(k._pending),
                   "paper_orders": paper, "events": events,
                   "gate": (k._gate.health() if k._gate is not None else None)}
    return normalise(out)


# ═══════════════════ settlement ═══════════════════
def run_settle_calls(case):
    inp = case["input"]
    events = []
    with legacy_sandbox({"POST_EMBED": lambda dest, emb: events.append([dest, emb.get("title")])}):
        with open(k.CALLS_FILE, "w") as f:
            json.dump(inp.get("existing_rows", []), f)
        k._call_pending.update(copy.deepcopy(inp["call_pending"]))
        results = inp["results"]
        with patched(k, market_result=lambda tk: results.get(tk, "")):
            rec = k.settle_calls()
        with open(k.CALLS_FILE) as f:
            rows = json.load(f)
        trades = []
        if os.path.exists(k.TRADES_CSV):
            with open(k.TRADES_CSV, newline="") as f:
                trades = list(csv.DictReader(f))
        out = {"record": rec, "rows": rows, "trades_csv": trades, "still_pending": sorted(k._call_pending),
               "events": events}
    return normalise(out)


def run_settle_pending(case):
    inp = case["input"]
    with legacy_sandbox():
        k._pending.update(copy.deepcopy(inp["pending"]))
        results = inp["results"]
        with patched(k, market_result=lambda tk: results.get(tk, "")):
            rows = k.settle_pending()
            stats = k.compute_stats(rows)
        out = {"rows": rows, "stats": stats, "still_pending": sorted(k._pending)}
    return normalise(out)


def run_log_call(case):
    inp = case["input"]
    with legacy_sandbox(inp.get("controls")):
        k.log_call(inp["coin"], copy.deepcopy(inp["r"]))
        out = {"call_pending": copy.deepcopy(k._call_pending)}
    return normalise(out)


def run_fees(case):
    return normalise({"fees": [[p, c, k.kalshi_fee_cents(p, c)] for p, c in case["input"]["grid"]]})


# ═══════════════════ backtests ═══════════════════
def _hist(pairs):
    return {int(t): float(c) for t, c in pairs}


def run_backtest_coin(case):
    inp = case["input"]
    cd = _hist(inp["history"])
    out = {}
    with legacy_sandbox():
        out["dashboard_backtest_coin"] = k.backtest_coin(inp["coin"], inp["cfg"], cd)
    tr = kb.backtest_coin(inp["coin"], inp["cfg"], cd)
    out["standalone_backtest_coin"] = tr
    out["standalone_metrics"] = {"block": kb.block(tr), "brier": kb.brier(tr), "logloss": kb.logloss(tr),
                                 "ece": kb.ece(tr)}
    return normalise(out)


def run_backtest_full(case):
    inp = case["input"]
    hist = {p: _hist(pairs) for p, pairs in inp["histories"].items()}
    with legacy_sandbox():
        with patched(k, fetch_history=lambda product, days: dict(hist.get(product, {}))):
            try:
                res = k.run_backtest(inp.get("range"))
            except Exception as e:
                res = _exc(e)
    return normalise({"run_backtest": res})


# ═══════════════════ perp overlay / live veto ═══════════════════
def run_overlay(case):
    out = []
    for a in case["input"]["calls"]:
        try:
            if a["fn"] == "overlay":
                out.append(pp.overlay(a["p_base_up"], a["p_b_up"], a["p_c_up"], a["alpha"], a["fav"],
                                      a["side_ask"], a["constants"], a.get("legacy_signal", True)))
            else:
                out.append(pp.decide(a["p_up"], a["fav"], a["side_ask"], a["constants"], a.get("legacy_signal", True)))
        except Exception as e:
            out.append(_exc(e))
    return normalise({"results": out})


def run_gate_inactive(case):
    d = tempfile.mkdtemp(prefix="kalshi_gate_")
    try:
        g = pl.LiveVetoGate.from_files(os.path.join(d, "none.json"), os.path.join(d, "p.json"),
                                       os.path.join(HERE, "step5_baseline_manifest.json"),
                                       os.path.join(HERE, "kalshi_dashboard.py"), os.path.join(d, "v.csv"),
                                       env_enabled=case["input"]["env_enabled"], kill_file=os.path.join(d, "kill"))
        with contextlib.redirect_stdout(io.StringIO()):
            res = [g.evaluate(r["coin"], r, None) for r in case["input"]["results"]]
        return normalise({"status": g.status, "active": g.active, "results": res})
    finally:
        shutil.rmtree(d, ignore_errors=True)


RUNNERS = {"evaluate": run_evaluate, "poller": run_poller, "settle_calls": run_settle_calls,
           "settle_pending": run_settle_pending, "log_call": run_log_call, "fees": run_fees,
           "backtest_coin": run_backtest_coin, "backtest_full": run_backtest_full, "overlay": run_overlay,
           "gate_inactive": run_gate_inactive}


def run_case(case):
    return RUNNERS[case["kind"]](case)


# ═══════════════════ comparison ═══════════════════
def compare(expected, actual, path=""):
    """List of human-readable differences (empty == identical within float tolerance)."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        diffs = []
        for key in sorted(set(expected) | set(actual)):
            if key not in actual:
                diffs.append(f"{path}/{key}: missing in actual")
            elif key not in expected:
                diffs.append(f"{path}/{key}: unexpected in actual")
            else:
                diffs += compare(expected[key], actual[key], f"{path}/{key}")
        return diffs
    if isinstance(expected, list) and isinstance(actual, list):
        if len(expected) != len(actual):
            return [f"{path}: length {len(expected)} != {len(actual)}"]
        out = []
        for i, (a, b) in enumerate(zip(expected, actual)):
            out += compare(a, b, f"{path}[{i}]")
        return out
    if isinstance(expected, bool) or isinstance(actual, bool):
        return [] if expected is actual else [f"{path}: {expected!r} != {actual!r}"]
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        if isinstance(expected, float) and isinstance(actual, float) and math.isnan(expected) and math.isnan(actual):
            return []
        if math.isclose(expected, actual, rel_tol=FLOAT_REL_TOL, abs_tol=FLOAT_ABS_TOL):
            return []
        return [f"{path}: {expected!r} != {actual!r}"]
    return [] if expected == actual else [f"{path}: {expected!r} != {actual!r}"]
