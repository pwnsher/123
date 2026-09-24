#!/usr/bin/env python3
"""
Strategy BASELINE manifest + EXTENDED strategy fingerprint.

    py -m kalshi_core.baseline --verify          # exit 0 = matches config/strategy_baseline.json
    py -m kalshi_core.baseline --print           # show the manifest the current code would produce
    py -m kalshi_core.baseline --write --i-intend-to-change-the-baseline

Two fingerprints protect the strategy:

1. legacy_strategy_fingerprint  (strategy_fingerprint.py, UNCHANGED, format v2)
   14 constants + 10 functions of kalshi_dashboard.py. The Step 6 live-veto gate and the
   promotion tooling depend on it, so it is only READ here and must equal the value pinned in
   step5_baseline_manifest.json.

2. extended_strategy_fingerprint  (this module, new in Step 1)
   A strict superset that also pins code the legacy fingerprint does not see but that changes
   what the bot calls, or how results are evaluated:
     * kalshi_dashboard.py: market selection/book parsing/strike parsing, the poller's
       first-signal + live-veto gating, the web-mutable filters (ACTIVE_COINS, DIRECTION), poll
       cadence, settlement/stat helpers, the built-in backtest and its constants;
     * kalshi_backtest.py: the standalone backtest + calibration metrics (methodology);
     * perp_probability.py / perp_live.py / perp_shadow.py: the overlay maths and the frozen
       live-veto safety limits.
   Same canonical-AST hashing as strategy_fingerprint.py (comments, docstrings, whitespace and
   line numbers are ignored; any semantic change is not).

config/strategy_baseline.json is REGENERATED from the source and compared as a whole, so a change
to any extracted value, function or class shows up as a named difference. The file contains no
timestamps or interpreter details, so it is identical on every supported Python (3.10-3.13).
Source files are parsed, never imported or executed. No network.
"""
import argparse
import ast
import hashlib
import json
import os
import sys

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import strategy_fingerprint as sf                                        # noqa: E402
from kalshi_core.no_call import LEGACY_GATE_ORDER, LEGACY_REASON_MAP, NoCallReason   # noqa: E402

BASELINE_PATH = os.path.join(HERE, "config", "strategy_baseline.json")
LEGACY_MANIFEST_PATH = os.path.join(HERE, "step5_baseline_manifest.json")
MANIFEST_NAME = "kalshi_strategy_baseline"
MANIFEST_SCHEMA_VERSION = 1
EXTENDED_FORMAT_VERSION = 1
EXTENDED_ALGORITHM = "canonical_ast_v2_sha256/extended_v1"
MODEL_ID = "legacy_lognormal_strike_cdf_v1"          # == kalshi_core.adapter.LEGACY_MODEL_ID
STRATEGY_VERSION = "legacy-callout-v1 (kalshi_optimized_final + perp research steps 1-6)"

# file -> what to pin. Constants: literal value, or the canonical AST hash of a non-literal expr.
EXTENDED_SPEC = {
    "kalshi_dashboard.py": {
        "constants": sf.STRATEGY_CONSTANTS + (
            "ACTIVE_COINS", "DIRECTION", "POLL_SECONDS", "GATE_PRICE", "BACKTEST_DAYS",
            "BACKTEST_RECENT_HOURS", "KALSHI_BASE", "COINBASE_BASE",
            "PERP_TELEMETRY_ENABLED", "PERP_SHADOW_ENABLED", "PERP_LIVE_VETO_ENABLED",
            "PERP_LIVE_PROMOTION_FILE", "PERP_LIVE_VETO_KILL_FILE", "PERP_STEP5_BASELINE_MANIFEST",
            "PERP_ALIGNMENT_MAX_LAG_SECONDS"),
        "functions": sf.STRATEGY_FUNCTIONS + (
            "strike_of", "_cents", "book_of", "current_market", "candles", "spot", "_get", "market_result",
            "settle_pending", "compute_stats", "call_record", "_vol_at", "backtest_coin", "run_backtest",
            "parse_range", "poller", "_gate_instance", "_gate_decision", "_append_paper", "paper_enter",
            "apply_control", "load_watcher_state", "set_watcher_state"),
        "classes": (),
    },
    "kalshi_backtest.py": {
        "constants": ("COINS", "INTERVAL_MIN", "MAX_ENTRY_MIN", "LAST_STOP_MIN", "MIN_CONF",
                      "VOL_LOOKBACK_MIN", "VOL_MULT", "FEE_CENTS", "GATE_PRICE", "COINBASE_BASE"),
        "functions": ("norm_cdf", "conf_side", "fetch_candles", "vol_at", "backtest_coin", "block", "_pw",
                      "brier", "logloss", "ece"),
        "classes": (),
    },
    "perp_probability.py": {
        "constants": ("OVERLAY_VERSION", "MAX_ABS_PERP_DELTA", "ALPHA_GRID", "LEGACY_ALPHA", "P_EPS",
                      "REQUIRED_CONSTANTS"),
        "functions": ("_finite", "clamp_p", "perp_delta", "cap_delta", "check_alpha", "blend", "favored",
                      "direction_flips", "integrated_conf", "edges", "check_constants", "decide", "overlay"),
        "classes": (),
    },
    "perp_live.py": {
        "constants": ("MODE", "CONFIRM_PHRASE", "DEFAULT_EXPIRY_DAYS", "MAX_EXPIRY_DAYS",
                      "MAX_CONSECUTIVE_GATE_ERRORS", "LIVE_VETO_ROLLING_WINDOW", "LIVE_VETO_MIN_WINDOW",
                      "LIVE_VETO_MAX_BLOCK_FRACTION", "LIVE_VETO_MAX_FAIL_OPEN_FRACTION", "MAX_GATE_LATENCY_MS",
                      "REQUIRED_SCOPE"),
        "functions": ("validate_promotion", "_fav"),
        "classes": ("LiveVetoGate",),
    },
    "perp_shadow.py": {
        "constants": ("MODE",),
        "functions": ("p_favored", "validate_policy"),
        "classes": ("FrozenPolicy",),
    },
}


class BaselineError(Exception):
    pass


def _sha(obj):
    return hashlib.sha256(sf.canonical_json(obj).encode()).hexdigest()


def _node_hash(node):
    return hashlib.sha256(sf.canonical_json(sf.canonicalize_ast(sf._strip_docstrings(node))).encode()).hexdigest()


def _parse(path):
    if not os.path.exists(path):
        raise BaselineError(f"source not found: {path}")
    with open(path, encoding="utf-8") as f:
        return ast.parse(f.read())


def _top_level(tree):
    consts, fns, classes = {}, {}, {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            consts.setdefault(node.targets[0].id, []).append(node.value)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            fns.setdefault(node.name, []).append(node)
        elif isinstance(node, ast.ClassDef):
            classes.setdefault(node.name, []).append(node)
    return consts, fns, classes


def _one(table, name, kind, path):
    nodes = table.get(name)
    if not nodes:
        raise BaselineError(f"{path}: {kind} {name} not found")
    if len(nodes) > 1:
        raise BaselineError(f"{path}: {kind} {name} defined more than once")
    return nodes[0]


def _const_value(expr):
    try:
        v = ast.literal_eval(expr)
        return json.loads(sf.canonical_json(v))           # tuples -> lists, deterministic
    except ValueError:
        return {"_expr": ast.unparse(expr), "_expr_sha256": _node_hash(expr)}


def extract_file(path, spec):
    consts, fns, classes = _top_level(_parse(path))
    out = {"constants": {}, "functions": {}, "classes": {}}
    for c in spec["constants"]:
        out["constants"][c] = _const_value(_one(consts, c, "constant", path))
    for f in spec["functions"]:
        out["functions"][f] = _node_hash(_one(fns, f, "function", path))
    for c in spec["classes"]:
        out["classes"][c] = _node_hash(_one(classes, c, "class", path))
    return out


def function_defaults(path, fn_name):
    fn = _one(_top_level(_parse(path))[1], fn_name, "function", path)
    names = [a.arg for a in fn.args.args][-len(fn.args.defaults):] if fn.args.defaults else []
    return {n: ast.literal_eval(d) for n, d in zip(names, fn.args.defaults)}


def extended_components(root=HERE):
    return {f: extract_file(os.path.join(root, f), spec) for f, spec in EXTENDED_SPEC.items()}


def extended_fingerprint(components, legacy_fp):
    return _sha({"extended_format_version": EXTENDED_FORMAT_VERSION, "algorithm": EXTENDED_ALGORITHM,
                 "legacy_strategy_fingerprint": legacy_fp, "components": components})


def build_manifest(root=HERE):
    dash = os.path.join(root, "kalshi_dashboard.py")
    legacy_fp, legacy_cfg_hash, _, legacy_fns = sf.current_fingerprint(dash)
    comp = extended_components(root)
    d = comp["kalshi_dashboard.py"]["constants"]
    bt = comp["kalshi_backtest.py"]["constants"]
    pp = comp["perp_probability.py"]["constants"]
    pl = comp["perp_live.py"]["constants"]
    chop = function_defaults(dash, "crossings")
    coins = d["COINS"]
    return {
        "manifest": MANIFEST_NAME,
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "regenerate_with": "py -m kalshi_core.baseline --write --i-intend-to-change-the-baseline",
        "verify_with": "py -m kalshi_core.baseline --verify",
        "note": ("Values under 'extracted' are read from the source by AST. Text under 'description' "
                 "is hand-written and verified against the code; the code is authoritative."),
        "strategy": {
            "strategy_version": STRATEGY_VERSION,
            "model_id": MODEL_ID,
            "supported_assets": sorted(coins),
            "markets": {c: {"kalshi_series": v["series"], "spot_product": v["product"]} for c, v in sorted(coins.items())},
            "description": {
                "prediction_method": (
                    "Driftless log-normal (Brownian) model of spot vs strike. sigma = sample stdev of 1-min "
                    "log returns of the newest VOL_LOOKBACK_MIN+1 COMPLETED Coinbase 1-min closes (the last, "
                    "possibly partial, candle is dropped) times VOL_MULT. z = ln(spot/strike) / "
                    "(sigma * sqrt(max(minutes_left, 1e-4))). P(UP) = Phi(z)."),
                "direction": "UP if P(UP) >= 50% else DOWN (ties go UP).",
                "confidence": "conf = max(P(UP), 100 - P(UP)) in percent: the model probability of the favoured side.",
                "edge": ("raw_edge = conf - side_ask (cents, side_ask = ask of the favoured side); "
                         "net_edge = raw_edge - ENTRY_COST_CENTS."),
                "calibration": ("NONE in the live path: the raw model probability is used directly. Calibration "
                                "is only MEASURED offline (kalshi_backtest.py: Brier, log loss, ECE on the newest "
                                "30% of settled backtest trades; kalshi_dashboard.run_backtest: 80/85/90/95 bands)."),
                "stop": ("rec_stop = round(side_ask * sl_pct) per coin (display/paper). Paper settlement: a stop "
                         "triggers when the lowest observed BID of the held side <= stop; exit at min(bid_lo, stop) "
                         "(gap modelled at the worse price) and the trade is a loss; otherwise settle at 100/0."),
                "sizing": ("Informational only (no orders): quarter-Kelly on (conf - 2)% vs ask, capped at 5% of "
                           "BANKROLL, 0 when net_edge <= 0."),
                "strategy_timing": ("The watcher evaluates every coin every POLL_SECONDS; the FIRST signal per "
                                    "market ticker produces the call; later signals for that ticker are ignored."),
            },
        },
        "extracted": {
            "entry_window": {"max_minutes_left": d["MAX_ENTRY_MIN"], "min_minutes_left": d["LAST_STOP_MIN"],
                             "inclusive_both_ends": True, "interval_minutes": d["INTERVAL_MIN"],
                             "source": "kalshi_dashboard.evaluate: in_win = LAST_STOP_MIN <= remain <= MAX_ENTRY_MIN"},
            "thresholds": {"MIN_CONF_pct": d["MIN_CONF"], "MIN_PRICE_cents": d["MIN_PRICE"],
                           "EDGE_THRESH_cents": d["EDGE_THRESH"], "ENTRY_COST_CENTS": d["ENTRY_COST_CENTS"],
                           "net_edge_rule": "net_edge > 0 AND net_edge >= EDGE_THRESH",
                           "CHOP_MAX_crossings": d["CHOP_MAX"], "chop_window_minutes": chop.get("window"),
                           "chop_rule": "strike crossings of the last window+1 1-min closes > CHOP_MAX"},
            "volatility": {"VOL_LOOKBACK_MIN": d["VOL_LOOKBACK_MIN"], "VOL_MULT": d["VOL_MULT"],
                           "candle_granularity_s": 60, "min_completed_closes": 20,
                           "source": "kalshi_dashboard.evaluate / candles (literals inside pinned functions)"},
            "stops": {c: {"sl_pct": v["sl_pct"]} for c, v in sorted(coins.items())},
            "settlement": {
                "source": "Kalshi GET /markets/{ticker} -> market.result ('yes' = UP won, 'no' = DOWN won)",
                "first_check_after_close_s": 20, "give_up_calls_after_close_min": 30,
                "give_up_stats_after_close_min": 10,
                "fee_model": "kalshi_fee_cents: ceil(0.07 * C * P * (1-P) * 100) cents, min 1 (0 when contracts == 0), P clamped to [0.01, 0.99]",
                "round_trip_fee": "fee(entry, 1) + fee(exit_price, 1) per contract",
                "unfilled_maker": "a resting maker order whose ask never traded down to the limit is not a trade",
                "ENTRY_MODE_default": d["ENTRY_MODE"], "FEE_CENTS_backtest": d["FEE_CENTS"]},
            "web_filters": {"ACTIVE_COINS": d["ACTIVE_COINS"], "DIRECTION": d["DIRECTION"],
                            "note": "baseline literals; mutable at runtime from the local web page"},
            "runtime": {"POLL_SECONDS": d["POLL_SECONDS"], "BANKROLL": d["BANKROLL"],
                        "KALSHI_BASE": d["KALSHI_BASE"], "COINBASE_BASE": d["COINBASE_BASE"]},
            "perp_overlay": {
                "telemetry_enabled_default": d["PERP_TELEMETRY_ENABLED"],
                "shadow_enabled_default": d["PERP_SHADOW_ENABLED"],
                "live_veto_enabled_default": d["PERP_LIVE_VETO_ENABLED"],
                "live_veto_requires": ["valid manual promotion artifact " + d["PERP_LIVE_PROMOTION_FILE"],
                                       "PERP_LIVE_VETO_ENABLED=1 in the environment",
                                       "legacy fingerprint == " + d["PERP_STEP5_BASELINE_MANIFEST"]],
                "kill_file_default": d["PERP_LIVE_VETO_KILL_FILE"],
                "mode": pl["MODE"], "scope": pl["REQUIRED_SCOPE"],
                "overlay_version": pp["OVERLAY_VERSION"], "max_abs_perp_delta": pp["MAX_ABS_PERP_DELTA"],
                "alpha_grid": pp["ALPHA_GRID"], "alignment_max_lag_s": d["PERP_ALIGNMENT_MAX_LAG_SECONDS"],
                "breaker": {"max_consecutive_errors": pl["MAX_CONSECUTIVE_GATE_ERRORS"],
                            "rolling_window": pl["LIVE_VETO_ROLLING_WINDOW"], "min_window": pl["LIVE_VETO_MIN_WINDOW"],
                            "max_block_fraction": pl["LIVE_VETO_MAX_BLOCK_FRACTION"],
                            "max_fail_open_fraction": pl["LIVE_VETO_MAX_FAIL_OPEN_FRACTION"],
                            "max_gate_latency_ms": pl["MAX_GATE_LATENCY_MS"]},
                "effect": "suppress-only: can remove an already-valid legacy call; never creates, flips or sizes"},
            "no_call_gates": {
                "evaluate_order": [{"legacy": g, "reason": LEGACY_REASON_MAP[g].value} for g in LEGACY_GATE_ORDER],
                "evaluate_data_guard": {"legacy": "BLOCK_STALE_DATA", "reason": NoCallReason.DATA_STALE.value},
                "evaluate_status": {"no market": NoCallReason.MARKET_UNAVAILABLE.value,
                                    "net error": NoCallReason.DATA_UNAVAILABLE.value,
                                    "error: ...": NoCallReason.EVALUATION_ERROR.value},
                "poller": [NoCallReason.ALREADY_CALLED_THIS_MARKET.value, NoCallReason.PERP_VETO_SUPPRESSED.value,
                           NoCallReason.WATCHER_PAUSED.value]},
            "required_data_feeds": [
                {"feed": "kalshi_market", "endpoint": d["KALSHI_BASE"] + "/markets?series_ticker=...&status=open",
                 "used_for": "strike (floor_strike|cap_strike|strike), yes/no bid/ask, close_time, ticker"},
                {"feed": "coinbase_spot", "endpoint": d["COINBASE_BASE"] + "/products/{product}/ticker",
                 "used_for": "spot price"},
                {"feed": "coinbase_candles", "endpoint": d["COINBASE_BASE"] + "/products/{product}/candles?granularity=60",
                 "used_for": "volatility, chop, display indicators (cached 30 s)"},
                {"feed": "kalshi_settlement", "endpoint": d["KALSHI_BASE"] + "/markets/{ticker}",
                 "used_for": "market.result for paper settlement"}],
            "backtest": {
                "dashboard_builtin": {"GATE_PRICE": d["GATE_PRICE"], "BACKTEST_DAYS": d["BACKTEST_DAYS"],
                                      "BACKTEST_RECENT_HOURS": d["BACKTEST_RECENT_HOURS"], "FEE_CENTS": d["FEE_CENTS"]},
                "standalone_kalshi_backtest": bt,
                "method": ("Coinbase 1-min closes; strike = close at the 15-min boundary; first minute in the "
                           "entry window with conf >= MIN_CONF; entry priced at the model confidence (NOT a "
                           "real Kalshi ask); stop when the model-priced favoured side <= entry * sl_pct; else "
                           "settle on close at boundary+15 >= strike. MIN_PRICE, edge and chop gates are NOT "
                           "applied in either backtest."),
                "known_discrepancy": "GATE_PRICE is 90.0 in kalshi_dashboard.py but 85.0 in kalshi_backtest.py"},
        },
        "config_classification": {
            "strategy_critical": sorted(set(sf.STRATEGY_CONSTANTS) | {"ACTIVE_COINS", "DIRECTION", "POLL_SECONDS",
                                                                     "GATE_PRICE"}),
            "infrastructure_env": ["KALSHI_PERP_TELEMETRY", "KALSHI_PERP_API_BASE", "KALSHI_PERP_TICKER_<COIN>",
                                   "KALSHI_PERP_SHADOW", "PERP_LIVE_VETO_KILL_FILE", "KALSHI_LOG_LEVEL",
                                   "KALSHI_LOG_FORMAT", "KALSHI_LOG_FILE", "KALSHI_DECISION_JOURNAL",
                                   "KALSHI_DASHBOARD_PORT", "KALSHI_ENV"],
            "safety_activation_env": ["PERP_LIVE_VETO_ENABLED"],
            "secrets_env": ["DISCORD_BOT_TOKEN", "KALSHI_API_KEY_ID", "KALSHI_PRIVATE_KEY_PATH (path)"]},
        "fingerprints": {
            "legacy_strategy_fingerprint": legacy_fp,
            "legacy_strategy_config_hash": legacy_cfg_hash,
            "legacy_fingerprint_format": f"{sf.FINGERPRINT_FORMAT_VERSION}/{sf.FINGERPRINT_ALGORITHM}",
            "legacy_manifest": "step5_baseline_manifest.json",
            "legacy_function_hashes": legacy_fns,
            "extended_format": f"{EXTENDED_FORMAT_VERSION}/{EXTENDED_ALGORITHM}",
            "extended_strategy_fingerprint": extended_fingerprint(comp, legacy_fp),
            "extended_components": comp},
    }


def _diff(a, b, path=""):
    out = []
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                out.append(f"{path}/{k}: added")
            elif k not in b:
                out.append(f"{path}/{k}: removed")
            else:
                out += _diff(a[k], b[k], f"{path}/{k}")
    elif a != b:
        out.append(f"{path}: {json.dumps(a, default=str)[:80]} -> {json.dumps(b, default=str)[:80]}")
    return out


def verify(root=HERE, baseline_path=None, legacy_manifest_path=None):
    """Returns (ok, problems). Checks (1) the committed baseline equals the regenerated one and
    (2) the legacy fingerprint still equals step5_baseline_manifest.json."""
    baseline_path = baseline_path or os.path.join(root, "config", "strategy_baseline.json")
    legacy_manifest_path = legacy_manifest_path or os.path.join(root, "step5_baseline_manifest.json")
    problems = []
    try:
        current = build_manifest(root)
    except (BaselineError, sf.FingerprintError, OSError, SyntaxError) as e:
        return False, [f"cannot fingerprint current code: {e}"]
    try:
        with open(baseline_path, encoding="utf-8") as f:
            committed = json.load(f)
    except (OSError, ValueError) as e:
        return False, [f"cannot read {baseline_path}: {e}"]
    if committed != current:
        problems += [f"baseline differs {p}" for p in _diff(committed, current)]
    try:
        with open(legacy_manifest_path, encoding="utf-8") as f:
            legacy = json.load(f)
        ok, why = sf.verify(os.path.join(root, "kalshi_dashboard.py"), legacy)
        if not ok:
            problems += [f"legacy: {w}" for w in why]
    except (OSError, ValueError) as e:
        problems.append(f"cannot read legacy manifest: {e}")
    return not problems, problems


def dumps(manifest):
    return json.dumps(manifest, indent=1, sort_keys=True, ensure_ascii=True) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser(description="Strategy baseline manifest / extended fingerprint")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--verify", action="store_true", help="verify against config/strategy_baseline.json (default)")
    g.add_argument("--print", action="store_true", help="print the manifest for the current code")
    g.add_argument("--write", action="store_true", help="rewrite config/strategy_baseline.json")
    ap.add_argument("--i-intend-to-change-the-baseline", action="store_true", dest="intend")
    a = ap.parse_args(argv)
    if a.print:
        sys.stdout.write(dumps(build_manifest()))
        return 0
    if a.write:
        if not a.intend:
            print("Refusing: rewriting the baseline accepts a STRATEGY CHANGE. Re-run with "
                  "--i-intend-to-change-the-baseline and record why in docs/BASELINE.md.", file=sys.stderr)
            return 2
        os.makedirs(os.path.dirname(BASELINE_PATH), exist_ok=True)
        with open(BASELINE_PATH, "w", encoding="utf-8", newline="\n") as f:
            f.write(dumps(build_manifest()))
        print(f"wrote {BASELINE_PATH}")
        return 0
    ok, problems = verify()
    m = build_manifest() if ok else None
    print("strategy baseline: " + ("MATCHES" if ok else "MISMATCH"))
    if m:
        print(f"  legacy_strategy_fingerprint   {m['fingerprints']['legacy_strategy_fingerprint']}")
        print(f"  extended_strategy_fingerprint {m['fingerprints']['extended_strategy_fingerprint']}")
    for p in problems:
        print(f"  - {p}")
    return 0 if ok else 3


if __name__ == "__main__":
    sys.exit(main())
