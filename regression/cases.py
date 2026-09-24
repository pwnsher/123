"""
Deterministic INPUTS for the strategy regression fixtures.

Inputs are synthetic but realistic-shaped (Kalshi market objects in the API's *_dollars
format, oldest-first Coinbase 1-minute closes). They are built once by generate.py and STORED
in strategy_cases.json together with the outputs of the CURRENT code, so the test never
depends on this module producing the same numbers again.

Each case has an "intent": the legacy code path it was designed to exercise. generate.py
asserts that the current code really takes that path (so a fixture cannot silently cover the
wrong branch); the stored expectation is always what the code actually returned.
"""
import datetime as _dt
import math

from regression.harness import FROZEN_UTC, FROZEN_EPOCH

BTC = {"series": "KXBTC15M", "product": "BTC-USD", "sl_pct": 0.75}
ETH = {"series": "KXETH15M", "product": "ETH-USD", "sl_pct": 0.60}
SOL = {"series": "KXSOL15M", "product": "SOL-USD", "sl_pct": 0.75}
XRP = {"series": "KXXRP15M", "product": "XRP-USD", "sl_pct": 0.65}
CFG = {"BTC": BTC, "ETH": ETH, "SOL": SOL, "XRP": XRP}


def iso_z(d):
    return d.isoformat().replace("+00:00", "Z")


def market(ticker, minutes_left, strike, yes_bid=None, yes_ask=None, no_bid=None, no_ask=None,
           strike_key="floor_strike"):
    m = {"ticker": ticker, "close_time": iso_z(FROZEN_UTC + _dt.timedelta(minutes=minutes_left)), strike_key: strike}
    for key, val in (("yes_bid_dollars", yes_bid), ("yes_ask_dollars", yes_ask),
                     ("no_bid_dollars", no_bid), ("no_ask_dollars", no_ask)):
        if val is not None:
            m[key] = f"{val / 100:.4f}"
    return m


def _lcg(seed):
    x = seed & 0xFFFFFFFF
    while True:
        x = (1664525 * x + 1013904223) & 0xFFFFFFFF
        yield (x / 4294967296.0 - 0.5) * math.sqrt(12.0)          # mean 0, variance 1


def walk(n, last, sigma, seed):
    """n closes, oldest first, ending exactly at `last`, log-increments ~ sigma * U(-sqrt3, sqrt3)."""
    g = _lcg(seed)
    out = [last]
    for _ in range(n - 1):
        out.append(out[-1] * math.exp(-sigma * next(g)))
    return [round(v, 6) for v in reversed(out)]


def candles(closes, spot):
    """Coinbase-shaped candle dict: completed closes + the current partial minute at `spot`."""
    cl = list(closes) + [spot]
    return {"close": cl, "high": [round(c * 1.0005, 6) for c in cl], "low": [round(c * 0.9995, 6) for c in cl]}


def ev_case(cid, desc, intent, coin, mkt, spot, closes, controls=None):
    return {"id": cid, "kind": "evaluate", "description": desc, "intent": intent,
            "input": {"coin": coin, "cfg": CFG[coin], "market": mkt, "spot": spot,
                      "candles": candles(closes, spot), "controls": controls or {}}}


def evaluate_cases():
    c = []
    calm = walk(80, 100.35, 0.0008, 11)            # stays well above 100 over the last 16 minutes
    c.append(ev_case("E01_above_strike_call", "spot clearly above strike, calm, 5 min left, ask 90",
                     "ENTER", "BTC", market("KXBTC15M-E01", 5.0, 100.0, 88, 90, 9, 11), 100.35, calm))
    calm_dn = walk(80, 99.65, 0.0008, 11)
    c.append(ev_case("E02_below_strike_call", "spot clearly below strike, calm, 5 min left, NO ask 90",
                     "ENTER", "BTC", market("KXBTC15M-E02", 5.0, 100.0, 9, 11, 88, 90), 99.65, calm_dn))
    near = [round(100.02 + 0.004 * ((i * 7) % 5), 6) for i in range(80)]
    c.append(ev_case("E03_near_strike", "spot 0.02% above strike, no crossings",
                     "WAIT_LOW_CONFIDENCE", "BTC", market("KXBTC15M-E03", 5.0, 100.0, 50, 52, 46, 48), 100.02, near))
    hv = walk(80, 100.35, 0.0040, 12)
    hv = [max(v, 100.05) for v in hv[:-16]] + [max(v, 100.05) for v in hv[-16:]]
    c.append(ev_case("E04_high_volatility", "same distance as E01 but ~5x volatility",
                     "WAIT_LOW_CONFIDENCE", "BTC", market("KXBTC15M-E04", 5.0, 100.0, 60, 62, 36, 38), 100.35, hv))
    lv = walk(80, 100.35, 0.0001, 13)
    c.append(ev_case("E05_low_volatility", "same distance, very low volatility, ask 97",
                     "ENTER", "BTC", market("KXBTC15M-E05", 5.0, 100.0, 96, 97, 2, 3), 100.35, lv))
    c.append(ev_case("E06_early_edge_of_window", "exactly 8.0 min left (inclusive upper bound)",
                     "ENTER", "BTC", market("KXBTC15M-E06", 8.0, 100.0, 88, 90, 9, 11), 100.45, walk(80, 100.45, 0.0006, 14)))
    c.append(ev_case("E07_late_edge_of_window", "exactly 2.0 min left (inclusive lower bound)",
                     "ENTER", "BTC", market("KXBTC15M-E07", 2.0, 100.0, 91, 93, 6, 8), 100.35, calm))
    c.append(ev_case("E08_too_early", "8.5 min left", "WAIT_TOO_EARLY", "BTC",
                     market("KXBTC15M-E08", 8.5, 100.0, 88, 90, 9, 11), 100.35, calm))
    c.append(ev_case("E09_too_late", "1.5 min left", "WAIT_LATE", "BTC",
                     market("KXBTC15M-E09", 1.5, 100.0, 97, 98, 1, 2), 100.35, calm))
    c.append(ev_case("E10_market_closed", "close time already passed (remain clamps to 0)", "WAIT_LATE", "BTC",
                     market("KXBTC15M-E10", -0.5, 100.0, 97, 98, 1, 2), 100.35, calm))
    c.append(ev_case("E11_confidence_below_threshold", "conf just under 80%", "WAIT_LOW_CONFIDENCE", "ETH",
                     market("KXETH15M-E11", 5.0, 100.0, 70, 72, 26, 28), 100.12, walk(80, 100.12, 0.0008, 15)))
    c.append(ev_case("E12_insufficient_edge", "conf above ask but net edge <= 0 after 2c costs",
                     "WAIT_NO_EDGE", "BTC", market("KXBTC15M-E12", 5.0, 100.0, 92, 94, 5, 7), 100.35, calm))
    c.append(ev_case("E13_price_below_threshold", "conf high but side ask below MIN_PRICE (85)",
                     "WAIT_BAD_PRICE", "BTC", market("KXBTC15M-E13", 5.0, 100.0, 78, 80, 19, 21), 100.35, calm))
    chop = walk(64, 100.3, 0.0008, 16) + [100.05, 99.95, 100.04, 99.96, 100.06, 99.94, 100.08, 100.1,
                                           100.15, 100.2, 100.25, 100.3, 100.32, 100.33, 100.34, 100.35]
    c.append(ev_case("E14_choppy", "closes cross the strike > CHOP_MAX times in the last 15 min",
                     "BLOCK_CHOP", "BTC", market("KXBTC15M-E14", 5.0, 100.0, 88, 90, 9, 11), 100.35, chop))
    rise = [100.1, 100.15, 100.2, 100.25, 100.28, 100.3, 100.31, 100.32, 100.33, 100.34]
    at_max = walk(65, 99.9, 0.0008, 18) + [99.95, 100.05, 99.94, 99.96, 100.06] + rise        # 3 crossings
    c.append(ev_case("E26_chop_exactly_max", "exactly CHOP_MAX (3) strike crossings -> not choppy",
                     "ENTER", "BTC", market("KXBTC15M-E26", 5.0, 100.0, 88, 90, 9, 11), 100.35, at_max))
    over = walk(65, 100.1, 0.0008, 19) + [100.05, 99.95, 100.04, 99.96, 100.06] + rise       # 4 crossings
    c.append(ev_case("E27_chop_max_plus_one", "CHOP_MAX + 1 (4) crossings -> choppy",
                     "BLOCK_CHOP", "BTC", market("KXBTC15M-E27", 5.0, 100.0, 88, 90, 9, 11), 100.35, over))
    c.append(ev_case("E28_confidence_at_threshold", "conf rounds to 80.0 and passes MIN_CONF (unrounded >= 80)",
                     "WAIT_NO_EDGE", "BTC", market("KXBTC15M-E28", 5.0, 100.0, 84, 86, 12, 14), 100.17,
                     walk(80, 100.17, 0.0008, 11)))
    c.append(ev_case("E29_price_at_threshold", "side ask exactly MIN_PRICE (85) with positive net edge",
                     "ENTER", "BTC", market("KXBTC15M-E29", 5.0, 100.0, 83, 85, 13, 15), 100.24,
                     walk(80, 100.24, 0.0008, 11)))
    c.append(ev_case("E15_stale_short_history", "only 10 candles", "BLOCK_STALE_DATA", "BTC",
                     market("KXBTC15M-E15", 5.0, 100.0, 88, 90, 9, 11), 100.35, calm[-10:]))
    c.append(ev_case("E16_stale_zero_volatility", "flat closes -> sigma 0", "BLOCK_STALE_DATA", "BTC",
                     market("KXBTC15M-E16", 5.0, 100.0, 88, 90, 9, 11), 100.35, [100.35] * 80))
    m17 = market("KXBTC15M-E17", 5.0, 100.0, 88, 90, 9, 11)
    del m17["floor_strike"]
    c.append(ev_case("E17_stale_missing_strike", "market without any strike field", "BLOCK_STALE_DATA", "BTC",
                     m17, 100.35, calm))
    c.append(ev_case("E18_stale_zero_spot", "spot 0", "BLOCK_STALE_DATA", "BTC",
                     market("KXBTC15M-E18", 5.0, 100.0, 88, 90, 9, 11), 0.0, calm))
    c.append(ev_case("E19_missing_side_ask", "favoured side has no ask and no opposite bid (legacy raises)",
                     "RAISES", "BTC", market("KXBTC15M-E19", 5.0, 100.0, 88, None, None, 11), 100.35, calm))
    c.append(ev_case("E20_derived_ask_from_opposite_bid", "yes_ask missing, derived as 100 - no_bid",
                     "ENTER", "BTC", market("KXBTC15M-E20", 5.0, 100.0, 88, None, 10, 12), 100.35, calm))
    c.append(ev_case("E21_cap_strike", "strike supplied as cap_strike", "ENTER", "BTC",
                     market("KXBTC15M-E21", 5.0, 100.0, 88, 90, 9, 11, strike_key="cap_strike"), 100.35, calm))
    c.append(ev_case("E22_coin_disabled", "BTC switched off from the web page", "BLOCK_COIN", "BTC",
                     market("KXBTC15M-E22", 5.0, 100.0, 88, 90, 9, 11), 100.35, calm,
                     {"ACTIVE_COINS": {"BTC": False, "ETH": True, "SOL": True, "XRP": True}}))
    c.append(ev_case("E23_direction_disabled", "DOWN-only while the model favours UP", "BLOCK_DIRECTION", "BTC",
                     market("KXBTC15M-E23", 5.0, 100.0, 88, 90, 9, 11), 100.35, calm, {"DIRECTION": "DOWN"}))
    xrp = walk(80, 2.5125, 0.0008, 17)
    c.append(ev_case("E24_small_price_rounding", "XRP-scale prices (< 100 -> 4-decimal rounding)",
                     "ENTER", "XRP", market("KXXRP15M-E24", 4.0, 2.5, 86, 88, 11, 13), 2.5125, xrp))
    c.append(ev_case("E25_settlement_sensitive_exact_strike", "spot exactly on the strike: P(UP)=50%, ties go UP",
                     "WAIT_LOW_CONFIDENCE", "BTC", market("KXBTC15M-E25", 5.0, 100.0, 49, 51, 49, 51), 100.0,
                     [round(100.01 + 0.004 * ((i * 3) % 4), 6) for i in range(80)]))
    return c


def poller_cases():
    mk = {"KXBTC15M": market("KXBTC15M-P1", 5.0, 100.0, 88, 90, 9, 11),
          "KXETH15M": market("KXETH15M-P1", 5.0, 100.0, 70, 72, 26, 28),
          "KXSOL15M": market("KXSOL15M-P1", 10.0, 100.0, 88, 90, 9, 11),
          "KXXRP15M": None}
    spots = {"BTC-USD": 100.35, "ETH-USD": 100.12, "SOL-USD": 100.35, "XRP-USD": 2.5}
    cds = {"BTC-USD": candles(walk(80, 100.35, 0.0008, 11), 100.35),
           "ETH-USD": candles(walk(80, 100.12, 0.0008, 15), 100.12),
           "SOL-USD": candles(walk(80, 100.35, 0.0008, 11), 100.35),
           "XRP-USD": candles(walk(80, 2.5, 0.0008, 17), 2.5)}
    base = {"markets": mk, "spots": spots, "candles": cds}
    return [
        {"id": "P01_cycle_gate_inactive", "kind": "poller", "intent": "one BTC call; live veto inactive",
         "description": "full poller cycle over 4 coins: BTC ENTER, ETH low conf, SOL too early, XRP no market",
         "input": dict(base, cycles=1)},
        {"id": "P02_second_cycle_same_ticker", "kind": "poller", "intent": "first signal per ticker only",
         "description": "two cycles: the BTC ticker is called once", "input": dict(base, cycles=2)},
        {"id": "P03_perp_veto_blocks", "kind": "poller", "intent": "gate BLOCK suppresses the call",
         "description": "live veto decision BLOCK (stubbed) -> no call logged, ticker marked handled",
         "input": dict(base, cycles=1, gate="block")},
        {"id": "P04_already_alerted", "kind": "poller", "intent": "restart state: ticker already alerted",
         "description": "ticker already in _alerted -> no call", "input": dict(base, cycles=1,
                                                                             already_alerted=["KXBTC15M-P1"])},
        {"id": "P05_paused", "kind": "poller", "intent": "paused watcher evaluates nothing",
         "description": "RUNNING cleared", "input": dict(base, cycles=1, paused=True)},
    ]


def _close(minutes_from_now):
    return iso_z(FROZEN_UTC + _dt.timedelta(minutes=minutes_from_now))


def _cp(coin, side, entry, stop, contracts, close_min, bid_lo, lo=None, hi=None, mode="taker", filled=True,
        limit=None, ticker=None):
    return {"coin": coin, "side": side, "quote_ask": entry, "entry_mode": mode, "entry_limit": limit or entry,
            "assumed_entry": entry, "entry": entry, "stop": stop, "contracts": contracts, "close": _close(close_min),
            "lo": lo if lo is not None else entry, "hi": hi if hi is not None else entry, "ask_lo": entry,
            "bid_lo": bid_lo, "maker_filled": filled, "ticker": ticker, "signal": True,
            "signal_ts_epoch_ms": int(FROZEN_EPOCH * 1000) - 600000, "signal_ts": "2026-09-21T13:50:00+00:00"}


def settlement_cases():
    cp = {
        "S-WIN": _cp("BTC", "UP", 90, 68, 10, -1, 88, 88, 99),
        "S-LOSS": _cp("ETH", "DOWN", 88, 53, 5, -1, 86),
        "S-STOP": _cp("SOL", "UP", 90, 68, 10, -2, 60, 60, 92),
        "S-GAP": _cp("XRP", "UP", 90, 59, 3, -2, 20),
        "S-STOP-EXACT": _cp("BTC", "DOWN", 92, 69, 4, -3, 69),
        "S-NOSTOP": _cp("BTC", "UP", 90, None, 2, -1, 80),
        "S-UNFILLED": _cp("ETH", "UP", 86, 52, 7, -1, 85, mode="maker", filled=False, limit=86),
        "S-MAKERFILLED": _cp("SOL", "DOWN", 87, 65, 6, -1, 84, mode="maker", filled=True, limit=87),
        "S-TOO-EARLY": _cp("BTC", "UP", 90, 68, 10, 0.2, 88),
        "S-NO-RESULT": _cp("BTC", "UP", 90, 68, 10, -5, 88),
        "S-GIVE-UP": _cp("BTC", "UP", 90, 68, 10, -31, 88),
        "S-ZERO-CONTRACTS": _cp("ETH", "UP", 97, 58, 0, -1, 96),
    }
    for t, v in cp.items():
        v["ticker"] = t
    results = {"S-WIN": "yes", "S-LOSS": "yes", "S-STOP": "yes", "S-GAP": "no", "S-STOP-EXACT": "no",
               "S-NOSTOP": "no", "S-UNFILLED": "yes", "S-MAKERFILLED": "no", "S-TOO-EARLY": "yes",
               "S-NO-RESULT": "", "S-GIVE-UP": "", "S-ZERO-CONTRACTS": "yes"}
    pend = {"Q-WIN": {"coin": "BTC", "fav_up": True, "entry": 90, "close": _close(-1)},
            "Q-LOSS": {"coin": "ETH", "fav_up": False, "entry": 88, "close": _close(-1)},
            "Q-NONE-ENTRY": {"coin": "SOL", "fav_up": True, "entry": None, "close": _close(-1)},
            "Q-EARLY": {"coin": "XRP", "fav_up": True, "entry": 91, "close": _close(0.1)},
            "Q-UNSETTLED": {"coin": "BTC", "fav_up": True, "entry": 90, "close": _close(-5)},
            "Q-DROP": {"coin": "BTC", "fav_up": True, "entry": 90, "close": _close(-11)}}
    pres = {"Q-WIN": "yes", "Q-LOSS": "yes", "Q-NONE-ENTRY": "no", "Q-EARLY": "yes"}
    r_up = {"ticker": "L-1", "fav": "UP", "up_bid": 88, "dn_bid": 9, "side_ask": 90, "rec_stop": 68, "conf": 94.2,
            "net_edge": 2.2, "close": _close(5), "signal": True, "spot_observed_ts": FROZEN_EPOCH}
    r_tight = dict(r_up, ticker="L-2", up_bid=89)
    r_dn_nobid = {"ticker": "L-3", "fav": "DOWN", "up_bid": 9, "dn_bid": None, "side_ask": 91, "rec_stop": 55,
                  "conf": 95.0, "net_edge": 2.0, "close": _close(5), "signal": True, "spot_observed_ts": None}
    return [
        {"id": "S01_settle_calls", "kind": "settle_calls", "intent": "stop/settle/fees/unfilled/timing",
         "description": "every settle_calls branch", "input": {"call_pending": cp, "results": results,
                                                               "existing_rows": []}},
        {"id": "S02_settle_pending_stats", "kind": "settle_pending", "intent": "live W/L stats",
         "description": "settle_pending + compute_stats", "input": {"pending": pend, "results": pres}},
        {"id": "S03_log_call_taker", "kind": "log_call", "intent": "taker entry assumption",
         "description": "log_call in taker mode", "input": {"coin": "BTC", "r": r_up}},
        {"id": "S04_log_call_maker", "kind": "log_call", "intent": "maker limit = bid+1 when spread > 1",
         "description": "log_call in maker mode", "input": {"coin": "BTC", "r": r_up,
                                                            "controls": {"ENTRY_MODE": "maker"}}},
        {"id": "S05_log_call_maker_tight", "kind": "log_call", "intent": "maker limit = bid when spread <= 1",
         "description": "log_call maker, 1c spread", "input": {"coin": "BTC", "r": r_tight,
                                                               "controls": {"ENTRY_MODE": "maker"}}},
        {"id": "S06_log_call_no_bid", "kind": "log_call", "intent": "no bid -> limit = ask; no signal ts",
         "description": "log_call without a bid", "input": {"coin": "ETH", "r": r_dn_nobid,
                                                            "controls": {"ENTRY_MODE": "maker"}}},
        {"id": "S07_fee_grid", "kind": "fees", "intent": "Kalshi fee formula",
         "description": "kalshi_fee_cents over prices x contracts",
         "input": {"grid": [[p, n] for p in (0, 1, 10, 50, 85, 90, 99, 100) for n in (0, 1, 10, 100)]}},
    ]


def _history(start_close, sigma, seed, minutes, drift=0.0):
    t0 = int(FROZEN_EPOCH) - minutes * 60
    t0 -= t0 % 60
    g = _lcg(seed)
    out, px = [], start_close
    for i in range(minutes):
        out.append([t0 + i * 60, round(px, 6)])
        px *= math.exp(drift + sigma * next(g))
    return out


def backtest_cases():
    return [
        {"id": "B01_backtest_coin_btc", "kind": "backtest_coin", "intent": "trades incl. stops",
         "description": "both backtesters on 12 h of synthetic BTC closes",
         "input": {"coin": "BTC", "cfg": BTC, "history": _history(100.0, 0.0009, 101, 720)}},
        {"id": "B02_backtest_coin_xrp_gappy", "kind": "backtest_coin", "intent": "missing minutes tolerated",
         "description": "XRP-scale history with every 7th minute missing",
         "input": {"coin": "XRP", "cfg": XRP,
                   "history": [p for i, p in enumerate(_history(2.5, 0.0012, 202, 720)) if i % 7]}},
        {"id": "B03_run_backtest_all", "kind": "backtest_full", "intent": "aggregation, calibration bands, by_hour",
         "description": "run_backtest over 4 synthetic coins (LOCAL_TZ=None -> UTC hours)",
         "input": {"range": "1d", "histories": {
             "BTC-USD": _history(100.0, 0.0009, 301, 1500), "ETH-USD": _history(100.0, 0.0011, 302, 1500),
             "SOL-USD": _history(100.0, 0.0014, 303, 1500), "XRP-USD": _history(2.5, 0.0010, 304, 1500)}}},
    ]


K = {"MIN_CONF": 80.0, "MIN_PRICE": 85.0, "EDGE_THRESH": 0.0, "ENTRY_COST_CENTS": 2.0}


def overlay_cases():
    calls = [
        dict(fn="overlay", p_base_up=0.94, p_b_up=0.90, p_c_up=0.91, alpha=0.5, fav="UP", side_ask=90, constants=K),
        dict(fn="overlay", p_base_up=0.94, p_b_up=0.90, p_c_up=0.70, alpha=1.0, fav="UP", side_ask=90, constants=K),
        dict(fn="overlay", p_base_up=0.52, p_b_up=0.60, p_c_up=0.40, alpha=1.0, fav="UP", side_ask=50, constants=K),
        dict(fn="overlay", p_base_up=0.84, p_b_up=0.80, p_c_up=0.70, alpha=1.0, fav="UP", side_ask=85, constants=K),
        dict(fn="overlay", p_base_up=0.93, p_b_up=0.80, p_c_up=0.79, alpha=0.25, fav="UP", side_ask=90, constants=K),
        dict(fn="overlay", p_base_up=0.06, p_b_up=0.10, p_c_up=0.12, alpha=0.75, fav="DOWN", side_ask=90, constants=K),
        dict(fn="overlay", p_base_up=0.94, p_b_up=0.90, p_c_up=0.91, alpha=0.5, fav="UP", side_ask=90, constants=K,
             legacy_signal=False),
        dict(fn="overlay", p_base_up=0.94, p_b_up=0.90, p_c_up=0.91, alpha=0.3, fav="UP", side_ask=90, constants=K),
        dict(fn="decide", p_up=0.95, fav="UP", side_ask=90, constants=K),
        dict(fn="decide", p_up=0.919, fav="UP", side_ask=90, constants=K),
    ]
    sig = {"coin": "BTC", "ticker": "G-1", "signal": True, "fav": "UP", "p_up": 94.0, "conf": 94.0, "side_ask": 90}
    nosig = dict(sig, signal=False, ticker="G-2")
    return [
        {"id": "V01_perp_overlay_math", "kind": "overlay", "intent": "allow / flip / low conf / no edge / cap / no-new-call",
         "description": "perp_probability.overlay and decide", "input": {"calls": calls}},
        {"id": "V02_live_veto_inactive_default", "kind": "gate_inactive", "intent": "no promotion -> INACTIVE",
         "description": "default production state: no promotion file, env flag off",
         "input": {"env_enabled": False, "results": [sig, nosig]}},
        {"id": "V03_live_veto_env_on_without_promotion", "kind": "gate_inactive", "intent": "env alone cannot activate",
         "description": "PERP_LIVE_VETO_ENABLED=1 but no promotion", "input": {"env_enabled": True, "results": [sig]}},
    ]


def all_cases():
    return evaluate_cases() + poller_cases() + settlement_cases() + backtest_cases() + overlay_cases()
