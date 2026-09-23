#!/usr/bin/env python3
"""
Kalshi 15m — Multi-Coin Dashboard + Auto Backtest

Live BTC/ETH/SOL/XRP 15-minute end-of-market watcher AND a backtest that runs
itself in the background every time you start it. Data comes from Kalshi's
public API (no key) and Coinbase. It pings Discord on a live setup and NEVER
places a trade.

RUN
    pip install requests
    python kalshi_dashboard.py
    open  http://localhost:8000

The backtest runs on a background thread, so the dashboard is usable instantly.
Its panel shows "running..." for a couple of minutes on first launch, then fills
in. Results are cached (kalshi_backtest_cache.json) and only re-run every
BACKTEST_REFRESH_HOURS, so restarts load instantly. Delete the cache file to
force a fresh run. For a custom window, the standalone kalshi_backtest.py still
works (python kalshi_backtest.py --days 60).
"""

import math
import sys
import os
import csv
import json
import time
import threading
import datetime as dt
import http_session
import requests
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ─────────────────────── ALERTS ───────────────────────
# All Discord posting goes through the bot (kalshi_bot.py) to real channels.
# Telegram is optional and fine to hardcode here (shared with trusted people).
TELEGRAM_BOT_TOKEN  = ""
TELEGRAM_CHAT_ID    = ""

# ─────────────────────── STRATEGY ───────────────────────
INTERVAL_MIN  = 15
MAX_ENTRY_MIN = 8.0
LAST_STOP_MIN = 2.0
MIN_PRICE     = 85.0
MIN_CONF      = 80.0
EDGE_THRESH   = 0.0      # minimum NET edge (cents); a signal ALWAYS needs net edge > 0
ENTRY_COST_CENTS = 2.0   # est. fees + slippage subtracted from raw edge to get NET edge
CHOP_MAX      = 3
VOL_LOOKBACK_MIN = 60
VOL_MULT         = 1.15
BANKROLL         = 1000.0   # USD, used to turn the Kelly % into a contract count
TZ_OFFSET_HOURS  = 0        # local-time offset for time-of-day stats (New York: -4 EDT / -5 EST)

COINS = {
    "BTC": {"series": "KXBTC15M", "product": "BTC-USD", "sl_pct": 0.75},
    "ETH": {"series": "KXETH15M", "product": "ETH-USD", "sl_pct": 0.60},
    "SOL": {"series": "KXSOL15M", "product": "SOL-USD", "sl_pct": 0.75},
    "XRP": {"series": "KXXRP15M", "product": "XRP-USD", "sl_pct": 0.65},
}

# ─────────────────────── BACKTEST ───────────────────────
BACKTEST_DAYS          = 30      # accepts a number of days, or "48h" / "10d" / "2w" / "3m"
BACKTEST_REFRESH_HOURS = 12
BACKTEST_RECENT_HOURS  = 12   # instant "recent behavior" read, sliced from the same history
BACKTEST_CACHE_FILE    = "kalshi_backtest_cache.json"
FEE_CENTS              = 2.0
ENTRY_MODE             = "taker"   # "taker" (fill at ask now) or "maker" (rest at limit; may not fill)

# ---- web-controllable engine filters (Stage 6). Data is always collected for
# every coin; these only decide what is ELIGIBLE for a call/paper trade. ----
ACTIVE_COINS = {"BTC": True, "ETH": True, "SOL": True, "XRP": True}
DIRECTION    = "BOTH"   # "UP" | "DOWN" | "BOTH"

def kalshi_fee_cents(price_cents, contracts):
    """Kalshi trading fee ~ ceil(0.07 * C * P * (1-P)) with P in dollars, returned in CENTS
    (rounded up to the next whole cent, min 1c per priced side)."""
    if not contracts:
        return 0.0
    p = max(min(price_cents / 100.0, 0.99), 0.01)
    return float(max(math.ceil(0.07 * contracts * p * (1 - p) * 100), 1))
GATE_PRICE             = 90.0    # entry price the expectancy figures assume (your typical fill)

KALSHI_BASE   = "https://external-api.kalshi.com/trade-api/v2"
COINBASE_BASE = "https://api.exchange.coinbase.com"
PORT          = 8000
POLL_SECONDS  = 4
RESULTS_FILE  = "kalshi_results.json"
CALLS_FILE    = "kalshi_calls.json"
TRADES_CSV    = "kalshi_trades.csv"   # every settled trade, one row, for spreadsheet analysis
PAPER_ORDERS_CSV = "kalshi_paper_orders.csv"  # the exact order+stop each call WOULD place (dry run)
KALSHI_MARKET_URL = "https://kalshi.com/markets/{series}"  # deep link; {series}/{event}/{ticker} available
EXPLAIN_MARK_FILE = ".explain_posted"
WEEKLY_MARK_FILE  = ".weekly_posted"

# ─────────────────────── PERP TELEMETRY (Step 1: passive, read-only) ───────────────────────
# Records Kalshi perpetual-futures data alongside each binary evaluation for later
# research. It CANNOT affect signals, confidence, edge, sizing, stops or calls, and it
# never places orders. Disable with PERP_TELEMETRY_ENABLED = False or env
# KALSHI_PERP_TELEMETRY=0. See perp_telemetry.py.
PERP_TELEMETRY_ENABLED = os.environ.get("KALSHI_PERP_TELEMETRY", "1") != "0"
PERP_API_BASE        = os.environ.get("KALSHI_PERP_API_BASE", "https://external-api.kalshi.com/trade-api/v2")
PERP_SAMPLE_SECONDS  = 4       # min seconds between /margin/markets fetches (== POLL_SECONDS)
PERP_STALE_SECONDS   = 15      # older than this -> source_status 'stale'
PERP_FUNDING_REFRESH_SECONDS = 60   # funding estimate is an 8h-period average; no need to poll fast
PERP_ERROR_BACKOFF_SECONDS   = 15   # after a failure, don't hammer the endpoint
PERP_HTTP_TIMEOUT    = 5
PERP_LOOKBACK_TOLERANCE_SECONDS = 10   # max gap allowed when picking the 30/60/180s-ago point
PERP_LOG_FILE        = "kalshi_perp_telemetry.csv"
# Step 2 — causal alignment. A binary row may only use a perp snapshot that was fully
# received AT OR BEFORE its spot observation, and no more than this many seconds before.
# 8 s = two sampler intervals: tolerates one missed sample, rejects real outages.
PERP_ALIGNMENT_MAX_LAG_SECONDS = 8
PERP_SNAPSHOT_HISTORY_SECONDS  = 1800   # 30 min of snapshots per coin (bounded by age...)
PERP_SNAPSHOT_HISTORY_MAXLEN   = 600    # ...and by count: 450 needed at 4 s; ~2.4k objects total
# Perp tickers are auto-discovered from /margin/markets. Override only if discovery is
# ambiguous, e.g. {"BTC": "<exact ticker>"} or env KALSHI_PERP_TICKER_BTC.
PERP_TICKERS = {c: os.environ.get(f"KALSHI_PERP_TICKER_{c}") for c in ("BTC", "ETH", "SOL", "XRP")}
# Step 4 — SHADOW-ONLY perp filter validation. Frozen policies (built offline from a real
# Step 3 candidate) are scored on the telemetry worker AFTER the real call was already
# made, and only logged. They can never change a signal, call, size, stop or post.
# No policy file (the normal state until real data yields a candidate) = nothing happens.
PERP_SHADOW_ENABLED = os.environ.get("KALSHI_PERP_SHADOW", "1") != "0"
PERP_SHADOW_POLICY_FILE = "perp_shadow_policies.json"
PERP_SHADOW_JOURNAL = "kalshi_perp_shadow.csv"
PERP_SHADOW_POLICY_MAX_AGE_DAYS = 30
# Step 6 — LIVE VETO (suppress an already-valid legacy call only). Needs BOTH a valid manual
# promotion artifact AND PERP_LIVE_VETO_ENABLED=1; default is OFF, i.e. legacy behaviour.
PERP_LIVE_VETO_ENABLED  = os.environ.get("PERP_LIVE_VETO_ENABLED", "0") == "1"
PERP_LIVE_PROMOTION_FILE = "perp_live_promotion.json"
PERP_LIVE_VETO_JOURNAL   = "kalshi_perp_live_veto.csv"
PERP_LIVE_VETO_KILL_FILE = os.environ.get("PERP_LIVE_VETO_KILL_FILE", "DISABLE_PERP_LIVE_VETO")
PERP_STEP5_BASELINE_MANIFEST = "step5_baseline_manifest.json"

# ─────────────────────── shared state ───────────────────────
STATE = {"coins": {}, "stats": {}, "backtest": {"status": "starting"}, "calls": {}, "updated": ""}
LOCK = threading.Lock()
_alerted = set()
_pending = {}
_call_pending = {}
_candle_cache = {}
RUNNING = threading.Event()
RUNNING.set()   # watcher active; the Discord bot pauses/resumes via this
WATCHER_STATE_FILE = ".watcher_state"   # persists RUNNING/PAUSED across restarts & reconnects

# real timezone for human-facing time-of-day stats (falls back to fixed offset)
try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("America/New_York")
except Exception:
    LOCAL_TZ = None

def local_hour(ts):
    d = dt.datetime.fromtimestamp(ts, dt.timezone.utc)
    if LOCAL_TZ is not None:
        return d.astimezone(LOCAL_TZ).hour
    return (d.hour + TZ_OFFSET_HOURS) % 24

def load_watcher_state():
    """Restore intended state; do NOT let a reconnect silently flip it."""
    st = _load(WATCHER_STATE_FILE, None)
    if st and st.get("state") == "paused":
        RUNNING.clear()
    else:
        RUNNING.set()
    return "paused" if not RUNNING.is_set() else "running"

def set_watcher_state(running: bool):
    (RUNNING.set if running else RUNNING.clear)()
    _save(WATCHER_STATE_FILE, {"state": "running" if running else "paused",
                               "at": dt.datetime.now(dt.timezone.utc).isoformat()})
ON_CALL = None  # optional callback(coin, r, crec); the bot sets this to post calls with buttons
POST_EMBED = None  # callback(dest, embed) set by the bot; dest in {calls,journal,explain}

def bot_post(dest, embed):
    """Post an embed to a Discord channel via the bot. No-op if the bot isn't attached."""
    if POST_EMBED:
        try: POST_EMBED(dest, embed)
        except Exception as e: print(f"  bot post failed ({dest}): {e}")

# ─────────────────────── math + indicators ───────────────────────
def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def conf_side(price, strike, vol, left_min):
    term = vol * math.sqrt(max(left_min, 1e-4))
    if term <= 0 or not strike:
        return None, None
    z = math.log(price / strike) / term
    cdf = norm_cdf(z)
    return max(cdf, 1 - cdf) * 100.0, cdf >= 0.5

def _sma(v, n):   return sum(v[-n:]) / n
def _stdev(v, n):
    s = v[-n:]; m = sum(s) / n
    return math.sqrt(sum((x - m) ** 2 for x in s) / n)

def rsi_series(closes, n=14):
    d = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    if len(d) < n: return []
    ag = sum(max(x, 0) for x in d[:n]) / n
    al = sum(-min(x, 0) for x in d[:n]) / n
    out = []
    def calc(g, l): return 100.0 if l == 0 else 100 - 100 / (1 + g / l)
    out.append(calc(ag, al))
    for x in d[n:]:
        ag = (ag * (n - 1) + max(x, 0)) / n
        al = (al * (n - 1) + (-min(x, 0))) / n
        out.append(calc(ag, al))
    return out

def stoch_rsi(closes, n=14):
    r = rsi_series(closes, n)
    if len(r) < n: return None, ""
    w = r[-n:]; lo, hi = min(w), max(w)
    val = 100.0 * (r[-1] - lo) / (hi - lo) if hi > lo else 50.0
    arrow = ""
    if len(r) >= n + 1:
        pw = r[-n - 1:-1]; plo, phi = min(pw), max(pw)
        pv = 100.0 * (r[-2] - plo) / (phi - plo) if phi > plo else 50.0
        arrow = "\u2191" if val >= pv else "\u2193"
    return val, arrow

def atr(highs, lows, closes, n=14):
    trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]),
               abs(lows[i] - closes[i - 1])) for i in range(1, len(closes))]
    return sum(trs[-n:]) / n if len(trs) >= n else None

def bollinger_pos(closes, n=20, k=2):
    if len(closes) < n: return "n/a"
    mid = _sma(closes, n); sd = _stdev(closes, n); c = closes[-1]
    if c >= mid + k * sd: return "Above"
    if c <= mid - k * sd: return "Below"
    return "Inside"

# ─────────────────────── alerts ───────────────────────
def post_discord(url, text):
    if not url: return
    try: requests.post(url, json={"content": text}, timeout=10)
    except requests.RequestException as e: print(f"  discord post failed: {e}")

def post_embed(url, embed):
    if not url: return
    try: requests.post(url, json={"embeds": [embed]}, timeout=10)
    except requests.RequestException as e: print(f"  discord embed failed: {e}")

def _limit_price(r):
    up = r["fav"] == "UP"
    side_bid = r["up_bid"] if up else r["dn_bid"]
    side_ask = r["side_ask"]
    if side_bid is None:
        return side_ask
    return side_bid + 1 if (side_ask - side_bid) > 1 else side_bid

def _suggest_order(r):
    up = r["fav"] == "UP"
    side_bid = r["up_bid"] if up else r["dn_bid"]
    side_ask = r["side_ask"]
    if side_bid is None:
        return f"Take **{side_ask:.0f}\u00a2** (only ask available)"
    limit = _limit_price(r)
    return (f"Rest a **limit buy at {limit:.0f}\u00a2** \u2014 maker order, aims to avoid fees (may not fill)\n"
            f"Or take **{side_ask:.0f}\u00a2** now for an instant fill (pays a small fee)")

def kalshi_link(ticker):
    parts = ticker.split("-")
    return KALSHI_MARKET_URL.format(series=parts[0].lower(),
                                    event="-".join(parts[:-1]), ticker=ticker)

def _append_paper(coin, r):
    cols = ["ts", "ticker", "coin", "side", "order", "limit_price", "contracts", "stop", "conf", "edge"]
    row = {"ts": dt.datetime.now(dt.timezone.utc).isoformat(), "ticker": r["ticker"], "coin": coin,
           "side": r["fav"], "order": "limit_buy", "limit_price": round(_limit_price(r)),
           "contracts": _contracts(r), "stop": r["rec_stop"], "conf": r["conf"], "edge": r["edge"]}
    try:
        new = not os.path.exists(PAPER_ORDERS_CSV)
        with open(PAPER_ORDERS_CSV, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            if new: w.writeheader()
            w.writerow(row)
    except OSError as e:
        print(f"  paper log failed: {e}")

def _size_fraction(r):
    """Quarter-Kelly risk fraction. Returns 0 (no floor) whenever there is no
    genuine positive-edge case: bad price, non-positive net edge, or Kelly <= 0."""
    conf = r.get("conf"); ask = r.get("side_ask"); net = r.get("net_edge")
    if conf is None or ask is None or net is None:
        return 0.0
    if ask <= 0 or ask >= 100:            # invalid price
        return 0.0
    if net <= 0:                          # no economic edge -> no size
        return 0.0
    q = max(conf - 2.0, 0) / 100.0        # 2-point humility haircut
    p = ask / 100.0
    kelly = q - (1 - q) * p / (1 - p) if 0 < p < 1 else 0.0
    if kelly <= 0:                        # Kelly says don't bet
        return 0.0
    return min(kelly / 4.0, 0.05)         # quarter-Kelly, capped at 5%, NO floor

def _contracts(r):
    price = (r.get("side_ask") or 0) / 100.0
    frac = _size_fraction(r)
    return int((frac * BANKROLL) / price) if (price > 0 and frac > 0) else 0

def _suggest_size(r):
    frac = _size_fraction(r); n = _contracts(r)
    if frac <= 0 or n < 1:
        return "**0** \u2014 no positive-edge size"
    return f"**{frac*100:.1f}%** of port \u2248 **{n} contracts** (\u00bc-Kelly, cap 5%)"

def _call_margin(r):
    """Expected profit as a % of the price paid, given confidence and the stop."""
    q = r["conf"] / 100.0
    entry = r["side_ask"] or 0
    stop = r["rec_stop"] or (entry * 0.7)
    if entry <= 0:
        return 0.0
    ev = q * (100 - entry) - (1 - q) * (entry - stop)
    return ev / entry * 100.0

def build_call_embed(coin, r, rec=None):
    avg = f"**{rec['pct']}%** ({rec['w']}-{rec['n']-rec['w']})" if rec and rec["n"] else "no history yet"
    return {
        "title": f"CALL \u2014 {coin} {r['fav']}",
        "color": 0x3ddc84 if r["fav"] == "UP" else 0xff5c5c,
        "fields": [
            {"name": "Confidence", "value": f"**{r['conf']:.0f}%**", "inline": True},
            {"name": "Edge (raw/net)", "value": f"**{r.get('raw_edge', 0):+.1f}** / **{r.get('net_edge', 0):+.1f}\u00a2**", "inline": True},
            {"name": "Time left", "value": f"**{r['remain']}m**", "inline": True},
            {"name": "Spot / Strike", "value": f"{r['spot']} / {r['strike']}  (**{r['dist']:+g}** {r['dir']})", "inline": False},
            {"name": "Order", "value": _suggest_order(r), "inline": False},
            {"name": "Open on Kalshi", "value": f"[{r['ticker']}]({kalshi_link(r['ticker'])})", "inline": False},
            {"name": "Stop", "value": f"Exit near **{r['rec_stop']}\u00a2** ({r['sl_pct']}% of entry)", "inline": True},
            {"name": "Size", "value": _suggest_size(r), "inline": True},
            {"name": f"{coin} win rate", "value": avg, "inline": True},
            {"name": "Profit margin", "value": f"**{_call_margin(r):+.1f}%** expected", "inline": True},
        ],
        "footer": {"text": f"{r['ticker']} \u00b7 size scaled to bankroll & edge"},
    }

def build_result_embed(row, rec):
    win = row["win"]; losses = rec["n"] - rec["w"]
    head = "WON" if win else "LOST"
    return {
        "title": f"{head} \u2014 {row['coin']} {row['side']}",
        "color": 0x3ddc84 if win else 0xff5c5c,
        "fields": [
            {"name": "Entry / Stop", "value": f"**{row['entry']:.0f}\u00a2** / {row['stop']:.0f}\u00a2", "inline": True},
            {"name": "Size", "value": f"**{row['contracts']}** contracts", "inline": True},
            {"name": "This order", "value": f"**{row['per_contract']:+.0f}\u00a2**/ct \u2192 **{row['pnl']:+.0f}\u00a2**", "inline": True},
            {"name": "Exit", "value": f"{row.get('exit_reason','settle')} @ {row.get('exit_price', 0):.0f}\u00a2", "inline": True},
            {"name": "Range (low/high)", "value": f"{row.get('low', row['entry']):.0f}\u00a2 / {row.get('high', row['entry']):.0f}\u00a2", "inline": True},
            {"name": "Avg win rate", "value": f"**{rec['pct']}%**  ({rec['w']}-{losses})", "inline": True},
            {"name": "Streak", "value": f"**{rec['streak']}W**", "inline": True},
            {"name": "Net", "value": f"**{rec['pnl']:+.0f}\u00a2** (\u2248 ${rec['pnl']/100:+.2f})", "inline": True},
            {"name": "Profit margin", "value": f"**{rec['margin']:+.1f}%** overall", "inline": True},
        ],
        "footer": {"text": "stop triggers on the bid (executable exit); gaps modeled at the worse price"},
    }

def build_explain_embed():
    sizing = ("Size to your edge (confidence minus the price you pay), not confidence alone.\n"
              "```\n"
              "Confidence   Portfolio\n"
              "under 85%    skip\n"
              "85 - 88%     1 - 2%\n"
              "88 - 90%     2 - 3%\n"
              "90 - 93%     3 - 4%\n"
              "93 - 95%     4 - 5%\n"
              "95%+         ~5% (cap)\n"
              "```\n"
              "\u2022 Cut size when several coins fire at once \u2014 they move together.\n"
              "\u2022 Shade your confidence down a couple of points as a buffer.")
    return {
        "title": "How this bot works",
        "color": 0x4a86e8,
        "description": ("Watches the Kalshi 15-minute up/down markets for **BTC, ETH, SOL and XRP** and flags "
                        "high-probability entries near the close. **Decision support only \u2014 it never places a trade.**"),
        "fields": [
            {"name": "Confidence", "value": ("The model's win probability, built from three things:\n"
                "\u2022 How far price sits from the strike\n"
                "\u2022 How much time is left\n"
                "\u2022 How volatile the coin is right now\n"
                "Far from the strike + little time + calm reads high; near the strike with lots of time reads near 50%."),
                "inline": False},
            {"name": "When it calls", "value": ("A call fires only when all of these line up:\n"
                "\u2022 Time left is in the **2 - 8 minute** window\n"
                "\u2022 Confidence is **80% or higher**\n"
                "\u2022 The contract costs **85\u00a2 or more**\n"
                "\u2022 The tape isn't crossing the strike (not choppy)\n"
                "Edge = confidence minus price. You only profit when your number beats the market's."),
                "inline": False},
            {"name": "Placing the order", "value": ("\u2022 Rest a **limit buy at the bid** to sit as a maker and avoid taker fees\n"
                "\u2022 It may not fill, but a resting order dodges fees and slippage\n"
                "\u2022 Taking the ask fills instantly but pays a small fee"),
                "inline": False},
            {"name": "Stop-loss", "value": ("\u2022 Exit if the contract falls to your per-coin stop (about **60 - 75%** of entry)\n"
                "\u2022 Taking stops is what keeps a high win rate profitable \u2014 one unstopped loss erases many wins"),
                "inline": False},
            {"name": "Position sizing", "value": sizing, "inline": False},
            {"name": "Channels", "value": ("\u2022 **Calls** \u2014 a setup fired, with the suggested order, stop and size\n"
                "\u2022 **Journal** \u2014 the result and running W/L\n"
                "\u2022 All net figures are per **1 contract**, held to settlement"),
                "inline": False},
            {"name": "Reality check", "value": ("\u2022 Backtested near 90% win rate and well-calibrated, but on idealized fills over a short sample\n"
                "\u2022 Real edge depends on the price you actually get\n"
                "\u2022 **Not financial advice** \u2014 only stake what you can afford to lose"),
                "inline": False},
        ],
    }

def maybe_post_explanation():
    mark = _load(EXPLAIN_MARK_FILE, None)
    if mark and _age_hours(mark.get("updated", "")) < 12:
        return
    bot_post("explain", build_explain_embed())
    _save(EXPLAIN_MARK_FILE, {"updated": dt.datetime.now().isoformat()})

def _parse_ts(s):
    d = dt.datetime.fromisoformat(s)
    return d.replace(tzinfo=dt.timezone.utc) if d.tzinfo is None else d

def _week_stats(rows):
    rows = [r for r in rows if r.get("win") is not None]   # skip unfilled maker orders
    n = len(rows); w = sum(1 for r in rows if r["win"])
    net = sum(r.get("pnl", 0) for r in rows)
    staked = sum(r.get("entry", 0) * r.get("contracts", 1) for r in rows)
    return {"n": n, "w": w, "l": n - w, "pct": round(100 * w / n) if n else 0,
            "net": round(net, 1), "margin": round(net / staked * 100, 1) if staked else 0.0}

def build_weekly_embed():
    now = dt.datetime.now(dt.timezone.utc)
    week = []
    for r in _load(CALLS_FILE, []):
        try:
            if (now - _parse_ts(r["ts"])).total_seconds() <= 7 * 86400:
                week.append(r)
        except Exception:
            pass
    if not week:
        return {"title": "Weekly report", "color": 0x4a86e8,
                "description": "No calls have settled in the last 7 days yet."}
    o = _week_stats(week)
    dows = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    table = "```\nDay   W-L    win%      net    margin\n"
    best = None
    for i, d in enumerate(dows):
        g = [r for r in week if _parse_ts(r["ts"]).weekday() == i]
        if g:
            s = _week_stats(g)
            table += f"{d}   {s['w']}-{s['l']:<3} {s['pct']:>4}%  ${s['net']/100:>+8.2f}  {s['margin']:>+6.1f}%\n"
            if best is None or s["pct"] > best[1]:
                best = (d, s["pct"])
    table += "```"
    desc = (f"**This week:** {o['w']}-{o['l']}  ({o['pct']}% win)  \u00b7  "
            f"net ${o['net']/100:+.2f}  \u00b7  profit margin {o['margin']:+.1f}%\n{table}")
    if best:
        desc += f"\n**Best day this week: {best[0]}** ({best[1]}% win)"
    by_hour = STATE.get("backtest", {}).get("by_hour", {})
    hrs = [(int(h), b) for h, b in by_hour.items() if b and b.get("n", 0) >= 20]
    if hrs:
        bh = max(hrs, key=lambda x: x[1]["wr"])
        lo = bh[0]; hi = (lo + 1) % 24
        tz = "ET" if LOCAL_TZ else (f"UTC{TZ_OFFSET_HOURS:+d}" if TZ_OFFSET_HOURS else "UTC")
        desc += (f"\n**Best time of day: {lo:02d}:00\u2013{hi:02d}:00 {tz}** "
                 f"({bh[1]['wr']:.0f}% win over {bh[1]['n']} trades, backtest)")
    return {"title": "Weekly report \u2014 W/L by day", "color": 0x4a86e8, "description": desc}

def build_update_embed():
    now = dt.datetime.now(dt.timezone.utc)
    aged = []
    for r in _load(CALLS_FILE, []):
        try:
            aged.append(((now - _parse_ts(r["ts"])).total_seconds() / 3600.0, r))
        except Exception:
            pass
    if not aged:
        return {"title": "Recent market update", "color": 0x4a86e8,
                "description": "No settled calls yet \u2014 check back after a few markets close."}
    def win(h): return [r for a, r in aged if a <= h]
    f2, f6, f24 = win(2), win(6), win(24)

    def perf(rows_):
        s = _week_stats(rows_)
        return f"{s['w']}-{s['l']}  ({s['pct']}% win)  ${s['net']/100:+.2f}" if s["n"] else "no calls"

    fields = [
        {"name": "Last 2 hours", "value": perf(f2), "inline": True},
        {"name": "Last 6 hours", "value": perf(f6), "inline": True},
        {"name": "Last 24 hours", "value": perf(f24), "inline": True},
    ]
    active = {c: _week_stats([r for r in f24 if r.get("coin") == c]) for c in COINS}
    active = {c: s for c, s in active.items() if s["n"]}
    if active:
        lines = "\n".join(f"{c}: {s['w']}-{s['l']} ({s['pct']}%)  ${s['net']/100:+.2f}"
                          for c, s in active.items())
        best = max(active.items(), key=lambda x: x[1]["net"])
        worst = min(active.items(), key=lambda x: x[1]["net"])
        fields.append({"name": "By coin (24h)", "value": lines, "inline": False})
        fields.append({"name": "Winner / loser (24h)",
                       "value": f"Most won: **{best[0]}** ${best[1]['net']/100:+.2f}  \u00b7  "
                                f"Most lost: **{worst[0]}** ${worst[1]['net']/100:+.2f}", "inline": False})
    up = _week_stats([r for r in f24 if r.get("side") == "UP"])
    dn = _week_stats([r for r in f24 if r.get("side") == "DOWN"])
    if up["n"] or dn["n"]:
        better = "UP" if up["pct"] >= dn["pct"] else "DOWN"
        fields.append({"name": "Up vs Down (24h)",
                       "value": f"UP: {up['w']}-{up['l']} ({up['pct']}%)  ${up['net']/100:+.2f}\n"
                                f"DOWN: {dn['w']}-{dn['l']} ({dn['pct']}%)  ${dn['net']/100:+.2f}\n"
                                f"Better side: **{better}** by {abs(up['pct']-dn['pct'])} pts", "inline": False})
    btrows = STATE.get("backtest", {}).get("rows", {})
    coins = STATE.get("coins", {})
    cond = []
    for c in COINS:
        v = btrows.get(c, {}).get("vol")
        vtxt = f"{v:.3f}%/min" if v is not None else "vol n/a"
        cond.append(f"**{c}** {vtxt} \u00b7 {(coins.get(c) or {}).get('verdict', '-')}")
    fields.append({"name": "Current conditions", "value": "\n".join(cond), "inline": False})
    by_hour = STATE.get("backtest", {}).get("by_hour", {})
    hrs = [(int(h), b) for h, b in by_hour.items() if b and b.get("n", 0) >= 20]
    if hrs:
        bh = max(hrs, key=lambda x: x[1]["wr"])
        tz = "ET" if LOCAL_TZ else (f"UTC{TZ_OFFSET_HOURS:+d}" if TZ_OFFSET_HOURS else "UTC")
        fields.append({"name": "Best time of day (backtest)",
                       "value": f"{bh[0]:02d}:00\u2013{(bh[0]+1)%24:02d}:00 {tz} \u2014 {bh[1]['wr']:.0f}% win",
                       "inline": False})
    return {"title": "Recent market update", "color": 0x4a86e8, "fields": fields,
            "footer": {"text": "net is stop-aware \u00b7 windows use settled calls only"}}

def send_alert(text):
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                           json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=10)
        except requests.RequestException as e: print(f"  telegram alert failed: {e}")

# ─────────────────────── kalshi / coinbase ───────────────────────
def _get(url, params=None):
    r = http_session.get(url, params=params, timeout=15)     # same URL/params/timeout; pooled connection
    r.raise_for_status()
    return r.json()

def discover_series():
    for s in _get(f"{KALSHI_BASE}/series", {"category": "Crypto"}).get("series", []):
        t = s.get("ticker", "")
        if any(k in t.upper() for k in ("BTC", "ETH", "SOL", "XRP", "15M")):
            print(f"{t:22s}  {s.get('title', '')}")

def current_market(series):
    data = _get(f"{KALSHI_BASE}/markets", {"series_ticker": series, "status": "open", "limit": 100})
    now = dt.datetime.now(dt.timezone.utc)
    def ct(m): return dt.datetime.fromisoformat(m["close_time"].replace("Z", "+00:00"))
    fut = [m for m in data.get("markets", []) if ct(m) > now]
    return min(fut, key=ct) if fut else None

def market_result(ticker):
    try:
        return _get(f"{KALSHI_BASE}/markets/{ticker}").get("market", {}).get("result", "") or ""
    except requests.RequestException:
        return ""

def strike_of(m):
    for k in ("floor_strike", "cap_strike", "strike"):
        if m.get(k) is not None: return float(m[k])
    return None

def _cents(m, key):
    v = m.get(key)
    try: return float(v) * 100.0 if v not in (None, "") else None
    except (TypeError, ValueError): return None

def book_of(m):
    ya = _cents(m, "yes_ask_dollars"); yb = _cents(m, "yes_bid_dollars")
    na = _cents(m, "no_ask_dollars");  nb = _cents(m, "no_bid_dollars")
    if ya is None and nb is not None: ya = 100 - nb
    if na is None and yb is not None: na = 100 - yb
    return yb, ya, nb, na

def candles(product):
    now = time.time()
    ts, data = _candle_cache.get(product, (0, None))
    if data is not None and now - ts < 30: return data
    raw = _get(f"{COINBASE_BASE}/products/{product}/candles", {"granularity": 60})
    raw.sort(key=lambda c: c[0])
    data = {"close": [c[4] for c in raw], "high": [c[2] for c in raw], "low": [c[1] for c in raw]}
    _candle_cache[product] = (now, data)
    return data

_web_candle_cache = {}
def web_candles(product, minutes=60):
    """Return the last `minutes` 1-min OHLC bars for charting: [{t,o,h,l,c}]."""
    nowt = time.time()
    ts, data = _web_candle_cache.get(product, (0, None))
    if data is not None and nowt - ts < 12:
        return data
    raw = _get(f"{COINBASE_BASE}/products/{product}/candles", {"granularity": 60})
    raw.sort(key=lambda c: c[0])   # Coinbase candle = [time, low, high, open, close, vol]
    data = [{"t": int(c[0]), "o": c[3], "h": c[2], "l": c[1], "c": c[4]} for c in raw[-minutes:]]
    _web_candle_cache[product] = (nowt, data)
    return data

def paper_enter(coin):
    """Manually log a paper trade for a coin from the WEBSITE and measure how long
    it took from request to logged order (the 'placement latency')."""
    t0 = time.time()
    cfg = COINS.get(coin)
    if not cfg:
        return {"ok": False, "error": "unknown coin"}
    r = evaluate(coin, cfg)                       # fresh fetch (this is where real latency lives)
    if r.get("status") != "ok" or r.get("side_ask") is None:
        return {"ok": False, "error": r.get("verdict", r.get("status", "no data")),
                "latency_ms": round((time.time() - t0) * 1000)}
    log_call(coin, r)
    try: _append_paper(coin, r)
    except Exception: pass
    _alerted.add(r["ticker"])
    latency = round((time.time() - t0) * 1000)
    return {"ok": True, "coin": coin, "side": r["fav"], "entry": r["side_ask"],
            "contracts": _contracts(r), "stop": r.get("rec_stop"), "latency_ms": latency}

def spot(product):
    return float(_get(f"{COINBASE_BASE}/products/{product}/ticker")["price"])

def crossings(closes, strike, window=15):
    seg = closes[-(window + 1):]
    return sum(1 for i in range(1, len(seg))
               if (seg[i] > strike and seg[i - 1] <= strike)
               or (seg[i] < strike and seg[i - 1] >= strike))

# ─────────────────────── live per-coin evaluation ───────────────────────
def evaluate(coin, cfg):
    m = current_market(cfg["series"])
    if not m: return {"coin": coin, "status": "no market"}
    strike = strike_of(m)
    up_bid, up_ask, dn_bid, dn_ask = book_of(m)
    _spot_t0 = time.time()
    px = spot(cfg["product"])                 # value unchanged; timing is telemetry-only
    _spot_t1 = time.time()
    cd = candles(cfg["product"])
    _spot_meta = {"spot_observed_ts": _spot_t1,
                  "spot_request_latency_ms": round((_spot_t1 - _spot_t0) * 1000.0, 1)}
    closes, highs, lows = cd["close"], cd["high"], cd["low"]

    # --- realized volatility from the NEWEST completed 1-min candles (bug fix) ---
    # Coinbase returns oldest-first, and the last candle can be the current partial
    # minute, so drop it and take the newest VOL_LOOKBACK_MIN+1 COMPLETED closes.
    completed = closes[:-1] if len(closes) > 1 else closes
    recent = [c for c in completed[-(VOL_LOOKBACK_MIN + 1):] if c and c > 0]
    close_t = dt.datetime.fromisoformat(m["close_time"].replace("Z", "+00:00"))
    remain = max((close_t - dt.datetime.now(dt.timezone.utc)).total_seconds() / 60.0, 0.0)

    # guard: malformed/missing data must NOT produce a fake confidence
    if len(recent) < 20 or not strike or strike <= 0 or not px or px <= 0:
        return {"coin": coin, "status": "stale", "reason": "BLOCK_STALE_DATA",
                "verdict": "STALE \u00b7 waiting for clean data", "spot": px,
                "strike": strike, "remain": round(remain, 1), "signal": False, **_spot_meta}
    rets = [math.log(recent[i] / recent[i - 1]) for i in range(1, len(recent))]
    mean = sum(rets) / len(rets)
    sig = math.sqrt(sum((x - mean) ** 2 for x in rets) / (len(rets) - 1)) * VOL_MULT
    if not math.isfinite(sig) or sig <= 0:
        return {"coin": coin, "status": "stale", "reason": "BLOCK_STALE_DATA",
                "verdict": "STALE \u00b7 no volatility", "spot": px, "strike": strike,
                "remain": round(remain, 1), "signal": False, **_spot_meta}

    term = sig * math.sqrt(max(remain, 1e-4))
    z = math.log(px / strike) / term if term > 0 else 0.0
    p_up = norm_cdf(z) * 100.0
    fav_up = p_up >= 50.0
    conf = max(p_up, 100 - p_up)
    side_ask = up_ask if fav_up else dn_ask

    # --- edge: raw, then NET after estimated entry costs (bug fix) ---
    raw_edge = (conf - side_ask) if side_ask is not None else None
    net_edge = (raw_edge - ENTRY_COST_CENTS) if raw_edge is not None else None

    st_val, st_arrow = stoch_rsi(closes); bb = bollinger_pos(closes); a = atr(highs, lows, closes)
    chop = crossings(closes, strike) > CHOP_MAX
    in_win = LAST_STOP_MIN <= remain <= MAX_ENTRY_MIN
    price_ok = side_ask is not None and side_ask >= MIN_PRICE
    conf_ok = conf >= MIN_CONF
    edge_ok = net_edge is not None and net_edge > 0 and net_edge >= EDGE_THRESH
    side = "UP" if fav_up else "DOWN"
    active_ok = ACTIVE_COINS.get(coin, True)             # web toggle: is this coin eligible?
    dir_ok = DIRECTION == "BOTH" or DIRECTION == side    # web toggle: UP / DOWN / BOTH
    signal = (in_win and not chop and conf_ok and price_ok and edge_ok
              and active_ok and dir_ok)                  # net edge > 0 AND eligible

    # --- machine-readable reason code, then a human verdict ---
    if   not active_ok:          reason = "BLOCK_COIN"
    elif not dir_ok:             reason = "BLOCK_DIRECTION"
    elif remain > MAX_ENTRY_MIN: reason = "WAIT_TOO_EARLY"
    elif remain < LAST_STOP_MIN: reason = "WAIT_LATE"
    elif chop:                   reason = "BLOCK_CHOP"
    elif not conf_ok:            reason = "WAIT_LOW_CONFIDENCE"
    elif not price_ok:           reason = "WAIT_BAD_PRICE"
    elif not edge_ok:            reason = "WAIT_NO_EDGE"
    else:                        reason = "ENTER"
    verdict = {
        "BLOCK_COIN": f"Off \u00b7 {coin} disabled",
        "BLOCK_DIRECTION": f"Off \u00b7 {DIRECTION} only",
        "WAIT_TOO_EARLY": f"Wait {side} \u00b7 too early",
        "WAIT_LATE": "Stand down \u00b7 last 2 min",
        "BLOCK_CHOP": "Sit out \u00b7 choppy",
        "WAIT_LOW_CONFIDENCE": f"Wait {side} \u00b7 {conf:.0f}% (needs {MIN_CONF:.0f}%)",
        "WAIT_BAD_PRICE": f"Wait {side} \u00b7 price {side_ask:.0f}\u00a2 < {MIN_PRICE:.0f}",
        "WAIT_NO_EDGE": f"Wait {side} \u00b7 net edge {net_edge:+.1f}\u00a2",
        "ENTER": f"ENTER {side}",
    }[reason]
    rec_stop = side_ask * cfg["sl_pct"] if side_ask is not None else None
    dec = 4 if px < 100 else 2
    if in_win and strike and conf >= MIN_CONF:
        _pending.setdefault(m["ticker"], {"coin": coin, "fav_up": fav_up,
                                          "entry": side_ask, "close": m["close_time"]})
    return {
        "coin": coin, "status": "ok", "ticker": m.get("ticker", ""),
        "remain": round(remain, 1), "spot": round(px, dec), "strike": round(strike, dec) if strike else None,
        "dist": round(px - strike, dec) if strike else None, "dir": "above" if strike and px >= strike else "below",
        "up_bid": up_bid, "up_ask": up_ask, "dn_bid": dn_bid, "dn_ask": dn_ask,
        "stoch": round(st_val) if st_val is not None else None, "stoch_arrow": st_arrow,
        "bb": bb, "atr": round(a, 2) if a is not None else None,
        "conf": round(conf, 1), "fav": side,
        "p_up": round(p_up, 4),   # informational: the SAME P(UP) that conf/fav derive from (percent)
        "spot_raw": px,           # informational: unrounded spot, for telemetry returns
        **_spot_meta,             # informational: spot receive time + request latency
        "edge": round(net_edge, 1) if net_edge is not None else None,   # 'edge' now = NET edge
        "raw_edge": round(raw_edge, 1) if raw_edge is not None else None,
        "net_edge": round(net_edge, 1) if net_edge is not None else None,
        "side_ask": side_ask, "rec_stop": round(rec_stop) if rec_stop is not None else None,
        "sl_pct": int(cfg["sl_pct"] * 100), "verdict": verdict, "reason": reason, "signal": signal,
        "close": m["close_time"],
    }

# ─────────────────────── live stats (settled outcomes) ───────────────────────
def _load(path, default):
    try:
        with open(path) as f: return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError): return default

def _save(path, obj):
    try:
        with open(path, "w") as f: json.dump(obj, f)
    except OSError as e: print(f"  write failed ({path}): {e}")

TRADES_CSV_COLUMNS = ["ts", "coin", "side", "entry", "stop", "contracts", "low", "high",
                      "bid_lo", "exit_price", "exit_reason", "settle_result",
                      "win", "per_contract", "pnl", "ticker", "signal_ts"]

def _trades_csv_ready(cols):
    """If an existing trades CSV has a different header (e.g. pre-provenance), rename it
    to *.legacy-<time>.csv so rows of two schemas are never mixed. Nothing is deleted."""
    try:
        if os.path.exists(TRADES_CSV) and os.path.getsize(TRADES_CSV) > 0:
            with open(TRADES_CSV, newline="") as f:
                hdr = next(csv.reader(f), None)
            if hdr != cols:
                base = TRADES_CSV[:-4] if TRADES_CSV.endswith(".csv") else TRADES_CSV
                os.replace(TRADES_CSV, f"{base}.legacy-{int(time.time())}.csv")
    except OSError as e:
        print(f"  csv schema check failed: {e}")

def _append_csv(row):
    cols = TRADES_CSV_COLUMNS
    try:
        _trades_csv_ready(cols)
        new = not os.path.exists(TRADES_CSV)
        with open(TRADES_CSV, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            if new: w.writeheader()
            w.writerow({c: row.get(c, "") for c in cols})
    except OSError as e:
        print(f"  csv write failed: {e}")

def settle_pending():
    now = dt.datetime.now(dt.timezone.utc)
    rows = _load(RESULTS_FILE, []); changed = False
    for tk, info in list(_pending.items()):
        close_t = dt.datetime.fromisoformat(info["close"].replace("Z", "+00:00"))
        if now < close_t + dt.timedelta(seconds=20): continue
        res = market_result(tk)
        if res not in ("yes", "no"):
            if now > close_t + dt.timedelta(minutes=10): _pending.pop(tk, None)
            continue
        win = (res == "yes") == info["fav_up"]; entry = info["entry"] or 0
        rows.append({"ts": now.isoformat(), "coin": info["coin"], "win": win,
                     "pnl": round((100 - entry) if win else -entry, 1)})
        _pending.pop(tk, None); changed = True
    if changed: _save(RESULTS_FILE, rows)
    return rows

def compute_stats(rows):
    now = dt.datetime.now(dt.timezone.utc)
    def within(r, hrs): return (now - dt.datetime.fromisoformat(r["ts"])).total_seconds() <= hrs * 3600
    def block(sub):
        n = len(sub); w = sum(1 for r in sub if r["win"])
        return {"w": w, "n": n, "pct": round(100 * w / n) if n else 0,
                "avg": round(sum(r["pnl"] for r in sub) / n, 1) if n else 0.0}
    def streak(sub):
        s = 0
        for r in reversed(sub):
            if r["win"]: s += 1
            else: break
        return s
    out = {}; groups = {"ALL": rows}
    for c in COINS: groups[c] = [r for r in rows if r["coin"] == c]
    for name, sub in groups.items():
        out[name] = {"all": block(sub), "h24": block([r for r in sub if within(r, 24)]),
                     "h2": block([r for r in sub if within(r, 2)]), "streak": streak(sub),
                     "sl": int(COINS[name]["sl_pct"] * 100) if name in COINS else "\u2014", "tp": "hold"}
    return out

# ─────────────────────── call journal + W/L ───────────────────────
def call_record(rows):
    settled = [r for r in rows if r.get("win") is not None]   # skip unfilled maker orders
    n = len(settled); w = sum(1 for r in settled if r["win"])
    s = 0
    for r in reversed(settled):
        if r["win"]: s += 1
        else: break
    net = sum(r.get("pnl", 0) for r in rows)
    staked = sum(r.get("entry", 0) * r.get("contracts", 1) for r in rows if r.get("win") is not None)
    return {"w": w, "n": n, "pct": round(100 * w / n) if n else 0, "streak": s,
            "pnl": round(net, 1), "margin": round(net / staked * 100, 1) if staked else 0.0}

def log_call(coin, r):
    fav_bid = r["up_bid"] if r["fav"] == "UP" else r["dn_bid"]
    ask = r["side_ask"]
    limit = _limit_price(r)
    # entry assumption depends on the mode:
    #  taker -> assume immediate fill at the ask
    #  maker -> rest at the limit; fill only if the ask later trades down to it
    assumed = ask if ENTRY_MODE == "taker" else limit
    _call_pending[r["ticker"]] = {"coin": coin, "side": r["fav"], "quote_ask": ask,
                                  "entry_mode": ENTRY_MODE, "entry_limit": limit,
                                  "assumed_entry": assumed, "entry": assumed,
                                  "stop": r["rec_stop"], "contracts": _contracts(r), "close": r["close"],
                                  "lo": ask, "hi": ask, "ask_lo": ask,
                                  "bid_lo": fav_bid if fav_bid is not None else ask,
                                  "maker_filled": ENTRY_MODE == "taker",
                                  # provenance only: exact join keys for later analysis
                                  "ticker": r["ticker"], "signal": bool(r.get("signal")),
                                  "signal_ts_epoch_ms": (int(round(r["spot_observed_ts"] * 1000))
                                                         if r.get("spot_observed_ts") else None),
                                  "signal_ts": (dt.datetime.fromtimestamp(r["spot_observed_ts"], dt.timezone.utc).isoformat()
                                                if r.get("spot_observed_ts") else None)}

def settle_calls():
    now = dt.datetime.now(dt.timezone.utc)
    rows = _load(CALLS_FILE, []); changed = False
    for tk, info in list(_call_pending.items()):
        close_t = dt.datetime.fromisoformat(info["close"].replace("Z", "+00:00"))
        if now < close_t + dt.timedelta(seconds=20): continue
        res = market_result(tk)
        if res not in ("yes", "no"):
            if now > close_t + dt.timedelta(minutes=30):
                print(f"  giving up on unsettled call {tk} (no result after 30 min)")
                _call_pending.pop(tk, None)
            continue
        settle_win = (res == "yes") == (info["side"] == "UP")
        entry = info["entry"] or 0
        stop = info.get("stop") or round(entry * 0.7, 1)
        contracts = info.get("contracts", 0)

        # Stage 4: a resting MAKER order that never got filled is NOT a trade.
        if not info.get("maker_filled", True):
            row = {"ts": now.isoformat(), "coin": info["coin"], "side": info["side"],
                   "entry": entry, "stop": stop, "contracts": 0, "entry_mode": info.get("entry_mode"),
                   "low": info.get("lo", entry), "high": info.get("hi", entry),
                   "bid_lo": info.get("bid_lo", entry), "exit_price": 0.0,
                   "exit_reason": "unfilled", "settle_result": res, "win": None,
                   "per_contract": 0.0, "pnl": 0.0,
                   "ticker": tk, "signal_ts": info.get("signal_ts"),
                   "signal_ts_epoch_ms": info.get("signal_ts_epoch_ms")}
            rows.append(row); _append_csv(row); _call_pending.pop(tk, None); changed = True
            print(f"  UNFILLED (maker): {info['coin']} {info['side']} limit {entry:.0f}c never traded")
            continue

        bid_lo = info.get("bid_lo", info.get("lo", entry))   # lowest executable exit seen
        # A stop is TRIGGERED only when the executable exit side (the bid) reaches it.
        # If the book gapped through the stop, exit at the worse (lower) price rather
        # than pretending an exact stop fill. A stopped trade is a loss regardless of
        # where the market settled afterward.
        stopped = bid_lo <= stop
        if stopped:
            exit_price = min(bid_lo, stop)
            win = False
            exit_reason = "stop"
        else:
            exit_price = 100.0 if settle_win else 0.0
            win = settle_win
            exit_reason = "settle"
        # realistic round-trip fee: entry fill + exit fill, per Kalshi's formula
        fee = kalshi_fee_cents(entry, 1) + kalshi_fee_cents(exit_price, 1)
        per_contract = round((exit_price - entry) - fee, 1)
        pnl = round(per_contract * contracts, 1)
        row = {"ts": now.isoformat(), "coin": info["coin"], "side": info["side"],
               "entry": entry, "stop": stop, "contracts": contracts,
               "entry_mode": info.get("entry_mode"),
               "low": info.get("lo", entry), "high": info.get("hi", entry),
               "bid_lo": round(bid_lo, 1), "exit_price": round(exit_price, 1),
               "exit_reason": exit_reason, "settle_result": res,
               "win": win, "per_contract": per_contract, "pnl": pnl,
               "ticker": tk, "signal_ts": info.get("signal_ts"),
               "signal_ts_epoch_ms": info.get("signal_ts_epoch_ms")}
        rows.append(row)
        _append_csv(row)
        _call_pending.pop(tk, None); changed = True
        rec = call_record(rows)
        print(f"  RESULT: {info['coin']} {info['side']} {'WON' if win else 'LOST'} "
              f"-> record {rec['w']}-{rec['n'] - rec['w']}, net {rec['pnl']:+.0f}c")
        bot_post("journal", build_result_embed(row, rec))
    if changed: _save(CALLS_FILE, rows)
    return call_record(rows)

# ─────────────────────── backtest (auto, cached) ───────────────────────
def fetch_history(product, days):
    end = dt.datetime.now(dt.timezone.utc)
    floor = end - dt.timedelta(days=days, minutes=VOL_LOOKBACK_MIN + 30)
    out, cursor = {}, end
    while cursor > floor:
        seg_start = max(cursor - dt.timedelta(minutes=300), floor)
        for _ in range(3):
            try:
                r = requests.get(f"{COINBASE_BASE}/products/{product}/candles",
                                 params={"granularity": 60, "start": seg_start.isoformat(),
                                         "end": cursor.isoformat()}, timeout=20)
                r.raise_for_status()
                for c in r.json(): out[int(c[0])] = float(c[4])
                break
            except requests.RequestException:
                time.sleep(1.0)
        cursor = seg_start
        time.sleep(0.3)
    return out

def _vol_at(cd, t):
    cs = [cd.get(t - i * 60) for i in range(VOL_LOOKBACK_MIN, -1, -1)]
    cs = [c for c in cs if c is not None]
    if len(cs) < 20: return None
    rets = [math.log(cs[i] / cs[i - 1]) for i in range(1, len(cs))]
    m = sum(rets) / len(rets)
    return math.sqrt(sum((x - m) ** 2 for x in rets) / (len(rets) - 1)) * VOL_MULT

def backtest_coin(coin, cfg, cd):
    trades = []
    if not cd: return trades
    keys = sorted(cd); t_min, t_max = keys[0], keys[-1]
    b = ((t_min // 900) + 1) * 900
    while b + INTERVAL_MIN * 60 <= t_max:
        strike = cd.get(b)
        if strike:
            for elapsed in range(int(INTERVAL_MIN - MAX_ENTRY_MIN), int(INTERVAL_MIN - LAST_STOP_MIN) + 1):
                t = b + elapsed * 60; left = INTERVAL_MIN - elapsed
                price = cd.get(t); vol = _vol_at(cd, t) if price is not None else None
                if price is None or vol is None: continue
                conf, fav_up = conf_side(price, strike, vol, left)
                if conf is None or conf < MIN_CONF: continue
                entry = conf; stop = entry * cfg["sl_pct"]; stopped = False; pnl = None
                for e2 in range(elapsed + 1, INTERVAL_MIN + 1):
                    p2 = cd.get(b + e2 * 60)
                    if p2 is None: continue
                    v2 = _vol_at(cd, b + e2 * 60) or vol
                    c2, fu2 = conf_side(p2, strike, v2, max(INTERVAL_MIN - e2, 1e-4))
                    if c2 is None: continue
                    our = c2 if fu2 == fav_up else 100 - c2
                    if our <= stop:
                        pnl = stop - entry - FEE_CENTS; stopped = True; break
                final = cd.get(b + INTERVAL_MIN * 60)
                win = None if final is None else ((final >= strike) == fav_up)
                if not stopped:
                    if win is None: break
                    pnl = (100 - entry - FEE_CENTS) if win else (-entry - FEE_CENTS)
                trades.append({"coin": coin, "ts": b, "conf": round(conf, 1), "win": win, "pnl": round(pnl, 1)})
                break
        b += 900
    return trades

def parse_range(v):
    """Turn 30, '30d', '48h', '2w', '3m' into a number of days."""
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().lower()
    try:
        if s.endswith("h"): return float(s[:-1]) / 24.0
        if s.endswith("d"): return float(s[:-1])
        if s.endswith("w"): return float(s[:-1]) * 7.0
        if s.endswith("m"): return float(s[:-1]) * 30.0
        return float(s)
    except ValueError:
        return 30.0

def run_backtest(rng=None):
    ndays = parse_range(rng if rng is not None else BACKTEST_DAYS)
    all_tr, spans, vol_by = [], [], {}
    for coin, cfg in COINS.items():
        try:
            cd = fetch_history(cfg["product"], ndays)
        except Exception:
            cd = {}
        if cd:
            spans.append((max(cd) - min(cd)) / 86400.0)
            cl = [cd[t] for t in sorted(cd)]
            rets = [math.log(cl[i] / cl[i - 1]) for i in range(1, len(cl)) if cl[i - 1] > 0]
            if len(rets) > 2:
                mu = sum(rets) / len(rets)
                vol_by[coin] = round((sum((x - mu) ** 2 for x in rets) / (len(rets) - 1)) ** 0.5 * 100, 3)
        all_tr += backtest_coin(coin, cfg, cd)
    days = max(spans) if spans else ndays
    def blk(tr):
        settled = [t for t in tr if t["win"] is not None]
        n = len(settled); w = sum(1 for t in settled if t["win"])
        wr = 100 * w / n if n else 0.0
        exp85 = (wr / 100) * (100 - GATE_PRICE) - (1 - wr / 100) * GATE_PRICE - FEE_CENTS
        return {"n": len(tr), "per_day": round(len(tr) / days, 1) if days else 0,
                "wr": round(wr, 1), "exp85": round(exp85, 2),
                "avg": round(sum(t["pnl"] for t in tr) / len(tr), 2) if tr else 0.0}
    rows = {"ALL": blk(all_tr)}
    for c in COINS: rows[c] = blk([t for t in all_tr if t["coin"] == c])
    rows["ALL"]["net"] = round(sum(t["pnl"] for t in all_tr), 1); rows["ALL"]["vol"] = None
    for c in COINS:
        rows[c]["net"] = round(sum(t["pnl"] for t in all_tr if t["coin"] == c), 1)
        rows[c]["vol"] = vol_by.get(c)
    cutoff = time.time() - BACKTEST_RECENT_HOURS * 3600
    rec_tr = [t for t in all_tr if t.get("ts", 0) >= cutoff]
    def rblk(tr):
        s = [t for t in tr if t["win"] is not None]; n = len(s)
        w = sum(1 for t in s if t["win"]); wr = 100 * w / n if n else 0.0
        exp85 = (wr / 100) * (100 - GATE_PRICE) - (1 - wr / 100) * GATE_PRICE - FEE_CENTS
        return {"n": len(tr), "wr": round(wr, 1), "exp85": round(exp85, 2)}
    recent = {"ALL": rblk(rec_tr)}
    for c in COINS: recent[c] = rblk([t for t in rec_tr if t["coin"] == c])
    dows = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    by_dow = {}
    for i, d in enumerate(dows):
        by_dow[d] = rblk([t for t in all_tr
                          if dt.datetime.fromtimestamp(t["ts"], dt.timezone.utc).weekday() == i])
    by_hour = {}
    for h in range(24):
        by_hour[h] = rblk([t for t in all_tr
                           if local_hour(t["ts"]) == h])
    calib = []; settled = [t for t in all_tr if t["win"] is not None]
    for lo in (80, 85, 90, 95):
        hi = lo + 5
        bucket = [t for t in settled if lo <= t["conf"] < (hi if lo < 95 else 100.1)]
        if bucket:
            wr = 100 * sum(1 for t in bucket if t["win"]) / len(bucket)
            calib.append({"band": f"{lo}-{hi}", "n": len(bucket), "pred": round((lo + hi) / 2), "actual": round(wr, 1)})
    return {"status": "ok", "days": round(days, 1), "rows": rows, "recent": recent,
            "recent_hours": BACKTEST_RECENT_HOURS, "by_dow": by_dow, "by_hour": by_hour,
            "calib": calib, "trades": len(all_tr), "updated": dt.datetime.now().isoformat()}

def _age_hours(iso):
    try:
        return (dt.datetime.now() - dt.datetime.fromisoformat(iso)).total_seconds() / 3600
    except Exception:
        return 1e9

def backtest_worker():
    cached = _load(BACKTEST_CACHE_FILE, None)
    if cached:
        with LOCK: STATE["backtest"] = cached
    while True:
        cached = _load(BACKTEST_CACHE_FILE, None)
        if not cached or _age_hours(cached.get("updated", "")) >= BACKTEST_REFRESH_HOURS:
            with LOCK:
                STATE["backtest"] = {**(cached or {}), "status": "running", "days": BACKTEST_DAYS}
            try:
                res = run_backtest(); _save(BACKTEST_CACHE_FILE, res)
                with LOCK: STATE["backtest"] = res
            except Exception as e:
                with LOCK: STATE["backtest"] = {"status": f"error: {e}"}
        time.sleep(3600)

def weekly_worker():
    while True:
        mark = _load(WEEKLY_MARK_FILE, None)
        due = (not mark) or _age_hours(mark.get("updated", "")) >= 24 * 7
        if due and STATE.get("backtest", {}).get("status") == "ok":
            try:
                bot_post("journal", build_weekly_embed())
                _save(WEEKLY_MARK_FILE, {"updated": dt.datetime.now().isoformat()})
            except Exception as e:
                print(f"  weekly report failed: {e}")
        time.sleep(3600)

# ─────────────────────── passive perp telemetry hook ───────────────────────
_perp = None
_perp_init_lock = threading.Lock()

def _perp_publish(rows):
    t = _perp
    health = None
    try:
        health = t.health() if t is not None else None
    except Exception as e:
        health = {"error": str(e)}
    views = _perp_vol_views(rows)
    with LOCK:
        STATE["perps"] = rows
        STATE["perp_vol"] = views
        if health is not None:
            STATE["perp_health"] = health
    _shadow_process(rows)          # Step 4: observation only, on this worker thread

def _perp_vol_views(rows):
    """Display-only volatility/stability summary per coin (schema v3 telemetry). Never raises and
    is never read by the strategy. A feature is labelled LIVE only while an ACTIVE promoted live
    veto actually uses it; everything else is observational telemetry."""
    try:
        import perp_telemetry as pt
        g = _gate
        live = (g.promotion or {}).get("feature_name") if g is not None and g.active else None
        out = {}
        for c, r in (rows or {}).items():
            v = pt.dashboard_view(r)
            v["live_veto_feature"] = live
            out[c] = v
        return out
    except Exception as e:
        return {"error": str(e)[:200]}

# ─────────────────────── Step 4: shadow-only perp filter validation ───────────────────────
_shadow = None
_shadow_init_lock = threading.Lock()

def _shadow_first_signal(ticker):
    """What the REAL bot did for this ticker (read-only): the time of the evaluation that
    produced its first logged call. Lets the shadow evaluator detect that the true first
    signal's telemetry row was dropped, instead of silently scoring a later signal."""
    cp = _call_pending.get(ticker)
    if not cp or not cp.get("signal"):
        return None
    return {"signal_ts_epoch_ms": cp.get("signal_ts_epoch_ms"), "rec_stop": cp.get("stop")}

def _shadow_evaluator():
    """Lazy, fail-open. The live path NEVER allows synthetic policies."""
    global _shadow
    if _shadow is not None:
        return _shadow
    with _shadow_init_lock:
        if _shadow is None:
            try:
                import perp_shadow as ps
                _shadow = ps.ShadowEvaluator.from_files(
                    PERP_SHADOW_POLICY_FILE, PERP_SHADOW_JOURNAL, first_signal_lookup=_shadow_first_signal,
                    max_age_days=PERP_SHADOW_POLICY_MAX_AGE_DAYS)
            except Exception as e:
                print(f"  perp shadow unavailable (continuing): {e}")
                return None
    return _shadow

def _shadow_process(rows):
    """Score COPIES of already-built causal telemetry rows. Never raises, never touches
    calls, signals, sizing or posts; only writes the shadow journal + STATE['perp_shadow']."""
    try:
        if not PERP_SHADOW_ENABLED:
            with LOCK:
                STATE["perp_shadow"] = {"status": "disabled"}
            return
        ev = _shadow_evaluator()
        if ev is None:
            with LOCK:
                STATE["perp_shadow"] = {"status": "error", "last_error": "evaluator unavailable"}
            return
        ev.process_rows({c: dict(r) for c, r in (rows or {}).items()})
        h = ev.health()
        with LOCK:
            STATE["perp_shadow"] = h
    except Exception as e:
        try:
            with LOCK:
                STATE["perp_shadow"] = {"status": "error", "last_error": str(e)[:200]}
        except Exception:
            pass

def _perp_telemetry():
    """Lazily build the telemetry recorder (never raises; returns None if it can't)."""
    global _perp
    if _perp is not None:
        return _perp
    with _perp_init_lock:
        if _perp is None:
            try:
                import perp_telemetry as pt
                # provider cache TTL is HALF the sampling interval, otherwise a
                # monotonic 4 s schedule would hit a 4 s cache every other tick.
                prov = pt.KalshiPerpProvider(
                    base_url=PERP_API_BASE, ticker_overrides=PERP_TICKERS,
                    sample_seconds=PERP_SAMPLE_SECONDS / 2.0,
                    funding_refresh_seconds=PERP_FUNDING_REFRESH_SECONDS,
                    error_backoff_seconds=PERP_ERROR_BACKOFF_SECONDS, timeout=PERP_HTTP_TIMEOUT)
                store = pt.SnapshotStore(list(COINS), PERP_SNAPSHOT_HISTORY_MAXLEN,
                                         PERP_SNAPSHOT_HISTORY_SECONDS)
                sampler = pt.PerpSampler(prov, list(COINS), store, interval_s=PERP_SAMPLE_SECONDS,
                                         error_backoff_s=PERP_ERROR_BACKOFF_SECONDS)
                _perp = pt.PerpTelemetry(prov, list(COINS), log_path=PERP_LOG_FILE,
                                         stale_seconds=PERP_STALE_SECONDS,
                                         lookback_tolerance_s=PERP_LOOKBACK_TOLERANCE_SECONDS,
                                         on_rows=_perp_publish, sampler=sampler,
                                         max_lag_s=PERP_ALIGNMENT_MAX_LAG_SECONDS,
                                         snapshot_maxlen=PERP_SNAPSHOT_HISTORY_MAXLEN,
                                         snapshot_max_age_s=PERP_SNAPSHOT_HISTORY_SECONDS)
                _perp.start_sampler()
            except Exception as e:
                print(f"  perp telemetry unavailable (continuing): {e}")
                return None
    return _perp

def _perp_submit(coins_out, spot_ts):
    """Hand one cycle's results to the telemetry worker thread. Observation only:
    it receives copies, runs off-thread, and its failures never reach the watcher."""
    try:
        if not PERP_TELEMETRY_ENABLED:
            with LOCK:
                STATE["perps"] = {c: {"coin": c, "source_status": "disabled"} for c in COINS}
                STATE["perp_health"] = {"sampler_alive": False, "disabled": True}
            return
        t = _perp_telemetry()
        if t is not None:
            t.submit(coins_out, spot_ts)
    except Exception as e:
        print(f"  perp telemetry skipped: {e}")

# ─────────────────────── Step 6: manually promoted live veto (suppress-only) ───────────────────────
_gate = None
_gate_lock = threading.Lock()

def _gate_instance():
    """Built once, at first use. Integrity is validated there; any failure leaves the gate
    inactive and the legacy call path untouched."""
    global _gate
    if _gate is not None:
        return _gate
    with _gate_lock:
        if _gate is None:
            try:
                import perp_live as pl
                _gate = pl.LiveVetoGate.from_files(
                    PERP_LIVE_PROMOTION_FILE, PERP_SHADOW_POLICY_FILE, PERP_STEP5_BASELINE_MANIFEST,
                    __file__, PERP_LIVE_VETO_JOURNAL, env_enabled=PERP_LIVE_VETO_ENABLED,
                    kill_file=PERP_LIVE_VETO_KILL_FILE,
                    runtime_config={"BANKROLL": BANKROLL, "ENTRY_MODE": ENTRY_MODE})
            except Exception as e:
                print(f"  live gate unavailable (continuing with legacy behaviour): {e}")
                return None
    return _gate

def _gate_banner():
    g = _gate_instance()
    print(g.banner() if g is not None else "PERP LIVE VETO: OFF — legacy call behavior")
    _gate_publish()

def _gate_publish():
    g = _gate
    try:
        h = g.health() if g is not None else {"configured": False, "active": False, "status": "INACTIVE"}
    except Exception as e:
        h = {"configured": False, "active": False, "status": "ERROR", "last_error": str(e)[:200]}
    with LOCK:
        STATE["perp_live_veto"] = h

def _gate_decision(coin, r):
    """Returns the gate result for an existing legacy signal, or None. Never raises."""
    try:
        g = _gate_instance()
        if g is None:
            return None
        t = _perp_telemetry() if PERP_TELEMETRY_ENABLED else None
        prev = t.preview_row(coin, r) if t is not None else None     # read-only, no network
        res = g.evaluate(coin, r, prev)
        _gate_publish()
        return res
    except Exception as e:
        print(f"  live gate skipped: {e}")
        return None

# ─────────────────────── live poller ───────────────────────
def poller():
    maybe_post_explanation()
    bot_post("calls", {"title": "Watcher online", "color": 0x3ddc84,
                       "description": "Kalshi 15m watcher started \u2014 calls are live."})
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_alert("Kalshi 15m watcher started.")
    while True:
        try:
            if not RUNNING.is_set():
                time.sleep(1); continue
            coins_out = {}
            spot_ts = {}      # when each coin's spot was observed (perp telemetry only)
            for coin, cfg in COINS.items():
                try: r = evaluate(coin, cfg)
                except requests.RequestException: r = {"coin": coin, "status": "net error"}
                except Exception as e: r = {"coin": coin, "status": f"error: {e}"}
                spot_ts[coin] = r.get("spot_observed_ts")   # captured at the spot response
                coins_out[coin] = r
                cp = _call_pending.get(r.get("ticker"))
                if cp is not None and r.get("status") == "ok":
                    sa = r["up_ask"] if cp["side"] == "UP" else r["dn_ask"]
                    if sa is not None:
                        cp["lo"] = min(cp.get("lo", sa), sa)
                        cp["hi"] = max(cp.get("hi", sa), sa)
                        cp["ask_lo"] = min(cp.get("ask_lo", sa), sa)
                        # a resting maker buy fills only if the ask trades down to the limit
                        if not cp.get("maker_filled") and sa <= cp.get("entry_limit", sa):
                            cp["maker_filled"] = True
                    sb = r["up_bid"] if cp["side"] == "UP" else r["dn_bid"]
                    if sb is not None:
                        cp["bid_lo"] = min(cp.get("bid_lo", sb), sb)
                if r.get("signal") and r.get("ticker") not in _alerted:
                    _g = _gate_decision(coin, r)          # AFTER evaluate(), BEFORE any call action
                    if _g is not None and _g.get("decision") == "BLOCK":
                        _alerted.add(r["ticker"])         # first signal handled: no callout, audit only
                        print(f"  CALL SUPPRESSED by validated gate: {coin} {r['fav']} "
                              f"({r['ticker']}) {_g.get('reason')}")
                        continue
                    try:
                        crec = call_record([x for x in _load(CALLS_FILE, []) if x.get("coin") == coin])
                        if ON_CALL:
                            try:
                                ON_CALL(coin, r, crec)
                            except Exception as e:
                                print(f"  call post failed: {e}")
                        else:
                            print(f"  {coin} {r['fav']} call ready (bot not attached; no channel post)")
                        log_call(coin, r)
                        _append_paper(coin, r)
                        _alerted.add(r["ticker"])
                        print(f"  CALL logged: {coin} {r['fav']} @ {r['side_ask']:.0f}c ({r['ticker']})")
                    except Exception as e:
                        print(f"  call-log error for {coin}: {e}")
            _perp_submit(coins_out, spot_ts)   # passive, non-blocking, cannot raise
            try:
                stats = compute_stats(settle_pending())
            except Exception as e:
                print(f"  settle_pending error: {e}"); stats = STATE.get("stats", {})
            try:
                calls = settle_calls()
            except Exception as e:
                print(f"  settle_calls error: {e}"); calls = STATE.get("calls", {})
            fresh = {}
            for c, rr in coins_out.items():
                if rr.get("status") == "ok":
                    fresh[c] = dt.datetime.now(dt.timezone.utc).isoformat()
            with LOCK:
                STATE["coins"] = coins_out; STATE["stats"] = stats; STATE["calls"] = calls
                STATE["controls"] = controls_snapshot()
                prev = STATE.get("fresh", {})
                prev.update(fresh); STATE["fresh"] = prev
                STATE["updated"] = dt.datetime.now().strftime("%H:%M:%S")
                STATE["running"] = RUNNING.is_set()
        except Exception as e:
            print(f"  poller loop error (continuing): {e}")
        time.sleep(POLL_SECONDS)

def controls_snapshot():
    return {"active": dict(ACTIVE_COINS), "direction": DIRECTION,
            "entry_mode": ENTRY_MODE, "bankroll": BANKROLL, "running": RUNNING.is_set()}

def apply_control(action, payload):
    """Mutate engine controls from a website POST. Returns the new snapshot."""
    global DIRECTION, ENTRY_MODE, BANKROLL
    if action == "pause":       set_watcher_state(False)
    elif action == "resume":    set_watcher_state(True)
    elif action == "coin":      ACTIVE_COINS[payload.get("coin")] = bool(payload.get("on"))
    elif action == "direction": DIRECTION = payload.get("value", "BOTH") if payload.get("value") in ("UP", "DOWN", "BOTH") else DIRECTION
    elif action == "entry_mode": ENTRY_MODE = "maker" if payload.get("value") == "maker" else "taker"
    elif action == "bankroll":
        try: BANKROLL = max(float(payload.get("value", BANKROLL)), 1.0)
        except (TypeError, ValueError): pass
    return controls_snapshot()

# ─────────────────────── web page ───────────────────────
PAGE = """<!doctype html><html><head><meta charset="utf-8"><title>Kalshi 15m Paper Desk</title>
<script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<style>
 :root{--bg:#0a0f0d;--panel:#0f1512;--line:#1d2a24;--dim:#5f7168;--fg:#d6e6df;--grn:#3ddc84;--amb:#e6b84c;--red:#ff5c5c;--blu:#4a86e8}
 *{box-sizing:border-box}
 body{margin:0;background:var(--bg);color:var(--fg);font:13px/1.4 "Cascadia Mono","Consolas",ui-monospace,monospace}
 .wrap{max-width:1180px;margin:0 auto;padding:14px}
 .top{display:flex;flex-wrap:wrap;gap:10px 18px;align-items:center;border:1px solid var(--line);background:var(--panel);border-radius:8px;padding:10px 12px;margin-bottom:12px}
 .top b{color:var(--fg)} .dim{color:var(--dim)} .grn{color:var(--grn)} .red{color:var(--red)} .amb{color:var(--amb)}
 .btn{background:#12201a;border:1px solid var(--line);color:var(--fg);border-radius:6px;padding:4px 9px;cursor:pointer;font:inherit}
 .btn:hover{border-color:var(--grn)} .btn.on{background:var(--grn);color:#08120c;font-weight:700;border-color:var(--grn)}
 .btn.off{opacity:.5}
 .seg{display:inline-flex;border:1px solid var(--line);border-radius:6px;overflow:hidden}
 .seg .btn{border:0;border-radius:0}
 .grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
 @media(max-width:820px){.grid{grid-template-columns:1fr}}
 .card{border:1px solid var(--line);background:var(--panel);border-radius:8px;padding:10px 12px}
 .chd{display:flex;justify-content:space-between;align-items:center;margin-bottom:6px}
 .sym{font-weight:800;font-size:15px} .tick{color:var(--dim);font-size:10px}
 .cd{font-weight:700;color:var(--grn)}
 .rowline{display:flex;justify-content:space-between;font-size:12px}
 .k{color:var(--dim)} .chart{height:150px;margin:8px 0}
 .verdict{border:1px solid var(--line);border-radius:6px;padding:6px 8px;margin-top:6px;text-align:center}
 .go{background:var(--grn);color:#08120c;font-weight:800}.warn{color:var(--amb)}.stop{color:var(--red)}.blk{color:var(--dim)}
 .mini{font-size:11px;color:var(--dim)}
 .foot{color:var(--dim);font-size:11px;margin-top:10px}
 input[type=number]{background:#0a120e;border:1px solid var(--line);color:var(--fg);width:74px;border-radius:5px;padding:3px 5px;font:inherit}
</style></head><body><div class="wrap"><div id="gatebar" class="dim" style="font-size:11px;margin:4px 0">Perp live veto: OFF</div>
 <div class="top">
   <b>Kalshi 15m Paper Desk</b>
   <span>watcher <b id="run" class="grn">?</b></span>
   <button class="btn" id="toggleRun">pause</button>
   <span class="dim">mode</span>
   <span class="seg"><button class="btn" data-em="taker">taker</button><button class="btn" data-em="maker">maker</button></span>
   <span class="dim">dir</span>
   <span class="seg"><button class="btn" data-dir="UP">UP</button><button class="btn" data-dir="DOWN">DOWN</button><button class="btn" data-dir="BOTH">BOTH</button></span>
   <span class="dim">bankroll $</span><input type="number" id="bank" min="1" step="50">
   <button class="btn" id="setBank">set</button>
   <span class="dim">coins</span><span id="coinToggles"></span>
   <span class="dim">latency</span><b id="lat" class="grn">–</b>
   <span class="dim">updated</span><span id="upd" class="dim">–</span>
 </div>
 <div class="grid" id="grid"></div>
 <div class="foot">Paper / demo only — no real orders are placed. Charts: 60m 1-min spot with the strike line.
  Latency is the round-trip to place a paper order once you press Enter.</div>
</div>
<script>
const COINS=["BTC","ETH","SOL","XRP"];
const charts={}, series={}, strikeLine={}, chartKind={};
function esc(s){return String(s==null?"":s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));}
function fmt(x){return x==null?"–":Math.round(x)+"¢";}

function buildCards(){
 const g=document.getElementById("grid"); g.innerHTML="";
 COINS.forEach(c=>{
  const el=document.createElement("div"); el.className="card"; el.id="card_"+c;
  el.innerHTML=`<div class="chd"><span><span class="sym">${c}</span> <span class="tick" id="tk_${c}"></span></span>
     <span><span class="seg"><button class="btn" data-kind="line" data-coin="${c}">line</button><button class="btn" data-kind="cand" data-coin="${c}">candle</button></span>
     &nbsp;<span class="cd" id="cd_${c}">–</span></span></div>
   <div class="rowline"><span class="k">spot / strike</span><span id="ss_${c}">–</span></div>
   <div class="rowline"><span class="k">UP bid/ask · DN bid/ask</span><span id="bk_${c}">–</span></div>
   <div class="rowline"><span class="k">conf · edge raw/net</span><span id="ce_${c}">–</span></div>
   <div class="rowline"><span class="k">stop · size</span><span id="st_${c}">–</span></div>
   <div class="rowline mini"><span class="k">perp · prem · fund · lead30 (observe only)</span><span id="pf_${c}">–</span></div>
   <div class="rowline mini"><span class="k" id="pvk_${c}">perp vol · stability (telemetry, observe only)</span><span id="pv_${c}">–</span></div>
   <div class="chart" id="ch_${c}"></div>
   <div class="verdict blk" id="vd_${c}">…</div>
   <div style="margin-top:6px;text-align:center"><button class="btn" id="pe_${c}">Enter paper trade</button> <span class="mini" id="pm_${c}"></span></div>`;
  g.appendChild(el);
  const ch=LightweightCharts.createChart(document.getElementById("ch_"+c),{
    height:150, layout:{background:{color:"transparent"},textColor:"#5f7168"},
    grid:{vertLines:{color:"#12201a"},horzLines:{color:"#12201a"}},
    timeScale:{timeVisible:true,secondsVisible:false,borderColor:"#1d2a24"},
    rightPriceScale:{borderColor:"#1d2a24"}});
  charts[c]=ch; chartKind[c]="line";
  series[c]=ch.addLineSeries({color:"#3ddc84",lineWidth:2});
  loadCandles(c);
 });
 // chart kind toggles
 document.querySelectorAll("[data-kind]").forEach(b=>b.onclick=()=>{setKind(b.dataset.coin,b.dataset.kind);});
 // paper-enter buttons
 COINS.forEach(c=>{document.getElementById("pe_"+c).onclick=()=>paperEnter(c);});
}
function setKind(c,kind){
 if(chartKind[c]===kind) return;
 charts[c].removeSeries(series[c]);
 series[c]= kind==="cand" ? charts[c].addCandlestickSeries({upColor:"#3ddc84",downColor:"#ff5c5c",wickUpColor:"#3ddc84",wickDownColor:"#ff5c5c",borderVisible:false})
                          : charts[c].addLineSeries({color:"#3ddc84",lineWidth:2});
 chartKind[c]=kind; loadCandles(c);
}
async function loadCandles(c){
 try{
  const bars=await (await fetch("/candles?coin="+c)).json();
  if(!Array.isArray(bars)) return;
  if(chartKind[c]==="cand") series[c].setData(bars.map(b=>({time:b.t,open:b.o,high:b.h,low:b.l,close:b.c})));
  else series[c].setData(bars.map(b=>({time:b.t,value:b.c})));
 }catch(e){}
}
function drawStrike(c,strike){
 if(strike==null) return;
 if(strikeLine[c]) series[c].removePriceLine(strikeLine[c]);
 strikeLine[c]=series[c].createPriceLine({price:strike,color:"#e6b84c",lineWidth:1,lineStyle:2,title:"strike"});
}
async function paperEnter(c){
 const t0=performance.now(); document.getElementById("pm_"+c).textContent="…";
 try{
  const res=await (await fetch("/paper_enter",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({coin:c})})).json();
  const rt=Math.round(performance.now()-t0);
  document.getElementById("pm_"+c).textContent= res.ok
    ? `entered ${res.side} x${res.contracts} @ ${fmt(res.entry)} · ${res.latency_ms}ms (rt ${rt}ms)`
    : `no: ${esc(res.error)} · ${res.latency_ms||rt}ms`;
 }catch(e){document.getElementById("pm_"+c).textContent="error";}
}
async function ping(){
 const t0=performance.now();
 try{ await fetch("/ping"); document.getElementById("lat").textContent=Math.round(performance.now()-t0)+"ms"; }catch(e){}
}
function post(action,extra){return fetch("/control",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(Object.assign({action},extra||{}))});}

function renderControls(ctrl){
 document.getElementById("run").textContent=ctrl.running?"RUNNING":"PAUSED";
 document.getElementById("run").className=ctrl.running?"grn":"red";
 document.getElementById("toggleRun").textContent=ctrl.running?"pause":"resume";
 document.querySelectorAll("[data-em]").forEach(b=>b.classList.toggle("on",b.dataset.em===ctrl.entry_mode));
 document.querySelectorAll("[data-dir]").forEach(b=>b.classList.toggle("on",b.dataset.dir===ctrl.direction));
 const ct=document.getElementById("coinToggles");
 if(!ct.dataset.built){ct.dataset.built="1";
   ct.innerHTML=COINS.map(c=>`<button class="btn" data-coin="${c}">${c}</button>`).join(" ");
   ct.querySelectorAll("[data-coin]").forEach(b=>b.onclick=async()=>{const on=!(b.classList.contains("on"));await post("coin",{coin:b.dataset.coin,on});});
 }
 ct.querySelectorAll("[data-coin]").forEach(b=>b.classList.toggle("on",!!ctrl.active[b.dataset.coin]));
 const bank=document.getElementById("bank"); if(document.activeElement!==bank) bank.value=ctrl.bankroll;
}

function renderCoin(c,r,freshIso){
 document.getElementById("tk_"+c).textContent=r&&r.ticker?r.ticker:"";
 const vd=document.getElementById("vd_"+c);
 if(!r||r.status!=="ok"){ vd.textContent=(r&&(r.verdict||r.status))||"…"; vd.className="verdict blk"; return; }
 document.getElementById("ss_"+c).textContent=`${r.spot} / ${r.strike==null?"–":r.strike} (${r.dist>0?"+":""}${r.dist})`;
 document.getElementById("bk_"+c).textContent=`${fmt(r.up_bid)}/${fmt(r.up_ask)} · ${fmt(r.dn_bid)}/${fmt(r.dn_ask)}`;
 document.getElementById("ce_"+c).textContent=`${r.conf}% · ${r.raw_edge>=0?"+":""}${r.raw_edge}/${r.net_edge>=0?"+":""}${r.net_edge}¢`;
 document.getElementById("st_"+c).textContent=`${fmt(r.rec_stop)} · ${r.signal?("x"+ (r.contracts||"?")):"–"}`;
 const cls=r.reason==="ENTER"?"verdict go":(String(r.reason||"").startsWith("BLOCK")||r.reason==="WAIT_LATE")?"verdict stop":"verdict warn";
 vd.className=cls; vd.textContent=r.verdict+"  ["+r.reason+"]";
 drawStrike(c,r.strike);
 // countdown
 const cd=document.getElementById("cd_"+c);
 if(r.remain!=null){const m=Math.floor(r.remain),s=Math.round((r.remain-m)*60);cd.textContent=`${m}:${String(s).padStart(2,"0")}`;}
}

function renderGate(g){
 const el=document.getElementById("gatebar"); if(!el) return;
 if(!g||!g.configured){el.textContent="Perp live veto: OFF";el.className="dim";return;}
 if(!g.active){el.textContent=`Perp live veto: ${g.status}`;el.className="amb";return;}
 el.textContent=`Perp live veto: ACTIVE (veto only) · policy ${(g.policy_id||"").slice(0,8)} · allowed ${g.allowed} · blocked ${g.blocked} · fail-open ${g.fail_open}`;
 el.className="";
}
function renderPerp(c,p){   // passive telemetry display only; never drives anything
 const el=document.getElementById("pf_"+c); if(!el) return;
 if(!p){el.textContent="–";return;}
 const n=(x,d)=>x==null?"–":Number(x).toFixed(d);
 const st=p.source_status||"?";
 const rd=p.analysis_ready===true?"ready":(p.analysis_ready===false?"not-ready":"");
 const lg=p.perp_lag_to_spot_ms==null?"":` lag ${(p.perp_lag_to_spot_ms/1000).toFixed(1)}s`;
 el.textContent=`${n(p.perp_mid,2)} · ${n(p.premium_bps,1)}bp · ${n(p.funding_rate,6)} · ${n(p.lead_30s_bps,1)}bp · ${st} ${rd}${lg}`;
 el.className=st==="fresh"?"":(st==="stale"?"amb":"dim");
 el.title=p.source_error||"";
}
function renderPerpVol(c,v){   // display only: labels come from perp_telemetry.dashboard_view; never drives anything
 const el=document.getElementById("pv_"+c), key=document.getElementById("pvk_"+c); if(!el) return;
 if(!v||typeof v!=="object"){el.textContent="–";el.className="dim";el.title="";return;}
 const u=x=>x==null||x===""?"UNKNOWN":String(x);
 const z=(x,d)=>x==null||!isFinite(x)?"–":(x>0?"+":"")+Number(x).toFixed(d);
 const r=(x,d)=>x==null||!isFinite(x)?"–":Number(x).toFixed(d);
 el.textContent=`${v.direction||"–"} ${z(v.momentum_z_60s,1)}σ60 · vol ${u(v.vol_regime)} ${r(v.vol_shock_60v300,2)}x · spot/perp ${u(v.agreement)} · spread ${u(v.spread_stress)} · prem ${u(v.premium_stress)} · ${u(v.stability)}`;
 el.className={STABLE:"",CAUTION:"amb",UNSTABLE:"red"}[v.stability]??"dim";
 el.title=`gap z60 ${z(v.gap_z_60s,2)} · spread ratio ${r(v.spread_ratio_5m,2)}x · premium |z| ${r(v.premium_stress_5m,2)} · `+
   `${v.stability_reasons||(v.stability==="STABLE"?"no stress triggers":"–")} · perp ${v.source_status||"?"}${v.lag_s==null?"":" lag "+v.lag_s+"s"}`;
 if(key) key.textContent=v.live_veto_feature?`perp vol · stability (observe only; LIVE veto uses ${v.live_veto_feature})`
                                            :"perp vol · stability (telemetry, observe only)";
}
let candleTick=0;
async function tick(){
 try{
  const d=await (await fetch("/data")).json();
  document.getElementById("upd").textContent=d.updated||"–";
  if(d.controls) renderControls(d.controls);
  COINS.forEach(c=>renderCoin(c,(d.coins||{})[c],(d.fresh||{})[c]));
  COINS.forEach(c=>renderPerp(c,(d.perps||{})[c]));
  COINS.forEach(c=>renderPerpVol(c,(d.perp_vol||{})[c]));
  renderGate(d.perp_live_veto);
  if((candleTick++)%5===0) COINS.forEach(loadCandles);   // refresh charts every ~15s
 }catch(e){}
}
// wire top controls
document.getElementById("toggleRun").onclick=async()=>{const running=document.getElementById("run").textContent==="RUNNING";await post(running?"pause":"resume");};
document.querySelectorAll("[data-em]").forEach(b=>b.onclick=async()=>{await post("entry_mode",{value:b.dataset.em});});
document.querySelectorAll("[data-dir]").forEach(b=>b.onclick=async()=>{await post("direction",{value:b.dataset.dir});});
document.getElementById("setBank").onclick=async()=>{await post("bankroll",{value:parseFloat(document.getElementById("bank").value)});};
buildCards(); tick(); setInterval(tick,3000); ping(); setInterval(ping,5000);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def _send(self, obj, code=200, ctype="application/json"):
        body = obj if isinstance(obj, bytes) else json.dumps(obj).encode()
        self.send_response(code); self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body))); self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        from urllib.parse import urlparse, parse_qs
        u = urlparse(self.path)
        if u.path == "/data":
            with LOCK: self._send(json.dumps(STATE).encode())
        elif u.path == "/ping":
            self._send({"t": int(time.time() * 1000)})           # latency round-trip
        elif u.path == "/candles":
            coin = (parse_qs(u.query).get("coin", ["BTC"])[0]).upper()
            cfg = COINS.get(coin)
            try:
                self._send(web_candles(cfg["product"]) if cfg else [])
            except Exception as e:
                self._send({"error": str(e)}, 502)
        else:
            self._send(PAGE.encode("utf-8"), ctype="text/html; charset=utf-8")

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        try:
            payload = json.loads(self.rfile.read(length) or b"{}")
        except Exception:
            payload = {}
        if self.path == "/control":
            self._send(apply_control(payload.get("action", ""), payload))
        elif self.path == "/paper_enter":
            self._send(paper_enter((payload.get("coin", "") or "").upper()))
        else:
            self._send({"error": "not found"}, 404)

def main():
    if "--discover" in sys.argv:
        discover_series(); return
    _gate_banner()
    threading.Thread(target=poller, daemon=True).start()
    threading.Thread(target=backtest_worker, daemon=True).start()
    threading.Thread(target=weekly_worker, daemon=True).start()
    print(f"Dashboard running \u2014 open http://localhost:{PORT}  (Ctrl+C to stop)")
    print("Backtest runs in the background; its panel fills in after a couple of minutes.")
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()

if __name__ == "__main__":
    main()
