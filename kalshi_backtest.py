#!/usr/bin/env python3
"""
Kalshi 15m End-of-Market — Backtester

Replays real crypto price history (from Coinbase, no key) to test the strategy's
core claim: when the model says a position is >=80% safe inside the 2-8 minute
window, how often does the coin ACTUALLY finish on that side?

It reconstructs each 15-minute window (strike = price at window open, the same
"finish above the opening reference" rule Kalshi uses), finds the first minute in
the entry window where model confidence clears the threshold, then either stops
out (model price falls to the per-coin stop level) or settles at window close.

What it CAN measure: the model's calibration and directional win rate, which
decide whether the strategy is viable at all.
What it CANNOT measure: the exact Kalshi asks you'd have paid, since historical
per-market prices aren't public. So it reports expectancy under clear pricing
assumptions rather than claiming a precise dollar return.

Run:
    pip install requests
    python kalshi_backtest.py --days 14
    (writes kalshi_backtest_trades.csv you can open in Excel)
"""

import math
import csv
import sys
import time
import argparse
import datetime as dt
import requests

# ─────────── config (mirrors the live dashboard) ───────────
COINS = {
    "BTC": {"product": "BTC-USD", "sl_pct": 0.75},
    "ETH": {"product": "ETH-USD", "sl_pct": 0.60},
    "SOL": {"product": "SOL-USD", "sl_pct": 0.75},
    "XRP": {"product": "XRP-USD", "sl_pct": 0.65},
}
INTERVAL_MIN     = 15
MAX_ENTRY_MIN    = 8.0     # earliest entry (minutes left)
LAST_STOP_MIN    = 2.0     # latest entry (minutes left)
MIN_CONF         = 80.0    # confidence threshold to enter, %
VOL_LOOKBACK_MIN = 60
VOL_MULT         = 1.15
FEE_CENTS        = 2.0     # approx Kalshi round-trip fee per contract
GATE_PRICE       = 85.0    # the "85c or higher" entry price to test expectancy at

COINBASE_BASE = "https://api.exchange.coinbase.com"

# ─────────── math ───────────
def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def conf_side(price, strike, vol, left_min):
    term = vol * math.sqrt(max(left_min, 1e-4))
    if term <= 0 or not strike:
        return None, None
    z = math.log(price / strike) / term
    cdf = norm_cdf(z)
    return max(cdf, 1 - cdf) * 100.0, cdf >= 0.5

# ─────────── data ───────────
def fetch_candles(product, days):
    """Return {epoch_second_at_minute: close} covering `days` back plus buffer."""
    end = dt.datetime.now(dt.timezone.utc)
    floor = end - dt.timedelta(days=days, minutes=VOL_LOOKBACK_MIN + 30)
    out, cursor = {}, end
    while cursor > floor:
        seg_start = max(cursor - dt.timedelta(minutes=300), floor)
        for attempt in range(3):
            try:
                r = requests.get(f"{COINBASE_BASE}/products/{product}/candles",
                                 params={"granularity": 60,
                                         "start": seg_start.isoformat(),
                                         "end": cursor.isoformat()}, timeout=20)
                r.raise_for_status()
                for c in r.json():
                    out[int(c[0])] = float(c[4])   # [time, low, high, open, close, vol]
                break
            except requests.RequestException:
                time.sleep(1.0)
        cursor = seg_start
        time.sleep(0.25)
    return out

def vol_at(cd, t):
    cs = [cd.get(t - i * 60) for i in range(VOL_LOOKBACK_MIN, -1, -1)]
    cs = [c for c in cs if c is not None]
    if len(cs) < 20:
        return None
    rets = [math.log(cs[i] / cs[i - 1]) for i in range(1, len(cs))]
    m = sum(rets) / len(rets)
    return math.sqrt(sum((x - m) ** 2 for x in rets) / (len(rets) - 1)) * VOL_MULT

# ─────────── backtest one coin ───────────
def backtest_coin(coin, cfg, cd):
    trades = []
    if not cd:
        return trades
    keys = sorted(cd)
    t_min, t_max = keys[0], keys[-1]
    b = ((t_min // 900) + 1) * 900            # first 15-min boundary
    while b + INTERVAL_MIN * 60 <= t_max:
        strike = cd.get(b)
        if strike:
            first_el = int(INTERVAL_MIN - MAX_ENTRY_MIN)   # 7
            last_el  = int(INTERVAL_MIN - LAST_STOP_MIN)   # 13
            for elapsed in range(first_el, last_el + 1):
                t = b + elapsed * 60
                left = INTERVAL_MIN - elapsed
                price = cd.get(t)
                vol = vol_at(cd, t) if price is not None else None
                if price is None or vol is None:
                    continue
                conf, fav_up = conf_side(price, strike, vol, left)
                if conf is None or conf < MIN_CONF:
                    continue
                # ── entered ──
                entry = conf                       # fair-value entry price (cents)
                stop = entry * cfg["sl_pct"]
                stopped, pnl_fair = False, None
                for e2 in range(elapsed + 1, INTERVAL_MIN + 1):
                    p2 = cd.get(b + e2 * 60)
                    if p2 is None:
                        continue
                    v2 = vol_at(cd, b + e2 * 60) or vol
                    c2, fu2 = conf_side(p2, strike, v2, max(INTERVAL_MIN - e2, 1e-4))
                    if c2 is None:
                        continue
                    our_price = c2 if fu2 == fav_up else 100 - c2
                    if our_price <= stop:
                        pnl_fair = stop - entry - FEE_CENTS
                        stopped = True
                        break
                final = cd.get(b + INTERVAL_MIN * 60)
                settle_win = None if final is None else ((final >= strike) == fav_up)
                if not stopped:
                    if settle_win is None:
                        break
                    pnl_fair = (100 - entry - FEE_CENTS) if settle_win else (-entry - FEE_CENTS)
                trades.append({
                    "coin": coin, "ts": b, "left": left, "conf": round(conf, 1),
                    "fav": "UP" if fav_up else "DOWN", "entry": round(entry, 1),
                    "exit": "stop" if stopped else "settle",
                    "win": settle_win, "pnl_fair": round(pnl_fair, 1),
                })
                break
        b += 900
    return trades

# ─────────── reporting ───────────
def block(trades):
    settled = [t for t in trades if t["win"] is not None]
    n = len(settled)
    wins = sum(1 for t in settled if t["win"])
    wr = 100 * wins / n if n else 0.0
    avg = sum(t["pnl_fair"] for t in trades) / len(trades) if trades else 0.0
    stops = 100 * sum(1 for t in trades if t["exit"] == "stop") / len(trades) if trades else 0.0
    exp85 = (wr / 100) * (100 - GATE_PRICE) - (1 - wr / 100) * GATE_PRICE - FEE_CENTS
    return {"n": len(trades), "wr": wr, "avg": avg, "stops": stops, "exp85": exp85}

# ---- model-validation metrics (probability quality, not P&L) ----
def _pw(t):
    return min(max(t["conf"] / 100.0, 1e-6), 1 - 1e-6)   # predicted prob for the favored side

def brier(trades):
    s = [t for t in trades if t["win"] is not None]
    return sum((_pw(t) - (1 if t["win"] else 0)) ** 2 for t in s) / len(s) if s else float("nan")

def logloss(trades):
    s = [t for t in trades if t["win"] is not None]
    if not s: return float("nan")
    return -sum((math.log(_pw(t)) if t["win"] else math.log(1 - _pw(t))) for t in s) / len(s)

def ece(trades, nbins=5):
    s = [t for t in trades if t["win"] is not None]
    if not s: return float("nan")
    tot, e = len(s), 0.0
    for b in range(nbins):
        lo, hi = b / nbins, (b + 1) / nbins
        bucket = [t for t in s if (lo <= _pw(t) < hi) or (b == nbins - 1 and _pw(t) >= hi)]
        if not bucket: continue
        conf = sum(_pw(t) for t in bucket) / len(bucket)
        acc = sum(1 for t in bucket if t["win"]) / len(bucket)
        e += (len(bucket) / tot) * abs(conf - acc)
    return e

def report(all_trades, days):
    print("\n" + "=" * 66)
    print(f"  Kalshi 15m End-of-Market backtest  ·  {days:.1f} days  ·  "
          f"conf>={MIN_CONF:.0f}%  entry {LAST_STOP_MIN:.0f}-{MAX_ENTRY_MIN:.0f} min left")
    print("=" * 66)

    # ---- MODEL VALIDATION: the honest headline (probability quality) ----
    settled = [t for t in all_trades if t["win"] is not None]
    settled.sort(key=lambda t: t["ts"])
    split = int(len(settled) * 0.7)
    test = settled[split:] if len(settled) > 40 else settled     # out-of-sample: newest 30%
    def dacc(s): return 100 * sum(1 for t in s if t["win"]) / len(s) if s else 0.0
    print("  MODEL VALIDATION")
    print(f"  Directional accuracy (all)       {dacc(settled):>5.1f}%   N={len(settled)}")
    print(f"  Directional accuracy (OOS test)  {dacc(test):>5.1f}%   N={len(test)}")
    print(f"  Brier score  (OOS, lower better) {brier(test):>7.4f}")
    print(f"  Log loss     (OOS, lower better) {logloss(test):>7.4f}")
    print(f"  Calibration error ECE (OOS)      {ece(test)*100:>5.1f} pts")

    print("\n  CONFIDENCE CALIBRATION (out-of-sample test window)")
    print(f"  {'band':>9} {'N':>6} {'predicted':>10} {'actual':>8} {'diff':>7}")
    print("  " + "-" * 46)
    for lo in (80, 85, 90, 95):
        hi = lo + 5
        bucket = [t for t in test if lo <= t["conf"] < (hi if lo < 95 else 100.1)]
        if not bucket: continue
        pred = sum(t["conf"] for t in bucket) / len(bucket)
        act = 100 * sum(1 for t in bucket if t["win"]) / len(bucket)
        print(f"  {lo:>4}-{hi:<4} {len(bucket):>6} {pred:>9.1f}% {act:>7.1f}% {act-pred:>+6.1f}")

    print("\n  RELIABILITY (predicted band -> observed win%, OOS)")
    for lo in (80, 85, 90, 95):
        hi = lo + 5
        bucket = [t for t in test if lo <= t["conf"] < (hi if lo < 95 else 100.1)]
        if bucket:
            act = 100 * sum(1 for t in bucket if t["win"]) / len(bucket)
            print(f"  {(lo+hi)//2:>3}% | {'#' * int(act/2)} {act:.0f}%")

    print("\n" + "=" * 66)
    print("  IDEALIZED EXECUTION SENSITIVITY  (hypothetical scenarios, NOT real P&L)")
    print("  Entries are model-priced; use these only to compare assumptions.")
    print("=" * 66)
    hdr = f"  {'':5} {'setups':>7} {'/day':>6} {'win%':>6} {'stop%':>6} {'avg(idl)':>9} {'exp@'+str(int(GATE_PRICE)):>7}"
    print(hdr); print("  " + "-" * 60)
    def line(name, tr):
        b = block(tr)
        print(f"  {name:5} {b['n']:>7} {b['n']/days:>6.1f} {b['wr']:>6.1f} "
              f"{b['stops']:>6.1f} {b['avg']:>11.2f} {b['exp85']:>9.2f}")
    line("ALL", all_trades)
    for c in COINS:
        line(c, [t for t in all_trades if t["coin"] == c])

    dows = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    print("\n  BY DAY OF WEEK")
    print(f"  {'day':>4} {'trades':>7} {'win%':>6} {'exp¢@85':>9}")
    print("  " + "-" * 30)
    best = None
    for i, d in enumerate(dows):
        day = [t for t in all_trades if dt.datetime.fromtimestamp(t["ts"], dt.timezone.utc).weekday() == i]
        b = block(day)
        if b["n"]:
            print(f"  {d:>4} {b['n']:>7} {b['wr']:>6.1f} {b['exp85']:>9.2f}")
            if best is None or b["wr"] > best[1]:
                best = (d, b["wr"])
    if best:
        print(f"  Best day: {best[0]} ({best[1]:.1f}% win)")

    print("\n  BY HOUR (UTC)")
    print(f"  {'hr':>4} {'trades':>7} {'win%':>6}")
    print("  " + "-" * 22)
    for h in range(24):
        hh = [t for t in all_trades if dt.datetime.fromtimestamp(t["ts"], dt.timezone.utc).hour == h]
        b = block(hh)
        if b["n"]:
            print(f"  {h:>4} {b['n']:>7} {b['wr']:>6.1f}")
    print("=" * 66)
    print("  NOTE: avg(idl) and exp@ are IDEALIZED, model-priced scenarios — not")
    print("  real trading profit. Judge the model by calibration and Brier above;")
    print("  judge tradeability only against real fills in paper/live testing.\n")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=float, default=14)
    ap.add_argument("--coins", default=",".join(COINS))
    args = ap.parse_args()
    want = [c.strip().upper() for c in args.coins.split(",") if c.strip()]

    all_trades, spans = [], []
    for coin in want:
        if coin not in COINS:
            continue
        print(f"fetching {coin} ...", flush=True)
        cd = fetch_candles(COINS[coin]["product"], args.days)
        if cd:
            spans.append((max(cd) - min(cd)) / 86400.0)
        all_trades += backtest_coin(coin, COINS[coin], cd)

    days = max(spans) if spans else args.days
    if not all_trades:
        print("No trades produced — check connectivity or widen --days.")
        return
    with open("kalshi_backtest_trades.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(all_trades[0].keys()))
        w.writeheader(); w.writerows(all_trades)
    report(all_trades, days)
    print(f"  {len(all_trades)} trades written to kalshi_backtest_trades.csv")

if __name__ == "__main__":
    main()
