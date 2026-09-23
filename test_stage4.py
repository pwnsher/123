#!/usr/bin/env python3
"""Stage 4 tests: realistic fees + unfilled maker orders. Run: py test_stage4.py"""
import os, json, datetime as dt
import kalshi_dashboard as k
def run(n, f): f(); print(f"PASS  {n}")
def test_fee_scales():
    assert k.kalshi_fee_cents(90, 10) >= k.kalshi_fee_cents(90, 1)
    assert k.kalshi_fee_cents(50, 10) > k.kalshi_fee_cents(90, 10)   # fee peaks near 50c
def test_unfilled_maker():
    k.CALLS_FILE = "_t4c.json"; k.TRADES_CSV = "_t4t.csv"; k.post_embed = lambda *a, **kw: None
    for p in (k.CALLS_FILE, k.TRADES_CSV):
        if os.path.exists(p): os.remove(p)
    json.dump([], open(k.CALLS_FILE, "w")); k._call_pending.clear()
    past = (dt.datetime.now(dt.timezone.utc)-dt.timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
    k.market_result = lambda tk: "yes"
    k._call_pending["M"] = {"coin": "BTC", "side": "UP", "entry": 88, "entry_limit": 88,
        "assumed_entry": 88, "entry_mode": "maker", "stop": 66, "contracts": 10, "close": past,
        "lo": 90, "hi": 95, "ask_lo": 90, "bid_lo": 89, "maker_filled": False}
    k.settle_calls()
    row = json.load(open(k.CALLS_FILE))[-1]
    assert row["exit_reason"] == "unfilled" and row["win"] is None and row["pnl"] == 0.0, row
    for p in (k.CALLS_FILE, k.TRADES_CSV):
        if os.path.exists(p): os.remove(p)
if __name__ == "__main__":
    run("fees scale with size and price", test_fee_scales)
    run("unfilled maker is a non-trade", test_unfilled_maker)
    print("\nAll Stage 4 tests passed.")
