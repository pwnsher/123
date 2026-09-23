#!/usr/bin/env python3
"""Stage 6 tests: web filters gate the engine. Run: py test_stage6.py"""
import datetime as dt
import kalshi_dashboard as k
CFG = {"series": "KXTEST", "product": "TEST-USD", "sl_pct": 0.75}
def _market(strike, up_ask, minutes_left=6.0):
    ct = (dt.datetime.now(dt.timezone.utc)+dt.timedelta(minutes=minutes_left)).isoformat().replace("+00:00", "Z")
    return {"ticker": "KXTEST-1", "close_time": ct, "floor_strike": strike,
            "yes_ask_dollars": f"{up_ask/100:.4f}", "yes_bid_dollars": f"{(up_ask-2)/100:.4f}",
            "no_ask_dollars": "0.1200", "no_bid_dollars": "0.1000"}
def _patch(m, px, closes):
    k.current_market = lambda s: m; k.spot = lambda p: px
    k.candles = lambda p: {"close": closes, "high": [c*1.0005 for c in closes], "low": [c*0.9995 for c in closes]}
newest = [100.30 + (0.01 if i % 2 else 0.0) for i in range(62)]
closes = [100.0]*200 + newest
def run(n, f): f(); print(f"PASS  {n}")
def test_direction_filter():
    k.DIRECTION = "BOTH"; k.ACTIVE_COINS["BTC"] = True
    _patch(_market(100.0, 88), 100.305, closes)
    r = k.evaluate("BTC", CFG); assert r["fav"] == "UP" and r["signal"], r
    k.DIRECTION = "DOWN"
    r2 = k.evaluate("BTC", CFG); assert not r2["signal"] and r2["reason"] == "BLOCK_DIRECTION", r2
    k.DIRECTION = "BOTH"
def test_coin_filter():
    k.ACTIVE_COINS["BTC"] = False
    _patch(_market(100.0, 88), 100.305, closes)
    r = k.evaluate("BTC", CFG); assert not r["signal"] and r["reason"] == "BLOCK_COIN", r
    k.ACTIVE_COINS["BTC"] = True
def test_controls_mutate():
    assert k.apply_control("entry_mode", {"value": "maker"})["entry_mode"] == "maker"
    k.apply_control("entry_mode", {"value": "taker"})
    assert k.apply_control("coin", {"coin": "XRP", "on": False})["active"]["XRP"] is False
    k.apply_control("coin", {"coin": "XRP", "on": True})
if __name__ == "__main__":
    run("direction filter blocks opposite side", test_direction_filter)
    run("coin toggle blocks calls", test_coin_filter)
    run("controls mutate", test_controls_mutate)
    print("\nAll Stage 6 tests passed.")
