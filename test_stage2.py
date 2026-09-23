#!/usr/bin/env python3
"""Stage 2 regression tests. Run:  py test_stage2.py"""
import os, json, datetime as dt
import kalshi_dashboard as k

def _setup(tmp="_t2"):
    k.CALLS_FILE = f"{tmp}_calls.json"
    k.TRADES_CSV = f"{tmp}_trades.csv"
    for p in (k.CALLS_FILE, k.TRADES_CSV):
        if os.path.exists(p): os.remove(p)
    json.dump([], open(k.CALLS_FILE, "w"))
    k.post_embed = lambda *a, **kw: None          # no network
    k._call_pending.clear()

def _past():
    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)).isoformat().replace("+00:00", "Z")

def run(name, fn):
    fn(); print(f"PASS  {name}")

# A position whose bid fell to/through the stop is a LOSS, even if it settles YES.
def test_stopped_out_is_a_loss_even_if_settles_yes():
    _setup()
    k.market_result = lambda tk: "yes"            # market ends up YES...
    k._call_pending["T1"] = {"coin": "BTC", "side": "UP", "entry": 90, "stop": 68,
                             "contracts": 10, "close": _past(),
                             "lo": 62, "hi": 96, "bid_lo": 62}   # ...but bid hit 62, below the 68 stop
    k.settle_calls()
    row = json.load(open(k.CALLS_FILE))[-1]
    assert row["exit_reason"] == "stop", row
    assert row["win"] is False, "stopped-out trade must be a loss"
    assert row["exit_price"] <= 68, row          # exited at/through the stop, not at 100
    assert row["per_contract"] < 0, row

# A position that never touched the stop settles by the actual result.
def test_clean_settle_win():
    _setup()
    k.market_result = lambda tk: "yes"
    k._call_pending["T2"] = {"coin": "ETH", "side": "UP", "entry": 88, "stop": 66,
                             "contracts": 5, "close": _past(),
                             "lo": 80, "hi": 99, "bid_lo": 80}   # bid low 80 > stop 66
    k.settle_calls()
    row = json.load(open(k.CALLS_FILE))[-1]
    assert row["exit_reason"] == "settle" and row["win"] is True, row
    assert row["exit_price"] == 100.0, row

# A gap THROUGH the stop is modeled at the worse price, not an exact stop fill.
def test_gap_through_stop_worse_fill():
    _setup()
    k.market_result = lambda tk: "no"
    k._call_pending["T3"] = {"coin": "SOL", "side": "UP", "entry": 90, "stop": 68,
                             "contracts": 4, "close": _past(),
                             "lo": 40, "hi": 95, "bid_lo": 40}   # gapped to 40, well below stop
    k.settle_calls()
    row = json.load(open(k.CALLS_FILE))[-1]
    assert row["exit_reason"] == "stop" and row["exit_price"] == 40, row
    assert row["per_contract"] < (68 - 90), "gap fill must be worse than the stop"

def _cleanup():
    for p in ("_t2_calls.json", "_t2_trades.csv"):
        if os.path.exists(p): os.remove(p)

if __name__ == "__main__":
    run("stop-out is a loss even if it settles yes", test_stopped_out_is_a_loss_even_if_settles_yes)
    run("clean settle counts the real result", test_clean_settle_win)
    run("gap through stop fills at the worse price", test_gap_through_stop_worse_fill)
    _cleanup()
    print("\nAll Stage 2 tests passed.")
