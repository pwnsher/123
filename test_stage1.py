#!/usr/bin/env python3
"""Stage 1 regression tests. Run:  py test_stage1.py"""
import datetime as dt
import kalshi_dashboard as k

CFG = {"series": "KXTEST", "product": "TEST-USD", "sl_pct": 0.75}

def _market(strike, up_ask, up_bid=None, dn_ask=12, dn_bid=10, minutes_left=6.0):
    up_bid = up_bid if up_bid is not None else up_ask - 2
    ct = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=minutes_left)) \
        .isoformat().replace("+00:00", "Z")
    return {"ticker": "KXTEST-1", "close_time": ct, "floor_strike": strike,
            "yes_ask_dollars": f"{up_ask/100:.4f}", "yes_bid_dollars": f"{up_bid/100:.4f}",
            "no_ask_dollars": f"{dn_ask/100:.4f}", "no_bid_dollars": f"{dn_bid/100:.4f}"}

def _patch(market, px, closes):
    k.current_market = lambda series: market
    k.spot = lambda product: px
    hi = [c * 1.0005 for c in closes]; lo = [c * 0.9995 for c in closes]
    k.candles = lambda product: {"close": closes, "high": hi, "low": lo}

def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except AssertionError as e:
        print(f"FAIL  {name}: {e}"); raise

# 1) Volatility must come from the NEWEST completed candles, not the oldest.
def test_vol_uses_newest():
    old = [95.0 if i % 2 else 105.0 for i in range(200)]      # huge volatility (old)
    newest = [100.30 + (0.01 if i % 2 else 0.0) for i in range(62)]  # tiny vol (new)
    closes = old + newest                                    # newest at the end
    _patch(_market(strike=100.0, up_ask=88), px=100.305, closes=closes)
    r = k.evaluate("BTC", CFG)
    assert r["status"] == "ok", r
    # tiny recent vol + being above strike => confidence must be very high.
    # If the old volatile block were used, confidence would sit near 50.
    assert r["conf"] > 90, f"expected high conf from calm newest data, got {r['conf']}"

# 2) A signal must never fire on non-positive NET edge.
def test_signal_requires_net_edge():
    newest = [100.30 + (0.01 if i % 2 else 0.0) for i in range(62)]
    closes = [100.0] * 200 + newest
    # ask 99 -> raw edge ~ (conf-99) tiny, net edge negative -> NO signal
    _patch(_market(strike=100.0, up_ask=99), px=100.305, closes=closes)
    r = k.evaluate("BTC", CFG)
    assert r["net_edge"] is not None and r["net_edge"] <= 0, r["net_edge"]
    assert r["signal"] is False, "negative net edge must not signal"
    assert r["reason"] == "WAIT_NO_EDGE", r["reason"]
    # ask 88 -> positive net edge -> signal fires
    _patch(_market(strike=100.0, up_ask=88), px=100.305, closes=closes)
    r2 = k.evaluate("BTC", CFG)
    assert r2["net_edge"] > 0 and r2["signal"] is True, r2

# 3) Sizing returns exactly 0 whenever there is no positive-edge case.
def test_size_zero_rules():
    assert k._size_fraction({"conf": 92, "side_ask": 95, "net_edge": -5}) == 0.0
    assert k._size_fraction({"conf": 80, "side_ask": 95, "net_edge": -17}) == 0.0
    assert k._size_fraction({"conf": 95, "side_ask": 100, "net_edge": 3}) == 0.0   # bad price
    assert k._size_fraction({"conf": 95, "side_ask": 85, "net_edge": 8}) > 0.0     # genuine edge
    assert k._contracts({"conf": 92, "side_ask": 95, "net_edge": -5}) == 0

# 4) Malformed data must not fabricate a confidence.
def test_stale_guard():
    _patch(_market(strike=100.0, up_ask=88), px=100.3, closes=[100.0] * 5)  # too few candles
    r = k.evaluate("BTC", CFG)
    assert r["status"] == "stale" and r["signal"] is False, r
    assert r["reason"] == "BLOCK_STALE_DATA", r["reason"]

if __name__ == "__main__":
    run("vol uses newest candles", test_vol_uses_newest)
    run("signal requires net edge", test_signal_requires_net_edge)
    run("zero size on no edge", test_size_zero_rules)
    run("stale-data guard", test_stale_guard)
    print("\nAll Stage 1 tests passed.")
