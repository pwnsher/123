#!/usr/bin/env python3
"""Stage 3 tests: calibration metrics behave. Run: py test_stage3.py"""
import time, kalshi_backtest as b
def mk(conf, win, i): return {"conf": conf, "win": win, "ts": int(time.time())-i*900,
                              "coin": "BTC", "exit": "settle", "pnl_fair": 1.0}
def run(n, f): f(); print(f"PASS  {n}")
def test_metrics():
    good = [mk(90, (i % 10 != 0), i) for i in range(200)]   # 90% predicted, ~90% actual
    bad  = [mk(95, (i % 10 < 6), i) for i in range(200)]    # 95% predicted, ~60% actual
    assert b.ece(good) < 0.05, b.ece(good)
    assert b.ece(bad) > 0.20, b.ece(bad)
    assert b.brier(good) < b.brier(bad)
    assert b.logloss(good) < b.logloss(bad)
if __name__ == "__main__":
    run("calibration metrics behave", test_metrics)
    print("\nAll Stage 3 tests passed.")
