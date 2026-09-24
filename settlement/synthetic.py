"""
Deterministic SYNTHETIC settlement data for tests and clearly-labelled demonstrations.

Everything produced here is fake. It is shaped like the external payloads (CF websocket frames,
Kalshi cfbenchmarks_value messages, CF REST history, Kalshi market objects) so the whole
parse -> store -> offline replay path is exercised.
    * observations() / as_synthetic() tag data with source "synthetic", which only the test policy
      (test_synthetic_v1) trusts;
    * demo_dataset() payloads, once parsed, carry the source names of the shapes they imitate. They are
      only ever written to TEMPORARY stores whose capture session is marked synthetic; the validation
      report excludes any store with a synthetic session from its real-data section and prints the demo
      under a separate "SYNTHETIC demonstration" heading.
No report may present synthetic data as real.
"""
import datetime as dt
import json
import math
from dataclasses import replace

from settlement.assets import ASSET_INDEX, MARKET_TZ
from settlement.types import SettlementObservation

SYNTHETIC_SOURCE = "synthetic"
_MON = ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")


def lcg(seed):
    x = seed & 0xFFFFFFFF
    while True:
        x = (1664525 * x + 1013904223) & 0xFFFFFFFF
        yield x / 4294967296.0


def price_path(start_ms, end_ms, base, step_ms=1000, vol_bp=1.0, seed=1):
    """{event_ts_ms: value} on a regular grid; log-increments ~ vol_bp basis points per step."""
    g = lcg(seed)
    out, v = {}, float(base)
    for t in range(start_ms, end_ms + 1, step_ms):
        out[t] = round(v, 2)
        v *= math.exp((next(g) - 0.5) * math.sqrt(12.0) * vol_bp * 1e-4)
    return out


def cf_frame(index_id, t, value, amend=None):
    f = {"type": "value", "id": index_id, "value": f"{value:.2f}", "time": int(t)}
    if amend is not None:
        f["amendTime"] = int(amend)
    return f


def kalshi_message(frame, sid=7, seq=None, last60=None, avg60=None):
    msg = {"data": json.dumps(frame)}
    if avg60 is not None:
        msg["avg_60s_data"] = {"value": f"{avg60:.6f}"}
    msg["last_60s_windowed_average_15min"] = {"value": f"{last60:.6f}"} if last60 is not None else None
    m = {"type": "cfbenchmarks_value", "sid": sid, "msg": msg}
    if seq is not None:
        m["seq"] = seq
    return m


def eastern_ticker(series, close_ms):
    try:
        from zoneinfo import ZoneInfo
        local = dt.datetime.fromtimestamp(close_ms / 1000, dt.timezone.utc).astimezone(ZoneInfo(MARKET_TZ))
    except Exception:
        return f"{series}-SYN{close_ms}"
    return f"{series}-{local:%y}{_MON[local.month - 1]}{local:%d%H%M}"


def market_json(asset, close_ms, strike, result=None, expiration_value=None, open_ms=None):
    series = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M", "XRP": "KXXRP15M"}[asset]
    iso = lambda ms: dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).isoformat().replace("+00:00", "Z")  # noqa: E731
    m = {"ticker": eastern_ticker(series, close_ms), "close_time": iso(close_ms),
         "open_time": iso(open_ms if open_ms is not None else close_ms - 900_000), "floor_strike": strike,
         "status": "settled" if result else "active", "strike_type": "greater"}
    if result is not None:
        m["result"] = result
    if expiration_value is not None:
        m["expiration_value"] = f"{expiration_value:.6f}"
    return m


def as_synthetic(observations):
    """Re-tag parsed observations as synthetic (so no production policy can ever trust them)."""
    return [replace(o, source=SYNTHETIC_SOURCE) for o in observations]


def observations(asset, path, receive_lag_ms=150):
    idx = ASSET_INDEX[asset]
    return [SettlementObservation(asset, idx, SYNTHETIC_SOURCE, v, t, receive_ts_ms=t + receive_lag_ms, seq=i)
            for i, (t, v) in enumerate(sorted(path.items()))]


def true_settlement(path, grid):
    """The mean the synthetic 'official' settlement uses (the path is observed exactly at each instant)."""
    return math.fsum(path[g] for g in grid) / len(grid)


def demo_dataset(n_markets=40, asset="BTC", first_close_ms=1_790_000_100_000, seed=7, receive_lag_ms=150,
                 gap_market=3, history_mismatch_market=5, base=100_000.0):
    """A self-consistent fake world, in EXTERNAL payload shapes:
        markets        Kalshi market objects (strike = previous window's settlement, result, expiration_value)
        live_lines     websocket capture lines {"receive_ts_ms", "seq", "message"} (Kalshi cfbenchmarks_value)
                       with a 5-second disconnect inside market `gap_market`'s settlement window
        history        a CF REST historical-values payload covering everything (one value altered in
                       market `history_mismatch_market`'s window, to exercise the overlap tool)
    The official settlement uses the DEFAULT window convention BY CONSTRUCTION, so agreement here
    proves the tooling, not the convention."""
    from settlement.policy import window_policy
    wpol = window_policy()
    idx = ASSET_INDEX[asset]
    closes = [first_close_ms + i * 900_000 for i in range(n_markets)]
    path = price_path(closes[0] - 900_000 - 120_000, closes[-1] + 30_000, base, 1000, vol_bp=2.0, seed=seed)
    markets = []
    for i, c in enumerate(closes):
        settle = true_settlement(path, wpol.grid(c))
        strike = round(true_settlement(path, wpol.grid(c - 900_000)), 2)
        result = "yes" if settle > strike else "no"
        markets.append(market_json(asset, c, strike, result, settle, open_ms=c - 900_000))
    gap_lo, gap_hi = closes[gap_market] - 20_000, closes[gap_market] - 15_000
    live_lines, seq = [], 0
    for t in sorted(path):
        if gap_lo <= t < gap_hi:
            continue
        last60 = None
        k = -((closes[0] - t) // 900_000)                   # ceil((t - first_close) / 900 s)
        c = closes[0] + k * 900_000
        if 0 <= k < len(closes) and c - 60_000 <= t <= c:
            g = [x for x in wpol.grid(c) if x <= t]
            if g:
                last60 = true_settlement(path, g)
        live_lines.append({"receive_ts_ms": t + receive_lag_ms, "seq": seq,
                           "message": kalshi_message(cf_frame(idx, t, path[t]), seq=seq, last60=last60)})
        seq += 1
    bad_t = closes[history_mismatch_market] - 30_000
    history = {"payload": [{"value": f"{(path[t] + (0.5 if t == bad_t else 0.0)):.2f}", "time": t} for t in sorted(path)]}
    return {"index_id": idx, "markets": markets, "live_lines": live_lines, "history": history,
            "closes": closes, "gap_market": markets[gap_market]["ticker"],
            "history_mismatch_market": markets[history_mismatch_market]["ticker"], "history_mismatch_ts": bad_t}
