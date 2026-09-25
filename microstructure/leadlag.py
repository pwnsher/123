"""
OFFLINE lead-lag analysis tool (research only; never a live feature).

For each pair of price sources (CF RTI, Coinbase, Kraken, Binance, Bybit, OKX books, Kalshi YES mid) the returns on
a regular receive-time grid are cross-correlated at lags -5 s .. +5 s:
    corr(lag) = corr(r_A(t), r_B(t + lag))       lag > 0: A LEADS B by lag
To avoid choosing the best lag on the data it is evaluated on, the time range is split CHRONOLOGICALLY into a
DISCOVERY part (the first `discovery_fraction`) and a HOLDOUT part (the rest, after an embargo of max |lag|): the
best lag is chosen on discovery only; the holdout correlation at THAT lag (and at lag 0) is reported. The holdout is
never used to pick anything.
Returns are log returns of the as-of value on the grid (a value older than max_age_ms counts as missing).
"""
import bisect
import math

from market_data.types import EventType as ET, MarketEvent
from microstructure.features.engine import MicroFeatureEngine, event_key
from microstructure.venues import EXCHANGE_BOOKS, VENUES

SOURCES = ("cf", "coinbase_l2", "kraken_book", "binance_usdm_book", "bybit_linear_book", "okx_swap_book", "kalshi_ws")


def price_series(events, asset, kalshi_ticker=None):
    """{source: (receive_times, values)} (Kalshi: YES mid cents of `kalshi_ticker`)."""
    eng = MicroFeatureEngine(asset)
    cf_t, cf_v = [], []
    for e in sorted(events, key=event_key):
        if isinstance(e, MarketEvent) and e.event_type == ET.INDEX_VALUE and e.asset == asset:
            cf_t.append(e.receive_ts_ms); cf_v.append(e.payload["value"])
        eng.ingest(e)
    out = {"cf": (cf_t, cf_v)} if cf_t else {}
    for v in EXCHANGE_BOOKS:
        s = eng.series.get((v, VENUES[v].symbols.get(asset)))
        if s:
            pts = [(t, (st[0] + st[2]) / 2) for t, st in zip(s.t, s.state) if st[0] is not None and st[2] is not None]
            out[v] = ([p[0] for p in pts], [p[1] for p in pts])
    if kalshi_ticker:
        s = eng.series.get(("kalshi_ws", kalshi_ticker))
        if s:
            pts = [(t, (st[0] + st[2]) / 2) for t, st in zip(s.t, s.state) if st[0] is not None and st[2] is not None]
            out["kalshi_ws"] = ([p[0] for p in pts], [p[1] for p in pts])
    return out


def _grid(series, start, end, step, max_age):
    t, v = series
    out = []
    for x in range(start, end + 1, step):
        i = bisect.bisect_right(t, x) - 1
        out.append(v[i] if i >= 0 and x - t[i] <= max_age else None)
    return out


def _rets(g, log=True):
    r = []
    for a, b in zip(g, g[1:]):
        if a is None or b is None or (log and (a <= 0 or b <= 0)):
            r.append(None)
        else:
            r.append(math.log(b / a) if log else b - a)
    return r


def _corr(x, y):
    pts = [(a, b) for a, b in zip(x, y) if a is not None and b is not None]
    if len(pts) < 30:
        return None, len(pts)
    mx = sum(a for a, _ in pts) / len(pts)
    my = sum(b for _, b in pts) / len(pts)
    vx = sum((a - mx) ** 2 for a, _ in pts)
    vy = sum((b - my) ** 2 for _, b in pts)
    if vx <= 0 or vy <= 0:
        return None, len(pts)
    return sum((a - mx) * (b - my) for a, b in pts) / math.sqrt(vx * vy), len(pts)


def _lagged(ra, rb, k):
    if k >= 0:
        return ra[:len(ra) - k] if k else ra, rb[k:]
    return ra[-k:], rb[:len(rb) + k]


def lead_lag(series, start, end, step_ms=250, max_lag_ms=5000, discovery_fraction=0.6, max_age_ms=5000, sources=SOURCES):
    lags = list(range(-max_lag_ms // step_ms, max_lag_ms // step_ms + 1))
    split = start + int((end - start) * discovery_fraction) // step_ms * step_ms
    embargo = max_lag_ms
    grids = {s: _grid(series[s], start, end, step_ms, max_age_ms) for s in sources if s in series}
    rets = {s: _rets(g, log=(s != "kalshi_ws")) for s, g in grids.items()}
    n_disc = (split - start) // step_ms
    h0 = n_disc + embargo // step_ms
    out = {"method": "chronological discovery/holdout split; best lag chosen on discovery only",
           "step_ms": step_ms, "max_lag_ms": max_lag_ms, "discovery": [start, split], "holdout": [split + embargo, end],
           "embargo_ms": embargo, "pairs": {}}
    names = sorted(rets)
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            ra, rb = rets[a], rets[b]
            disc = {}
            for k in lags:
                x, y = _lagged(ra[:n_disc], rb[:n_disc], k)
                c, n = _corr(x, y)
                disc[k * step_ms] = c
            valid = {k: c for k, c in disc.items() if c is not None}
            if not valid:
                out["pairs"][f"{a}|{b}"] = {"status": "INSUFFICIENT_DATA"}
                continue
            best = max(valid, key=lambda k: (abs(valid[k]), -abs(k)))
            hx, hy = _lagged(ra[h0:], rb[h0:], best // step_ms)
            hc, hn = _corr(hx, hy)
            z0x, z0y = _lagged(ra[h0:], rb[h0:], 0)
            h0c, _ = _corr(z0x, z0y)
            out["pairs"][f"{a}|{b}"] = {"status": "OK", "best_lag_ms_discovery": best, "discovery_corr_at_best": valid[best],
                                        "discovery_corr_at_0": disc.get(0), "holdout_corr_at_discovery_best": hc,
                                        "holdout_corr_at_0": h0c, "holdout_n": hn,
                                        "interpretation": (f"{a} leads {b}" if best > 0 else f"{b} leads {a}" if best < 0 else "synchronous")
                                        + " on DISCOVERY; holdout confirms only if its correlation at that lag is comparable"}
    return out
