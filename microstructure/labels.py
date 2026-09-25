"""
POST-EVENT RESEARCH LABELS - price impact. These use data received AFTER the event / checkpoint by construction and
are therefore NEVER features: nothing in microstructure.features imports this module (enforced by test_stage21),
and datasets write them to a separate file with the time each label becomes known (available_at_ms).

    trade_impact_labels(events, asset, venue, horizons_ms)
        for every trade of `venue` (by receive time t): signed impact
            impact_bps.h = sign * (mid(t + h) - mid(t)) / mid(t) * 1e4
        with mid(x) = the venue's reconstructed book mid as of receive time x (the book must be valid at t and t + h).
    checkpoint_forward_labels(events, asset, checkpoints, horizons_ms)
        for every checkpoint T and exchange book: forward mid move (mid(T + h) / mid(T) - 1) * 1e4 (unsigned)
Horizons default to 250 ms / 500 ms / 1 s / 5 s.
"""
import bisect

from microstructure.features.definitions import SHORT
from microstructure.features.engine import MicroFeatureEngine, event_key
from microstructure.venues import EXCHANGE_BOOKS, TRADE_SOURCE, VENUES

HORIZONS_MS = (250, 500, 1000, 5000)


def _mid_series(events, asset, venues):
    """Replay once; return {venue: (times, mids, valid_intervals)} of the reconstructed book (receive-time axis)."""
    eng = MicroFeatureEngine(asset, venues=tuple(venues))
    for e in sorted(events, key=event_key):
        eng.ingest(e)
    out = {}
    for v in venues:
        key = (v, VENUES[v].symbols.get(asset))
        s = eng.series.get(key)
        tr = eng.recon.tracks.get(key)
        if s is None or tr is None:
            continue
        mids = [((st[0] + st[2]) / 2) if st[0] is not None and st[2] is not None else None for st in s.state]
        out[v] = (list(s.t), mids, [tuple(x) for x in tr.intervals])
    return out, eng


def _asof(series, x):
    t, m, iv = series
    ok = any(lo <= x and (hi is None or hi > x) for lo, hi in iv)
    if not ok:
        return None
    i = bisect.bisect_right(t, x) - 1
    return m[i] if i >= 0 else None


def trade_impact_labels(events, asset, venue, horizons_ms=HORIZONS_MS):
    series, eng = _mid_series(events, asset, (venue,))
    s = series.get(venue)
    trades = eng.trades.get(TRADE_SOURCE[venue]) if venue != "kalshi_ws" else None
    out = []
    if s is None or trades is None:
        return out
    for k, t in enumerate(trades.t):
        sg = trades.sign[k]
        m0 = _asof(s, t)
        row = {"venue": venue, "asset": asset, "trade_receive_ts_ms": t, "price": trades.px[k], "qty": trades.qty[k],
               "sign": sg, "mid_at_trade": m0, "label_kind": "POST_EVENT_RESEARCH_LABEL"}
        for h in horizons_ms:
            m1 = _asof(s, t + h)
            row[f"impact_bps.{h}ms"] = (sg * (m1 - m0) / m0 * 1e4) if (m0 and m1 and sg) else None
            row[f"available_at_ms.{h}ms"] = t + h
        out.append(row)
    return out


def checkpoint_forward_labels(events, asset, checkpoints_ms, horizons_ms=HORIZONS_MS, venues=EXCHANGE_BOOKS):
    series, _eng = _mid_series(events, asset, venues)
    out = []
    for T in checkpoints_ms:
        row = {"asset": asset, "checkpoint_ts_ms": T, "label_kind": "POST_EVENT_RESEARCH_LABEL"}
        for v in venues:
            s = series.get(v)
            m0 = _asof(s, T) if s else None
            for h in horizons_ms:
                m1 = _asof(s, T + h) if s else None
                row[f"micro_label.{SHORT[v]}.fwd_mid_bps.{h}ms"] = ((m1 / m0 - 1) * 1e4) if (m0 and m1) else None
        row["available_at_ms"] = T + max(horizons_ms)
        out.append(row)
    return out
