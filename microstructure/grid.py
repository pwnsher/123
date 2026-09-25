"""
Sub-second checkpoint grids (100 / 250 / 500 / 1000 ms) for microstructure research.

A grid finer than a source's native update interval would only repeat stale values, so each source carries its
native interval (UPDATE_INTERVAL_MS) and is INCLUDED in a grid only if interval <= step. Excluded sources are listed
in the output (`excluded_sources`) instead of being forward-filled - e.g. the CF RTI (~1 value / s) and Kalshi REST
polling (1 s) never appear on the 100 / 250 / 500 ms grids; event-driven books (update_interval None) are eligible
at every step.

    rows = subsecond_rows(asset, events, start_ms, end_ms, step_ms)   # streaming engine, causal, one pass
"""
from microstructure.features.definitions import SHORT
from microstructure.features.engine import MicroFeatureEngine, event_key
from microstructure.venues import VENUES

STEPS_MS = (100, 250, 500, 1000)
UPDATE_INTERVAL_MS = dict({v: s.update_interval_ms for v, s in VENUES.items()},
                          cf=1000, kalshi_rest=1000, coinbase_ticker=None, kraken_ticker=None)


def eligible(step_ms):
    if step_ms not in STEPS_MS:
        raise ValueError(f"step must be one of {STEPS_MS}")
    inc = sorted(s for s, iv in UPDATE_INTERVAL_MS.items() if iv is None or iv <= step_ms)
    exc = sorted(s for s, iv in UPDATE_INTERVAL_MS.items() if not (iv is None or iv <= step_ms))
    return inc, exc


def subsecond_rows(asset, events, start_ms, end_ms, step_ms, config=None, venues=None):
    inc, exc = eligible(step_ms)
    books = tuple(v for v in VENUES if v in inc and (venues is None or v in venues) and v != "kalshi_ws")
    prefixes = tuple(f"micro.{SHORT[v]}." for v in books)
    eng = MicroFeatureEngine(asset, config, venues=books)
    evs = sorted(events, key=event_key)
    rows, i = [], 0
    for T in range(start_ms, end_ms + 1, step_ms):
        while i < len(evs) and evs[i].receive_ts_ms <= T:
            eng.ingest(evs[i])
            i += 1
        r = eng.features_at(T)
        keep = [n for n in r.values if n.startswith(prefixes)]
        rows.append({"t_ms": T, "values": {n: r.values[n] for n in keep}, "status": {n: r.status[n].value for n in keep}})
    return {"asset": asset, "step_ms": step_ms, "included_sources": list(books), "excluded_sources": exc,
            "note": "sources whose native update interval exceeds the step are excluded, never forward-filled", "rows": rows}
