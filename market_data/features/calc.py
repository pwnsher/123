"""Pure feature math (no state, no clocks). None in -> None out; nothing becomes 0 by accident."""
import math
import statistics

from market_data.features.definitions import Status

_ORDER = (Status.MISSING, Status.NOT_READY, Status.UNAVAILABLE, Status.UNDEFINED, Status.PARTIAL, Status.READY)


def worst_status(*sts):
    present = [s for s in sts if s is not None]
    for s in _ORDER:
        if s in present:
            return s
    return Status.MISSING


def median(vals):
    return statistics.median(vals) if vals else None


def grid(price_at, t_end, w_ms, step_ms=1000):
    """Price as of each grid instant t_end - k*step (ascending), None where unavailable."""
    n = w_ms // step_ms
    return [price_at(t_end - k * step_ms) for k in range(n, -1, -1)]


def _logrets(samples):
    pts = [(i, math.log(v)) for i, v in enumerate(samples) if v is not None]
    return [b[1] - a[1] for a, b in zip(pts, pts[1:])], pts


def realized(samples):
    """(rv, rvar, mean |r|) of consecutive 1-s log returns (missing points skipped only in partial mode)."""
    r, _ = _logrets(samples)
    if not r:
        return None, None, None
    rvar = math.fsum(x * x for x in r)
    return math.sqrt(rvar), rvar, math.fsum(abs(x) for x in r) / len(r)


def slope_path_persistence(samples, step_s=1.0):
    r, pts = _logrets(samples)
    if len(pts) < 2:
        return None, None, None
    xs = [i * step_s for i, _ in pts]
    ys = [y for _, y in pts]
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    sxx = math.fsum((x - mx) ** 2 for x in xs)
    slope = math.fsum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx if sxx > 0 else None
    path = math.fsum(abs(x) for x in r)
    pers = abs(ys[-1] - ys[0]) / path if path > 0 else None
    return slope, path, pers


def lead_lag(g_a, g_b, max_lag):
    """Lag (s) in [-max_lag, max_lag] maximising corr(r_a(t), r_b(t+lag)); (None, None) if undefined."""
    ra = [math.log(b / a) for a, b in zip(g_a, g_a[1:])]
    rb = [math.log(b / a) for a, b in zip(g_b, g_b[1:])]
    best = None
    for lag in range(-max_lag, max_lag + 1):
        if lag >= 0:
            x, y = ra[:len(ra) - lag], rb[lag:]
        else:
            x, y = ra[-lag:], rb[:len(rb) + lag]
        if len(x) < 10:
            continue
        c = corr(x, y)
        if c is not None and (best is None or c > best[1] or (c == best[1] and abs(lag) < abs(best[0]))):
            best = (lag, c)
    return best if best is not None else (None, None)


def corr(x, y):
    mx, my = statistics.fmean(x), statistics.fmean(y)
    sx = math.fsum((a - mx) ** 2 for a in x)
    sy = math.fsum((b - my) ** 2 for b in y)
    if sx <= 0 or sy <= 0:
        return None
    return math.fsum((a - mx) * (b - my) for a, b in zip(x, y)) / math.sqrt(sx * sy)


def basis_bps(cf, ref):
    if cf is None or ref is None or ref <= 0:
        return None
    return (cf - ref) / ref * 1e4


def kalshi_quote(p):
    yb, ya, nb, na = p.get("yes_bid"), p.get("yes_ask"), p.get("no_bid"), p.get("no_ask")
    both_y = yb is not None and ya is not None
    both_n = nb is not None and na is not None
    ym = (yb + ya) / 2 if both_y else None
    return {"yes_bid": yb, "yes_ask": ya, "no_bid": nb, "no_ask": na, "yes_mid": ym,
            "yes_spread": ya - yb if both_y else None, "no_mid": (nb + na) / 2 if both_n else None,
            "no_spread": na - nb if both_n else None, "exec_yes_ask": ya, "exec_no_ask": na,
            "implied_prob": ym / 100.0 if ym is not None else None}


def book_top(p):
    bids, asks = p.get("bids") or [], p.get("asks") or []
    bq = bids[0][1] if bids else None
    aq = asks[0][1] if asks else None
    out = {"yes_bid_qty": bq, "yes_ask_qty": aq, "imbalance": None, "weighted_mid": None}
    if bq is not None and aq is not None and bq + aq > 0:
        out["imbalance"] = (bq - aq) / (bq + aq)
        out["weighted_mid"] = (bids[0][0] * aq + asks[0][0] * bq) / (bq + aq)
    return out
