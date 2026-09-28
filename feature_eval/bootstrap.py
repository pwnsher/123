"""
MARKET-CLUSTERED uncertainty. The 15-minute Kalshi market (contract) is the resampling unit: checkpoints of one
market share one outcome, so rows are NEVER resampled independently.

    cluster_bootstrap(markets, stat_fn, reps, seed)      -> {"estimate", "ci_low", "ci_high", "reps", "unit": "market"}
        stat_fn(list_of_row_indices) -> float | None     computed on the rows of the resampled markets (a market drawn
                                                         twice contributes its rows twice)
    sign_flip_pvalue(market_deltas, reps, seed)          two-sided market-level sign-flip permutation p-value for a mean
                                                         paired difference (H0: no difference, exchangeable signs)
"""
import random


def groups(markets):
    """market id per row -> {market: [row indices]} (deterministic order)."""
    g = {}
    for i, m in enumerate(markets):
        g.setdefault(m, []).append(i)
    return {k: g[k] for k in sorted(g)}


def resampling_units(markets):
    """The unit a bootstrap replicate draws: markets (never rows)."""
    return sorted(groups(markets))


def cluster_bootstrap(markets, stat_fn, reps=1000, seed=12345, alpha=0.05):
    g = groups(markets)
    keys = resampling_units(markets)
    est = stat_fn([i for k in keys for i in g[k]])
    rng = random.Random(seed)
    vals = []
    for _ in range(reps):
        idx = []
        for _k in range(len(keys)):
            idx.extend(g[keys[rng.randrange(len(keys))]])
        v = stat_fn(idx)
        if v is not None:
            vals.append(v)
    vals.sort()
    if not vals:
        return {"estimate": est, "ci_low": None, "ci_high": None, "reps": 0, "unit": "market", "clusters": len(keys)}
    lo = vals[int((alpha / 2) * (len(vals) - 1))]
    hi = vals[int((1 - alpha / 2) * (len(vals) - 1))]
    return {"estimate": est, "ci_low": lo, "ci_high": hi, "reps": len(vals), "unit": "market", "clusters": len(keys),
            "alpha": alpha}


def market_means(markets, values, weights=None):
    """Per-market weighted mean of a per-row quantity (e.g. a paired loss difference)."""
    g = groups(markets)
    out = {}
    for k, idx in g.items():
        ws = [weights[i] if weights is not None else 1.0 for i in idx]
        sw = sum(ws)
        out[k] = sum(values[i] * wi for i, wi in zip(idx, ws)) / sw if sw else 0.0
    return out


def sign_flip_pvalue(market_deltas, reps=5000, seed=54321):
    d = [market_deltas[k] for k in sorted(market_deltas)]
    n = len(d)
    if n == 0:
        return None
    obs = abs(sum(d) / n)
    rng = random.Random(seed)
    hits = 0
    for _ in range(reps):
        s = sum(x if rng.random() < 0.5 else -x for x in d) / n
        if abs(s) >= obs - 1e-15:
            hits += 1
    return (hits + 1) / (reps + 1)
