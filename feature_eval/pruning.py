"""
STRUCTURAL pruning - TRAINING-LABEL-INDEPENDENT (it never reads y). Runs on the TRAINING rows of a fold only.

Removed (with reason), in this order:
    NOT_MODEL_CANDIDATE     role EXECUTION_STATE / ALIAS / DATA_QUALITY in the frozen universe (never model inputs)
    NEVER_AVAILABLE         no READY value in the training rows (the source never publishes it for these assets)
    CONSTANT                a single distinct READY value
    NEAR_CONSTANT           the most frequent READY value covers >= near_constant_share of READY values
    EXACT_DUPLICATE         identical READY values and identical availability as an earlier-kept feature
    HIGHLY_CORRELATED       |Pearson r| >= corr_threshold with an earlier-kept feature on rows where both are READY
                            (redundant horizons / permutations); the kept representative is the one with the most
                            READY values, then the name order - a purely structural rule
Removed fields stay in the raw data and in the research rows (provenance, execution, compatibility).
The result (kept, removed, reasons) is fingerprinted.
"""
import hashlib
import json
import math
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class PruneConfig:
    near_constant_share: float = 0.995
    corr_threshold: float = 0.995
    min_ready_rows: int = 20
    max_features_for_corr: int = 400

    def to_dict(self):
        return asdict(self)


def _col(rows, j):
    return [r["x"][j] if r["st"][j] == "READY" else None for r in rows]


def structural_prune(rows, candidates, col_index, role_of, cfg=None):
    """rows: TRAINING rows only. candidates: feature names. -> {"kept", "removed": {name: reason}, "fingerprint"}"""
    cfg = cfg or PruneConfig()
    removed = {}
    cols = {}
    for n in candidates:
        if role_of.get(n, "MODEL_CANDIDATE") != "MODEL_CANDIDATE":
            removed[n] = "NOT_MODEL_CANDIDATE"
            continue
        v = _col(rows, col_index[n])
        ready = [x for x in v if isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)]
        if len(ready) < cfg.min_ready_rows:
            removed[n] = "NEVER_AVAILABLE"
            continue
        counts = {}
        for x in ready:
            counts[x] = counts.get(x, 0) + 1
        top = max(counts.values())
        if len(counts) == 1:
            removed[n] = "CONSTANT"
            continue
        if top / len(ready) >= cfg.near_constant_share:
            removed[n] = "NEAR_CONSTANT"
            continue
        cols[n] = v
    kept = []
    sig = {}
    order = sorted(cols, key=lambda n: (-sum(1 for x in cols[n] if x is not None), n))
    for n in order:
        key = tuple(cols[n])
        if key in sig:
            removed[n] = f"EXACT_DUPLICATE of {sig[key]}"
            continue
        sig[key] = n
        kept.append(n)
    final = []
    for n in kept[:cfg.max_features_for_corr]:
        dup = None
        for m in final:
            r = _corr(cols[n], cols[m])
            if r is not None and abs(r) >= cfg.corr_threshold:
                dup = m
                break
        if dup:
            removed[n] = f"HIGHLY_CORRELATED with {dup}"
        else:
            final.append(n)
    for n in kept[cfg.max_features_for_corr:]:
        final.append(n)
    final.sort()
    body = {"kept": final, "removed": dict(sorted(removed.items())), "config": cfg.to_dict()}
    body["fingerprint"] = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    body["counts"] = {"candidates": len(candidates), "kept": len(final), "removed": len(removed)}
    return body


def _corr(a, b):
    xs = [(x, y) for x, y in zip(a, b) if x is not None and y is not None]
    n = len(xs)
    if n < 10:
        return None
    mx = sum(x for x, _ in xs) / n
    my = sum(y for _, y in xs) / n
    vx = sum((x - mx) ** 2 for x, _ in xs)
    vy = sum((y - my) ** 2 for _, y in xs)
    if vx <= 0 or vy <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in xs) / math.sqrt(vx * vy)


def redundancy_report(rows, names, col_index, threshold=0.95, max_pairs=200):
    """TRAIN-only report of highly correlated groups / near-duplicate horizons (descriptive)."""
    cols = {n: _col(rows, col_index[n]) for n in names}
    pairs = []
    ns = sorted(names)
    for i, a in enumerate(ns):
        for b in ns[i + 1:]:
            r = _corr(cols[a], cols[b])
            if r is not None and abs(r) >= threshold:
                pairs.append({"a": a, "b": b, "r": round(r, 5)})
                if len(pairs) >= max_pairs:
                    return {"threshold": threshold, "pairs": pairs, "truncated": True}
    return {"threshold": threshold, "pairs": pairs, "truncated": False}
