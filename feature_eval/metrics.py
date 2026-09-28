"""
Probability, calibration and discrimination metrics (pure Python, WEIGHTED; weights are market-normalized by default).

    brier, log_loss                        primary probability metrics
    calibration_intercept_slope            logistic recalibration  y ~ a + b * logit(p)   (perfect: a = 0, b = 1)
    reliability_bins(p, y, w, n_bins=10)   equal-width bins: n, weight, mean predicted, observed frequency
    ece                                    expected calibration error = sum_bins weight_share * |mean_p - observed|
                                           over 10 equal-width bins (weights market-normalized)
    roc_auc, pr_auc                        weighted Mann-Whitney AUC; weighted average precision
    classification                         accuracy / precision / recall / win rate at 0.5 - NEVER a selection metric
"""
import math

EPS = 1e-6


def clip(p):
    return min(1 - EPS, max(EPS, p))


def logit(p):
    p = clip(p)
    return math.log(p / (1 - p))


def sigmoid(z):
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    e = math.exp(z)
    return e / (1.0 + e)


def _w(w, n):
    return w if w is not None else [1.0] * n


def brier(p, y, w=None):
    w = _w(w, len(p))
    sw = sum(w)
    return sum(wi * (pi - yi) ** 2 for pi, yi, wi in zip(p, y, w)) / sw if sw else None


def log_loss(p, y, w=None):
    w = _w(w, len(p))
    sw = sum(w)
    return -sum(wi * (yi * math.log(clip(pi)) + (1 - yi) * math.log(1 - clip(pi))) for pi, yi, wi in zip(p, y, w)) / sw if sw else None


def calibration_intercept_slope(p, y, w=None, iters=50):
    """Weighted logistic regression of y on logit(p) (Newton). Returns (intercept, slope) or (None, None)."""
    w = _w(w, len(p))
    x = [logit(pi) for pi in p]
    a, b = 0.0, 1.0
    for _ in range(iters):
        g0 = g1 = h00 = h01 = h11 = 0.0
        for xi, yi, wi in zip(x, y, w):
            mu = sigmoid(a + b * xi)
            r = wi * (yi - mu)
            v = wi * mu * (1 - mu)
            g0 += r
            g1 += r * xi
            h00 += v
            h01 += v * xi
            h11 += v * xi * xi
        det = h00 * h11 - h01 * h01
        if abs(det) < 1e-12:
            return None, None
        da = (h11 * g0 - h01 * g1) / det
        db = (-h01 * g0 + h00 * g1) / det
        a, b = a + da, b + db
        if abs(da) + abs(db) < 1e-10:
            break
    if not (math.isfinite(a) and math.isfinite(b)):
        return None, None
    return a, b


def reliability_bins(p, y, w=None, n_bins=10):
    w = _w(w, len(p))
    bins = [{"lo": i / n_bins, "hi": (i + 1) / n_bins, "n": 0, "w": 0.0, "sp": 0.0, "sy": 0.0} for i in range(n_bins)]
    for pi, yi, wi in zip(p, y, w):
        b = bins[min(n_bins - 1, int(pi * n_bins))]
        b["n"] += 1
        b["w"] += wi
        b["sp"] += wi * pi
        b["sy"] += wi * yi
    out = []
    for b in bins:
        out.append({"lo": b["lo"], "hi": b["hi"], "n": b["n"], "weight": b["w"],
                    "mean_predicted": b["sp"] / b["w"] if b["w"] else None,
                    "observed_frequency": b["sy"] / b["w"] if b["w"] else None})
    return out


def ece(p, y, w=None, n_bins=10):
    bins = reliability_bins(p, y, w, n_bins)
    tw = sum(b["weight"] for b in bins)
    if not tw:
        return None
    return sum(b["weight"] / tw * abs(b["mean_predicted"] - b["observed_frequency"]) for b in bins if b["weight"])


def roc_auc(p, y, w=None):
    """Weighted probability that a random positive outranks a random negative (ties = 1/2)."""
    w = _w(w, len(p))
    pos = sum(wi for yi, wi in zip(y, w) if yi == 1)
    neg = sum(wi for yi, wi in zip(y, w) if yi == 0)
    if not pos or not neg:
        return None
    order = sorted(range(len(p)), key=lambda i: p[i])
    acc = 0.0
    neg_below = 0.0
    i = 0
    while i < len(order):
        j = i
        while j < len(order) and p[order[j]] == p[order[i]]:
            j += 1
        tie_pos = sum(w[order[k]] for k in range(i, j) if y[order[k]] == 1)
        tie_neg = sum(w[order[k]] for k in range(i, j) if y[order[k]] == 0)
        acc += tie_pos * (neg_below + 0.5 * tie_neg)
        neg_below += tie_neg
        i = j
    return acc / (pos * neg)


def pr_auc(p, y, w=None):
    """Weighted average precision (step-wise, descending score)."""
    w = _w(w, len(p))
    pos = sum(wi for yi, wi in zip(y, w) if yi == 1)
    if not pos:
        return None
    order = sorted(range(len(p)), key=lambda i: -p[i])
    tp = fp = 0.0
    ap = 0.0
    for i in order:
        if y[i] == 1:
            tp += w[i]
            ap += w[i] * (tp / (tp + fp))
        else:
            fp += w[i]
    return ap / pos


def classification(p, y, w=None, threshold=0.5):
    w = _w(w, len(p))
    tp = fp = tn = fn = 0.0
    for pi, yi, wi in zip(p, y, w):
        pred = 1 if pi >= threshold else 0
        if pred and yi:
            tp += wi
        elif pred and not yi:
            fp += wi
        elif not pred and yi:
            fn += wi
        else:
            tn += wi
    tot = tp + fp + tn + fn
    return {"accuracy": (tp + tn) / tot if tot else None, "precision": tp / (tp + fp) if (tp + fp) else None,
            "recall": tp / (tp + fn) if (tp + fn) else None,
            "win_rate_favoured_side": (tp + tn) / tot if tot else None,
            "note": "descriptive only - never used to select anything"}


def summary(p, y, w=None):
    a, b = calibration_intercept_slope(p, y, w)
    return {"n_rows": len(p), "brier": brier(p, y, w), "log_loss": log_loss(p, y, w), "calibration_intercept": a,
            "calibration_slope": b, "ece_10_bins": ece(p, y, w), "roc_auc": roc_auc(p, y, w), "pr_auc": pr_auc(p, y, w),
            "classification": classification(p, y, w)}


CONFIDENCE_BUCKETS = ((0.50, 0.60), (0.60, 0.70), (0.70, 0.80), (0.80, 0.85), (0.85, 0.90), (0.90, 0.92), (0.92, 0.95),
                      (0.95, 0.97), (0.97, 0.98), (0.98, 0.99), (0.99, 1.0000001))


def confidence_buckets(p, y, w=None, markets=None, extra=None, min_n=30, min_markets=20):
    """Favoured-side confidence max(p, 1-p) in fixed buckets. Buckets below min_n rows or min_markets independent
    markets are listed as INSUFFICIENT_SAMPLE (never a percentage claim). `extra`: per-row dicts with 'price' and
    'net_edge' (favoured side, probability units) where executable."""
    w = _w(w, len(p))
    out = []
    for lo, hi in CONFIDENCE_BUCKETS:
        idx = [i for i, pi in enumerate(p) if lo <= max(pi, 1 - pi) < hi]
        mk = {markets[i] for i in idx} if markets is not None else set()
        entry = {"bucket": f"{lo:.2f}-{min(hi, 1.0):.2f}", "n_rows": len(idx), "n_markets": len(mk)}
        if len(idx) < min_n or (markets is not None and len(mk) < min_markets):
            entry["status"] = "INSUFFICIENT_SAMPLE"
            out.append(entry)
            continue
        ws = sum(w[i] for i in idx)
        conf = [max(p[i], 1 - p[i]) for i in idx]
        won = [1 if (p[i] >= 0.5) == (y[i] == 1) else 0 for i in idx]
        pm = sum(w[i] * c for i, c in zip(idx, conf)) / ws
        obs = sum(w[i] * o for i, o in zip(idx, won)) / ws
        wins = sum(won)
        entry.update(status="OK", predicted_mean_confidence=pm, observed_win_frequency=obs,
                     wins=f"{wins} / {len(idx)}", calibration_error=obs - pm,
                     brier_contribution=sum(w[i] * (p[i] - y[i]) ** 2 for i in idx) / sum(w),
                     note="counts, not an accuracy claim: always read with n and the confidence interval")
        if extra is not None:
            px = [extra[i].get("price") for i in idx if extra[i] and extra[i].get("price") is not None]
            ne = [extra[i].get("net_edge") for i in idx if extra[i] and extra[i].get("net_edge") is not None]
            entry["average_executable_price"] = sum(px) / len(px) if px else None
            entry["average_net_edge"] = sum(ne) / len(ne) if ne else None
            entry["executable_rows"] = len(px)
        out.append(entry)
    return out
