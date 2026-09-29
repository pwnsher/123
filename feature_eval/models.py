"""
Research models (pure Python, deterministic; no dependency beyond the standard library).

    A  LegacyModel            the frozen production probability as is (no fitting)
       LegacyRecalibrated     logistic on the legacy logit only (the fair same-class baseline)
    B  LogisticModel          logistic regression on standardized research inputs (+ legacy logit), tiny L2 for
                              numerical stability (1e-4)
    C  RidgeLogisticModel     L2-regularized logistic; lambda chosen from a PREDECLARED grid by a MARKET-LEVEL,
                              close-group, PURGED chronological inner split of the TRAINING data only
                              (feature_eval.splits.inner_split; never rows, never test, never holdout)
    D  BoostedStumps          gradient-boosted depth-1 trees on train-quantile bins, starting from the legacy logit,
                              with learned default directions for missing values (native missingness handling)

Preprocessing is fit on TRAINING rows only (Preprocessor.fit): weighted mean / std standardization and the missing-data
strategy:
    "indicator"      a missing value becomes the TRAINING mean AND an explicit missing-indicator column is added for
                     every feature with any missing training value (the model sees that it was missing)
    "complete_case"  rows with any missing selected feature are dropped from that model (sample loss reported)
A missing value is never silently turned into 0 (tested; mutation S9).
All training uses weights; by default each market's rows sum to 1 (market-normalized), so markets with more READY
checkpoints get no extra influence.
"""
import math
from operator import mul

from feature_eval.metrics import clip, logit, sigmoid

RIDGE_GRID = (0.1, 1.0, 10.0, 100.0)


def market_weights(markets):
    cnt = {}
    for m in markets:
        cnt[m] = cnt.get(m, 0) + 1
    return [1.0 / cnt[m] for m in markets]


class Preprocessor:
    def __init__(self, strategy="indicator"):
        if strategy not in ("indicator", "complete_case"):
            raise ValueError("missing-data strategy must be 'indicator' or 'complete_case'")
        self.strategy = strategy

    def fit(self, X, w):
        k = len(X[0]) if X else 0
        self.k = k
        self.mean, self.std, self.has_missing = [], [], []
        for j in range(k):
            vals = [(x[j], wi) for x, wi in zip(X, w) if x[j] is not None]
            sw = sum(wi for _, wi in vals)
            mu = sum(v * wi for v, wi in vals) / sw if sw else 0.0
            var = sum(wi * (v - mu) ** 2 for v, wi in vals) / sw if sw else 0.0
            self.mean.append(mu)
            self.std.append(math.sqrt(var) if var > 1e-18 else 1.0)
            self.has_missing.append(any(x[j] is None for x in X))
        return self

    def keep_row(self, x):
        return self.strategy != "complete_case" or all(v is not None for v in x)

    def transform_row(self, x):
        out = []
        ind = []
        for j in range(self.k):
            v = x[j]
            if v is None:
                out.append(0.0)                              # standardized TRAIN MEAN (not a raw zero) ...
                if self.has_missing[j]:
                    ind.append(1.0)                          # ... and the model is told it was missing
            else:
                out.append((v - self.mean[j]) / self.std[j])
                if self.has_missing[j]:
                    ind.append(0.0)
        return out + (ind if self.strategy == "indicator" else [])

    @property
    def n_out(self):
        return self.k + (sum(self.has_missing) if self.strategy == "indicator" else 0)


def _solve(A, b):
    n = len(b)
    M = [row[:] + [b[i]] for i, row in enumerate(A)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(M[r][c]))
        if abs(M[p][c]) < 1e-14:
            return None
        M[c], M[p] = M[p], M[c]
        for r in range(n):
            if r != c:
                f = M[r][c] / M[c][c]
                if f:
                    for cc in range(c, n + 1):
                        M[r][cc] -= f * M[c][cc]
    return [M[i][n] / M[i][i] for i in range(n)]


def fit_logistic(Z, y, w, l2=1e-4, offset=None, iters=30, unpenalized=0):
    """Weighted L2 logistic regression by Newton. Z: rows of features (no intercept). The intercept and the first
    `unpenalized` columns (the legacy logit) are not shrunk.
    Column-wise (C-level dot products) so it stays fast in pure Python."""
    n = len(Z)
    k = len(Z[0]) if n else 0
    cols = [[1.0] * n] + [[z[j] for z in Z] for j in range(k)]
    beta = [0.0] * (k + 1)
    off = list(offset) if offset else [0.0] * n
    sw = sum(w) or 1.0
    for _ in range(iters):
        eta = off[:]
        for bj, col in zip(beta, cols):
            if bj:
                eta = list(map(lambda e, x, b=bj: e + b * x, eta, col))
        mu = [sigmoid(e) for e in eta]
        r = [wi * (yi - mi) for wi, yi, mi in zip(w, y, mu)]
        v = [wi * mi * (1 - mi) for wi, mi in zip(w, mu)]
        g = [sum(map(mul, r, col)) for col in cols]
        H = [[0.0] * (k + 1) for _ in range(k + 1)]
        for a_ in range(k + 1):
            va = list(map(mul, v, cols[a_]))
            for c in range(a_, k + 1):
                H[a_][c] = H[c][a_] = sum(map(mul, va, cols[c]))
        for a_ in range(1 + unpenalized, k + 1):
            g[a_] -= l2 * sw * beta[a_]
            H[a_][a_] += l2 * sw
        step = _solve(H, g)
        if step is None:
            break
        beta = [b_ + s_ for b_, s_ in zip(beta, step)]
        if max(abs(s_) for s_ in step) < 1e-7:
            break
    return beta


def predict_logistic(beta, Z, offset=None):
    off = offset or [0.0] * len(Z)
    return [sigmoid(o + beta[0] + sum(map(mul, beta[1:], z))) for z, o in zip(Z, off)]


class LegacyModel:
    name = "A_legacy_production_probability"
    n_params = 0

    def fit(self, X, legacy, y, w):
        return self

    def predict(self, X, legacy):
        return [clip(p) for p in legacy]


class LegacyRecalibrated:
    name = "A2_legacy_recalibrated"

    def fit(self, X, legacy, y, w):
        self.beta = fit_logistic([[logit(p)] for p in legacy], y, w)
        self.n_params = 2
        return self

    def predict(self, X, legacy):
        return predict_logistic(self.beta, [[logit(p)] for p in legacy])


class LogisticModel:
    name = "B_logistic"

    def __init__(self, strategy="indicator", l2=1e-4):
        self.pre = Preprocessor(strategy)
        self.l2 = l2

    def _Z(self, X, legacy):
        return [[logit(p)] + self.pre.transform_row(x) for x, p in zip(X, legacy)]

    def fit(self, X, legacy, y, w, scaler_X=None, scaler_w=None):
        """scaler_X / scaler_w: the rows the preprocessing is fit on - always the TRAINING rows (default: X)."""
        self.pre.fit(X if scaler_X is None else scaler_X, w if scaler_w is None else scaler_w)
        self.beta = fit_logistic(self._Z(X, legacy), y, w, self.l2, unpenalized=1)   # legacy logit unshrunk
        self.n_params = 1 + 1 + self.pre.n_out
        return self

    def predict(self, X, legacy):
        return predict_logistic(self.beta, self._Z(X, legacy))


class RidgeLogisticModel(LogisticModel):
    name = "C_ridge_logistic"

    def __init__(self, strategy="indicator", grid=RIDGE_GRID, inner_fraction=0.25, inner_split_cfg=None):
        super().__init__(strategy)
        self.grid = tuple(grid)
        self.inner_fraction = inner_fraction
        self.inner_split_cfg = inner_split_cfg

    def fit(self, X, legacy, y, w, meta=None, scaler_X=None, scaler_w=None):
        """meta: per-row dicts (market_ticker, asset, close_ts_ms, checkpoint_ts_ms) aligned with X. Lambda is chosen
        on a market-level, close-group, purged inner split of THESE (training) rows; each candidate's preprocessing
        is fit on the inner-training rows only. Without market metadata no tuning is possible: the most conservative
        lambda (the largest) is used and the reason recorded."""
        from feature_eval.metrics import log_loss
        from feature_eval.splits import SplitConfig, inner_split, market_table
        scores, info = {}, {"status": "NO_MARKET_METADATA"}
        tr_m = va_m = purged = []
        if meta is not None:
            cfg = self.inner_split_cfg or SplitConfig()
            tr_m, va_m, purged, info = inner_split(market_table(meta), self.inner_fraction, cfg)
        a = [i for i, r in enumerate(meta or []) if r["market_ticker"] in set(tr_m)]
        b = [i for i, r in enumerate(meta or []) if r["market_ticker"] in set(va_m)]
        if a and b and len(set(y[i] for i in a)) == 2:
            wa = market_weights([meta[i]["market_ticker"] for i in a])
            wb = market_weights([meta[i]["market_ticker"] for i in b])
            for lam in self.grid:
                m = LogisticModel(self.pre.strategy, lam).fit([X[i] for i in a], [legacy[i] for i in a],
                                                              [y[i] for i in a], wa)
                scores[lam] = log_loss(m.predict([X[i] for i in b], [legacy[i] for i in b]), [y[i] for i in b], wb)
            info["scaler_rows"] = len(a)
        elif info.get("status") == "OK":
            info["status"] = "ONE_CLASS_OR_EMPTY"
        self.l2 = min(scores, key=lambda lam: (scores[lam], -lam)) if scores else max(self.grid)
        self.inner_scores = scores
        self.inner_info = dict(info, train_markets=list(tr_m), validation_markets=list(va_m), purged_markets=list(purged),
                               used_train_markets=sorted({meta[i]["market_ticker"] for i in a}),
                               used_validation_markets=sorted({meta[i]["market_ticker"] for i in b}))
        super().fit(X, legacy, y, w, scaler_X, scaler_w)
        self.n_params = 1 + 1 + self.pre.n_out
        return self


class BoostedStumps:
    """Gradient-boosted stumps on the log-loss, starting from the legacy logit. Bins are TRAIN quantiles (<= n_bins);
    a missing value goes to the side learned at each split (native missingness)."""
    name = "D_boosted_stumps"

    def __init__(self, rounds=40, learning_rate=0.1, n_bins=16, min_leaf_weight=5.0, l2=1.0):
        self.rounds, self.lr, self.n_bins, self.min_leaf, self.l2 = rounds, learning_rate, n_bins, min_leaf_weight, l2

    def _bin(self, v, j):
        if v is None:
            return -1
        cuts = self.cuts[j]
        lo, hi = 0, len(cuts)
        while lo < hi:
            mid = (lo + hi) // 2
            if v > cuts[mid]:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def fit(self, X, legacy, y, w):
        k = len(X[0]) if X else 0
        self.cuts = []
        for j in range(k):
            vals = sorted(x[j] for x in X if x[j] is not None)
            cuts = []
            for q in range(1, self.n_bins):
                if vals:
                    c = vals[min(len(vals) - 1, (len(vals) * q) // self.n_bins)]
                    if not cuts or c > cuts[-1]:
                        cuts.append(c)
            self.cuts.append(cuts)
        B = [[self._bin(x[j], j) for j in range(k)] for x in X]
        f = [logit(p) for p in legacy]
        self.trees = []
        for _ in range(self.rounds):
            p = [sigmoid(v) for v in f]
            g = [wi * (pi - yi) for pi, yi, wi in zip(p, y, w)]
            h = [wi * pi * (1 - pi) for pi, wi in zip(p, w)]
            best = None
            for j in range(k):
                nb = len(self.cuts[j]) + 1
                G = [0.0] * nb
                Hh = [0.0] * nb
                Gm = Hm = 0.0
                for bi, gi, hi in zip(B, g, h):
                    b = bi[j]
                    if b < 0:
                        Gm += gi
                        Hm += hi
                    else:
                        G[b] += gi
                        Hh[b] += hi
                GT, HT = sum(G) + Gm, sum(Hh) + Hm
                GL = HL = 0.0
                for s in range(nb - 1):
                    GL += G[s]
                    HL += Hh[s]
                    for miss_left in (True, False):
                        gl, hl = (GL + Gm, HL + Hm) if miss_left else (GL, HL)
                        gr, hr = GT - gl, HT - hl
                        if hl < self.min_leaf * 0.01 or hr < self.min_leaf * 0.01:
                            continue
                        gain = gl * gl / (hl + self.l2) + gr * gr / (hr + self.l2) - GT * GT / (HT + self.l2)
                        if best is None or gain > best[0] + 1e-12:
                            best = (gain, j, s, miss_left, -gl / (hl + self.l2), -gr / (hr + self.l2))
            if best is None or best[0] <= 1e-9:
                break
            _, j, s, miss_left, vl, vr = best
            self.trees.append((j, s, miss_left, self.lr * vl, self.lr * vr))
            for i, bi in enumerate(B):
                b = bi[j]
                left = (b < 0 and miss_left) or (0 <= b <= s)
                f[i] += self.lr * (vl if left else vr)
        self.n_params = len(self.trees)          # predeclared: one effective parameter per shrunken stump
        return self

    def predict(self, X, legacy):
        out = []
        for x, p in zip(X, legacy):
            v = logit(p)
            for j, s, miss_left, vl, vr in self.trees:
                b = self._bin(x[j], j)
                left = (b < 0 and miss_left) or (0 <= b <= s)
                v += vl if left else vr
            out.append(sigmoid(v))
        return out


MODEL_SPECS = {
    "A_legacy_production_probability": lambda: LegacyModel(),
    "A2_legacy_recalibrated": lambda: LegacyRecalibrated(),
    "B_logistic": lambda: LogisticModel(),
    "C_ridge_logistic": lambda: RidgeLogisticModel(),
    "D_boosted_stumps": lambda: BoostedStumps(),
}
