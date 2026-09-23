#!/usr/bin/env python3
"""
analyze_perp_predictive.py — STEP 3 offline research: does perp data add predictive
information beyond the existing binary model and ordinary spot behaviour?

    python analyze_perp_predictive.py
    python analyze_perp_predictive.py --telemetry kalshi_perp_telemetry.csv \
        --labels kalshi_binary_outcomes.csv --outdir analysis_output

Reads two local CSVs. Makes NO network requests and imports no HTTP client.
Nothing here changes the bot: no thresholds, no filters, no confidence changes,
no P&L optimisation. Output is statistical description for Step 4 to consider.

Design (full detail in SETUP.txt and the JSON report's config snapshot):
  * One observation per (ticker, horizon): the latest analysis_ready row at or before
    close - {8,6,4,2} min, no more than HORIZON_MAX_EARLY_SECONDS early. Never after.
  * Nested logistic models, scored out-of-fold on identical rows:
        A: logit P = a + b*base_logit
        B: A + spot controls
        C: B + g*feature                       (directional family)
        C: B + g*feature + d*feature*base_logit (reliability family)
  * Expanding walk-forward folds split on whole close-time groups (so contemporaneous
    BTC/ETH/SOL/XRP markets are never split between train and test).
  * All preprocessing (1/99 winsorisation, mean/std) fit on training rows only;
    within coin for the pooled ALL group.
  * Ticker-clustered bootstrap CIs; BH-FDR across the 8 predeclared primary features
    per cohort x group x horizon; conservative sample-size gates.

step3_v2 (telemetry schema 3 / step2_v2) adds the volatility-regime research features:
  * ONE new primary: perp_momentum_z_60s (60 s perp return / its 300 s realized-vol horizon
    sigma), spot-controlled by spot_momentum_z_60s, so it only counts if it adds information
    beyond equally normalised spot momentum. Every other new feature is SECONDARY (it is
    screened and reported, but can never become a candidate).
  * Normalised gaps are directional; spread ratio and premium stress are reliability features.
  * A predeclared 3-member INTERACTION family asks whether perp predictive value changes with
    volatility / liquidity (perp feature x modifier beyond both main effects AND the spot
    control x modifier term). It is research only: its status is never PROMISING_CANDIDATE,
    it never enters the candidate manifest, and Step 4 policies cannot represent it.
  * Descriptive (never used for any status): base-model metrics by vol_regime / stability state.
"""
import argparse
import bisect
from array import array
import csv
import datetime as dt
import hashlib
import json
import math
import os
import random
import sys
from collections import defaultdict

ANALYSIS_CODE_VERSION = "step3_v2"
EXPECTED_SCHEMA_VERSION = "3"          # == perp_telemetry.TELEMETRY_SCHEMA_VERSION (checked by tests)
EXPECTED_FEATURE_VERSION = "step2_v2"  # == perp_telemetry.FEATURE_VERSION

HORIZONS_MIN = (8, 6, 4, 2)
HORIZON_MAX_EARLY_SECONDS = 10.0
ENTRY_WINDOW_MIN = (2.0, 8.0)          # existing strategy entry window (minutes left)
PROB_CLAMP = 1e-6
WINSOR_PCT = (1.0, 99.0)
WF_BOUNDS = (0.40, 0.55, 0.70, 0.85, 1.00)
MIN_TRAIN_ROWS_PER_FOLD = 30
MIN_TEST_ROWS_PER_FOLD = 10
MIN_TRAIN_ROWS_PER_COIN_SCALER = 10
ZERO_STD = 1e-12
LOGIT_RIDGE = 1e-4                     # fixed, tiny, numerical stabilisation only; intercept unpenalised
LOGIT_MAX_ITER = 100
LOGIT_TOL = 1e-9
CALIBRATION_BINS = 10
SEED = 20260919
BOOTSTRAP_REPS = 1000
SECONDARY_BOOTSTRAP_REPS = 200         # secondary features can never become candidates
CAUSAL_TOL_MS = 1                      # int-ms rounding in the CSV

DEFAULT_THRESHOLDS = {
    "min_analysis_markets": 200,
    "min_candidate_markets": 500,
    "min_class_count": 100,
    "min_valid_folds": 3,
    "q_max": 0.05,
    "sign_consistency_min_pct": 75.0,
    "min_leadlag_anchors": 300,
}

GROUPS = ("ALL", "BTC", "ETH", "SOL", "XRP")
COINS = ("BTC", "ETH", "SOL", "XRP")

# ─────────────── predeclared feature families (POSITIVE ALLOWLIST) ───────────────
PRIMARY_FEATURES = ("causal_premium_bps", "premium_change_60s_bps", "premium_z_5m",
                    "causal_perp_ret_60s_bps", "momentum_gap_60s_bps",
                    "perp_vol_shock_60v300", "perp_spread_bps",
                    "perp_momentum_z_60s")                     # step3_v2: the only new primary
DIRECTIONAL_FEATURES = (
    "causal_premium_bps", "mark_index_premium_bps", "last_index_premium_bps", "mid_mark_basis_bps",
    "premium_change_30s_bps", "premium_change_60s_bps", "premium_change_180s_bps",
    "premium_z_5m", "premium_z_15m",
    "causal_perp_ret_30s_bps", "causal_perp_ret_60s_bps", "causal_perp_ret_180s_bps",
    "momentum_gap_30s_bps", "momentum_gap_60s_bps", "momentum_gap_180s_bps",
    # step3_v2 volatility-normalised momentum and perp-vs-spot disagreement
    "perp_momentum_z_30s", "perp_momentum_z_60s", "perp_momentum_z_180s",
    "momentum_gap_z_30s", "momentum_gap_z_60s", "momentum_gap_z_180s")
RELIABILITY_FEATURES = ("perp_spread_bps", "perp_rv_60s_bps", "perp_rv_300s_bps", "perp_rv_900s_bps",
                        "perp_vol_shock_60v300", "perp_vol_shock_60v900",
                        # step3_v2 liquidity deterioration / basis instability (secondary)
                        "perp_spread_ratio_5m", "premium_stress_5m")
EXPLORATORY_FUNDING = ("funding_rate",)          # units unverified: never a candidate
SPOT_CONTROL_FEATURES = ("causal_spot_ret_30s_bps", "causal_spot_ret_60s_bps", "causal_spot_ret_180s_bps",
                         "spot_rv_60s_bps", "spot_rv_300s_bps", "spot_rv_900s_bps",
                         "spot_vol_shock_60v300", "spot_vol_shock_60v900",
                         "spot_momentum_z_30s", "spot_momentum_z_60s", "spot_momentum_z_180s")
# Descriptive categorical telemetry (never a predictor; only grouped for description).
CATEGORICAL_COLUMNS = ("vol_regime", "perp_stability_state")
# Predeclared interaction family: (perp directional feature, modifier). Small on purpose.
#   base  = A + spot control + feature + modifier + spot control x modifier
#   full  = base + feature x modifier
# Does the perp feature's incremental value CHANGE with volatility shock / spread stress, beyond
# the same change for the matched spot control? Research only (see module docstring).
INTERACTIONS = (("perp_momentum_z_60s", "perp_vol_shock_60v300"),
                ("momentum_gap_z_60s", "perp_vol_shock_60v300"),
                ("perp_momentum_z_60s", "perp_spread_ratio_5m"))
IX_INSUFFICIENT = "INSUFFICIENT_DATA"
IX_NONE = "NO_EVIDENCE_OF_INTERACTION"
IX_EVIDENCE = "INTERACTION_EVIDENCE_RESEARCH_ONLY"
CANDIDATE_FEATURES = DIRECTIONAL_FEATURES + RELIABILITY_FEATURES + EXPLORATORY_FUNDING
FEATURE_ALLOWLIST = frozenset(CANDIDATE_FEATURES + SPOT_CONTROL_FEATURES)
# belt-and-braces: even an allowlisted name may never look like a label
FORBIDDEN_PREDICTOR_PATTERNS = ("outcome", "settle", "result", "win", "pnl", "exit", "future", "label")

PREMIUM_BASIS = ("causal_premium_bps", "mark_index_premium_bps", "last_index_premium_bps", "mid_mark_basis_bps",
                 "premium_change_30s_bps", "premium_change_60s_bps", "premium_change_180s_bps",
                 "premium_z_5m", "premium_z_15m", "funding_rate")
SECOND_CONTROL_MIN_COVERAGE = 0.80   # add spot_vol_shock_60v300 to premium/basis controls if >=80% available

LEADLAG_FEATURES = ("causal_perp_ret_30s_bps", "causal_perp_ret_60s_bps", "causal_perp_ret_180s_bps",
                    "momentum_gap_30s_bps", "momentum_gap_60s_bps", "momentum_gap_180s_bps",
                    "causal_premium_bps", "premium_change_30s_bps", "premium_change_60s_bps",
                    "premium_change_180s_bps",
                    "perp_momentum_z_60s", "momentum_gap_z_60s")          # step3_v2
FUTURE_HORIZONS_S = (10, 30, 60, 120)
FUTURE_SPOT_TOLERANCE_SECONDS = 8.0
LEADLAG_MAX_INTERNAL_GAP_SECONDS = 30.0
LEADLAG_CONTROL = {10: "causal_spot_ret_30s_bps", 30: "causal_spot_ret_30s_bps",
                   60: "causal_spot_ret_60s_bps", 120: "causal_spot_ret_180s_bps"}

ST_INSUFFICIENT = "INSUFFICIENT_DATA"
ST_EXPLORATORY = "EXPLORATORY_ONLY"
ST_NO_VALUE = "NO_INCREMENTAL_VALUE"
ST_UNSTABLE = "UNSTABLE"
ST_CANDIDATE = "PROMISING_CANDIDATE"
LL_INSUFFICIENT = "INSUFFICIENT_DATA"
LL_NONE = "NO_EVIDENCE_OF_INCREMENTAL_LEAD"
LL_SIGNAL = "CONSISTENT_INCREMENTAL_SIGNAL"


class SchemaError(Exception):
    pass


class CausalInvariantError(Exception):
    pass


class SingularMatrixError(Exception):
    pass


def assert_allowed_predictor(name):
    """Positive allowlist + label-pattern guard. Raises for anything else."""
    if name not in FEATURE_ALLOWLIST:
        raise ValueError(f"'{name}' is not an allowlisted predictor")
    low = name.lower()
    for pat in FORBIDDEN_PREDICTOR_PATTERNS:
        if pat in low:
            raise ValueError(f"'{name}' looks like a label/outcome field ({pat})")


def feature_family(name):
    if name in EXPLORATORY_FUNDING:
        return "EXPLORATORY_FUNDING"
    if name in RELIABILITY_FEATURES:
        return "reliability"
    if name in DIRECTIONAL_FEATURES:
        return "directional"
    raise ValueError(name)


def spot_controls_for(feature, rows=None):
    """Predeclared spot-control mapping. rows (optional) decides whether the second
    premium/basis control has enough coverage (fixed 80% rule, not tuned)."""
    if feature.startswith("perp_momentum_z_") or feature.startswith("momentum_gap_z_"):
        return ["spot_momentum_z_" + feature.rsplit("_", 1)[-1]]      # equally normalised spot momentum
    if feature in ("perp_spread_ratio_5m", "premium_stress_5m"):
        return ["spot_vol_shock_60v300"]                              # market-wide stress control
    if feature.startswith("causal_perp_ret_") or feature.startswith("momentum_gap_"):
        h = feature.split("_")[-2]                          # '30s' / '60s' / '180s'
        return [f"causal_spot_ret_{h}_bps"]
    if feature.startswith("perp_rv_"):
        return ["spot_" + feature[len("perp_"):]]
    if feature.startswith("perp_vol_shock_"):
        return ["spot_" + feature[len("perp_"):]]
    if feature == "perp_spread_bps":
        return ["spot_vol_shock_60v300"]
    if feature in PREMIUM_BASIS:
        ctl = ["causal_spot_ret_60s_bps"]
        if rows:
            base = [r for r in rows if _ok(r["f"].get(feature)) and _ok(r["f"].get("causal_spot_ret_60s_bps"))]
            if base:
                cov = sum(1 for r in base if _ok(r["f"].get("spot_vol_shock_60v300"))) / len(base)
                if cov >= SECOND_CONTROL_MIN_COVERAGE:
                    ctl.append("spot_vol_shock_60v300")
        return ctl
    raise ValueError(f"no spot control mapping for {feature}")


# ═══════════════════════ numerics (stdlib only, deterministic) ═══════════════════════
def _ok(x):
    return x is not None and isinstance(x, float) and math.isfinite(x)


def clamp_p(p):
    return min(max(p, PROB_CLAMP), 1.0 - PROB_CLAMP)


def logit(p):
    p = clamp_p(p)
    return math.log(p / (1.0 - p))


def sigmoid(z):
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    e = math.exp(z)
    return e / (1.0 + e)


def mean(xs):
    return sum(xs) / len(xs) if xs else None


def brier(p, y):
    return sum((pi - yi) ** 2 for pi, yi in zip(p, y)) / len(y) if y else None


def logloss(p, y):
    if not y:
        return None
    s = 0.0
    for pi, yi in zip(p, y):
        pc = clamp_p(pi)
        s -= yi * math.log(pc) + (1 - yi) * math.log(1.0 - pc)
    return s / len(y)


def _avg_ranks(xs):
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    ranks = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        r = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = r
        i = j + 1
    return ranks


def auc(p, y):
    """Mann-Whitney AUC with average ranks for ties; None if one class is absent."""
    n1 = sum(y)
    n0 = len(y) - n1
    if n1 == 0 or n0 == 0:
        return None
    r = _avg_ranks(list(p))
    s = sum(ri for ri, yi in zip(r, y) if yi == 1)
    return (s - n1 * (n1 + 1) / 2.0) / (n1 * n0)


def calibration_bins(p, y, bins=CALIBRATION_BINS):
    """Equal-width bins on [0,1]; the last bin includes 1.0."""
    acc = [[0, 0.0, 0.0] for _ in range(bins)]
    for pi, yi in zip(p, y):
        b = min(int(pi * bins), bins - 1) if pi >= 0 else 0
        acc[b][0] += 1; acc[b][1] += pi; acc[b][2] += yi
    out = []
    for i, (n, sp, sy) in enumerate(acc):
        out.append({"bin": f"[{i / bins:.1f},{(i + 1) / bins:.1f}{']' if i == bins - 1 else ')'}", "n": n,
                    "mean_pred": (sp / n) if n else None, "actual_freq": (sy / n) if n else None,
                    "difference": ((sy - sp) / n) if n else None})
    return out


def ece(p, y, bins=CALIBRATION_BINS):
    """Expected calibration error = sum_b (n_b/N) * |mean_pred_b - actual_freq_b|."""
    if not y:
        return None
    return sum(b["n"] / len(y) * abs(b["mean_pred"] - b["actual_freq"])
               for b in calibration_bins(p, y, bins) if b["n"])


def percentile(xs, q):
    """Linear interpolation between closest ranks (numpy default)."""
    v = sorted(xs)
    if not v:
        return None
    if len(v) == 1:
        return v[0]
    pos = (len(v) - 1) * q / 100.0
    lo = int(math.floor(pos))
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def pearson(x, y):
    n = len(x)
    if n < 3:
        return None
    mx, my = sum(x) / n, sum(y) / n
    sxx = sum((a - mx) ** 2 for a in x)
    syy = sum((b - my) ** 2 for b in y)
    if sxx <= ZERO_STD or syy <= ZERO_STD:
        return None
    return sum((a - mx) * (b - my) for a, b in zip(x, y)) / math.sqrt(sxx * syy)


def spearman(x, y):
    if len(x) < 3:
        return None
    return pearson(_avg_ranks(list(x)), _avg_ranks(list(y)))


def solve_linear(A, b):
    """Gaussian elimination with partial pivoting. Raises SingularMatrixError."""
    n = len(b)
    M = [list(map(float, A[i])) + [float(b[i])] for i in range(n)]
    scale = max((abs(M[i][i]) for i in range(n)), default=1.0) or 1.0
    for c in range(n):
        piv = max(range(c, n), key=lambda r: abs(M[r][c]))
        if abs(M[piv][c]) <= 1e-12 * scale:
            raise SingularMatrixError(f"singular at column {c}")
        M[c], M[piv] = M[piv], M[c]
        for r in range(c + 1, n):
            f = M[r][c] / M[c][c]
            if f:
                for k in range(c, n + 1):
                    M[r][k] -= f * M[c][k]
    x = [0.0] * n
    for r in range(n - 1, -1, -1):
        x[r] = (M[r][n] - sum(M[r][k] * x[k] for k in range(r + 1, n))) / M[r][r]
    return x


def fit_logistic(X, y, ridge=LOGIT_RIDGE, max_iter=LOGIT_MAX_ITER, tol=LOGIT_TOL):
    """Newton-Raphson/IRLS with step-halving. X rows exclude the intercept.
    Fixed tiny ridge on slopes only. Returns {'beta': [b0, b1..], 'converged', 'iterations'}."""
    rows = [[1.0] + list(x) for x in X]
    k = len(rows[0]) if rows else 1
    beta = [0.0] * k

    def nll(b):
        s = 0.0
        for r, yi in zip(rows, y):
            z = sum(bj * xj for bj, xj in zip(b, r))
            s += (max(z, 0.0) + math.log1p(math.exp(-abs(z)))) - yi * z
        return s + 0.5 * ridge * sum(bj * bj for bj in b[1:])

    cur = nll(beta)
    converged = False
    it = 0
    for it in range(1, max_iter + 1):
        g = [0.0] * k
        H = [[0.0] * k for _ in range(k)]
        for r, yi in zip(rows, y):
            p = sigmoid(sum(bj * xj for bj, xj in zip(beta, r)))
            w = max(p * (1.0 - p), 1e-12)
            e = yi - p
            for a in range(k):
                ra = r[a]
                g[a] += e * ra
                wa = w * ra
                Ha = H[a]
                for c in range(a, k):
                    Ha[c] += wa * r[c]
        for a in range(k):
            for c in range(a):
                H[a][c] = H[c][a]
        for a in range(1, k):
            g[a] -= ridge * beta[a]
            H[a][a] += ridge
        step = solve_linear(H, g)
        t = 1.0
        while True:
            nb = [bj + t * sj for bj, sj in zip(beta, step)]
            new = nll(nb)
            if new <= cur + 1e-12 or t < 1e-8:
                break
            t *= 0.5
        moved = max(abs(t * sj) for sj in step)
        beta, cur = nb, new
        if moved < tol:
            converged = True
            break
    if not all(math.isfinite(b) for b in beta):
        raise SingularMatrixError("non-finite coefficients")
    return {"beta": beta, "converged": converged, "iterations": it}


def predict_logistic(beta, X):
    return [sigmoid(beta[0] + sum(b * x for b, x in zip(beta[1:], row))) for row in X]


def fit_ols(X, y):
    """OLS via normal equations with singularity detection. X rows exclude the intercept."""
    k = (len(X[0]) if X else 0) + 1
    A = [[0.0] * k for _ in range(k)]
    b = [0.0] * k
    for x, yi in zip(X, y):                      # one pass: accumulate X'X and X'y
        r = (1.0,) + tuple(x)
        for i in range(k):
            ri = r[i]
            b[i] += ri * yi
            Ai = A[i]
            for j in range(i, k):
                Ai[j] += ri * r[j]
    for i in range(k):
        for j in range(i):
            A[i][j] = A[j][i]
    return solve_linear(A, b)


def predict_ols(beta, X):
    return [beta[0] + sum(b * x for b, x in zip(beta[1:], row)) for row in X]


def bh_qvalues(pvals):
    """Benjamini-Hochberg: q_(i) = min_{j>=i} p_(j) * m / j, capped at 1. None stays None
    and is not counted in m."""
    idx = [i for i, p in enumerate(pvals) if p is not None]
    m = len(idx)
    q = [None] * len(pvals)
    if not m:
        return q
    order = sorted(idx, key=lambda i: pvals[i])
    run = 1.0
    for rank in range(m, 0, -1):
        i = order[rank - 1]
        run = min(run, pvals[i] * m / rank)
        q[i] = min(run, 1.0)
    return q


def cluster_bootstrap(clusters, deltas, reps, seed):
    """Resample CLUSTERS (tickers) with replacement. deltas: {name: per-row values}.
    Each replicate statistic = sum(resampled row deltas) / number of resampled rows.
    Returns {name: [replicate values]}."""
    keys = sorted(set(clusters))
    pos = {c: i for i, c in enumerate(keys)}
    n_c = len(keys)
    cnt = [0] * n_c
    sums = {nm: [0.0] * n_c for nm in deltas}
    for i, c in enumerate(clusters):
        j = pos[c]
        cnt[j] += 1
        for nm, v in deltas.items():
            sums[nm][j] += v[i]
    rng = random.Random(seed)
    out = {nm: [] for nm in deltas}
    idx = range(n_c)
    for _ in range(reps):
        pick = rng.choices(idx, k=n_c)
        n = sum(map(cnt.__getitem__, pick))
        for nm in deltas:
            out[nm].append(sum(map(sums[nm].__getitem__, pick)) / n)
    return out


def ci95(reps):
    return (percentile(reps, 2.5), percentile(reps, 97.5)) if reps else (None, None)


def bootstrap_p_improvement(reps):
    """One-sided p for H0: improvement <= 0.  p = (#{rep <= 0} + 1) / (B + 1)."""
    if not reps:
        return None
    return (sum(1 for d in reps if d <= 0) + 1) / (len(reps) + 1)


class Scaler:
    """Train-only 1/99 winsorisation + standardisation for one column."""

    def __init__(self, values):
        self.lo = percentile(values, WINSOR_PCT[0])
        self.hi = percentile(values, WINSOR_PCT[1])
        w = [min(max(v, self.lo), self.hi) for v in values]
        self.mean = sum(w) / len(w)
        self.std = math.sqrt(sum((v - self.mean) ** 2 for v in w) / len(w))
        if self.std <= ZERO_STD:
            raise ValueError("zero variance")

    def __call__(self, v):
        return (min(max(v, self.lo), self.hi) - self.mean) / self.std

    def stats(self):
        return [self.lo, self.hi, self.mean, self.std]

    @classmethod
    def from_stats(cls, stats):
        """Rebuild a frozen scaler (Step 4 shadow policies) without refitting."""
        lo, hi, mu, sd = (float(x) for x in stats)
        if not all(math.isfinite(x) for x in (lo, hi, mu, sd)) or sd <= ZERO_STD or lo > hi:
            raise ValueError("invalid frozen scaler")
        obj = cls.__new__(cls)
        obj.lo, obj.hi, obj.mean, obj.std = lo, hi, mu, sd
        return obj


def fit_scalers(train, cols, by_coin):
    """{(coin|'*', col): Scaler} from TRAINING rows only. Raises ValueError on zero variance."""
    out = {}
    keys = sorted({r["coin"] for r in train}) if by_coin else ["*"]
    for key in keys:
        sub = [r for r in train if not by_coin or r["coin"] == key]
        if len(sub) < MIN_TRAIN_ROWS_PER_COIN_SCALER:
            continue
        for c in cols:
            try:
                out[(key, c)] = Scaler([r["f"][c] for r in sub])
            except ValueError:
                raise ValueError(f"zero variance: {c} ({key})")
    return out


def _scaled(row, col, scalers, by_coin):
    s = scalers.get((row["coin"] if by_coin else "*", col))
    return None if s is None else s(row["f"][col])


def model_design(o, model, controls, feature, reliability, scalers, by_coin):
    """The Step 3 nested-model design (single definition, reused by Step 4):
         A: [base_logit]
         B: A + scaled spot controls
         C: B + scaled feature (+ feature * base_logit for the reliability family)"""
    b = o["base_logit"]
    x = [b]
    if model in ("B", "C"):
        x += [_scaled(o, c, scalers, by_coin) for c in controls]
    if model == "C":
        z = _scaled(o, feature, scalers, by_coin)
        x.append(z)
        if reliability:
            x.append(z * b)
    return x


# ═══════════════════════ loading + validation ═══════════════════════
REQUIRED_COLUMNS = ("telemetry_schema_version", "feature_version", "telemetry_session_id", "ts_utc",
                    "coin", "binary_status", "binary_ticker", "binary_close_time", "minutes_left",
                    "spot_price", "base_p_up", "fav", "binary_signal", "spot_observed_ts_epoch_ms",
                    "perp_snapshot_ts_epoch_ms", "perp_lag_to_spot_ms", "feature_end_ts_epoch_ms",
                    "causal_pair_ok", "analysis_ready") + tuple(sorted(FEATURE_ALLOWLIST)) + CATEGORICAL_COLUMNS


def _f(v):
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except ValueError:
        return None
    return x if math.isfinite(x) else None


def _parse_iso(s):
    if not s:
        return None
    try:
        d = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.timestamp()


LEADLAG_COLUMNS = tuple(dict.fromkeys(LEADLAG_FEATURES + tuple(LEADLAG_CONTROL.values())))
NAN = float("nan")


def _row_from_csv(i, r):
    p = _f(r.get("base_p_up"))
    spot_ms = _f(r.get("spot_observed_ts_epoch_ms"))
    return {
        "i": i, "session": r.get("telemetry_session_id") or "", "coin": r.get("coin") or "",
        "ticker": (r.get("binary_ticker") or "").strip(), "close_raw": r.get("binary_close_time") or "",
        "close": _parse_iso(r.get("binary_close_time")), "status": r.get("binary_status") or "",
        "minutes_left": _f(r.get("minutes_left")), "spot": _f(r.get("spot_price")),
        "p": (p / 100.0) if p is not None else None, "fav": r.get("fav") or "",
        "signal": (r.get("binary_signal") or "").strip() == "True",
        "ready": (r.get("analysis_ready") or "").strip() == "True",
        "causal_ok": (r.get("causal_pair_ok") or "").strip() == "True",
        "spot_ms": spot_ms, "perp_ms": _f(r.get("perp_snapshot_ts_epoch_ms")),
        "fe_ms": _f(r.get("feature_end_ts_epoch_ms")), "lag_ms": _f(r.get("perp_lag_to_spot_ms")),
        "ts": spot_ms / 1000.0 if spot_ms is not None else None,
        "f": {c: _f(r.get(c)) for c in FEATURE_ALLOWLIST},
        "cat": {c: (r.get(c) or "").strip() or None for c in CATEGORICAL_COLUMNS},
    }


def causal_violation(r):
    """None if the row is fine; otherwise the reason. Only analysis_ready rows are checked."""
    if not r["ready"]:
        return None
    s, p, fe, lag = r["spot_ms"], r["perp_ms"], r["fe_ms"], r["lag_ms"]
    if None in (s, p, fe, lag):
        return "missing causal timestamp"
    if p > s + CAUSAL_TOL_MS:
        return "perp_snapshot_ts > spot_observed_ts"
    if fe > s + CAUSAL_TOL_MS:
        return "feature_end_ts > spot_observed_ts"
    if lag < -CAUSAL_TOL_MS:
        return "perp_lag_to_spot_ms < 0"
    if abs((s - p) - lag) > 2 * CAUSAL_TOL_MS:
        return "perp_lag_to_spot_ms inconsistent with timestamps"
    return None


def check_causal_invariants(rows):
    """Hard failure if any analysis_ready row could contain future perp information."""
    bad = [(r["i"], r["coin"], r["ticker"], why) for r in rows for why in [causal_violation(r)] if why]
    if bad:
        raise CausalInvariantError(f"{len(bad)} analysis_ready rows violate causal invariants, e.g. {bad[:5]}")


class SpotStream:
    """Column store for one (session, coin): 8 bytes per value, so lead/lag over weeks of
    data stays small. Missing features are NaN."""
    __slots__ = ("ts", "spot", "ready", "ticker", "feats")

    def __init__(self):
        self.ts, self.spot, self.ready = array("d"), array("d"), array("b")
        self.ticker = []
        self.feats = {c: array("d") for c in LEADLAG_COLUMNS}

    def add(self, ts, spot, ready, ticker, f):
        self.ts.append(ts); self.spot.append(spot); self.ready.append(1 if ready else 0)
        self.ticker.append(ticker)
        for c in LEADLAG_COLUMNS:
            v = f.get(c)
            self.feats[c].append(NAN if v is None else v)

    def finalize(self):
        """Sort by time once (stable) and drop duplicate timestamps (keep first)."""
        n = len(self.ts)
        order = sorted(range(n), key=self.ts.__getitem__)
        keep, last = [], None
        for j in order:
            if last is None or self.ts[j] > last:
                keep.append(j); last = self.ts[j]
        if keep == list(range(n)):
            return self
        self.ts = array("d", (self.ts[j] for j in keep))
        self.spot = array("d", (self.spot[j] for j in keep))
        self.ready = array("b", (self.ready[j] for j in keep))
        self.ticker = [self.ticker[j] for j in keep]
        self.feats = {c: array("d", (v[j] for j in keep)) for c, v in self.feats.items()}
        return self


def streams_from_rows(rows):
    by = defaultdict(SpotStream)
    for r in sorted(rows, key=lambda r: (r["session"], r["coin"], r["ts"] if r["ts"] is not None else 0, r["i"])):
        if r["ts"] is not None and r["spot"] is not None and r["spot"] > 0:
            by[(r["session"], r["coin"])].add(r["ts"], r["spot"], r["ready"], r["ticker"], r["f"])
    return {k: v.finalize() for k, v in by.items()}


def _horizon_candidate(r, max_early=HORIZON_MAX_EARLY_SECONDS):
    if not (r["ready"] and r["status"] == "ok" and r["p"] is not None and r["close"] is not None
            and r["ts"] is not None and r["ticker"]):
        return False
    for h in HORIZONS_MIN:
        d = (r["close"] - 60.0 * h) - r["ts"]
        if 0.0 <= d <= max_early:
            return True
    return False


def _signal_candidate(r):
    lo, hi = ENTRY_WINDOW_MIN
    return (r["signal"] and r["status"] == "ok" and r["p"] is not None and r["ts"] is not None and r["ticker"]
            and r["minutes_left"] is not None and lo <= r["minutes_left"] <= hi)


def load_telemetry(path):
    """ONE read-only streaming pass. Validates schema on EVERY row (a mixed file fails) and
    causal invariants on EVERY analysis_ready row. Retains full rows only where the analysis
    can use them (horizon candidates, each ticker's earliest signal row); everything the
    lead/lag analysis needs goes into compact per-(session, coin) column stores.
    Returns (kept_rows, meta); meta['streams'] holds the column stores."""
    kept, first_sig = [], {}
    streams = defaultdict(SpotStream)
    tk_coins, tk_closes = defaultdict(set), defaultdict(set)
    violations, n_violations = [], 0
    meta = {"rows": 0, "analysis_ready_rows": 0, "first_ts_utc": None, "last_ts_utc": None,
            "sessions": set(), "coins": set()}
    with open(path, newline="") as f:
        rd = csv.DictReader(f)
        missing = [c for c in REQUIRED_COLUMNS if c not in (rd.fieldnames or [])]
        if missing:
            raise SchemaError(f"telemetry is missing required columns: {missing[:8]}"
                              f"{' ...' if len(missing) > 8 else ''} (Step 1 file? run Step 2 telemetry first)")
        bad = {}
        for i, raw in enumerate(rd):
            sv, fv = raw.get("telemetry_schema_version"), raw.get("feature_version")
            if sv != EXPECTED_SCHEMA_VERSION or fv != EXPECTED_FEATURE_VERSION:
                bad[(sv, fv)] = bad.get((sv, fv), 0) + 1
                continue
            r = _row_from_csv(i, raw)
            meta["rows"] += 1
            meta["analysis_ready_rows"] += r["ready"]
            ts = raw.get("ts_utc") or ""
            if ts and (meta["first_ts_utc"] is None or ts < meta["first_ts_utc"]):
                meta["first_ts_utc"] = ts
            if ts and (meta["last_ts_utc"] is None or ts > meta["last_ts_utc"]):
                meta["last_ts_utc"] = ts
            meta["sessions"].add(r["session"]); meta["coins"].add(r["coin"])
            why = causal_violation(r)
            if why:
                n_violations += 1
                if len(violations) < 5:
                    violations.append((i, r["coin"], r["ticker"], why))
            if r["ticker"]:
                tk_coins[r["ticker"]].add(r["coin"]); tk_closes[r["ticker"]].add(r["close"])
            if r["ts"] is not None and r["spot"] is not None and r["spot"] > 0:
                streams[(r["session"], r["coin"])].add(r["ts"], r["spot"], r["ready"], r["ticker"], r["f"])
            if _horizon_candidate(r):
                kept.append(r)
            elif _signal_candidate(r):
                cur = first_sig.get(r["ticker"])
                if cur is None or (r["ts"], r["i"]) < (cur["ts"], cur["i"]):
                    first_sig[r["ticker"]] = r
        if bad:
            raise SchemaError(f"incompatible/mixed schema rows {dict((str(k), v) for k, v in bad.items())}; "
                              f"expected schema {EXPECTED_SCHEMA_VERSION} / {EXPECTED_FEATURE_VERSION}")
    kept_ids = {r["i"] for r in kept}
    kept += [r for r in first_sig.values() if r["i"] not in kept_ids]
    meta["sessions"] = sorted(meta["sessions"])
    meta["coins"] = sorted(meta["coins"])
    meta["streams"] = {k: v.finalize() for k, v in streams.items()}
    meta["ticker_coins"], meta["ticker_closes"] = tk_coins, tk_closes
    meta["causal_violations"] = (n_violations, violations)
    return kept, meta


LABEL_COLUMNS = ("binary_ticker", "coin", "binary_close_time", "outcome_up", "label_status")


def load_labels(path):
    labels, statuses = {}, defaultdict(int)
    with open(path, newline="") as f:
        rd = csv.DictReader(f)
        missing = [c for c in LABEL_COLUMNS if c not in (rd.fieldnames or [])]
        if missing:
            raise SchemaError(f"labels file missing columns {missing}")
        for r in rd:
            statuses[r.get("label_status") or ""] += 1
            if r.get("label_status") != "final" or r.get("outcome_up") not in ("0", "1"):
                continue
            labels[r["binary_ticker"]] = {"y": int(r["outcome_up"]), "coin": r.get("coin") or "",
                                          "close": _parse_iso(r.get("binary_close_time"))}
    return labels, dict(statuses)


def ticker_metadata(rows, labels, coins=None, closes=None):
    """One coin and one close per ticker (telemetry AND label file must agree)."""
    if coins is None:
        coins, closes = defaultdict(set), defaultdict(set)
        for r in rows:
            if r["ticker"]:
                coins[r["ticker"]].add(r["coin"])
                closes[r["ticker"]].add(r["close"])
    excluded = {}
    for tk in coins:
        if len(coins[tk]) > 1 or len(closes[tk]) > 1 or None in closes[tk]:
            excluded[tk] = f"inconsistent metadata coins={sorted(coins[tk])} closes={len(closes[tk])}"
        elif tk in labels:
            lb = labels[tk]
            if lb["coin"] and lb["coin"] not in coins[tk]:
                excluded[tk] = "label coin disagrees with telemetry"
            elif lb["close"] is not None and abs(lb["close"] - next(iter(closes[tk]))) > 1e-6:
                excluded[tk] = "label close_time disagrees with telemetry"
    return excluded


def index_by_ticker(rows):
    """Group once, sort once (by spot observation time)."""
    by = defaultdict(list)
    for r in rows:
        if r["ticker"] and r["ts"] is not None:
            by[r["ticker"]].append(r)
    for tk in by:
        by[tk].sort(key=lambda r: (r["ts"], r["i"]))
    return by


def select_horizon_row(ready_rows, ready_ts, target_ts, max_early=HORIZON_MAX_EARLY_SECONDS):
    """Latest row with ts <= target_ts and target_ts - ts <= max_early. Never later."""
    i = bisect.bisect_right(ready_ts, target_ts) - 1
    if i < 0:
        return None
    r = ready_rows[i]
    return r if target_ts - r["ts"] <= max_early else None


def _obs(r, y, horizon, target=None):
    p = r["p"]
    return {"ticker": r["ticker"], "coin": r["coin"], "session": r["session"], "close": r["close"],
            "ts": r["ts"], "horizon": horizon, "y": y, "p": p, "base_logit": logit(p), "fav": r["fav"],
            "f": r["f"], "cat": r.get("cat") or {}, "ready": r["ready"], "row_index": r["i"],
            "horizon_early_ms": int(round((target - r["ts"]) * 1000)) if target is not None else None,
            "favored_correct": (y == 1) if r["fav"] == "UP" else ((y == 0) if r["fav"] == "DOWN" else None),
            "base_error": y - p}


def build_horizon_observations(by_ticker, labels, excluded, horizons=HORIZONS_MIN,
                               max_early=HORIZON_MAX_EARLY_SECONDS):
    """{horizon: [one observation per labeled, valid ticker]}"""
    out = {h: [] for h in horizons}
    for tk in sorted(by_ticker):
        if tk in excluded or tk not in labels:
            continue
        ready = [r for r in by_ticker[tk] if r["ready"] and r["status"] == "ok" and r["p"] is not None]
        if not ready:
            continue
        ts = [r["ts"] for r in ready]
        close = ready[0]["close"]
        for h in horizons:
            target = close - 60.0 * h
            r = select_horizon_row(ready, ts, target, max_early)
            if r is not None:
                out[h].append(_obs(r, labels[tk]["y"], h, target))
    return out


def build_first_signal_cohort(by_ticker, labels, excluded):
    """FIRST row per ticker where the existing bot signalled inside its entry window."""
    out = []
    lo, hi = ENTRY_WINDOW_MIN
    for tk in sorted(by_ticker):
        if tk in excluded or tk not in labels:
            continue
        for r in by_ticker[tk]:
            if (r["signal"] and r["status"] == "ok" and r["p"] is not None and r["minutes_left"] is not None
                    and lo <= r["minutes_left"] <= hi):
                out.append(_obs(r, labels[tk]["y"], "first_signal"))
                break
    return out


# ═══════════════════════ walk-forward validation ═══════════════════════
def walk_forward_folds(keys, bounds=WF_BOUNDS):
    """Expanding-window folds over WHOLE time groups. keys: per-observation time key
    (close time for markets, anchor time for lead/lag). Every train key < every test key.
    Returns [(fold_no, train_idx, test_idx)]."""
    counts = defaultdict(int)
    for k in keys:
        counts[k] += 1
    uniq = sorted(counts)
    n = len(keys)
    cuts = []
    for b in bounds:
        target = int(round(b * n))
        g = c = 0
        while g < len(uniq) and c < target:
            c += counts[uniq[g]]
            g += 1
        cuts.append(g)
    folds = []
    for f in range(len(bounds) - 1):
        tr, te = set(uniq[:cuts[f]]), set(uniq[cuts[f]:cuts[f + 1]])
        train = [i for i, k in enumerate(keys) if k in tr]
        test = [i for i, k in enumerate(keys) if k in te]
        if train and test:
            folds.append((f + 1, train, test))
    return folds


def _derived_seed(seed, *parts):
    h = hashlib.sha256(("|".join(map(str, (seed,) + parts))).encode()).hexdigest()
    return int(h[:12], 16)


def _sign(x):
    return 1 if x > 1e-12 else (-1 if x < -1e-12 else 0)


def sign_stats(coefs, attempted):
    pos = sum(1 for c in coefs if _sign(c) > 0)
    neg = sum(1 for c in coefs if _sign(c) < 0)
    zero_failed = (attempted - len(coefs)) + sum(1 for c in coefs if _sign(c) == 0)
    valid = len(coefs)
    dom = "+" if pos > neg else ("-" if neg > pos else "0")
    return {"positive_folds": pos, "negative_folds": neg, "zero_or_failed_folds": zero_failed,
            "dominant_sign": dom, "sign_consistency_pct": (100.0 * max(pos, neg) / valid) if valid else None,
            "coefficients": [round(c, 6) for c in coefs]}


# ═══════════════════════ nested-model feature screen ═══════════════════════
def screen_feature(obs, feature, group="ALL", horizon=None, cohort="primary", reps=BOOTSTRAP_REPS,
                   seed=SEED, thresholds=None, keep_oof=False):
    """MODEL A vs B vs C for ONE feature on ONE (cohort, group, horizon) dataset.
    All three models are scored on EXACTLY the same out-of-fold rows."""
    th = dict(DEFAULT_THRESHOLDS, **(thresholds or {}))
    assert_allowed_predictor(feature)
    if feature in SPOT_CONTROL_FEATURES:
        raise ValueError(f"{feature} is a spot CONTROL, not a perp candidate")
    fam = feature_family(feature)
    reliability = fam == "reliability"
    controls = spot_controls_for(feature, obs)
    for c in controls:
        assert_allowed_predictor(c)
    need = controls + [feature]
    by_coin = group == "ALL"
    avail = sorted((o for o in obs if all(_ok(o["f"].get(c)) for c in need) and _ok(o["base_logit"])),
                   key=lambda o: (o["close"], o["ticker"]))
    n_obs = len(obs)
    res = {"cohort": cohort, "group": group, "coin": group, "horizon_min": horizon, "feature_name": feature,
           "feature_family": fam, "primary_or_secondary": "primary" if feature in PRIMARY_FEATURES else "secondary",
           "controls": controls, "model_type": "reliability_interaction" if reliability else "directional",
           "unique_markets": n_obs, "n_available": len(avail),
           "coverage_pct": round(100.0 * len(avail) / n_obs, 2) if n_obs else None,
           "yes_count": sum(o["y"] for o in avail), "no_count": len(avail) - sum(o["y"] for o in avail),
           "valid_folds": 0, "folds": [], "notes": []}
    if len(avail) < max(th["min_analysis_markets"], MIN_TRAIN_ROWS_PER_FOLD + MIN_TEST_ROWS_PER_FOLD):
        res["notes"].append("n_available below min_analysis_markets: not analysed")
        return res
    oof, gammas, deltas, quint = [], [], [], []
    folds = walk_forward_folds([o["close"] for o in avail])
    for fno, tr_i, te_i in folds:
        train, test = [avail[i] for i in tr_i], [avail[i] for i in te_i]
        fd = {"fold": fno, "train_n": len(train), "test_n": len(test),
              "train_close_max": max(o["close"] for o in train), "test_close_min": min(o["close"] for o in test)}
        res["folds"].append(fd)
        ys = [o["y"] for o in train]
        if len(train) < MIN_TRAIN_ROWS_PER_FOLD or len(test) < MIN_TEST_ROWS_PER_FOLD or sum(ys) in (0, len(ys)):
            fd["skipped"] = "too few rows or single-class training data"; continue
        try:
            scalers = fit_scalers(train, need, by_coin)
        except ValueError as e:
            fd["skipped"] = str(e); continue
        if by_coin:
            ok_coins = {k for k, c in scalers if c == feature}
            dropped = sum(1 for o in train + test if o["coin"] not in ok_coins)
            train = [o for o in train if o["coin"] in ok_coins]
            test = [o for o in test if o["coin"] in ok_coins]
            if dropped:
                fd["dropped_rows_coin_without_scaler"] = dropped
            if len(test) < MIN_TEST_ROWS_PER_FOLD or not train:
                fd["skipped"] = "too few rows after within-coin scaling"; continue
        fd["scalers"] = {f"{k}:{c}": [round(v, 10) for v in s.stats()] for (k, c), s in sorted(scalers.items())}

        def design(o, model):
            return model_design(o, model, controls, feature, reliability, scalers, by_coin)
        Xtr = {m: [design(o, m) for o in train] for m in "ABC"}
        Xte = {m: [design(o, m) for o in test] for m in "ABC"}
        try:
            fits = {m: fit_logistic(Xtr[m], [o["y"] for o in train]) for m in "ABC"}
        except SingularMatrixError as e:
            fd["skipped"] = f"singular fit: {e}"; continue
        preds = {m: predict_logistic(fits[m]["beta"], Xte[m]) for m in "ABC"}
        bC = fits["C"]["beta"]
        g = bC[-2] if reliability else bC[-1]
        gammas.append(g)
        fd["gamma"] = g
        if reliability:
            deltas.append(bC[-1]); fd["delta_interaction"] = bC[-1]
        fd["converged"] = all(fits[m]["converged"] for m in "ABC")
        for o, pa, pb, pc in zip(test, preds["A"], preds["B"], preds["C"]):
            oof.append({"ticker": o["ticker"], "coin": o["coin"], "horizon": horizon, "fold": fno, "y": o["y"],
                        "p_base": o["p"], "pa": pa, "pb": pb, "pc": pc, "fv": o["f"][feature], "fav": o["fav"]})
        if feature in PRIMARY_FEATURES:                   # train-only quintile boundaries (diagnostic)
            bounds = [percentile([o["f"][feature] for o in train], q) for q in (20, 40, 60, 80)]
            fd["quintile_bounds"] = bounds
            for o in test:
                quint.append((bisect.bisect_right(bounds, o["f"][feature]), o))
    res["valid_folds"] = len(gammas)
    res["attempted_folds"] = len(folds)
    if not oof:
        res["notes"].append("no valid folds")
        return res
    y = [r["y"] for r in oof]
    P = {m: [r["p" + m.lower()] for r in oof] for m in "ABC"}
    for m in "ABC":
        res[f"oof_brier_{m.lower()}"] = brier(P[m], y)
        res[f"oof_logloss_{m.lower()}"] = logloss(P[m], y)
        res[f"auc_{m.lower()}"] = auc(P[m], y)
    res["oof_n"] = len(oof)
    res["brier_improvement_vs_a"] = res["oof_brier_a"] - res["oof_brier_c"]
    res["brier_improvement_vs_b"] = res["oof_brier_b"] - res["oof_brier_c"]
    res["logloss_improvement_vs_a"] = res["oof_logloss_a"] - res["oof_logloss_c"]
    res["logloss_improvement_vs_b"] = res["oof_logloss_b"] - res["oof_logloss_c"]
    res["auc_improvement_vs_b"] = (res["auc_c"] - res["auc_b"]) if None not in (res["auc_c"], res["auc_b"]) else None

    def ll1(p, yy):
        pc = clamp_p(p)
        return -(yy * math.log(pc) + (1 - yy) * math.log(1 - pc))
    d_brier = [(r["pb"] - r["y"]) ** 2 - (r["pc"] - r["y"]) ** 2 for r in oof]
    d_ll = [ll1(r["pb"], r["y"]) - ll1(r["pc"], r["y"]) for r in oof]
    boot = cluster_bootstrap([r["ticker"] for r in oof], {"brier": d_brier, "ll": d_ll}, reps,
                             _derived_seed(seed, cohort, group, horizon, feature))
    res["bootstrap_reps"] = reps
    res["brier_ci_low"], res["brier_ci_high"] = ci95(boot["brier"])
    res["logloss_ci_low"], res["logloss_ci_high"] = ci95(boot["ll"])
    res["p_value"] = bootstrap_p_improvement(boot["brier"])
    ss = sign_stats(gammas, len(folds))
    res.update({"gamma_" + k: v for k, v in ss.items()})
    res["coefficient_sign"] = ss["dominant_sign"]
    if reliability:
        ds = sign_stats(deltas, len(folds))
        res.update({"interaction_" + k: v for k, v in ds.items()})
        res["interaction_sign"] = ds["dominant_sign"]
        # predeclared: for the reliability family the stability criterion is the interaction term
        res["sign_consistency_pct"] = ds["sign_consistency_pct"]
    else:
        res["sign_consistency_pct"] = ss["sign_consistency_pct"]
    if feature in PRIMARY_FEATURES:
        res.update(_primary_diagnostics(oof, quint, feature))
    if keep_oof:
        res["_oof"] = oof
    return res


def _primary_diagnostics(oof, quint, feature):
    """Diagnostics only (never used for status): residual correlation vs MODEL B,
    holdout quintiles on TRAIN-derived boundaries, and UP/DOWN symmetry."""
    fv = [r["fv"] for r in oof]
    resid = [r["y"] - r["pb"] for r in oof]
    d = {"residual_pearson": pearson(fv, resid), "residual_spearman": spearman(fv, resid)}
    qs = []
    for k in range(5):
        rows = [o for b, o in quint if b == k]
        qs.append({"quintile": k + 1, "n": len(rows),
                   "mean_feature": mean([o["f"][feature] for o in rows]),
                   "mean_base_p_up": mean([o["p"] for o in rows]),
                   "actual_up_rate": mean([float(o["y"]) for o in rows]),
                   "mean_residual_vs_base": mean([o["y"] - o["p"] for o in rows])})
    d["residual_quintiles"] = qs
    sym = {}
    for side in ("UP", "DOWN"):
        rs = [r for r in oof if r["fav"] == side]
        sym[side] = {"n": len(rs),
                     "brier_improvement_vs_b": (brier([r["pb"] for r in rs], [r["y"] for r in rs]) -
                                                brier([r["pc"] for r in rs], [r["y"] for r in rs])) if rs else None,
                     "residual_pearson": pearson([r["fv"] for r in rs], [r["y"] - r["pb"] for r in rs])}
    u, dn = sym["UP"], sym["DOWN"]
    sym["flag"] = ("DIRECTION_REVERSAL" if (u["n"] >= 30 and dn["n"] >= 30 and u["residual_pearson"] is not None
                   and dn["residual_pearson"] is not None and abs(u["residual_pearson"]) > 0.1
                   and abs(dn["residual_pearson"]) > 0.1 and _sign(u["residual_pearson"]) != _sign(dn["residual_pearson"]))
                   else None)
    d["direction_symmetry"] = sym
    return d


def classify(res, thresholds=None, invariants_ok=True):
    """Machine-readable status + reasons. Never uses outcome data beyond OOF metrics."""
    th = dict(DEFAULT_THRESHOLDS, **(thresholds or {}))
    reasons = []
    n = res.get("n_available", 0)
    if n < th["min_analysis_markets"] or not res.get("valid_folds"):
        return ST_INSUFFICIENT, [f"n_available {n} < {th['min_analysis_markets']} or no valid folds"]
    if res.get("primary_or_secondary") != "primary" or res.get("feature_family") == "EXPLORATORY_FUNDING":
        return ST_EXPLORATORY, ["secondary/exploratory feature: never promoted"]
    bi, li = res.get("brier_improvement_vs_b"), res.get("logloss_improvement_vs_b")
    lo, q, sc = res.get("brier_ci_low"), res.get("q_value"), res.get("sign_consistency_pct")
    metrics_ok = (bi is not None and bi > 0 and li is not None and li > 0 and lo is not None and lo > 0
                  and q is not None and q <= th["q_max"])
    sign_ok = sc is not None and sc >= th["sign_consistency_min_pct"]
    sample_ok = (n >= th["min_candidate_markets"] and res.get("yes_count", 0) >= th["min_class_count"]
                 and res.get("no_count", 0) >= th["min_class_count"] and res["valid_folds"] >= th["min_valid_folds"])
    if not metrics_ok:
        reasons.append("OOF improvement vs MODEL B not established (Brier/logloss/CI/q)")
    if not sign_ok:
        reasons.append(f"sign consistency {sc} < {th['sign_consistency_min_pct']}")
    if not sample_ok:
        reasons.append("below candidate sample thresholds (markets/class counts/folds)")
    if not invariants_ok:
        reasons.append("data-quality invariant violated")
    if metrics_ok and sign_ok and sample_ok and invariants_ok:
        return ST_CANDIDATE, ["all predeclared criteria met (statistical candidate only; changes nothing)"]
    if not sample_ok:
        return ST_EXPLORATORY, reasons
    if not sign_ok and (metrics_ok or (bi is not None and bi > 0)):
        return ST_UNSTABLE, reasons
    return ST_NO_VALUE, reasons


def apply_bh_and_classify(results, thresholds=None, invariants_ok=True):
    """BH-FDR across PRIMARY features within each (cohort, group, horizon); then status."""
    fam = defaultdict(list)
    for r in results:
        if r["primary_or_secondary"] == "primary" and r.get("p_value") is not None:
            fam[(r["cohort"], r["group"], r["horizon_min"])].append(r)
    for rs in fam.values():
        for r, q in zip(rs, bh_qvalues([r["p_value"] for r in rs])):
            r["q_value"] = q
    for r in results:
        r.setdefault("q_value", None)
        r["status"], r["status_reasons"] = classify(r, thresholds, invariants_ok)
    return results


# ═══════════════════════ step3_v2: predeclared interaction family (research only) ═══════════════════════
def interaction_design(o, model, controls, feature, modifier, scalers, by_coin):
    """base: [base_logit, spot controls, feature, modifier, control_i x modifier]
       full: base + [feature x modifier]           (all terms train-only scaled)"""
    zc = [_scaled(o, c, scalers, by_coin) for c in controls]
    zf = _scaled(o, feature, scalers, by_coin)
    zm = _scaled(o, modifier, scalers, by_coin)
    x = [o["base_logit"]] + zc + [zf, zm] + [c * zm for c in zc]
    if model == "full":
        x.append(zf * zm)
    return x


def screen_interaction(obs, feature, modifier, group="ALL", horizon=None, cohort="primary", reps=BOOTSTRAP_REPS,
                       seed=SEED, thresholds=None):
    """Does feature x modifier improve OUT-OF-FOLD fit beyond both main effects and the matched
    spot control x modifier term? Same safeguards as screen_feature: walk-forward folds on whole
    close-time groups, train-only winsorised scaling (within coin for ALL), identical OOF rows for
    both models, ticker-clustered bootstrap. Research only: never a candidate."""
    th = dict(DEFAULT_THRESHOLDS, **(thresholds or {}))
    if (feature, modifier) not in INTERACTIONS:
        raise ValueError(f"({feature}, {modifier}) is not a predeclared interaction")
    assert_allowed_predictor(feature)
    assert_allowed_predictor(modifier)
    controls = spot_controls_for(feature)
    for c in controls:
        assert_allowed_predictor(c)
    need = list(dict.fromkeys(controls + [feature, modifier]))
    by_coin = group == "ALL"
    avail = sorted((o for o in obs if all(_ok(o["f"].get(c)) for c in need) and _ok(o["base_logit"])),
                   key=lambda o: (o["close"], o["ticker"]))
    n_obs = len(obs)
    res = {"cohort": cohort, "group": group, "coin": group, "horizon_min": horizon, "feature_name": feature,
           "modifier_name": modifier, "interaction": f"{feature} x {modifier}", "controls": controls,
           "model_type": "interaction_research_only", "unique_markets": n_obs, "n_available": len(avail),
           "coverage_pct": round(100.0 * len(avail) / n_obs, 2) if n_obs else None,
           "yes_count": sum(o["y"] for o in avail), "no_count": len(avail) - sum(o["y"] for o in avail),
           "valid_folds": 0, "folds": [], "notes": []}
    if len(avail) < max(th["min_analysis_markets"], MIN_TRAIN_ROWS_PER_FOLD + MIN_TEST_ROWS_PER_FOLD):
        res["notes"].append("n_available below min_analysis_markets: not analysed")
        return res
    oof, coefs = [], []
    folds = walk_forward_folds([o["close"] for o in avail])
    for fno, tr_i, te_i in folds:
        train, test = [avail[i] for i in tr_i], [avail[i] for i in te_i]
        fd = {"fold": fno, "train_n": len(train), "test_n": len(test)}
        res["folds"].append(fd)
        ys = [o["y"] for o in train]
        if len(train) < MIN_TRAIN_ROWS_PER_FOLD or len(test) < MIN_TEST_ROWS_PER_FOLD or sum(ys) in (0, len(ys)):
            fd["skipped"] = "too few rows or single-class training data"; continue
        try:
            scalers = fit_scalers(train, need, by_coin)
        except ValueError as e:
            fd["skipped"] = str(e); continue
        if by_coin:
            ok_coins = {k for k, c in scalers if c == feature}
            train = [o for o in train if o["coin"] in ok_coins]
            test = [o for o in test if o["coin"] in ok_coins]
            if len(test) < MIN_TEST_ROWS_PER_FOLD or not train:
                fd["skipped"] = "too few rows after within-coin scaling"; continue
        X = {m: ([interaction_design(o, m, controls, feature, modifier, scalers, by_coin) for o in train],
                 [interaction_design(o, m, controls, feature, modifier, scalers, by_coin) for o in test])
             for m in ("base", "full")}
        try:
            fits = {m: fit_logistic(X[m][0], [o["y"] for o in train]) for m in ("base", "full")}
        except SingularMatrixError as e:
            fd["skipped"] = f"singular fit: {e}"; continue
        pb = predict_logistic(fits["base"]["beta"], X["base"][1])
        pf = predict_logistic(fits["full"]["beta"], X["full"][1])
        coefs.append(fits["full"]["beta"][-1])
        fd["interaction_coef"] = fits["full"]["beta"][-1]
        fd["converged"] = fits["base"]["converged"] and fits["full"]["converged"]
        for o, b_, f_ in zip(test, pb, pf):
            oof.append({"ticker": o["ticker"], "y": o["y"], "pb": b_, "pf": f_})
    res["valid_folds"] = len(coefs)
    res["attempted_folds"] = len(folds)
    if not oof:
        res["notes"].append("no valid folds")
        return res
    y = [r["y"] for r in oof]
    P = {"base": [r["pb"] for r in oof], "full": [r["pf"] for r in oof]}
    res["oof_n"] = len(oof)
    for m in ("base", "full"):
        res[f"oof_brier_{m}"], res[f"oof_logloss_{m}"], res[f"auc_{m}"] = brier(P[m], y), logloss(P[m], y), auc(P[m], y)
    res["brier_improvement"] = res["oof_brier_base"] - res["oof_brier_full"]
    res["logloss_improvement"] = res["oof_logloss_base"] - res["oof_logloss_full"]

    def ll1(p, yy):
        pc = clamp_p(p)
        return -(yy * math.log(pc) + (1 - yy) * math.log(1 - pc))
    d_brier = [(r["pb"] - r["y"]) ** 2 - (r["pf"] - r["y"]) ** 2 for r in oof]
    d_ll = [ll1(r["pb"], r["y"]) - ll1(r["pf"], r["y"]) for r in oof]
    boot = cluster_bootstrap([r["ticker"] for r in oof], {"brier": d_brier, "ll": d_ll}, reps,
                             _derived_seed(seed, "interaction", cohort, group, horizon, feature, modifier))
    res["bootstrap_reps"] = reps
    res["brier_ci_low"], res["brier_ci_high"] = ci95(boot["brier"])
    res["logloss_ci_low"], res["logloss_ci_high"] = ci95(boot["ll"])
    res["p_value"] = bootstrap_p_improvement(boot["brier"])
    ss = sign_stats(coefs, len(folds))
    res.update({"interaction_" + k: v for k, v in ss.items()})
    res["interaction_sign"] = ss["dominant_sign"]
    res["sign_consistency_pct"] = ss["sign_consistency_pct"]
    return res


def classify_interaction(res, thresholds=None):
    """Research status only. The best possible status is IX_EVIDENCE, which is NOT a Step 3
    candidate and is never written to the candidate manifest."""
    th = dict(DEFAULT_THRESHOLDS, **(thresholds or {}))
    n = res.get("n_available", 0)
    if n < th["min_analysis_markets"] or res.get("valid_folds", 0) < th["min_valid_folds"]:
        return IX_INSUFFICIENT, [f"n_available {n} < {th['min_analysis_markets']} or valid folds "
                                 f"{res.get('valid_folds', 0)} < {th['min_valid_folds']}"]
    reasons = []
    bi, li, lo, q = (res.get(k) for k in ("brier_improvement", "logloss_improvement", "brier_ci_low", "q_value"))
    if not (bi is not None and bi > 0 and li is not None and li > 0 and lo is not None and lo > 0
            and q is not None and q <= th["q_max"]):
        reasons.append("OOF improvement over the main-effects model not established (Brier/logloss/CI/q)")
    sc = res.get("sign_consistency_pct")
    if sc is None or sc < th["sign_consistency_min_pct"]:
        reasons.append(f"interaction sign consistency {sc} < {th['sign_consistency_min_pct']}")
    if n < th["min_candidate_markets"] or res.get("yes_count", 0) < th["min_class_count"] \
            or res.get("no_count", 0) < th["min_class_count"]:
        reasons.append("below candidate-level sample thresholds")
    if reasons:
        return IX_NONE, reasons
    return IX_EVIDENCE, ["interaction evidence (research only: not a candidate, changes nothing)"]


def apply_bh_interactions(results, thresholds=None):
    """BH-FDR across the predeclared interaction family within each (cohort, group, horizon)."""
    fam = defaultdict(list)
    for r in results:
        if r.get("p_value") is not None:
            fam[(r["cohort"], r["group"], r["horizon_min"])].append(r)
    for rs in fam.values():
        for r, q in zip(rs, bh_qvalues([r["p_value"] for r in rs])):
            r["q_value"] = q
    for r in results:
        r.setdefault("q_value", None)
        r["status"], r["status_reasons"] = classify_interaction(r, thresholds)
    return results


VOL_REGIME_CATEGORIES = ("LOW", "NORMAL", "HIGH", "EXTREME", "UNKNOWN")      # == perp_telemetry.VOL_REGIMES
STABILITY_CATEGORIES = ("STABLE", "CAUTION", "UNSTABLE", "UNKNOWN")         # == perp_telemetry.STABILITY_STATES


def categorical_regime_description(obs):
    """DESCRIPTIVE ONLY (never used for any status): the existing model's raw metrics within each
    fixed telemetry category. Categories come from predeclared telemetry rules, not from data."""
    out = {}
    for col, cats in (("vol_regime", VOL_REGIME_CATEGORIES), ("perp_stability_state", STABILITY_CATEGORIES)):
        d = {}
        for cat in cats:
            rs = [o for o in obs if ((o.get("cat") or {}).get(col) if (o.get("cat") or {}).get(col) in cats
                                     else "UNKNOWN") == cat]
            p, y = [o["p"] for o in rs], [o["y"] for o in rs]
            d[cat] = {"n": len(rs), "yes_rate": (sum(y) / len(y)) if y else None,
                      "brier": brier(p, y) if y else None, "logloss": logloss(p, y) if y else None,
                      "favored_win_rate": mean([1.0 if o["favored_correct"] else 0.0 for o in rs
                                                if o["favored_correct"] is not None])}
        out[col] = d
    return out


# ═══════════════════════ baseline (existing model) ═══════════════════════
def baseline_metrics(obs):
    y = [o["y"] for o in obs]
    p = [o["p"] for o in obs]
    if not obs:
        return {"unique_markets": 0}
    return {"unique_markets": len(obs), "yes_count": sum(y), "no_count": len(y) - sum(y),
            "yes_rate": sum(y) / len(y), "brier": brier(p, y), "logloss": logloss(p, y), "auc": auc(p, y),
            "ece": ece(p, y), "mean_pred_p_up": mean(p), "actual_up_rate": sum(y) / len(y),
            "favored_win_rate": mean([1.0 if o["favored_correct"] else 0.0 for o in obs
                                      if o["favored_correct"] is not None]),
            "mean_horizon_early_ms": mean([o["horizon_early_ms"] for o in obs if o["horizon_early_ms"] is not None]),
            "calibration": calibration_bins(p, y)}


def regime_description(obs):
    """Optional, descriptive: base-model metrics by spot_rv_300s tercile, terciles fit on
    the initial 40% training block only, reported on the remaining 60%."""
    rows = sorted((o for o in obs if _ok(o["f"].get("spot_rv_300s_bps"))), key=lambda o: (o["close"], o["ticker"]))
    folds = walk_forward_folds([o["close"] for o in rows])
    if len(rows) < 60 or not folds:
        return {"note": "insufficient rows"}
    train = [rows[i] for i in folds[0][1]]
    rest = [o for o in rows if o["close"] > max(t["close"] for t in train)]
    cuts = [percentile([o["f"]["spot_rv_300s_bps"] for o in train], q) for q in (100 / 3, 200 / 3)]
    out = {"tercile_bounds_from_training": cuts}
    for k, name in enumerate(("low", "medium", "high")):
        rs = [o for o in rest if bisect.bisect_right(cuts, o["f"]["spot_rv_300s_bps"]) == k]
        out[name] = {"n": len(rs), "brier": brier([o["p"] for o in rs], [o["y"] for o in rs]) if rs else None,
                     "logloss": logloss([o["p"] for o in rs], [o["y"] for o in rs]) if rs else None}
    return out


# ═══════════════════════ lead / lag (future spot returns) ═══════════════════════
def build_leadlag_anchors(rows, horizon_s, tol=FUTURE_SPOT_TOLERANCE_SECONDS,
                          max_gap=LEADLAG_MAX_INTERNAL_GAP_SECONDS, spacing=None, streams=None):
    """Anchors = analysis_ready observations. Label y = (spot at ~t+H / spot at t - 1) * 1e4,
    using an ACTUAL later observation within tol of t+H, in the same session AND coin, with
    no internal gap > max_gap (nothing is interpolated). Anchors are then greedily thinned
    to be >= spacing (default H) apart. The label is stored as 'y' and never in the features.
    Pass either row dicts (rows) or prebuilt column stores (streams)."""
    spacing = horizon_s if spacing is None else spacing
    if streams is None:
        streams = streams_from_rows(rows)
    anchors = []
    for key in sorted(streams):
        st = streams[key]
        ts, sp, n = st.ts, st.spot, len(st.ts)
        next_gap = [n] * n          # smallest j >= i with ts[j+1]-ts[j] > max_gap (n = none)
        for i in range(n - 2, -1, -1):
            next_gap[i] = i if ts[i + 1] - ts[i] > max_gap else next_gap[i + 1]
        last = None
        for i in range(n):
            if not st.ready[i] or not st.ticker[i]:
                continue
            if last is not None and ts[i] - last < spacing:
                continue
            target = ts[i] + horizon_s
            k = bisect.bisect_left(ts, target)
            best = None
            for j in (k - 1, k):
                if 0 <= j < n and ts[j] > ts[i] and abs(ts[j] - target) <= tol:
                    if best is None or abs(ts[j] - target) < abs(ts[best] - target):
                        best = j
            if best is None or next_gap[i] < best:
                continue
            f = {}
            for c, col in st.feats.items():
                v = col[i]
                f[c] = None if v != v else v
            anchors.append({"ticker": st.ticker[i], "coin": key[1], "session": key[0], "ts": ts[i], "f": f,
                            "y": (sp[best] / sp[i] - 1.0) * 1e4, "label_ts": ts[best]})
            last = ts[i]
    anchors.sort(key=lambda a: (a["ts"], a["coin"], a["session"]))
    return anchors


def screen_leadlag(anchors, feature, horizon_s, group="ALL", reps=BOOTSTRAP_REPS, seed=SEED, thresholds=None):
    th = dict(DEFAULT_THRESHOLDS, **(thresholds or {}))
    assert_allowed_predictor(feature)
    control = LEADLAG_CONTROL[horizon_s]
    assert_allowed_predictor(control)
    need = [control, feature]
    by_coin = group == "ALL"
    avail = [a for a in anchors if all(_ok(a["f"].get(c)) for c in need) and _ok(a["y"])]
    res = {"group": group, "coin": group, "future_horizon_s": horizon_s, "feature_name": feature,
           "control": control, "n_anchors": len(avail), "valid_folds": 0, "folds": []}
    if avail:
        res["pearson"] = pearson([a["f"][feature] for a in avail], [a["y"] for a in avail])
        res["spearman"] = spearman([a["f"][feature] for a in avail], [a["y"] for a in avail])
    if len(avail) < max(th["min_leadlag_anchors"], MIN_TRAIN_ROWS_PER_FOLD + MIN_TEST_ROWS_PER_FOLD):
        return res
    oof, gammas = [], []
    folds = walk_forward_folds([a["ts"] for a in avail])
    for fno, tr_i, te_i in folds:
        test = [avail[i] for i in te_i]
        t0 = min(a["ts"] for a in test)
        train = [avail[i] for i in tr_i if avail[i]["label_ts"] < t0]        # purge label overlap
        fd = {"fold": fno, "train_n": len(train), "test_n": len(test), "purged": len(tr_i) - len(train)}
        res["folds"].append(fd)
        if len(train) < MIN_TRAIN_ROWS_PER_FOLD or len(test) < MIN_TEST_ROWS_PER_FOLD:
            fd["skipped"] = "too few rows"; continue
        try:
            sc = fit_scalers(train, need, by_coin)
        except ValueError as e:
            fd["skipped"] = str(e); continue
        if by_coin:
            ok = {k for k, c in sc if c == feature}
            train = [a for a in train if a["coin"] in ok]
            test = [a for a in test if a["coin"] in ok]
            if len(test) < MIN_TEST_ROWS_PER_FOLD or len(train) < MIN_TRAIN_ROWS_PER_FOLD:
                fd["skipped"] = "too few rows after within-coin scaling"; continue
        # scale each column ONCE per fold (training statistics only)
        ctr = [_scaled(a, control, sc, by_coin) for a in train]
        ftr = [_scaled(a, feature, sc, by_coin) for a in train]
        cte = [_scaled(a, control, sc, by_coin) for a in test]
        fte = [_scaled(a, feature, sc, by_coin) for a in test]
        ytr = [a["y"] for a in train]
        try:
            bb = fit_ols([(c,) for c in ctr], ytr)
            ba = fit_ols(list(zip(ctr, ftr)), ytr)
        except SingularMatrixError as e:
            fd["skipped"] = f"singular: {e}"; continue
        ybar = mean(ytr)
        pb = [bb[0] + bb[1] * c for c in cte]
        pa = [ba[0] + ba[1] * c + ba[2] * f_ for c, f_ in zip(cte, fte)]
        gammas.append(ba[-1]); fd["gamma"] = ba[-1]
        for a, b_, a_ in zip(test, pb, pa):
            oof.append({"ticker": a["ticker"], "y": a["y"], "pb": b_, "pa": a_, "ybar": ybar})
    res["valid_folds"] = len(gammas)
    if not oof:
        return res
    y = [r["y"] for r in oof]
    sst = sum((r["y"] - r["ybar"]) ** 2 for r in oof)
    for m, key in (("base", "pb"), ("aug", "pa")):
        e = [r["y"] - r[key] for r in oof]
        res[f"mse_{m}"] = sum(v * v for v in e) / len(e)
        res[f"mae_{m}"] = sum(abs(v) for v in e) / len(e)
        res[f"r2_oos_{m}"] = (1.0 - sum(v * v for v in e) / sst) if sst > 0 else None
    res["oof_n"] = len(oof)
    res["mse_improvement"] = res["mse_base"] - res["mse_aug"]
    d = [(r["y"] - r["pb"]) ** 2 - (r["y"] - r["pa"]) ** 2 for r in oof]
    boot = cluster_bootstrap([r["ticker"] for r in oof], {"mse": d}, reps,
                             _derived_seed(seed, "leadlag", group, horizon_s, feature))
    res["mse_ci_low"], res["mse_ci_high"] = ci95(boot["mse"])
    res["p_value"] = bootstrap_p_improvement(boot["mse"])
    ss = sign_stats(gammas, len(folds))
    res.update({"gamma_" + k: v for k, v in ss.items()})
    res["sign_consistency_pct"] = ss["sign_consistency_pct"]
    return res


def classify_leadlag(res, thresholds=None):
    th = dict(DEFAULT_THRESHOLDS, **(thresholds or {}))
    if res["n_anchors"] < th["min_leadlag_anchors"] or res["valid_folds"] < th["min_valid_folds"]:
        return LL_INSUFFICIENT
    ok = (res.get("mse_improvement") is not None and res["mse_improvement"] > 0
          and res.get("mse_ci_low") is not None and res["mse_ci_low"] > 0
          and res.get("q_value") is not None and res["q_value"] <= th["q_max"]
          and (res.get("sign_consistency_pct") or 0) >= th["sign_consistency_min_pct"])
    return LL_SIGNAL if ok else LL_NONE


# ═══════════════════════ orchestration + reports ═══════════════════════
SCREEN_CSV_COLUMNS = [
    "group", "coin", "horizon_min", "cohort", "feature_name", "feature_family", "primary_or_secondary",
    "controls", "unique_markets", "n_available", "yes_count", "no_count", "coverage_pct", "valid_folds",
    "model_type", "oof_n", "oof_brier_a", "oof_brier_b", "oof_brier_c", "brier_improvement_vs_a",
    "brier_improvement_vs_b", "oof_logloss_a", "oof_logloss_b", "oof_logloss_c", "logloss_improvement_vs_a",
    "logloss_improvement_vs_b", "auc_a", "auc_b", "auc_c", "brier_ci_low", "brier_ci_high", "logloss_ci_low",
    "logloss_ci_high", "p_value", "q_value", "coefficient_sign", "sign_consistency_pct",
    "gamma_positive_folds", "gamma_negative_folds", "gamma_zero_or_failed_folds",
    "interaction_sign", "interaction_positive_folds", "interaction_negative_folds",
    "interaction_sign_consistency_pct", "residual_pearson", "residual_spearman", "bootstrap_reps", "status"]
LEADLAG_CSV_COLUMNS = ["group", "coin", "future_horizon_s", "feature_name", "control", "n_anchors", "valid_folds",
                       "oof_n", "mse_base", "mse_aug", "mse_improvement", "mse_ci_low", "mse_ci_high",
                       "mae_base", "mae_aug", "r2_oos_base", "r2_oos_aug", "pearson", "spearman", "p_value",
                       "q_value", "gamma_dominant_sign", "sign_consistency_pct", "status"]
INTERACTION_CSV_COLUMNS = ["group", "coin", "horizon_min", "cohort", "feature_name", "modifier_name", "controls",
                           "unique_markets", "n_available", "yes_count", "no_count", "coverage_pct", "valid_folds",
                           "oof_n", "oof_brier_base", "oof_brier_full", "brier_improvement", "oof_logloss_base",
                           "oof_logloss_full", "logloss_improvement", "auc_base", "auc_full", "brier_ci_low",
                           "brier_ci_high", "p_value", "q_value", "interaction_sign",
                           "interaction_positive_folds", "interaction_negative_folds", "sign_consistency_pct",
                           "bootstrap_reps", "status"]


def _group_obs(obs, group):
    return obs if group == "ALL" else [o for o in obs if o["coin"] == group]


def _file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def run_analysis(telemetry, labels_path, outdir="analysis_output", seed=SEED, reps=BOOTSTRAP_REPS,
                 thresholds=None, write_oof=True, quality_report=None, log=print):
    th = dict(DEFAULT_THRESHOLDS, **(thresholds or {}))
    rows, meta = load_telemetry(telemetry)
    nv, examples = meta["causal_violations"]      # every analysis_ready row was checked while streaming
    if nv:
        raise CausalInvariantError(f"{nv} analysis_ready rows violate causal invariants, e.g. {examples}")
    labels, label_status = load_labels(labels_path)
    excluded = ticker_metadata(rows, labels, meta["ticker_coins"], meta["ticker_closes"])
    by_ticker = index_by_ticker(rows)
    tele_tickers = set(meta["ticker_coins"])
    horizon_obs = build_horizon_observations(by_ticker, labels, excluded)
    signal_obs = build_first_signal_cohort(by_ticker, labels, excluded)
    analyzed = {o["ticker"] for h in horizon_obs.values() for o in h} | {o["ticker"] for o in signal_obs}

    baseline, regimes, screens, oof_rows = {}, {}, [], []
    cat_regimes, interactions = {}, []
    for h in HORIZONS_MIN:
        for g in GROUPS:
            obs = _group_obs(horizon_obs[h], g)
            baseline[f"{g}|{h}"] = baseline_metrics(obs)
            regimes[f"{g}|{h}"] = regime_description(obs)
            cat_regimes[f"{g}|{h}"] = categorical_regime_description(obs)
            for feat, mod in INTERACTIONS:
                interactions.append(screen_interaction(obs, feat, mod, group=g, horizon=h, cohort="primary",
                                                       reps=reps, seed=seed, thresholds=th))
            for feat in CANDIDATE_FEATURES:
                r = screen_feature(obs, feat, group=g, horizon=h, cohort="primary",
                                   reps=reps if feat in PRIMARY_FEATURES else min(reps, SECONDARY_BOOTSTRAP_REPS),
                                   seed=seed, thresholds=th, keep_oof=write_oof and feat in PRIMARY_FEATURES)
                if "_oof" in r:
                    for o in r.pop("_oof"):
                        oof_rows.append(dict(o, group=g, feature_name=feat))
                screens.append(r)
    first_signal = {}
    for g in GROUPS:
        obs = _group_obs(signal_obs, g)
        wins = [o for o in obs if o["favored_correct"] is True]
        losses = [o for o in obs if o["favored_correct"] is False]
        desc = {"calls": len(obs), "base_brier": brier([o["p"] for o in obs], [o["y"] for o in obs]) if obs else None,
                "base_logloss": logloss([o["p"] for o in obs], [o["y"] for o in obs]) if obs else None,
                "favored_win_rate": (len(wins) / len(obs)) if obs else None, "features": {}}
        for feat in PRIMARY_FEATURES:
            cov = [o for o in obs if _ok(o["f"].get(feat))]
            desc["features"][feat] = {
                "coverage_pct": round(100.0 * len(cov) / len(obs), 2) if obs else None,
                "mean_among_wins": mean([o["f"][feat] for o in wins if _ok(o["f"].get(feat))]),
                "mean_among_losses": mean([o["f"][feat] for o in losses if _ok(o["f"].get(feat))])}
            screens.append(screen_feature(obs, feat, group=g, horizon="first_signal", cohort="first_signal",
                                          reps=reps, seed=seed, thresholds=th))
        for feat, mod in INTERACTIONS:
            interactions.append(screen_interaction(obs, feat, mod, group=g, horizon="first_signal",
                                                   cohort="first_signal", reps=reps, seed=seed, thresholds=th))
        cat_regimes[f"{g}|first_signal"] = categorical_regime_description(obs)
        first_signal[g] = desc
    apply_bh_and_classify(screens, th, invariants_ok=True)
    apply_bh_interactions(interactions, th)

    leadlag = []
    for H in FUTURE_HORIZONS_S:
        anchors = build_leadlag_anchors(None, H, streams=meta["streams"])
        for g in GROUPS:
            a_g = anchors if g == "ALL" else [a for a in anchors if a["coin"] == g]
            fam = [screen_leadlag(a_g, feat, H, group=g, reps=reps, seed=seed, thresholds=th)
                   for feat in LEADLAG_FEATURES]
            for r, q in zip(fam, bh_qvalues([r.get("p_value") for r in fam])):
                r["q_value"] = q
                r["status"] = classify_leadlag(r, th)
            leadlag += fam

    candidates = [{"cohort": r["cohort"], "coin": r["group"], "horizon": r["horizon_min"],
                   "feature": r["feature_name"], "feature_family": r["feature_family"],
                   "coefficient_sign": r["coefficient_sign"], "interaction_sign": r.get("interaction_sign"),
                   "sample_size": r["n_available"], "oof_brier_improvement_vs_b": r["brier_improvement_vs_b"],
                   "oof_logloss_improvement_vs_b": r["logloss_improvement_vs_b"],
                   "brier_ci95": [r["brier_ci_low"], r["brier_ci_high"]], "q_value": r["q_value"],
                   "sign_consistency_pct": r["sign_consistency_pct"]}
                  for r in screens if r["status"] == ST_CANDIDATE]
    qr = None
    if quality_report:
        try:
            with open(quality_report) as f:
                qr = json.load(f).get("structural")
        except (OSError, ValueError) as e:
            qr = {"error": str(e)}
    report = {
        "title": "STEP 3 OF 6 — PERP PREDICTIVE ANALYSIS",
        "dataset_manifest": {
            "telemetry_path": os.path.abspath(telemetry), "telemetry_size_bytes": os.path.getsize(telemetry),
            "telemetry_sha256": _file_sha256(telemetry), "telemetry_schema_version": EXPECTED_SCHEMA_VERSION,
            "feature_version": EXPECTED_FEATURE_VERSION, "labels_path": os.path.abspath(labels_path),
            "labels_sha256": _file_sha256(labels_path), "rows": meta["rows"],
            "analysis_ready_rows": meta["analysis_ready_rows"],
            "first_telemetry_ts": meta["first_ts_utc"], "last_telemetry_ts": meta["last_ts_utc"],
            "unique_tickers_telemetry": len(tele_tickers), "unique_tickers_labeled": len(tele_tickers & set(labels)),
            "unlabeled_tickers": len(tele_tickers - set(labels)), "unique_tickers_analyzed": len(analyzed),
            "excluded_tickers": excluded, "label_status_counts": label_status,
            "sessions": meta["sessions"], "coins": meta["coins"],
            "analysis_code_version": ANALYSIS_CODE_VERSION, "bootstrap_seed": seed},
        "config": {
            "target_horizons_min": list(HORIZONS_MIN), "horizon_max_early_s": HORIZON_MAX_EARLY_SECONDS,
            "entry_window_min": list(ENTRY_WINDOW_MIN),
            "walk_forward": {"method": "expanding window over whole close-time groups", "bounds": list(WF_BOUNDS)},
            "bootstrap_reps_primary": reps, "bootstrap_reps_secondary": min(reps, SECONDARY_BOOTSTRAP_REPS),
            "bootstrap_p_definition": "(count(delta_boot <= 0) + 1) / (B + 1), delta = Brier_B - Brier_C",
            "thresholds": th, "primary_features": list(PRIMARY_FEATURES),
            "directional_features": list(DIRECTIONAL_FEATURES), "reliability_features": list(RELIABILITY_FEATURES),
            "exploratory_funding": list(EXPLORATORY_FUNDING), "spot_controls": list(SPOT_CONTROL_FEATURES),
            "spot_control_mapping": {f: spot_controls_for(f) for f in CANDIDATE_FEATURES},
            "interactions": {"members": [list(x) for x in INTERACTIONS],
                             "base_model": "A + spot control + feature + modifier + spot control x modifier",
                             "full_model": "base + feature x modifier",
                             "multiple_testing": "BH-FDR across the interaction family per cohort x group x horizon",
                             "status": "research only; never PROMISING_CANDIDATE; never in the candidate manifest"},
            "categorical_descriptive": {"vol_regime": list(VOL_REGIME_CATEGORIES),
                                        "perp_stability_state": list(STABILITY_CATEGORIES),
                                        "use": "descriptive only; never used for any status"},
            "second_premium_control": f"spot_vol_shock_60v300 added when >= {SECOND_CONTROL_MIN_COVERAGE:.0%} available",
            "probability_clamp": PROB_CLAMP, "winsorization_percentiles": list(WINSOR_PCT),
            "logistic_ridge": LOGIT_RIDGE, "pooled_ALL_scaling": "within-coin, training rows only",
            "leadlag": {"future_horizons_s": list(FUTURE_HORIZONS_S), "tolerance_s": FUTURE_SPOT_TOLERANCE_SECONDS,
                        "max_internal_gap_s": LEADLAG_MAX_INTERNAL_GAP_SECONDS, "anchor_spacing": "= horizon",
                        "controls": LEADLAG_CONTROL, "features": list(LEADLAG_FEATURES),
                        "r2_definition": "1 - SSE / sum((y - training-fold mean)^2)"}},
        "quality_report_structural": qr,
        "baseline": baseline, "regime_descriptive": regimes,
        "volatility_regime_descriptive": cat_regimes,
        "interaction_screen": [{k: v for k, v in r.items() if not k.startswith("_")} for r in interactions],
        "feature_screen": [{k: v for k, v in r.items() if not k.startswith("_")} for r in screens],
        "first_signal_cohort": first_signal, "leadlag": leadlag, "candidates": candidates,
    }
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "perp_predictive_report.json"), "w") as f:
        json.dump(dict(report, generated_at_utc=dt.datetime.now(dt.timezone.utc).isoformat()),
                  f, indent=1, sort_keys=True, default=str)
    _write_csv(os.path.join(outdir, "perp_feature_screen.csv"), SCREEN_CSV_COLUMNS, screens)
    _write_csv(os.path.join(outdir, "perp_leadlag_report.csv"), LEADLAG_CSV_COLUMNS, leadlag)
    _write_csv(os.path.join(outdir, "perp_interaction_screen.csv"), INTERACTION_CSV_COLUMNS, interactions)
    with open(os.path.join(outdir, "perp_candidate_manifest.json"), "w") as f:
        json.dump(candidates, f, indent=1, sort_keys=True)
    if write_oof and len(oof_rows) <= 300_000:
        cols = ["group", "ticker", "coin", "horizon", "fold", "y", "p_base", "pa", "pb", "pc", "feature_name", "fv"]
        _write_csv(os.path.join(outdir, "perp_oof_predictions.csv"), cols, oof_rows,
                   rename={"y": "outcome_up", "p_base": "base_p_up", "pa": "model_a_prediction",
                           "pb": "model_b_prediction", "pc": "model_c_prediction", "fv": "feature_value"})
    if log:
        log(render(report))
    return report


def _fmt(v, n=4):
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.{n}f}"
    return str(v)


def _write_csv(path, cols, rows, rename=None):
    rename = rename or {}
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([rename.get(c, c) for c in cols])
        for r in rows:
            w.writerow(["" if r.get(c) is None else (";".join(r[c]) if isinstance(r.get(c), list) else r.get(c))
                        for c in cols])


def render(rep):
    m = rep["dataset_manifest"]
    L = [rep["title"], "", "Offline statistics only. Nothing here changes the bot.", "",
         "DATASET",
         f"  rows {m['rows']}  analysis-ready rows {m['analysis_ready_rows']}",
         f"  labeled unique markets {m['unique_tickers_labeled']}  unlabeled {m['unlabeled_tickers']}  "
         f"analyzed {m['unique_tickers_analyzed']}  excluded (metadata) {len(m['excluded_tickers'])}",
         f"  range {m['first_telemetry_ts']} -> {m['last_telemetry_ts']}",
         f"  coins {','.join(m['coins'])}  sessions {len(m['sessions'])}", "",
         "BASELINE (existing binary probability, raw)",
         f"  {'group':5s} {'h':>3s} {'N':>6s} {'Brier':>8s} {'logloss':>8s} {'AUC':>7s} {'ECE':>7s}"]
    for key, b in rep["baseline"].items():
        g, h = key.split("|")
        L.append(f"  {g:5s} {h:>3s} {b.get('unique_markets', 0):>6d} {_fmt(b.get('brier')):>8s} "
                 f"{_fmt(b.get('logloss')):>8s} {_fmt(b.get('auc'), 3):>7s} {_fmt(b.get('ece')):>7s}")
    L += ["", "PRIMARY FEATURES (C vs B = incremental beyond base + spot control; C vs A shown for context)",
          f"  {'feature':24s} {'grp':4s} {'h':>3s} {'N':>5s} {'cov%':>5s} {'dBrier(B)':>10s} {'dLL(B)':>9s} "
          f"{'dBrier(A)':>10s} {'CI95(Brier,B)':>22s} {'q':>6s} {'sign%':>6s}  status"]
    for r in rep["feature_screen"]:
        if r["primary_or_secondary"] != "primary" or r["cohort"] != "primary":
            continue
        ci = (f"[{_fmt(r.get('brier_ci_low'), 5)},{_fmt(r.get('brier_ci_high'), 5)}]"
              if r.get("brier_ci_low") is not None else "-")
        L.append(f"  {r['feature_name']:24s} {r['group']:4s} {str(r['horizon_min']):>3s} {r['n_available']:>5d} "
                 f"{_fmt(r['coverage_pct'], 1):>5s} {_fmt(r.get('brier_improvement_vs_b'), 5):>10s} "
                 f"{_fmt(r.get('logloss_improvement_vs_b'), 5):>9s} {_fmt(r.get('brier_improvement_vs_a'), 5):>10s} "
                 f"{ci:>22s} {_fmt(r.get('q_value'), 3):>6s} {_fmt(r.get('sign_consistency_pct'), 0):>6s}  {r['status']}")
    L += ["", "FIRST-SIGNAL COHORT (existing bot's first call per market)"]
    for g, d in rep["first_signal_cohort"].items():
        sts = sorted({r["status"] for r in rep["feature_screen"] if r["cohort"] == "first_signal" and r["group"] == g})
        L.append(f"  {g:4s} calls {d['calls']}  base Brier {_fmt(d['base_brier'])}  logloss {_fmt(d['base_logloss'])}"
                 f"  favored win rate {_fmt(d['favored_win_rate'], 3)}  feature statuses {sts}")
    L += ["", "LEAD/LAG (future spot return; spot-only baseline vs + perp feature)"]
    for H in FUTURE_HORIZONS_S:
        rs = [r for r in rep["leadlag"] if r["future_horizon_s"] == H]
        cnt = defaultdict(int)
        for r in rs:
            cnt[r["status"]] += 1
        best = [r for r in rs if r["status"] == LL_SIGNAL]
        L.append(f"  +{H:>3d}s  " + "  ".join(f"{k}={v}" for k, v in sorted(cnt.items()))
                 + ("" if not best else "   signal: " + ", ".join(f"{r['group']}:{r['feature_name']}" for r in best)))
    L += ["", "INTERACTIONS (research only: can never become a candidate; full vs main-effects model)"]
    ix = rep.get("interaction_screen", [])
    for feat, mod in INTERACTIONS:
        rs = [r for r in ix if r["feature_name"] == feat and r["modifier_name"] == mod]
        cnt = defaultdict(int)
        for r in rs:
            cnt[r["status"]] += 1
        hits = [r for r in rs if r["status"] == IX_EVIDENCE]
        L.append(f"  {feat} x {mod}: " + "  ".join(f"{k}={v}" for k, v in sorted(cnt.items()))
                 + ("" if not hits else "   evidence: " + ", ".join(f"{r['cohort']}:{r['group']}:{r['horizon_min']}"
                                                                     for r in hits)))
    L += ["", f"CANDIDATE MANIFEST: {len(rep['candidates'])} PROMISING_CANDIDATE entr"
          f"{'y' if len(rep['candidates']) == 1 else 'ies'}"]
    if not rep["candidates"]:
        L.append("  No feature currently meets the predeclared Step 4 candidate standard.")
    for c in rep["candidates"]:
        L.append(f"  {c['cohort']} {c['coin']} {c['horizon']} {c['feature']} sign {c['coefficient_sign']} "
                 f"N={c['sample_size']} dBrier={_fmt(c['oof_brier_improvement_vs_b'], 5)} q={_fmt(c['q_value'], 3)}")
    return "\n".join(L)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Offline Step 3 perp predictive analysis (no network).")
    ap.add_argument("--telemetry", default="kalshi_perp_telemetry.csv")
    ap.add_argument("--labels", default="kalshi_binary_outcomes.csv")
    ap.add_argument("--outdir", default="analysis_output")
    ap.add_argument("--seed", type=int, default=SEED)
    ap.add_argument("--bootstrap-reps", type=int, default=BOOTSTRAP_REPS)
    ap.add_argument("--quality-report", default=None, help="optional analyze_perp_quality.py --json output")
    ap.add_argument("--no-oof", action="store_true", help="don't write perp_oof_predictions.csv")
    for k, v in DEFAULT_THRESHOLDS.items():
        ap.add_argument("--" + k.replace("_", "-"), type=type(v), default=v)
    a = ap.parse_args(argv)
    th = {k: getattr(a, k) for k in DEFAULT_THRESHOLDS}
    for p in (a.telemetry, a.labels):
        if not os.path.exists(p):
            print(f"no such file: {p}", file=sys.stderr)
            return 2
    try:
        run_analysis(a.telemetry, a.labels, a.outdir, a.seed, a.bootstrap_reps, th,
                     write_oof=not a.no_oof, quality_report=a.quality_report)
    except (SchemaError, CausalInvariantError) as e:
        print(f"ANALYSIS FAILED ({type(e).__name__}): {e}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
