"""
FEATURE-FAMILY ABLATION and MODEL COMPARISON on DEVELOPMENT walk-forward folds (research only; FINAL_HOLDOUT is never
read here).

Stage 1  predeclared LAYER hypotheses (baseline = the frozen legacy probability, refit as A2 on each fold):
         H01 +settlement  H02 +spot  H03 +perps  H04 +micro  H05 +settlement+spot  H06 +spot+perps  H07 +spot+micro
         H08 +perps+micro  H09 +all validated layers
Stage 2  leave-one-family-out on the best DEVELOPMENT configuration of stage 1 (lowest DEV out-of-fold log loss; the
         holdout is never consulted)
Stage 3  hierarchical: families inside a layer are tested one by one ONLY when that layer's stage-1 hypothesis is
         PROMISING_RESEARCH_ONLY

Per fold (feature_eval.splits.walk_forward, purged, chronological):
    TRAIN rows only  -> structural pruning (label independent, per family, fingerprinted)
                     -> conservative screening: top-k features by |market-weighted correlation with (y - legacy p)|,
                        k bounded by max_selected_features AND by the complexity budget
                     -> preprocessing fit, model fit (market-normalized weights), ridge lambda by an inner
                        chronological split of the training rows
    TEST block       -> predictions; the paired baselines (A legacy, A2 legacy recalibrated) predict the SAME rows
Each comparison: per-row log-loss / Brier difference, market-level means, market-cluster bootstrap CI, market sign-flip
p-value; Benjamini-Hochberg q-values per stage (analyze_perp_predictive.bh_qvalues); practical-improvement threshold;
stability by fold / asset / direction / checkpoint / volatility slice.

Result classes (never APPROVED_FOR_PRODUCTION): INSUFFICIENT_DATA, INSUFFICIENT_DATA_FOR_COMPLEXITY, SYNTHETIC_ONLY,
NO_INCREMENTAL_VALUE, PROMISING_RESEARCH_ONLY, UNSTABLE, DEGRADED (+ UNAVAILABLE for a hypothesis without inputs).
"""
import math
from dataclasses import asdict, dataclass, field

from feature_eval import bootstrap as bs
from feature_eval import metrics as mt
from feature_eval.experiment import SEED
from feature_eval.gates import ComplexityGates, complexity_gate, direction_of, effective_params
from feature_eval.labels import is_gold
from feature_eval.models import (BoostedStumps, LegacyRecalibrated, LogisticModel, RidgeLogisticModel, market_weights)
from feature_eval.pruning import PruneConfig, structural_prune

LAYER_HYPOTHESES = (
    ("H01", ("STEP2_SETTLEMENT",)),
    ("H02", ("STEP3_SPOT",)),
    ("H03", ("STEP4_PERP",)),
    ("H04", ("STEP5_MICRO",)),
    ("H05", ("STEP2_SETTLEMENT", "STEP3_SPOT")),
    ("H06", ("STEP3_SPOT", "STEP4_PERP")),
    ("H07", ("STEP3_SPOT", "STEP5_MICRO")),
    ("H08", ("STEP4_PERP", "STEP5_MICRO")),
    ("H09", ("STEP2_SETTLEMENT", "STEP3_SPOT", "STEP4_PERP", "STEP5_MICRO")),
)
REGIME_VARIABLES = ("vol.cf.rv.5m", "vol.coinbase.rv.5m")


@dataclass(frozen=True)
class AblationConfig:
    models: tuple = ("B_logistic", "C_ridge_logistic", "D_boosted_stumps")
    primary_model: str = "C_ridge_logistic"
    missing_strategy: str = "indicator"
    max_selected_features: int = 12
    min_ready_rows_for_selection: int = 20
    practical_logloss_improvement: float = 0.002
    practical_brier_improvement: float = 0.0005
    q_threshold: float = 0.10
    bootstrap_reps: int = 1000
    permutation_reps: int = 2000
    min_slice_markets: int = 20
    stable_share: float = 2.0 / 3.0
    boosted_rounds: int = 40
    ridge_inner_fraction: float = 0.25                 # latest share of close groups (market level) validates lambda
    ridge_inner_lookback_ms: int = 1_500_000            # purge of the inner split = lookback + label horizon
    ridge_inner_label_horizon_ms: int = 0
    seed: int = SEED
    prune: PruneConfig = field(default_factory=PruneConfig)
    complexity: ComplexityGates = field(default_factory=ComplexityGates)

    def to_dict(self):
        return asdict(self)


class HoldoutLeak(AssertionError):
    pass


# ---------------------------------------------------------------- helpers
def _num(v):
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float)) and math.isfinite(v):
        return float(v)
    return None


def feature_value(row, j):
    return _num(row["x"][j]) if row["st"][j] == "READY" else None


def _X(rows, idx):
    return [[feature_value(r, j) for j in idx] for r in rows]


def usable_rows(rows):
    """Gold-labelled rows with a legacy probability (paired comparisons need both)."""
    return [r for r in rows if r.get("y") is not None and is_gold(r.get("label_source")) and r.get("legacy_p_up") is not None]


def select_features(train_rows, names, col_index, k, min_ready=20):
    """TRAIN-ONLY conservative screening: rank by |market-weighted corr(feature, y - legacy_p)| on READY rows."""
    if k <= 0:
        return [], {}
    w = market_weights([r["market_ticker"] for r in train_rows])
    res = [r["y"] - r["legacy_p_up"] for r in train_rows]
    score = {}
    for n in names:
        j = col_index[n]
        pts = [(feature_value(r, j), e, wi) for r, e, wi in zip(train_rows, res, w)]
        pts = [(v, e, wi) for v, e, wi in pts if v is not None]
        if len(pts) < min_ready:
            continue
        sw = sum(wi for _, _, wi in pts)
        mx = sum(v * wi for v, _, wi in pts) / sw
        me = sum(e * wi for _, e, wi in pts) / sw
        vx = sum(wi * (v - mx) ** 2 for v, _, wi in pts)
        ve = sum(wi * (e - me) ** 2 for _, e, wi in pts)
        if vx <= 0 or ve <= 0:
            continue
        score[n] = sum(wi * (v - mx) * (e - me) for v, e, wi in pts) / math.sqrt(vx * ve)
    ranked = sorted(score, key=lambda n: (-abs(score[n]), n))
    return sorted(ranked[:k]), {n: score[n] for n in ranked[:k]}


def _budget(train_rows, cfg):
    outcome = {}
    for r in train_rows:
        outcome[r["market_ticker"]] = r["y"]
    pos = sum(1 for v in outcome.values() if v == 1)
    neg = len(outcome) - pos
    b = min(min(pos, neg) / cfg.complexity.min_events_per_parameter,
            len(outcome) / cfg.complexity.min_train_markets_per_parameter)
    return int(math.floor(b)), len(outcome), pos, neg


def make_model(name, cfg, strategy=None):
    s = strategy or cfg.missing_strategy
    if name == "B_logistic":
        return LogisticModel(s)
    if name == "C_ridge_logistic":
        from feature_eval.splits import SplitConfig
        return RidgeLogisticModel(s, inner_fraction=cfg.ridge_inner_fraction, inner_split_cfg=SplitConfig(
            max_causal_lookback_ms=cfg.ridge_inner_lookback_ms, label_horizon_ms=cfg.ridge_inner_label_horizon_ms))
    if name == "D_boosted_stumps":
        return BoostedStumps(rounds=cfg.boosted_rounds)
    raise ValueError(name)


# ---------------------------------------------------------------- one fold
def fit_fold(train_rows, test_rows, families, fam_names, col_index, role_of, model_name, cfg, holdout_markets,
             prune_cache=None, fold_id=None, strategy=None, extra_fn=None):
    """Everything fit on TRAIN rows only. Returns predictions for test rows (+ paired baselines) and the audit trail."""
    tr_m = {r["market_ticker"] for r in train_rows}
    te_m = {r["market_ticker"] for r in test_rows}
    if (tr_m | te_m) & set(holdout_markets):
        raise HoldoutLeak("a FINAL_HOLDOUT market reached a development fold")
    if tr_m & te_m:
        raise HoldoutLeak("a market is in both the training rows and the test block of a fold")
    strategy = strategy or cfg.missing_strategy
    kept, removed, prune_fps = [], {}, {}
    for fam in sorted(families):
        key = (fold_id, fam)
        if prune_cache is not None and key in prune_cache:
            pr = prune_cache[key]
        else:
            pr = structural_prune(train_rows, fam_names.get(fam, []), col_index, role_of, cfg.prune)
            if prune_cache is not None:
                prune_cache[key] = pr
        kept += pr["kept"]
        removed.update(pr["removed"])
        prune_fps[fam] = pr["fingerprint"]
    budget, n_train_m, pos, neg = _budget(train_rows, cfg)
    info = {"train_markets": n_train_m, "test_markets": len(te_m), "train_rows": len(train_rows),
            "test_rows": len(test_rows), "positives": pos, "negatives": neg, "candidate_features": sum(
                len(fam_names.get(f, [])) for f in families), "post_pruning_features": len(kept),
            "pruning_fingerprints": prune_fps, "selection_markets": sorted(tr_m), "model": model_name}
    if model_name == "D_boosted_stumps":
        k = cfg.max_selected_features
        eff = effective_params(model_name, k, rounds=cfg.boosted_rounds)
    else:
        per = 2 if strategy == "indicator" else 1
        k = max(0, min(cfg.max_selected_features, (budget - 2) // per))
        eff = None
    sel, scores = select_features(train_rows, kept, col_index, k, cfg.min_ready_rows_for_selection)
    info["selected_features"] = sel
    info["selection_scores"] = scores
    y = [r["y"] for r in train_rows]
    w = market_weights([r["market_ticker"] for r in train_rows])
    leg_tr = [r["legacy_p_up"] for r in train_rows]
    leg_te = [r["legacy_p_up"] for r in test_rows]
    a2 = LegacyRecalibrated().fit(None, leg_tr, y, w)
    base_a2 = a2.predict(None, leg_te)
    idx = [col_index[n] for n in sel]
    Xtr, Xte = _X(train_rows, idx), _X(test_rows, idx)
    if extra_fn is not None:
        Xtr = [x + extra_fn(r) for x, r in zip(Xtr, train_rows)]
        Xte = [x + extra_fn(r) for x, r in zip(Xte, test_rows)]
    if not sel and extra_fn is None:
        info["complexity"] = complexity_gate(n_train_m, pos, neg, info["candidate_features"], len(kept), 0,
                                             cfg.complexity)
        info["status"] = "NO_FEATURES_AVAILABLE"
        return None, base_a2, info
    if eff is None:
        eff = effective_params(model_name, len(Xtr[0]),
                               n_indicator_cols=(sum(1 for c in range(len(Xtr[0])) if any(x[c] is None for x in Xtr))
                                                 if strategy == "indicator" else 0))
    cg = complexity_gate(n_train_m, pos, neg, info["candidate_features"], len(kept), eff, cfg.complexity)
    info["complexity"] = cg
    if cg["status"] != "OK":
        info["status"] = cg["status"]
        return None, base_a2, info
    m = make_model(model_name, cfg, strategy)
    keep_tr = list(range(len(Xtr)))
    keep_te = list(range(len(Xte)))
    if strategy == "complete_case" and model_name != "D_boosted_stumps":
        keep_tr = [i for i, x in enumerate(Xtr) if all(v is not None for v in x)]
        keep_te = [i for i, x in enumerate(Xte) if all(v is not None for v in x)]
        info["complete_case_train_rows_dropped"] = len(Xtr) - len(keep_tr)
        info["complete_case_test_rows_dropped"] = len(Xte) - len(keep_te)
        if not keep_tr or len({y[i] for i in keep_tr}) < 2:
            info["status"] = "INSUFFICIENT_DATA"
            return None, base_a2, info
        wcc = market_weights([train_rows[i]["market_ticker"] for i in keep_tr])
        Xtr_, y_, l_ = [Xtr[i] for i in keep_tr], [y[i] for i in keep_tr], [leg_tr[i] for i in keep_tr]
    else:
        wcc, Xtr_, y_, l_ = w, Xtr, y, leg_tr
    if model_name == "C_ridge_logistic":
        meta = [train_rows[i] for i in keep_tr]                   # market metadata for the market-level inner split
        m.fit(Xtr_, l_, y_, wcc, meta=meta, scaler_X=Xtr_, scaler_w=wcc)
        info["ridge_lambda"] = m.l2
        info["ridge_inner_scores"] = {str(k_): v for k_, v in m.inner_scores.items()}
        info["ridge_inner"] = m.inner_info
    elif model_name == "B_logistic":
        m.fit(Xtr_, l_, y_, wcc, scaler_X=Xtr_, scaler_w=wcc)
    else:
        m.fit(Xtr_, l_, y_, wcc)
    pred = [None] * len(Xte)
    pk = m.predict([Xte[i] for i in keep_te], [leg_te[i] for i in keep_te])
    for i, p in zip(keep_te, pk):
        pred[i] = p
    info["status"] = "OK"
    info["n_params_fitted"] = getattr(m, "n_params", None)
    info["_model"] = m
    return pred, base_a2, info


# ---------------------------------------------------------------- comparison
def paired(test_rows, pred, base, cfg, fold_of=None, regime_of=None):
    """Paired comparison on IDENTICAL rows (rows where both predictions exist)."""
    idx = [i for i, (p, b) in enumerate(zip(pred, base)) if p is not None and b is not None]
    rows = [test_rows[i] for i in idx]
    p = [pred[i] for i in idx]
    b = [base[i] for i in idx]
    y = [r["y"] for r in rows]
    mk = [r["market_ticker"] for r in rows]
    if not rows:
        return {"status": "NO_PAIRED_ROWS"}
    w = market_weights(mk)

    def ll(pi, yi):
        pi = mt.clip(pi)
        return -(yi * math.log(pi) + (1 - yi) * math.log(1 - pi))
    d_ll = [ll(pi, yi) - ll(bi, yi) for pi, bi, yi in zip(p, b, y)]            # < 0 = model better
    d_br = [(pi - yi) ** 2 - (bi - yi) ** 2 for pi, bi, yi in zip(p, b, y)]
    mdel = bs.market_means(mk, d_ll)
    sw = sum(w)

    def mean_delta(ix):
        s = sum(w[i] for i in ix)
        return sum(d_ll[i] * w[i] for i in ix) / s if s else None
    boot = bs.cluster_bootstrap(mk, mean_delta, reps=cfg.bootstrap_reps, seed=cfg.seed)
    bbr = bs.cluster_bootstrap(mk, lambda ix: (sum(d_br[i] * w[i] for i in ix) / sum(w[i] for i in ix)) if ix else None,
                               reps=cfg.bootstrap_reps, seed=cfg.seed + 1)
    pval = bs.sign_flip_pvalue(mdel, reps=cfg.permutation_reps, seed=cfg.seed + 2)
    out = {"status": "OK", "n_rows": len(rows), "n_markets": len(set(mk)),
           "model": mt.summary(p, y, w), "baseline": mt.summary(b, y, w),
           "delta_log_loss": sum(d * wi for d, wi in zip(d_ll, w)) / sw,
           "delta_brier": sum(d * wi for d, wi in zip(d_br, w)) / sw,
           "delta_log_loss_ci": [boot["ci_low"], boot["ci_high"]], "delta_brier_ci": [bbr["ci_low"], bbr["ci_high"]],
           "log_loss_improvement": -sum(d * wi for d, wi in zip(d_ll, w)) / sw,
           "p_value_sign_flip": pval, "bootstrap_unit": boot["unit"], "bootstrap_reps": boot["reps"]}
    out["stability"] = stability(rows, d_ll, w, cfg, fold_of, regime_of)
    return out


def stability(rows, d_ll, w, cfg, fold_of=None, regime_of=None):
    dims = {"asset": lambda r: r["asset"], "direction": direction_of, "checkpoint": lambda r: r["checkpoint_s"]}
    if fold_of is not None:
        dims = {"fold": lambda r: fold_of.get(r["market_ticker"]), **dims}
    if regime_of is not None:
        dims["volatility_slice"] = regime_of
    out, unstable = {}, []
    for dim, fn in dims.items():
        sl = {}
        for r, d, wi in zip(rows, d_ll, w):
            k = fn(r)
            if k is None:
                continue
            e = sl.setdefault(k, [0.0, 0.0, set()])
            e[0] += d * wi
            e[1] += wi
            e[2].add(r["market_ticker"])
        rep = {str(k): {"log_loss_improvement": -v[0] / v[1] if v[1] else None, "markets": len(v[2]),
                        "evaluable": len(v[2]) >= cfg.min_slice_markets} for k, v in sorted(sl.items(), key=lambda t: str(t[0]))}
        ev = [v for v in rep.values() if v["evaluable"]]
        share = (sum(1 for v in ev if v["log_loss_improvement"] > 0) / len(ev)) if ev else None
        flag = bool(ev) and len(ev) >= 2 and share < cfg.stable_share
        if flag:
            unstable.append(dim)
        out[dim] = {"slices": rep, "evaluable_slices": len(ev), "share_improved": share, "unstable": flag}
    return {"by": out, "unstable_dimensions": unstable, "UNSTABLE": bool(unstable)}


def classify(cmp, cfg, q, synthetic, degraded):
    if cmp.get("status") != "OK":
        return cmp.get("status", "INSUFFICIENT_DATA")
    sig = (cmp["delta_log_loss_ci"][1] is not None and cmp["delta_log_loss_ci"][1] < 0 and q is not None
           and q <= cfg.q_threshold)
    practical = cmp["log_loss_improvement"] >= cfg.practical_logloss_improvement
    if not (sig and practical):
        cls = "NO_INCREMENTAL_VALUE"
    elif cmp["stability"]["UNSTABLE"]:
        cls = "UNSTABLE"
    elif degraded:
        cls = "DEGRADED"
    else:
        cls = "PROMISING_RESEARCH_ONLY"
    return cls


# ---------------------------------------------------------------- runner
class Study:
    """Holds the DEVELOPMENT data of one research matrix and runs predeclared hypotheses on the walk-forward folds."""

    def __init__(self, matrix, records, parts, folds, cfg=None, coinbase_allowed=False):
        self.cfg = cfg or AblationConfig()
        self.m = matrix
        self.col_index = matrix.col_index
        self.parts = parts
        self.folds = folds
        self.holdout = set(parts["FINAL_HOLDOUT"])
        rows = usable_rows(matrix.rows)
        self.excluded_rows = len(matrix.rows) - len(rows)
        self.rows = [r for r in rows if r["market_ticker"] not in self.holdout]
        self.by_market = {}
        for r in self.rows:
            self.by_market.setdefault(r["market_ticker"], []).append(r)
        self.role_of = {r["name"]: r["role"] for r in records}
        self.rec = {r["name"]: r for r in records if r["name"] in self.col_index}
        gated = set()
        if not coinbase_allowed:
            gated = {n for n, r in self.rec.items() if "COINBASE_L2_SEQUENCE" in r.get("tags", [])}
        self.coinbase_gated = sorted(gated)
        self.fam_names, self.fam_layer = {}, {}
        for n, r in self.rec.items():
            if r["role"] != "MODEL_CANDIDATE" or n in gated:
                continue
            self.fam_names.setdefault(r["family"], []).append(n)
            self.fam_layer[r["family"]] = r["layer"]
        for f in self.fam_names:
            self.fam_names[f].sort()
        self.prune_cache = {}
        self.fold_of = {m: f["fold"] for f in folds for m in f["test"]}
        self.degraded = any(v.get("verdict") == "DEGRADED" for v in matrix.meta.get("quality_reports", {}).values())
        self.synthetic = matrix.synthetic_only
        self.regime_of, self.regime_info = self._regimes()

    def _regimes(self):
        train = set(self.parts["TRAIN"])
        for var in REGIME_VARIABLES:
            if var not in self.col_index:
                continue
            j = self.col_index[var]
            vals = sorted(v for r in self.rows if r["market_ticker"] in train for v in [feature_value(r, j)] if v is not None)
            if len(vals) < 30:
                continue
            c1, c2 = vals[len(vals) // 3], vals[(2 * len(vals)) // 3]

            def f(r, j=j, c1=c1, c2=c2):
                v = feature_value(r, j)
                if v is None:
                    return None
                return "VOL_LOW" if v <= c1 else ("VOL_MID" if v <= c2 else "VOL_HIGH")
            return f, {"variable": var, "cutpoints_from": "TRAIN partition terciles", "cutpoints": [c1, c2],
                       "note": "no pre-existing regime variable in the research rows; continuous volatility slices"}
        return None, {"variable": None, "note": "no volatility variable available - regime slices not evaluated"}

    def families_of_layers(self, layers):
        return sorted(f for f, lay in self.fam_layer.items() if lay in layers)

    def fold_rows(self, fold):
        tr = [r for m in fold["train"] for r in self.by_market.get(m, [])]
        te = [r for m in fold["test"] for r in self.by_market.get(m, [])]
        return tr, te

    def run(self, families, model_name, strategy=None, extra_fn=None, base="A2"):
        """-> out-of-fold predictions over every DEV test block + paired baselines + fold audit."""
        oof_rows, pred, base_a2, audits = [], [], [], []
        for f in self.folds:
            tr, te = self.fold_rows(f)
            if not tr or not te or len({r["y"] for r in tr}) < 2:
                audits.append({"fold": f["fold"], "status": "INSUFFICIENT_DATA"})
                continue
            p, b, info = fit_fold(tr, te, families, self.fam_names, self.col_index, self.role_of, model_name, self.cfg,
                                  self.holdout, self.prune_cache, f["fold"], strategy, extra_fn)
            info.pop("_model", None)
            info["fold"] = f["fold"]
            audits.append(info)
            oof_rows += te
            pred += p if p is not None else [None] * len(te)
            base_a2 += b
        return oof_rows, pred, base_a2, audits

    def evaluate(self, hid, families, model_name, strategy=None, extra_fn=None):
        rows, pred, a2, audits = self.run(families, model_name, strategy, extra_fn)
        statuses = {a.get("status") for a in audits}
        res = {"hypothesis": hid, "families": list(families), "model": model_name, "folds": audits}
        if not families or all(not self.fam_names.get(f) for f in families):
            res["comparison_vs_A2"] = {"status": "UNAVAILABLE"}
            return res, rows, pred, a2
        if all(p is None for p in pred):
            st = "INSUFFICIENT_DATA_FOR_COMPLEXITY" if "INSUFFICIENT_DATA_FOR_COMPLEXITY" in statuses else (
                "UNAVAILABLE" if statuses <= {"NO_FEATURES_AVAILABLE"} else "INSUFFICIENT_DATA")
            res["comparison_vs_A2"] = {"status": st}
            return res, rows, pred, a2
        leg = [r["legacy_p_up"] for r in rows]
        res["comparison_vs_A2"] = paired(rows, pred, a2, self.cfg, self.fold_of, self.regime_of)
        res["comparison_vs_A"] = paired(rows, pred, leg, self.cfg, self.fold_of, self.regime_of)
        res["legacy_A2_vs_A"] = paired(rows, a2, leg, self.cfg)
        return res, rows, pred, a2


def apply_bh(results, cfg, synthetic, degraded):
    from analyze_perp_predictive import bh_qvalues
    ps = [r["comparison_vs_A2"].get("p_value_sign_flip") if r["comparison_vs_A2"].get("status") == "OK" else None
          for r in results]
    qs = bh_qvalues(ps)
    for r, q in zip(results, qs):
        r["q_value_bh"] = q
        r["result_class"] = classify(r["comparison_vs_A2"], cfg, q, synthetic, degraded)
        r["synthetic_only"] = bool(synthetic)
    return results
