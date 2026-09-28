"""
Predeclared, conservative MINIMUM-SAMPLE and MODEL-COMPLEXITY gates. They are configuration (fingerprinted), never
tuned on results. Every failed requirement is reported by name - a failed gate yields INSUFFICIENT_DATA (or
INSUFFICIENT_DATA_FOR_COMPLEXITY), never a quiet "no result".

    sample_gate(rows, cfg, synthetic)           settled independent GOLD-labelled markets overall / per asset /
                                                per direction / per checkpoint / per volatility slice, positive and
                                                negative outcomes
    complexity_gate(n_train_markets, pos, neg, candidate_features, kept_features, effective_params, cfg)
                                                refuses a model whose effective complexity is too large for the
                                                number of independent training markets / minority outcomes
Rows are counted per MARKET (the independent unit), never per checkpoint row.
"""
from dataclasses import asdict, dataclass

from feature_eval.labels import is_gold


@dataclass(frozen=True)
class SampleGates:
    min_total_markets: int = 400
    min_markets_per_asset: int = 75
    min_markets_per_direction: int = 100        # direction = the side the legacy probability favours at the checkpoint
    min_markets_per_checkpoint: int = 200
    min_markets_per_regime: int = 50
    min_positive_outcomes: int = 100
    min_negative_outcomes: int = 100

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True)
class ComplexityGates:
    min_events_per_parameter: float = 10.0      # min(positives, negatives) of independent TRAIN markets per parameter
    min_train_markets_per_parameter: float = 20.0

    def to_dict(self):
        return asdict(self)


def direction_of(row):
    p = row.get("legacy_p_up")
    if p is None:
        return None
    return "UP" if p >= 0.5 else "DOWN"


def _markets(rows, key=None):
    out = {}
    for r in rows:
        k = key(r) if key else "ALL"
        if k is None:
            continue
        out.setdefault(k, set()).add(r["market_ticker"])
    return {k: len(v) for k, v in out.items()}


def sample_gate(rows, cfg=None, synthetic=False, assets=("BTC", "ETH", "SOL", "XRP"), checkpoints_s=None,
                regime_of=None, regimes=None):
    """regime_of(row) -> slice label (continuous-volatility slice, train cutpoints); regimes: expected labels."""
    cfg = cfg or SampleGates()
    gold = [r for r in rows if r.get("y") is not None and is_gold(r.get("label_source"))]
    failed = []

    def need(name, have, want):
        if have < want:
            failed.append({"requirement": name, "have": have, "need": want})
    total = len({r["market_ticker"] for r in gold})
    need("settled_independent_markets", total, cfg.min_total_markets)
    by_asset = _markets(gold, lambda r: r["asset"])
    for a in assets:
        need(f"markets_asset_{a}", by_asset.get(a, 0), cfg.min_markets_per_asset)
    by_dir = _markets(gold, direction_of)
    for d in ("UP", "DOWN"):
        need(f"markets_direction_{d}", by_dir.get(d, 0), cfg.min_markets_per_direction)
    by_cp = _markets(gold, lambda r: r["checkpoint_s"])
    for c in checkpoints_s or sorted(by_cp):
        need(f"markets_checkpoint_{c}s", by_cp.get(c, 0), cfg.min_markets_per_checkpoint)
    by_reg = {}
    if regime_of is not None:
        by_reg = _markets(gold, regime_of)
        for g in regimes or sorted(by_reg):
            need(f"markets_regime_{g}", by_reg.get(g, 0), cfg.min_markets_per_regime)
    outcome = {}
    for r in gold:
        outcome[r["market_ticker"]] = r["y"]
    pos = sum(1 for v in outcome.values() if v == 1)
    neg = sum(1 for v in outcome.values() if v == 0)
    need("positive_outcomes", pos, cfg.min_positive_outcomes)
    need("negative_outcomes", neg, cfg.min_negative_outcomes)
    excluded = len({r["market_ticker"] for r in rows}) - total
    if synthetic:
        status = "SYNTHETIC_ONLY"
    elif failed:
        status = "INSUFFICIENT_DATA"
    else:
        status = "SUFFICIENT_FOR_RESEARCH"
    return {"status": status, "config": cfg.to_dict(), "failed_requirements": failed,
            "counts": {"gold_markets": total, "markets_excluded_non_gold_label": excluded, "positive": pos,
                       "negative": neg, "by_asset": by_asset, "by_direction": by_dir,
                       "by_checkpoint": {str(k): v for k, v in sorted(by_cp.items())},
                       "by_regime": by_reg},
            "note": "counts are independent settled markets with a gold label (official or verified reconstruction)"}


def complexity_gate(n_train_markets, positives, negatives, candidate_features, kept_features, effective_params,
                    cfg=None):
    cfg = cfg or ComplexityGates()
    minority = min(positives, negatives)
    need_events = cfg.min_events_per_parameter * effective_params
    need_markets = cfg.min_train_markets_per_parameter * effective_params
    reasons = []
    if minority < need_events:
        reasons.append(f"minority outcomes {minority} < {cfg.min_events_per_parameter:g} x {effective_params} parameters")
    if n_train_markets < need_markets:
        reasons.append(f"training markets {n_train_markets} < {cfg.min_train_markets_per_parameter:g} x "
                       f"{effective_params} parameters")
    return {"status": "INSUFFICIENT_DATA_FOR_COMPLEXITY" if reasons else "OK",
            "independent_training_markets": n_train_markets, "positive_outcomes": positives,
            "negative_outcomes": negatives, "candidate_feature_count": candidate_features,
            "post_pruning_feature_count": kept_features, "effective_complexity_parameters": effective_params,
            "reasons": reasons, "config": cfg.to_dict()}


def effective_params(model_name, n_features, n_indicator_cols=0, rounds=None):
    """Predeclared effective complexity per model family (not a fitted quantity)."""
    if model_name.startswith("A_"):
        return 0
    if model_name.startswith("A2_"):
        return 2
    if model_name.startswith(("B_", "C_")):
        return 2 + n_features + n_indicator_cols
    if model_name.startswith("D_"):
        return rounds or 40                  # one effective parameter per shrunken stump (predeclared)
    raise ValueError(model_name)
