"""
feature_eval — STEP 6: real-data evaluation, feature-FAMILY ablation and model comparison. RESEARCH ONLY.

It answers, with chronological out-of-sample evidence on REAL captured sessions, which of the frozen Step 2-5 inputs
add information beyond the frozen legacy production probability, and whether any improvement survives executable
Kalshi prices and fees. It produces evidence only:
    * it never modifies the production model, thresholds, entry window, stops, sizing, the perp veto or execution;
    * it never promotes a model (no result class APPROVED_FOR_PRODUCTION exists);
    * synthetic data can only test code (every synthetic result is flagged SYNTHETIC_ONLY);
    * with too little real data every question is answered INSUFFICIENT_DATA - never a fake winner.

Layers
    universe      frozen, fingerprinted Step 2-5 feature universe (the ONLY candidate inputs)
    quality       real-session validator (PASS / DEGRADED / REJECT per session and per source/asset)
    coinbase_seq  empirical Coinbase Advanced Trade sequence_num validator (UNVERIFIED_REAL_FEED until proven)
    labels        settlement-label gate (official result / verified reconstruction / SETTLEMENT_UNVERIFIED)
    legacy        the frozen legacy probability, computed by the UNMODIFIED production evaluate()
    dataset       research matrix (features + execution state + labels + weights), immutable cache, fingerprints
    splits        purged chronological TRAIN / DEVELOPMENT / FINAL_HOLDOUT and walk-forward folds
    pruning       training-label-independent structural pruning
    models        legacy, logistic, ridge logistic, boosted stumps (pure Python, deterministic)
    metrics       Brier, log loss, calibration, ECE, ROC / PR AUC, buckets
    calibration   none / Platt / isotonic with predeclared sample gates, strictly chronological
    economics     depth-VWAP executable edge with versioned taker fees (research only, SIMULATED)
    bootstrap     market-clustered resampling
    ablation      predeclared family hypotheses, walk-forward, paired vs legacy, BH-FDR, stability, classes
    ledger        append-only hash-chained research ledger (holdout access recorded permanently)
"""

STEP6_SCHEMA_VERSION = 1
FEATURE_UNIVERSE_VERSION = "feature_universe_v3"     # v3 (Step 6.2): settlement source fingerprint re-baselined
LABEL_VERSION = "labels_v3"                         # v3 (Step 6.2): per-market rule provenance gates gold labels
APP_VERSION = "kalshi-local-step6"
RESEARCH_ONLY = True

# every result class Step 6 may emit (there is deliberately no production-approval class)
RESULT_CLASSES = ("INSUFFICIENT_DATA", "INSUFFICIENT_DATA_FOR_COMPLEXITY", "SYNTHETIC_ONLY", "NO_INCREMENTAL_VALUE",
                  "PROMISING_RESEARCH_ONLY", "UNSTABLE", "DEGRADED")
FEATURE_CLASSES = ("KEEP_CANDIDATE", "NO_INCREMENTAL_VALUE", "REDUNDANT", "INSUFFICIENT_DATA", "UNSTABLE", "UNAVAILABLE")
