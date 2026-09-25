"""
market_data — high-resolution market-data and research-feature pipeline (Step 3). RESEARCH ONLY.

    RAW MESSAGES -> NORMALIZATION (sources/) -> EVENT STORE (storage) -> CAUSAL REPLAY (replay)
                 -> FEATURE ENGINE (features/) -> FEATURE DATASET (features/dataset)

Nothing here can create, veto, flip or size a production call: kalshi_dashboard / kalshi_core /
run_local never import this package, and it imports no production strategy or execution module
(enforced by test_stage19.py). CF Benchmarks data reuses the Step-2 settlement parsers/types; the
settlement package itself never imports market_data.

Timestamps: every event keeps the SOURCE event time (event_ts_ms, None when the source gives none)
and the LOCAL receive time (receive_ts_ms, UTC wall clock) separately, plus a monotonic receive
clock and a global arrival counter. The one causal rule: an event is usable at T iff receive_ts_ms <= T.
"""

MARKET_DATA_SCHEMA_VERSION = 1
FEATURE_SET_VERSION = "hires_features_v1"
APP_VERSION = "kalshi-local-step3"
RESEARCH_ONLY = True
