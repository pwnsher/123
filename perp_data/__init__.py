"""
perp_data — causal, multi-venue PERPETUAL-FUTURES telemetry (Step 4). RESEARCH / OBSERVATION ONLY.

    RAW PERP MESSAGES -> NORMALIZED PerpEvents (sources/) -> RAW EVENT STORE (Step-3 storage, <session>/perp/)
        -> CAUSAL REPLAY (replay.py) -> PERP FEATURE ENGINE (features/) -> JOINT DATASET with Step 3 (dataset.py)

This package is a NEW research dataset. It is not the existing Kalshi-perp research / shadow / promotion /
live-veto chain (perp_telemetry.py ... perp_live.py) and it never feeds it: none of those modules, nor
kalshi_dashboard / kalshi_core / run_local, import perp_data, and perp_data imports none of them
(enforced by test_stage20.py). Nothing here can create, veto, flip, size or delay a production call, and
nothing places orders: every request is a GET or a public websocket subscription.

It reuses Step 3's timestamp model (event_ts / receive_ts / receive_mono / ingest_seq / LIVE|BACKFILLED),
append-only store, feed states, gap records, alignment rule and status vocabulary. The one causal rule is
unchanged: an observation is usable at T iff receive_ts_ms <= T.
"""

PERP_SCHEMA_VERSION = 1
PERP_FEATURE_SET_VERSION = "perp_features_v1"
APP_VERSION = "kalshi-local-step4"
RESEARCH_ONLY = True
