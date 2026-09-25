"""
microstructure — causal ORDER-BOOK and MICROSTRUCTURE telemetry for spot, perpetual futures and the Kalshi
binary market (Step 5). RESEARCH / OBSERVATION ONLY.

    RAW BOOK / TRADE MESSAGES -> MicroEvents (sources/) -> RAW EVENT STORE (<session>/micro/, Step-3 storage)
        -> CAUSAL REPLAY -> LOCAL BOOK RECONSTRUCTION (book.py, reconstruction.py) -> MICRO FEATURES (features/)
        -> JOINT DATASET with Steps 2-4 (dataset.py);   post-event PRICE-IMPACT LABELS (labels.py) kept apart

Reuses Step 3 / Step 4 infrastructure: the event-time model (event_ts / receive_ts / receive_mono / ingest_seq /
LIVE|BACKFILLED), the append-only store, feed runners and health, gap records, replay order and the status
vocabulary. The causal rule is unchanged: an observation (book update, snapshot, resnapshot, trade, gap notice)
is usable at T iff receive_ts_ms <= T.

Nothing here can influence a production call: kalshi_dashboard / kalshi_core / run_local, the existing perp veto
chain, settlement, market_data and perp_data never import microstructure (enforced by test_stage21.py), and
every request is a GET or a read-only websocket subscription.
"""

MICRO_SCHEMA_VERSION = 1
MICRO_FEATURE_SET_VERSION = "micro_features_v1"
APP_VERSION = "kalshi-local-step5"
RESEARCH_ONLY = True
