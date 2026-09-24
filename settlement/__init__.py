"""
settlement — settlement-data and settlement-reconstruction layer (Step 2). RESEARCH / OBSERVATION ONLY.

Kalshi's 15-minute crypto markets resolve against CF Benchmarks Real-Time Index (RTI) data, not
against Coinbase spot. This package models that resolution value explicitly, causally and with
provenance, so that later steps can test a settlement-aware predictor. It never creates, vetoes,
flips or sizes a production call: nothing in kalshi_dashboard / kalshi_core imports it, and it
imports neither of them (enforced by test_stage18.py).

Modules
    types          typed observations, markets, official resolutions, samples, states, results
    policy         SettlementWindowPolicy (window/boundary convention) + ReconstructionPolicy
    schemas        expected external schemas, validation and schema fingerprints
    assets         asset -> CF index id mapping, Kalshi ticker/close-time consistency checks
    sources        parsers: Kalshi CF websocket feed, CF websocket/REST, Kalshi markets, perp telemetry (proxy)
    engine         shared causal sampling core (used by both the accumulator and reconstruction)
    quality        measurable quality classification
    accumulator    incremental live accumulator
    reconstruction deterministic, causal historical reconstruction with provenance
    checkpoints    time-to-close checkpoint dataset with FEATURES and LABELS kept apart
    resolution     reconstructed outcome vs official Kalshi outcome
    overlap        live-vs-history observation comparison
    cache          append-safe JSONL store with checksums, for offline replay
    fingerprint    settlement-research fingerprint (separate from the Step-1 strategy baseline)
"""

ENGINE_VERSION = "settlement_engine_v1"
RECONSTRUCTION_VERSION = "reconstruction_v1"
RECORD_SCHEMA_VERSION = 1
