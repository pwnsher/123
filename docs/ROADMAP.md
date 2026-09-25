# Roadmap (placeholders — nothing below is implemented)

Step 1 (this baseline) froze the legacy strategy. Every later phase must:

* keep `py run_all_tests.py` green;
* either leave `config/strategy_baseline.json` and `regression/strategy_cases.json` unchanged, or
  rewrite them **deliberately** (`--i-intend-to-change-the-baseline`) with the reason and evidence
  recorded in `docs/BASELINE.md`;
* respect fail-closed, causality (no future data in features), reproducibility and the
  prediction → signal → risk → execution separation;
* show out-of-sample incremental value before any new feature or model reaches the live path.

| # | Phase | Scope placeholder | Status |
|---|---|---|---|
| 1 | Settlement engine improvement | model the real CF Benchmarks settlement (RTI average) instead of Coinbase proxies; settlement-aware labels | **foundation built (Step 2, research only)**: `settlement/`, docs/SETTLEMENT_ENGINE.md; awaiting real captured data to verify the window convention |
| 2 | High-resolution spot pipeline | sub-minute / streaming spot with explicit data-health states; connect to `settlement.SettlementAccumulator` / `SettlementState` (see SETTLEMENT_ENGINE.md) | **built (Step 3, research only)**: `market_data/`, `collect_market_data.py`, docs/HIGH_RESOLUTION_DATA.md; CF RTI + Coinbase + Kraken + Kalshi capture, causal replay, 452 status-masked features; awaiting real captured sessions |
| 3 | Expanded perp telemetry | more of the perp book if the API exposes it; still observation-first | **built (Step 4, research only)**: `perp_data/`, `collect_research_data.py`, docs/PERP_HIGH_RESOLUTION_DATA.md; Binance / Bybit / OKX / Kalshi perps: funding, OI, liquidations, basis, aggressive flow, CVD, book; joint dataset with Step 3; the existing perp veto untouched; awaiting real sessions |
| 4 | Spot/perp microstructure | research features only | **built (Step 5, research only)**: `microstructure/`, docs/MICROSTRUCTURE.md; local incremental books for Coinbase / Kraken / Binance / Bybit / OKX / Kalshi with verified sequence policies, OFI / MLOFI, depth deltas, cancel ESTIMATES, sweeps, REPLENISHMENT_PATTERN, VPIN-style toxicity, post-event impact labels, offline lead-lag, sub-second grids, joint Step 2-5 dataset; **the last feature-building step** |
| 5 | Feature research | offline, walk-forward, pre-declared tests | **next (Step 6)**: feature-FAMILY evaluation and ablation on real captured Step 3-5 sessions (no new indicators); inputs ready (joint datasets with provenance and family registries) |
| 6 | Prediction ensemble | only against the legacy model as baseline | not started |
| 7 | Regime detection | research → validated → gated | not started |
| 8 | Probability calibration | first real live calibration step (today: none) | not started |
| 9 | Entry-window optimisation | evidence-based; today fixed at 2–8 min | not started |
| 10 | EV-based signal engine | replaces the threshold gates only after validation | not started |
| 11 | Walk-forward evaluation | shared framework for all phases | not started |
| 12 | Execution simulator | realistic fills, queue, fees, latency | not started |
| 13 | Risk manager | implements `kalshi_core.interfaces.RiskManager` | not started |
| 14 | Kalshi execution engine | behind `kalshi_core.execution`; LIVE stays unavailable until 17 | not started |
| 15 | Local dashboard | successor to the legacy page | not started |
| 16 | Shadow validation | full pipeline in shadow against real markets | not started |
| 17 | Micro-live validation | smallest possible real exposure, explicit gates and kill switch | not started |
