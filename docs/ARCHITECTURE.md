# Architecture (as of the Step 1 baseline)

This describes the code that exists. Where the docs and the code disagree, the code wins. Files are
in the repository root unless a folder is given.

## 1. What the system is

A **decision-support / paper-trading watcher** for Kalshi's 15-minute crypto up/down binary markets
(BTC, ETH, SOL, XRP). Every few seconds it estimates the probability that each coin finishes above
the market's strike. When confidence, price and edge all pass fixed gates inside the 2–8
minutes-left window, it makes a **call**: it logs a paper trade and, optionally, posts it to Discord.
**It places no orders.** Paper trades are settled against Kalshi's official result.

A separate research track (the "perp steps" 1–6) records Kalshi perpetual-futures data and tests,
offline, whether it adds predictive value. It can only ever **suppress** an existing call, through a
manually promoted, inactive-by-default live veto.

## 2. Component map

| Layer (future name) | Where it lives today | Notes |
|---|---|---|
| DATA | `kalshi_dashboard.py`: `current_market`, `book_of`, `strike_of`, `_cents`, `spot`, `candles`, `market_result`, `_get`; `http_session.py` | REST polling only (no websockets). Kalshi public API + Coinbase Exchange public API, no keys. |
| FEATURES | inside `evaluate()`: sigma from 1-min log returns, strike crossings (`crossings`), minutes left | Display-only indicators (`stoch_rsi`, `bollinger_pos`, `atr`) do not affect the decision. |
| MODEL | inside `evaluate()` + `norm_cdf` / `conf_side` | Log-normal strike model: `P(UP) = Φ(ln(S/K) / (σ·√minutes_left))`. |
| CALIBRATION | **none in the live path** | Only measured offline (`kalshi_backtest.py` Brier/log-loss/ECE; `run_backtest` bands). Step 3 research fits a recalibrated "model A", which is never used live. |
| SIGNAL | inside `evaluate()` (gates → `reason`, `signal`) + `poller()` (first signal per ticker, live veto) | Standardised by `kalshi_core.adapter` → `SignalDecision`. |
| RISK | informational sizing only: `_size_fraction`, `_contracts` (quarter-Kelly, 5% cap) | No risk manager. Sizes are shown and paper-logged, never enforced. |
| EXECUTION | **none**. Paper journal: `log_call`, `_append_paper`, `settle_calls` | `kalshi_core/execution.py` is the fenced future boundary (LIVE unavailable). `kalshi_api_learn.py` is a separate, demo-locked, DRY_RUN learning client that no core module imports. |
| STORAGE | JSON/CSV files in the working directory (see §7) | No database. |
| DASHBOARD | `kalshi_dashboard.py`: `PAGE`, `Handler` (stdlib `ThreadingHTTPServer` on 127.0.0.1:8000) | Also exposes web controls (pause, coin/direction filters, entry mode, bankroll). |
| NOTIFICATION (legacy) | `kalshi_bot.py` (Discord), `send_alert` (Telegram, off: empty literal token), `post_discord`/`post_embed` (dead webhook helpers) | Optional; see §8. |

## 3. Entry points

| Command | What starts |
|---|---|
| `py run_local.py` | **Recommended.** Same threads as `kalshi_dashboard.main()` plus structured logging, a SignalDecision observer and graceful shutdown. No Discord. |
| `py run_local.py --check` | Offline self-check: config, secret presence, legacy + extended fingerprints. |
| `py kalshi_dashboard.py` | Legacy: live-veto banner, `poller`, `backtest_worker`, `weekly_worker`, web server. No Discord. |
| `py kalshi_dashboard.py --discover` | Lists Kalshi crypto series. |
| `py kalshi_bot.py` | Legacy optional Discord adapter: the same threads started from `on_ready`, plus slash commands and channel posts. |
| `py kalshi_backtest.py --days N` | Standalone backtest + calibration report (network: Coinbase candles). |
| Research tools | `label_binary_outcomes.py`, `analyze_perp_quality.py`, `analyze_perp_predictive.py`, `build_perp_shadow_policy.py`, `analyze_perp_shadow.py`, `build_perp_integration_experiment.py`, `analyze_perp_integration.py`, `promote_perp_integration.py`, `check_perp_deployment.py` |
| Integrity | `py strategy_fingerprint.py --verify step5_baseline_manifest.json`, `py -m kalshi_core.baseline --verify`, `py -m regression.generate --check` |
| Research data capture (Step 3) | `py collect_market_data.py [--dry-run]` (read-only collector + status page on 127.0.0.1:8766), `scripts/replay_market_data.py`, `scripts/build_market_features.py`, `scripts/bench_market_data.py`, `scripts/mutation_test_market_data.py`, `py -m market_data.fingerprint --verify` |
| Perp research data (Step 4) | `py collect_research_data.py [--dry-run]` (Step 3 + Step 4 read-only, one session), `scripts/replay_perp_data.py`, `scripts/build_research_dataset.py`, `scripts/bench_perp_data.py`, `scripts/mutation_test_perp_data.py`, `py -m perp_data.fingerprint --verify` |
| Tests | `py run_all_tests.py` (stages 1–20, each in its own process) |

## 4. Prediction and signal path (per coin, every `POLL_SECONDS` = 4 s)

```
poller()                                         kalshi_dashboard.py
 └─ evaluate(coin, cfg)
     ├─ current_market(series)   Kalshi GET /markets?series_ticker=…&status=open → nearest future close
     ├─ strike_of(m)             floor_strike | cap_strike | strike
     ├─ book_of(m)               yes/no bid/ask in cents; missing ask derived as 100 − opposite bid
     ├─ spot(product)            Coinbase GET /products/{p}/ticker   (spot_observed_ts recorded)
     ├─ candles(product)         Coinbase GET /products/{p}/candles?granularity=60 (cached 30 s)
     ├─ data guard               < 20 usable completed closes, bad strike/spot, σ ≤ 0 → status "stale"
     ├─ σ = stdev(1-min log returns of newest 61 COMPLETED closes) × VOL_MULT (1.15)
     ├─ z = ln(spot/strike) / (σ·√max(minutes_left, 1e-4));  P(UP) = Φ(z)
     ├─ side = UP if P(UP) ≥ 50 % else DOWN;  conf = max(P(UP), 100 − P(UP))
     ├─ raw_edge = conf − side_ask;  net_edge = raw_edge − ENTRY_COST_CENTS (2)
     ├─ gates (first failure = reason): coin enabled → direction allowed → too early (> 8 min)
     │     → too late (< 2 min) → chop (> 3 strike crossings in 15 min) → conf < 80
     │     → side_ask < 85 → net_edge ≤ 0 or < EDGE_THRESH → ENTER
     └─ rec_stop = round(side_ask × sl_pct)   (BTC/SOL 0.75, ETH 0.60, XRP 0.65)
 ├─ if signal and ticker not yet alerted:
 │     _gate_decision()  → live veto (inactive by default) may return BLOCK → call suppressed
 │     ON_CALL hook (Discord bot or run_local log) → log_call() → _append_paper() → _alerted
 ├─ _perp_submit()       hands copies to the perp telemetry worker (observation only)
 ├─ settle_pending() / settle_calls()   paper settlement against Kalshi market.result
 └─ STATE (served by /data)
```

`kalshi_core.adapter.from_evaluation(r)` turns each result into a `SignalDecision` without changing
anything (§9). The live path has no calibration step, so `calibrated_probability_up` stays `None`.

## 5. Perp-overlay path (research → optional suppress-only veto)

```
Step 1/2  perp_telemetry.py   PerpSampler polls GET /margin/markets (+ funding estimate) into a bounded store;
                              each binary row joins the newest snapshot received AT OR BEFORE its spot
                              observation (≤ 8 s old) → kalshi_perp_telemetry.csv (schema 3). Observation only.
Step 3    label_binary_outcomes.py → kalshi_binary_outcomes.csv (Kalshi market.result)
          analyze_perp_predictive.py → nested logistic models A/B/C, walk-forward, BH-FDR → candidate manifest
Step 4    build_perp_shadow_policy.py → perp_shadow_policies.json (frozen, SHADOW_ONLY)
          live: perp_shadow.py scores first signals AFTER the call → kalshi_perp_shadow.csv (never blocks)
          analyze_perp_shadow.py → prospective validation status
Step 5    build_perp_integration_experiment.py / analyze_perp_integration.py (perp_probability.py overlay maths:
          p = clip(p_base + α·clip(P_C − P_B, ±0.05)), α ∈ {0.25, 0.5, 0.75, 1.0})
Step 6    promote_perp_integration.py → perp_live_promotion.json (manual, expiring)
          perp_live.LiveVetoGate: active ONLY with a valid promotion AND PERP_LIVE_VETO_ENABLED=1 AND an
          unchanged legacy fingerprint; kill file; circuit breaker; per-call failures fail OPEN (call proceeds).
```

Today there is no validated candidate, so the gate is `INACTIVE_NO_PROMOTION` and the legacy behaviour
is unchanged. This is pinned by fixture `V02`/`V03` and stage 12.

**CF Benchmarks:** there is no direct CF Benchmarks integration. The only CF-derived value is the perp
`reference_price` (Kalshi's CF Benchmarks index, scaled per perp contract), recorded as telemetry
`index_price`. Binary settlement uses Kalshi's own `market.result`. The backtest's strike and
settlement use Coinbase 1-minute closes as a **proxy**.

## 6. Evaluation path

* **Live paper record:** `settle_calls()`. A taker call assumes a fill at the ask. A maker call
  fills only if the ask later trades down to the limit; unfilled makers are non-trades. The stop
  triggers when the lowest observed **bid** ≤ stop, and exits at `min(bid_lo, stop)`. Otherwise the
  call settles at 100/0. Round-trip Kalshi fee `ceil(0.07·C·P·(1−P))` is charged per side.
  `settle_pending()` keeps a simpler W/L tally for the stats panel.
* **Backtests** (`kalshi_backtest.py` and `kalshi_dashboard.run_backtest`): these replay Coinbase
  1-minute closes. Entry is the first minute in the window with conf ≥ 80, **priced at the model
  confidence**, not a real ask. `MIN_PRICE`, edge and chop gates are **not** applied. The stop is
  model-priced. The report gives calibration (Brier, log loss, ECE on the newest 30 %) plus
  idealised expectancy.
* **Walk-forward / shadow validation:** only in the perp research tools (Step 3 chronological
  walk-forward on close-time groups; Step 4/5 prospective, frozen-threshold evaluation).
* **Regression contract (new):** `regression/` replays the unmodified functions on 47 fixed
  scenarios with a frozen clock and no network (`test_stage16.py`).

## 7. Storage (all local files, working directory)

| File | Writer | Content |
|---|---|---|
| `kalshi_calls.json`, `kalshi_trades.csv` | `settle_calls` | settled paper calls |
| `kalshi_paper_orders.csv` | `_append_paper` | the order + stop each call would place |
| `kalshi_results.json` | `settle_pending` | simple W/L stats |
| `kalshi_backtest_cache.json` | `backtest_worker` | last built-in backtest (12 h refresh) |
| `kalshi_perp_telemetry.csv`, `kalshi_perp_shadow.csv`, `kalshi_perp_live_veto.csv` | perp modules | research/audit journals |
| `.watcher_state`, `.explain_posted`, `.weekly_posted` | dashboard | small state markers |
| `perp_shadow_policies.json`, `perp_live_promotion.json`, `analysis_output/` | research tools | frozen artifacts / reports |
| `decisions.jsonl` (opt-in) | `run_local.py --journal` | one `SignalDecision` per coin per cycle |

## 8. Discord (legacy, optional)

* `kalshi_bot.py` is the **only** module that imports `discord` (plus the `check_channel.py`
  diagnostic, which imports the bot). The core (`kalshi_dashboard`, strategy, perp, research,
  `kalshi_core`, `run_local`) imports cleanly with `discord` blocked; stage 17 test 5 enforces this.
* The coupling is two optional callbacks the core already exposed: `kalshi_dashboard.ON_CALL` and
  `kalshi_dashboard.POST_EMBED`. With neither set, the watcher behaves identically and makes no
  posts. `run_local.py` points both hooks at the local log instead.
* Without `discord.py`, `kalshi_bot.py` exits with a one-line explanation. Without a token (or with
  the shipped placeholder), it refuses to log in. Channel/guild IDs can be overridden by environment
  variables, with the same defaults.
* Telegram (`send_alert`) and the webhook helpers are left untouched but inert: empty token literal,
  no callers.

## 9. New Step 1 layer (`kalshi_core/`, `regression/`, `run_local.py`)

| Module | Role | May import |
|---|---|---|
| `kalshi_core/no_call.py` | `NoCallReason` enum mapped 1:1 from the legacy reason/status codes | stdlib |
| `kalshi_core/data_health.py` | `FeedStatus`, `FeedHealth`, mapping from legacy statuses (descriptive only) | stdlib |
| `kalshi_core/signal.py` | `SignalDecision` (alias `TradeCandidate`), exact JSON round-trip | stdlib |
| `kalshi_core/adapter.py` | legacy `evaluate()` dict → `SignalDecision`; fails closed | core only |
| `kalshi_core/interfaces.py` | `Protocol`s for DATA…STORAGE; `MarketSnapshot`, `ProbabilityEstimate`, `RiskDecision` | core only |
| `kalshi_core/execution.py` | execution safety boundary: `get_execution_engine(LIVE)` always raises | core only, **no network** |
| `kalshi_core/baseline.py` | `config/strategy_baseline.json` + extended fingerprint (AST, never imports the strategy) | `strategy_fingerprint` |
| `kalshi_core/config.py` | infra env config, secret names (presence only), `.env` loader | stdlib |
| `kalshi_core/logging_setup.py` | key=value / JSON logging with secret redaction | stdlib |
| `regression/` | frozen-clock, no-network harness + stored expectations | legacy modules (read-only use) |
| `run_local.py` | local entry point | core + `kalshi_dashboard` |

Dependency rules (enforced in stage 17): prediction/signal/evaluation code never imports
`kalshi_core.execution` or `kalshi_api_learn`, and no file other than `kalshi_api_learn.py`
references an order endpoint.

## 10. Settlement research layer (Step 2, observation only)

`settlement/` models what Kalshi resolves against: the 60-s average of the CF Benchmarks RTI before
close. It has an explicit window convention, a live incremental accumulator, causal historical
reconstruction, checkpoint datasets with features and labels kept apart, resolution verification
against Kalshi's `result` / `expiration_value`, a live-vs-history overlap comparison, and an
append-only store for offline replay. Scripts are in `scripts/`; see `docs/SETTLEMENT_ENGINE.md`.

It is not wired into the watcher. Production code never imports it, and it imports no production,
network, Discord or execution module (stage 18). Its read-only `SettlementState` is the intended
Step 3+ input; in Step 2 it is not allowed to influence any call.

## 11. High-resolution market data + research features (Step 3, observation only)

`market_data/` captures, normalizes, stores and causally replays high-resolution data. The sources
are the CF RTI (via Kalshi or direct), Coinbase and Kraken websockets, and Kalshi public REST. It
builds 452 research features, each with a status mask, under one rule: **an event is usable at T iff
`receive_ts <= T`**. Checkpoint datasets keep features, labels and provenance in separate files.

```
collect_market_data.py ─ runner.py (one thread per source, backoff, feed states)
   └─ sources/*.py (raw text → MarketEvent) ─ collector.py ─ storage.py (gzip JSONL + manifest)
                                                          └─ settlement_data/capture-*.jsonl (Step-2 store)
replay.py ─ features/engine.py (causal, incremental) ─ features/dataset.py (features | labels | provenance)
```

It is not wired into the watcher. `kalshi_dashboard`, `kalshi_core`, `run_local`, the perp code and
`settlement` never import it. It imports no strategy, execution or Discord module, makes only
read-only requests, and has its own fingerprint (`config/market_data_baseline.json`). Stage 19
checks all of this. Its status page is a separate localhost port. See `docs/HIGH_RESOLUTION_DATA.md`.

## 12. Expanded perpetual-futures telemetry (Step 4, observation only)

`perp_data/` captures Binance USDⓈ-M, Bybit linear and OKX swaps (websockets plus GET-only REST), Kalshi
perps (public REST) and a USDT/USD rate. It normalizes them into `PerpEvent`s, which use the same
timestamps as Step 3. Raw messages are stored in `<session>/perp/`, reusing Step 3's store. It replays
deterministically (raw → normalization is reproducible) and builds 980 status-masked research features in
ten FAMILIES:

* per venue: price, basis, funding, OI, liquidations, flow, CVD, book;
* cross-exchange aggregates;
* perp-vs-spot and perp-vs-CF divergence.

`perp_data.dataset` joins them with the Step-3 features at the same checkpoint T under `receive_ts <= T`
(features, labels and provenance in separate files).

It is a NEW research dataset beside the existing Kalshi-perp research → shadow → overlay → promotion →
live-veto chain (§5), which is unchanged and pinned. It never feeds that chain:
* nothing production, veto, settlement or market_data imports `perp_data`;
* `perp_data` imports none of them.

Stage 20 checks this, along with a separate fingerprint (`config/perp_data_baseline.json`, which also
records the veto chain's hashes). See `docs/PERP_HIGH_RESOLUTION_DATA.md`.

## 13. Future boundaries (not implemented)

```
Market Data → Feature Engine → Prediction Model → Calibration → Signal Engine
            → Risk Manager → Execution Engine → Kalshi
```

* A model returns a probability. It never sees an account or an order.
* The signal engine returns a `SignalDecision` (CALL / NO_CALL + reasons). It never sizes.
* The risk manager returns a `RiskDecision`. It never places an order.
* Only an execution engine may call an order endpoint, and it accepts only an `OrderIntent`: a CALL
  plus an approving `RiskDecision` within its contract limit. In this build every engine refuses,
  and LIVE cannot be constructed.

## 14. Failure handling (existing behaviour, unchanged)

* The poller catches every per-coin exception and records it as a status (`net error`,
  `error: …`); the loop continues.
* Malformed data never produces a fake confidence (`BLOCK_STALE_DATA`).
* Perp telemetry, the shadow evaluator and the live gate are lazily built, fail open for the call
  path, and never raise into the watcher.
* One known sharp edge: if the favoured side has **no ask and no opposite bid**, `evaluate()` raises
  `TypeError` while formatting its verdict table. The poller then records `error: …` and makes no call
  (fails closed). This is pinned by fixture `E19` and listed in `docs/BASELINE.md`.
