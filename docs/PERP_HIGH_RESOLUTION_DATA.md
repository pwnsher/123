# Expanded perpetual-futures telemetry (Step 4)

**Research / observation only.** `perp_data/` is a new research dataset built beside the existing
Kalshi-perp research / shadow / promotion / live-veto chain. It does not replace that chain, and it never
feeds it:
* none of the veto modules (nor `kalshi_dashboard`, `kalshi_core`, `run_local`, `settlement`, `market_data`)
  import `perp_data`;
* `perp_data` imports none of them;
* the 14 files of the existing chain are byte-identical and pinned (stage 20 tests 2–3).

Nothing here can create, veto, flip, size or delay a call. Every request is a GET or a public websocket
subscription.

```
PERP RAW SOURCES ─> NORMALIZED PerpEvents ─> RAW EVENT STORE ─> CAUSAL ALIGNMENT ─> PERP FEATURE ENGINE ─> JOINT DATASET
 (exact text)      perp_data/sources/*      <session>/perp/    receive_ts <= T     perp_data/features/   + Step-3 at the same T
                                            (Step-3 storage)                                             features | labels | provenance
```

## 1. Audit of the existing perp system (preserved unchanged)

| Aspect | What exists (verified in the code) |
|---|---|
| Source | Kalshi perpetuals, public REST only: `GET /margin/markets` gives bid, ask, last, `settlement_mark_price{price,ts_ms}`, `reference_price{price,ts_ms}` (CF-based, **per contract**), contract size and multiplier. `GET /margin/funding_rates/estimate` gives the in-progress funding estimate, `next_funding_time` and `computed_time`. The API publishes no OI, liquidations, trade sides or depth. |
| Telemetry | `perp_telemetry.py` (schema 3, `feature_version step2_v2`). A `PerpSampler` thread polls every 4 s into a bounded 30-min `SnapshotStore` and writes one CSV row per coin per watcher cycle to `kalshi_perp_telemetry.csv`. Columns: perp bid/ask/mid/last/mark, index, premium bps; funding rate / next / computed time; 30/60/180-s perp and spot returns; spread; mark/last/mid-vs-index premia; premium changes and z-scores (5/15 min); RV 60/300/900 s; vol shocks; vol-normalized momentum and gaps; spread baseline and ratio; premium stress; vol regime; stability state; quality flags; `analysis_ready`; queue drop counters. |
| Timestamps / alignment | Each binary row uses the newest perp snapshot whose `available_ts <= spot receive time` (`feature_end_ts_epoch_ms`), and only if it is ≤ `PERP_ALIGNMENT_MAX_LAG_SECONDS` (8 s) old. Lookbacks use real timestamps with a tolerance (10 s). "causal_*" means "uses no future data", not causation. |
| Research stages | **Step 3** `analyze_perp_predictive.py`: one observation per (ticker, horizon ∈ {8,6,4,2} min before close). Nested logistic models A (base logit) → B (+ spot controls) → C (+ feature; the reliability family adds feature × base logit), scored out of fold. **Step 4** `build_perp_shadow_policy.py` / `analyze_perp_shadow.py`: a frozen SHADOW_ONLY veto policy, then prospective validation. **Step 5** `build_perp_integration_experiment.py` / `analyze_perp_integration.py`: a capped probability-overlay experiment with a tuning/holdout split. **Step 6** `promote_perp_integration.py` → `perp_live.py`: manual promotion plus a suppress-only live veto. |
| Matched spot controls | Every perp feature is tested beyond equally-built spot controls (`causal_spot_ret_*`, `spot_rv_*`, `spot_vol_shock_*`, `spot_momentum_z_*`). The interaction family also adds control × modifier. |
| Walk-forward | Expanding folds at 40/55/70/85/100 % of whole close-time groups (contemporaneous BTC/ETH/SOL/XRP markets are never split). Min 30 train / 10 test rows per fold, ≥ 3 valid folds. Winsorization (1/99) and scaling are fit on training rows only. |
| Evaluation | Out-of-fold Brier score and log-loss (plus calibration bins) of C vs B on identical rows. |
| Bootstrap | Ticker-clustered bootstrap CIs (1000 reps in Step 3, 200 for secondary features; 2000 in the shadow and overlay steps), fixed seed 20260919. |
| Multiple testing | Benjamini–Hochberg FDR (q ≤ 0.05) across the 8 pre-declared primary features per cohort × group × horizon. Secondary and interaction features can never become candidates. Funding is exploratory only ("units unverified"). |
| Sample gates | ≥ 200 analysis markets, ≥ 500 candidate markets, ≥ 100 per class, sign consistency ≥ 75 %. Shadow: ≥ 7 calendar days, ≥ 200 settled, ≥ 30 blocked, ≥ 150 allowed. Overlay: ≥ 14 days, ≥ 200 tuning + ≥ 200 holdout settled. |
| Shadow validation | Frozen threshold (most conservative qualifying point on a 5–25th-percentile grid of out-of-fold conflict scores). It is validated only on data AFTER the discovery cutoff. Status `SHADOW_VALIDATED_CANDIDATE` is required to continue. |
| Promotion gates | The chain hash-binds candidate → policy → shadow report → experiment → promotion. Promotion requires `--confirm LIVE_VETO_ONLY`, expires (14 d default, max 30 d), refuses synthetic data, rejects forbidden keys, and binds the strategy fingerprint (`step5_baseline_manifest.json`). |
| Live veto | `perp_live.LiveVetoGate` is active only with a valid promotion artifact AND `PERP_LIVE_VETO_ENABLED=1`. It can only suppress. It fails open per call (missing / stale data, numeric error, 25 ms timeout); a kill file switches it off; it latches off after 3 consecutive gate errors, when the rolling block fraction exceeds 0.35 or the fail-open fraction exceeds 0.50. Current state: `check_perp_deployment.py` → **INACTIVE** (no candidate has ever qualified). |
| Strategy fingerprints | Promotion and activation re-verify the legacy strategy fingerprint `8d94f241…` against `step5_baseline_manifest.json`. Any drift refuses activation. |
| Failure behaviour | Telemetry never raises into the watcher (status fields `fresh / stale / unavailable / error / disabled`). The sampler has error backoff. Integrity failures refuse activation; data failures fail open. |

**What Step 4 keeps.** The chain above stays the ONLY path from perp data to a call. Its statistical safeguards
are the template for evaluating the new families later:
* pre-declared families (§8) with matched spot controls (Step-3 features at the same T);
* walk-forward on whole close-time groups;
* out-of-fold Brier score and log-loss;
* clustered bootstrap;
* BH-FDR;
* shadow → overlay → manual promotion.

The existing-chain fingerprint is `499c1e16da5d9cc763babe7dea31db7104ebc3f1460a17241af83240c8de523b`
(sha256 over the 14 files' hashes, `perp_data.fingerprint.PERP_VETO_FILES`). It is also recorded in
`config/perp_data_baseline.json`, so `py -m perp_data.fingerprint --verify` flags any change to that chain.

## 2. Venues and why

| Venue | Contracts | Data used | Timestamps | Trade side | Auth | Limitations |
|---|---|---|---|---|---|---|
| **Binance USDⓈ-M** (`binance_usdm`) | BTCUSDT … XRPUSDT perpetual | ws: `aggTrade`, `bookTicker`, `markPrice@1s` (mark, index, funding, next time), `forceOrder`, `depth{5,10,20}@100ms`. REST: `openInterest` (polled 2 s), `fundingInfo`, `fundingRate` history, `aggTrades?fromId=` backfill | `T` trade / transaction time, `E` event time (ms) | `m` = "is the buyer the market maker", so aggressor = SELL when true (**inverted maker flag**) | none | Liquidations are a **sample** (only the largest per symbol per 1000 ms). OI only via REST polling. The funding interval is published only for adjusted symbols (else the documented 8 h default, flagged). |
| **Bybit linear** (`bybit_linear`) | BTCUSDT … USDT perpetual | ws: `publicTrade`, `tickers` (snapshot + delta: mark, index, funding, `fundingIntervalHour`, next time, OI in coin and value, bid1/ask1), `orderbook.50`, `allLiquidation`. REST: funding history, recent trades | `T` trade time, message `ts` / `cts` | `S` = side of the **taker** | none | Ticker/book deltas must be merged (a delta before a snapshot is refused). Contiguity of book `u` is not documented, so no book sequence-gap rule. Trade ids are not contiguous. |
| **OKX swaps** (`okx_swap`) | BTC-USDT-SWAP … | ws: `trades`, `books5`, `mark-price`, `index-tickers`, `funding-rate` (+ `nextFundingRate` when published), `open-interest` (contracts, `oiCcy`, `oiUsd`), `liquidation-orders`. REST: `instruments` (ctVal), funding history, trades | `ts` (ms) | `side` = **taker** side | none | Sizes are **contracts**: coin units only after ctVal is verified from `instruments` (the static table is never trusted alone). Depth is limited to 5 levels. Liquidations are reported as sampled and priced at the bankruptcy price. |
| **Kalshi perps** (`kalshi_perp`) | discovered (override `KALSHI_PERP_TICKER_<COIN>`) | REST `/margin/markets` (2 s), funding estimate (60 s) | `ts_ms` of mark / reference; `computed_time` | none published | none | Per-contract prices and funding units are **unverified**, so only ratios are READY (returns, mark / index premium). No OI, liquidations, sides or depth. An independent reader, not the existing telemetry. |
| **Coinbase USDT-USD** (`coinbase_usdt`) | spot ticker | USDT/USD mid | `time` (RFC3339) | – | none | Used only to convert USDT prices to USD before comparing with the USD spot reference / CF RTI. |

**Why these venues.**
* Binance and Bybit are the two largest perp venues for all four coins.
* OKX is third, with a strong documented API, and it is the venue that exercises contract-value
  normalization and a published next-period funding forecast.
* Kalshi perps connect the new dataset to the market the existing chain studies.

All publish public, documented websocket semantics with millisecond venue timestamps.

**Not assumed identical across venues.** The venue table in `perp_data/venues.py` records, per venue:
* size unit and contract values (and whether they are verified);
* trade-side semantics;
* liquidation-side semantics and sampling;
* funding-interval source;
* whether a forecast is published;
* OI unit;
* maximum book depth.

Adding a venue = one `VenueSpec`, one adapter, and a synthetic branch for tests.

## 3. Event schema (`perp_data/types.py`)

`PerpEvent` has the same timestamp and ordering fields as Step 3's `MarketEvent`:

* identity and timing: `source`, `asset`, `event_type`, `symbol`, `event_ts_ms` (venue time or None),
  `receive_ts_ms`, `receive_mono_ns`, `ingest_seq` (shared with Step 3 when collected together);
* provenance: `raw_seq` (the raw message it came from), `source_seq` (venue id / sequence),
  `channel` (`ws` / `rest`);
* state: `mode` (`LIVE` / `BACKFILLED`), `quality` (`OK` / `ESTIMATE` / `SAMPLED` / `UNVERIFIED_UNITS` /
  `DERIVED`), `flags`;
* `payload` (fixed fields per type plus `native`: the venue's own fields kept for verification).

The types are `PERP_TRADE`, `PERP_QUOTE`, `PERP_MARK_PRICE`, `PERP_INDEX_PRICE` (last, mark and index are
never substituted for one another), `FUNDING_RATE`, `PREDICTED_FUNDING`, `FUNDING_SETTLED`, `OPEN_INTEREST`,
`LIQUIDATION`, `ORDERBOOK_TOP` (top N after applying deltas), `INSTRUMENT` (contract value / funding
interval), `STABLECOIN_RATE` and `HEARTBEAT`. `ORDERBOOK_UPDATE` is reserved: Bybit diffs are applied in the
adapter, and the resulting top-N book is stored together with the raw diff text.

## 4. Funding normalization

Every venue publishes a **fraction per funding interval**. The native value is always stored. Where the
interval is known:
* `rate_per_hour = native / interval_h`;
* `rate_8h = rate_per_hour × 8`;
* `rate_annual_simple = rate_per_hour × 8760` (simple, not compounded).

| Venue | "Current" rate (FUNDING_RATE) | Interval | Forecast (PREDICTED_FUNDING) |
|---|---|---|---|
| Binance | `markPrice.r`, the rate for the settlement at `T` (estimate, changes until then) | `fundingInfo` for adjusted symbols, else the documented 8 h default (`DEFAULT_INTERVAL` flag) | not published, so none produced |
| Bybit | ticker `fundingRate` for `nextFundingTime` (estimate) | ticker `fundingIntervalHour` (unknown → normalized None) | not published |
| OKX | `fundingRate` for the settlement at `fundingTime` | `nextFundingTime − fundingTime` | `nextFundingRate` only when non-empty (empty for current-period collection) |
| Kalshi perps | funding estimate | **unverified**, so normalized None | – |

Settled rates come from each venue's history endpoint (`FUNDING_SETTLED`, BACKFILLED). Missing settlements
are a gap (§10). No funding value is labelled bullish or bearish.

## 5. Open-interest normalization

`oi_native` + `oi_unit` are always stored. `oi_coin` is filled only when reliable:
* Binance and Bybit publish base coin;
* OKX publishes `oiCcy` (coin) beside `oi` (contracts), or the engine uses contracts × **verified** ctVal.

`oi_notional = oi_coin × mark` (USDT). Raw contract counts never enter a feature or an aggregate (mutation
P7 proves the tests catch it). Price × OI states are stored as **raw components** (e.g. `mark_logret.30s`
and `oi_change_coin.30s`), never as an encoded interpretation.

## 6. Liquidation semantics

| Venue | Field | Meaning | Stored as |
|---|---|---|---|
| Binance `forceOrder` | `o.S` | side of the **forced order** | `forced_side`; `liquidated_position` derived (a forced SELL can only close a LONG); flags `POSITION_SIDE_DERIVED`, `SAMPLED_STREAM`; quality `SAMPLED` |
| Bybit `allLiquidation` | `S` | **position** side: "Buy" means a **long** was liquidated | `liquidated_position` as documented; `forced_side` = the opposite order side |
| OKX `liquidation-orders` | `side`, `posSide` | forced-order side and position side (`net` in one-way mode → derived from the order side, flagged) | both; `bkPx` is the bankruptcy price (`native.price_is_bankruptcy_price`) |

Features are computed on BOTH forced-sell / forced-buy (always documented) and long / short liquidated.
Sampled venues give **lower bounds**, stated in each feature's `completeness` field.

## 7. Causality, gaps, backfill, storage, replay

* **Rule:** an observation is usable at T iff `receive_ts <= T`. This covers funding, OI, liquidations,
  trades, books, marks and indices. The engine has the same guards as Step 3: availability-order ingest,
  no features before the last ingested receive time, labels never ingested. Step-3 and perp events merge
  in `(receive_ts, ingest_seq, family)` order.
* **Liveness** counts only websocket traffic for websocket venues (REST polls continue during an outage).
  On disconnect the collector records a **`DISCONNECT_OPEN` gap known at that moment**, closed by the
  `DISCONNECT` gap at reconnect. Windows overlapping an outage are MISSING while it is in progress, not
  only once it is over. (This was found and fixed during Step 4, see §11.)
* **Gaps**, source-appropriate (never a universal one-per-second rule):

  | Kind | Applies to |
  |---|---|
  | `TRADE_ID` | Binance aggregate ids; exact missing count |
  | `SEQUENCE` | Binance depth `pu` ≠ previous `u` |
  | `CADENCE` | Binance 1-s mark stream (> 3 s) |
  | `FUNDING` | missing settlement records |
  | `POLL_CADENCE` | a REST stream without success for > max(2.5 × interval, interval + 5 s) |
  | `DISCONNECT_OPEN` / `DISCONNECT` | transport outages |

  Each gap records the source, asset, stream / event type, reason, start/end, recoverability and when it
  became known.
* **Backfill:**
  * Binance `aggTrades?fromId=last+1` recovers an outage exactly; Bybit and OKX use recent trades.
  * Backfilled events are `BACKFILLED`, keep their event time, and have `receive_ts` = retrieval time, so
    they are never visible to an earlier checkpoint (mutation P4).
  * Duplicates collapse by trade id.
* **Storage:** Step 3's append-only gzip-JSONL store with CRC, under `<session>/perp/`, with its own
  `manifest.json`. The manifest holds the derivatives sources, venue semantics, contract values, funding
  intervals, per-venue counters, gaps and fingerprints. RAW messages are stored first.
* **Replay:** `py scripts/replay_perp_data.py <session> --digest --renormalize`
  * Replay is deterministic, with the same digest on every run.
  * `--renormalize` re-runs the adapters over the stored raw text and must reproduce every stored event,
    including snapshot/delta state and contract values learned from INSTRUMENT metadata.
  * Replay supports event-by-event, 1× and accelerated speeds (Step 3's `Replayer`).

## 8. Features (`perp_data/features/definitions.py`: 980, `perp_features_v1`)

Every feature has a FAMILY for later family-level ablation: `PRICE`, `BASIS`, `FUNDING`, `OPEN_INTEREST`,
`LIQUIDATION`, `FLOW`, `CVD`, `ORDERBOOK`, `CROSS_EXCHANGE`, `PERP_SPOT_DIVERGENCE`. Statuses are Step 3's
(`READY`, `NOT_READY`, `MISSING`, `UNAVAILABLE`, `UNDEFINED`, `PARTIAL`, the last only in research mode).
Missing is never 0: a liquidation window is 0 only if the venue was observed throughout it. The one
exception is `x.n_venues_fresh`, which is a count.

Per venue (`perp.<venue>.…`):

| Family | Features |
|---|---|
| PRICE | `last`, `mid`, `mark`, `index`; `mid_logret` and `mark_logret` over 1s–5m; `mid_rv.60s` / `.5m`; `move_over_rv` |
| BASIS | `mark_minus_index_bps`, `mid_minus_index_bps`, `basis_z_5m`; `mark_minus_ref_usd` / `_bps` (USDT converted to USD, vs the Step-3 median spot reference); `mark_minus_ref_raw_bps` (unconverted); `mid_minus_coinbase_bps`, `mid_minus_kraken_bps`, `mark_minus_cf_bps`; `basis_change_bps` (5s–5m); `basis_accel_bps` (15s, 60s); `basis_over_vol` |
| FUNDING | `funding_rate_native` / `_8h` / `_annual`; `funding_interval_h`; `funding_secs_to_next`; `funding_predicted_8h`; `funding_change_8h` (60s, 5m); `funding_accel_8h.5m`; `funding_percentile` (session-relative, NOT_READY until 30 obs over 5 min) |
| OPEN_INTEREST | `oi_coin`, `oi_notional`, `oi_age_s`; `oi_change_coin`, `oi_pct_change`, `oi_notional_change` (5s–5m); `oi_accel_coin` (15s, 60s, 5m) |
| LIQUIDATION (1s–3m) | forced-sell / forced-buy / long / short / total notional; `imbalance`; `count`; `max_notional`; `accel`; `over_trade_notional` |
| FLOW (1s–5m) | aggressive buy / sell / signed notional; `imbalance`; `count_imbalance`; `count`; average and max size (coin) |
| CVD (5s–5m) | windowed `cvd_notional` (no endless accumulator); `cvd_slope`; `cvd_accel` |
| ORDERBOOK | bid, ask, sizes, `spread_bps`; `top_imbalance`; `depth_imbalance_5` / `_10`; `weighted_mid`; `depth_notional_5`; `spread_ratio_5m`; spread / depth / pressure change (5s, 60s) |

Cross-exchange (`x.…`, never a plain average):
* medians and disagreement: `median_mid_logret`, `disagreement_bps`, `max_venue_dev_bps` ("one venue
  unusual"), `sign_agreement`, `oi_weighted_logret`;
* funding and basis spread: `funding_median_8h` / `dispersion`, `basis_median` / `dispersion`,
  `spread_median`;
* totals: `oi_total_notional` / change, liquidation totals, `cvd_total_notional`, `flow_imbalance`.

Totals require **every** size venue to be observed (else MISSING). Medians use the fresh venues and
report `n`.

Divergence (`div.…`): `perp_minus_spot_ret` and `perp_minus_cf_ret` (5s–3m), `perp_minus_spot_flow_imb`
(vs Coinbase spot flow), `perp_over_spot_rv` (60s, 5m), `basis_accel_median_bps.60s`.

Shock telemetry (the measurements are exposed; no classifier is built):
* `liq_*` bursts and accel (liquidation cascade);
* `oi_pct_change` / `oi_accel_coin` (OI collapse);
* `basis_z_5m`, `basis_accel` (basis dislocation);
* `book_spread_ratio_5m` (spread expansion);
* `x.disagreement_bps`, `x.max_venue_dev_bps` (cross-exchange disagreement);
* `funding_change_8h` / `accel` (abrupt funding changes).

Normalized variants (no silent clipping; winsorization would be an explicit research transform):
* `liq_over_trade_notional`;
* `flow_imbalance` (signed / total);
* `basis_over_vol`;
* `move_over_rv`;
* `oi_pct_change`.

## 9. Joint dataset (Step 3 + Step 4)

`py scripts/build_research_dataset.py <session> --assets BTC --out analysis_output/research_btc`

It makes ONE pass over the merged Step-3 and perp events. At each Kalshi checkpoint T (close − 600 … 0 s),
the row holds:
* Step-3 features (spot, CF, Kalshi, and the Step-2 settlement state as of T);
* perp features at the same T;
* each side's own status masks.

There is no forward fill across the join. The dataset is tested equal to both batch paths at every
checkpoint. Labels (final reconstruction plus the official result) go to `labels.jsonl`. Provenance goes to
`provenance.json`: feature definitions WITH families, the family → names map, versions, sessions, venues,
fingerprints (including the untouched veto chain), and `feeds_existing_perp_veto: false`.

## 10. Real-data collection (Windows PowerShell)

```powershell
py collect_research_data.py --dry-run                     # validates config, lists sources / symbols / GET polls,
                                                          # credentials (presence only), fingerprints; no network, writes nothing
py collect_research_data.py --assets BTC,ETH,SOL,XRP --cf --coinbase --secondary --kalshi --perps --duration 3600
py collect_research_data.py --assets BTC --coinbase --secondary --perps --perp-venues binance_usdm,bybit_linear,okx_swap
#  research status page: http://127.0.0.1:8767/   (read-only; separate from the production dashboard)
py scripts/replay_perp_data.py market_data_sessions\<session> --digest --renormalize
py scripts/build_research_dataset.py market_data_sessions\<session> --assets BTC --out analysis_output\research_btc
py scripts/bench_perp_data.py
py scripts/mutation_test_perp_data.py
py -m perp_data.fingerprint --verify
```

The perp sources need no credentials. CF RTI via Kalshi still needs the Step-3 read-only key setup.

## 11. Tests, mutations, performance

Stage 20 has 26 checks, all offline.

`py scripts/mutation_test_perp_data.py` runs each mutation as-is and with the engine's causality guards disabled (results: `analysis_output/perp_data_mutation_results.json`):

| # | Mutation | As-is | Guards disabled |
|---|---|---|---|
| P1 | receive timestamps ignored (perp availability decided by event time) | CAUGHT (CausalityError guard) | CAUGHT (test 18) |
| P2 | future liquidations become visible | CAUGHT (CausalityError guard) | CAUGHT (test 18) |
| P3 | future open-interest updates become visible | CAUGHT (CausalityError guard) | CAUGHT (test 18) |
| P4 | backfilled trades become historically visible (receive time := event time) | CAUGHT (test 16) | CAUGHT (test 16) |
| P5 | missing liquidation volume becomes zero | CAUGHT (test 7) | CAUGHT (test 7) |
| P6 | maker side interpreted as taker side (Binance aggTrade m) | CAUGHT (test 4) | CAUGHT (test 4) |
| P7 | raw contract OI compared across venues without normalization | CAUGHT (test 6) | CAUGHT (test 6) |
| P8 | settlement labels enter the perp features (official result merged into the row) | CAUGHT (test 21) | CAUGHT (test 21) |
| P9 | the existing perp veto imports Step-4 features | CAUGHT (test 2) | CAUGHT (test 2) |

Controls: unmutated copy PASS; guards disabled without a mutation PASS. Result: every mutation caught. P9 is also caught independently by test 3 (import isolation: `('perp_live.py', ['perp_data.features.engine'])`), and P1 with the guards disabled by test 18's "delete everything received after T" invariant.

Benchmark (`py scripts/bench_perp_data.py`, SYNTHETIC: 4 assets × Binance / Bybit / OKX streams + Kalshi perps + USDT,
a 2-minute liquidation cascade, REST OI / funding polls; CPython 3.11.15; NOT a production latency claim):

| Measure | Result |
|---|---|
| Joint ingest (Step-3 + perp raw → normalize → store, fsync per flush) | 4,635 messages/s (53,062 perp + 26,288 Step-3 raw messages, 72,568 perp events, 2,560 liquidations) |
| Perp store flush latency | p50 5.53 ms, p99 7.98 ms |
| Perp storage | 122.7 bytes/event including raw text (8.5 MB for 10 min × 4 assets) |
| Replay (iterate) | 3,496,364 events/s; loading + CRC-verifying both stores 7.934 s |
| Perp engine over 101,256 merged events + a 980-feature row every 2 s | 6.298 s |
| `features_at` (980 perp features, one asset) | p50 42.91 ms, p99 70.27 ms, max 71.83 ms |
| Joint dataset (4 assets) | 16 checkpoint rows in 1.365 s |
| Memory (load both stores + one perp engine over them (separate pass)) | peak 352.1 MB traced |

Found and fixed during Step 4:
* **REST polls masked outages.** Websocket-venue liveness counted REST polls, so a websocket outage
  looked like a quiet market. Liveness now counts websocket traffic only.
* **Outages were learned only at reconnect.** An outage in progress now makes windows MISSING through the
  open gap.
* **Failed first connection.** An open gap from a failed FIRST connection attempt would never have been
  closed.
* **Synthetic Bybit book crossed.** The synthetic Bybit book deltas left stale levels; the engine correctly
  refused the crossed book.

## 12. Integrity

`config/perp_data_baseline.json` pins:
* every `perp_data` module (canonical AST);
* the feature registry with families;
* the default config;
* the venue semantics table;
* the existing perp-veto chain's file hashes.

Verify with `py -m perp_data.fingerprint --verify`. Rewriting it needs
`--write --i-intend-to-change-the-perp-data-baseline` plus a row below. The Step-1 strategy, Step-2
settlement and Step-3 market-data baselines are unchanged by Step 4.

| Date | What changed | Why |
|---|---|---|
| 2026-09-25 | initial baseline (`perp_features_v1`, 980 features, 4 perp venues + USDT) | Step 4 created the package |

## 13. Limitations

* **No real capture was possible here.** The network policy blocks every exchange host. Parsers follow the
  documented message shapes and are tested on synthetic look-alikes. Semantics were confirmed from public
  documentation summaries, since the pages themselves were not fetchable. Unknown shapes become recorded
  parse failures.
* **Sampled liquidations.** Binance (and OKX, as documented) push samples, so liquidation totals are lower
  bounds. Bybit's stream is complete but batched every 500 ms.
* **Coarse OI.** Binance OI is REST-polled every 2 s, so short-horizon OI changes are coarse.
* **Session-relative funding percentile.** There is no long funding history, so the percentile is
  session-relative.
* **OKX sizes.** OKX size features need ctVal confirmed from `instruments` at session start (until then,
  UNAVAILABLE).
* **Kalshi perps.** Units are unverified: only ratios are used, and funding normalization is None.
* **No per-venue spot books.** Each venue's own index plays that role; the USD comparisons use the Step-3
  Coinbase/Kraken reference after USDT conversion.
* **Load.** `features_at` builds ~980 features per asset (about 50 ms here). That is fine for 1-s research
  checkpoints; production latency is not claimed.
* **No proven value.** Nothing here has been shown to add predictive value. That is the job of later,
  pre-declared, walk-forward family ablations (§1).

### Sources for the documented semantics

* Binance USDⓈ-M: liquidation order streams, aggregate trades, mark price stream:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams ,
  …/Aggregate-Trade-Streams , …/Mark-Price-Stream
* Bybit v5: https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation , …/ticker , …/trade
  (liquidation side mapping cross-checked against the Tardis.dev Bybit mapper)
* OKX v5: https://www.okx.com/docs-v5/en/ (trades, books5, mark-price, index-tickers, funding-rate,
  open-interest, liquidation-orders, instruments)
* Kalshi perps: the endpoints documented in `perp_telemetry.py` (docs.kalshi.com perps OpenAPI)
