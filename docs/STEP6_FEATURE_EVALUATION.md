# Step 6 — real-data evaluation, feature-family ablation and model comparison (research only)

`feature_eval/` answers one question with chronological out-of-sample evidence on **real** captured sessions:
*which of the frozen Step 2–5 inputs add information beyond the frozen legacy production probability, and does any
improvement survive executable Kalshi prices and fees?*

It **only produces evidence**:

* it never modifies the production model, call / confidence thresholds, entry windows, stops, risk sizing, the
  existing perp veto or any execution path;
* it places no orders;
* it never promotes a model. **There is no automatic promotion**: the result classes are `INSUFFICIENT_DATA`,
  `INSUFFICIENT_DATA_FOR_COMPLEXITY`, `SYNTHETIC_ONLY`, `NO_INCREMENTAL_VALUE`, `PROMISING_RESEARCH_ONLY`, `UNSTABLE`
  and `DEGRADED`. `APPROVED_FOR_PRODUCTION` does not exist.

Synthetic data only tests code. It never selects features, tunes predictive parameters, supports a claim of
predictive improvement or promotes anything. Every synthetic result is flagged `synthetic_only` and written to a
separate file.

**Current real-data status: `INSUFFICIENT_DATA`.** There are no real captured research sessions in this
repository. Every real output file says so and names the unmet requirements. See §15.

Stage 22 (`test_stage22.py`) enforces all of this, and `scripts/mutation_test_step6.py` proves the tests catch the
14 methodological errors S1–S14, plus S15–S20 for the Step 6.1 corrections (§18).

---

## 1. Commands

```powershell
py run_step6_research.py --sessions market_data_sessions --assets BTC,ETH,SOL,XRP   # everything
py run_step6_research.py --dry-run          # plan, frozen-universe check, every predeclared config; writes nothing
py run_step6_research.py --validate-only    # quality + Coinbase sequence + settlement provenance only
py run_step6_research.py --dataset-only     # + build (or load) the immutable research matrix
py run_step6_research.py --family-ablation  # + ablation / model comparison (calibration stages NOT_RUN)
py run_step6_research.py --calibration      # + calibration, confidence buckets, economics (all stages)
py run_step6_research.py --report           # summary of the existing step6_*.json outputs
py run_step6_research.py --synthetic-selftest [--workers 2]    # code self-test on a SYNTHETIC matrix
py validate_research_session.py market_data_sessions\<session> [--json out.json]
py research_status.py                       # real sessions, hours, settled markets, unmet gates (no ETA)
py scripts/mutation_test_step6.py           # S1–S20
py scripts/bench_step6.py                   # SYNTHETIC throughput
py -m feature_eval.fingerprint --verify     # separate Step-6 fingerprint
```

`--workers N` uses a process pool (spawn start method, Windows-safe). `ProcessPoolExecutor.map` keeps job order, so
the results are identical for any N (tested).

The FINAL_HOLDOUT is evaluated only with `--final-holdout --confirm-single-use`. This is allowed once per dataset
fingerprint and is recorded permanently in the research ledger (§12).

Outputs go to `analysis_output/`:

* `step6_data_quality.json`
* `step6_family_ablation.json`
* `step6_calibration.json`
* `step6_confidence_buckets.json`
* `step6_economic_metrics.json`
* `step6_research_ledger.json` plus the hash-chained `step6_research_ledger.jsonl`

The synthetic self-test goes only to `step6_synthetic_selftest.json`.

## 2. Frozen feature universe (`feature_eval/universe.py`, `config/step6_feature_universe.json`)

The universe has 2047 records, built from the frozen registries:

| Layer | Records |
|---|---|
| Step-2 settlement | 11 |
| Step-3 spot / Kalshi | 441 |
| Step-4 perps | 980 |
| Step-5 microstructure | 615 |

Each record holds:

* name, layer, family, source, asset applicability, window, units;
* status rules, minimum warm-up, causal availability rule (`receive_ts_ms <= T`);
* feature version, source fingerprint;
* role and tags.

The finer families already in the repo are kept as they are. Step-3 groups map to `SPOT_PRICE_MOMENTUM`,
`SPOT_VOLATILITY`, `SPOT_FLOW`, `SPOT_VOLUME`, `CROSS_EXCHANGE_SPOT`, `CF_VS_SPOT`, `STRIKE_DISTANCE`,
`KALSHI_MARKET_STATE` and `SETTLEMENT`. The ten Step-4 and ten Step-5 families are unchanged.

**Roles:**

| Role | Count | Meaning |
|---|---|---|
| `MODEL_CANDIDATE` | 1988 | may be a model input |
| `EXECUTION_STATE` | 16 | Kalshi prices and sizes: used for economics, never a model input |
| `ALIAS` | 4 | `no_mid` = `yes_mid`, … |
| `DATA_QUALITY` | 39 | readiness, latency, counters |

**Tags:**

* `SESSION_NORMALIZED` (9): ranks within a session;
* `SETTLEMENT_CONVENTION_DEPENDENT` (11);
* `COINBASE_L2_SEQUENCE` (112): see §4.

The universe version is `feature_universe_v2` (Step 6.1: the settlement layer's source fingerprint changed). Its
fingerprint is `920c0a8aae81535f…`; v1 (`e4c856ba2b63df06…`) is archived in `config/history/`. `verify_frozen()` refuses any record change without a
`FEATURE_UNIVERSE_VERSION` change (S12), a hand edit, or a version mismatch.

`max_lookback_ms()` is 1 500 000 ms: the longest retention of any frozen engine. It is used for the purge (§6).

## 3. Real-session quality validator (`feature_eval/quality.py`, `validate_research_session.py`)

Verdicts are **PASS / DEGRADED / REJECT**, per session and per source/asset. A synthetic session gets the
real-data verdict `SYNTHETIC_ONLY` and is never usable for research.

Checks:

* raw files: gzip CRC / JSON / member errors cause REJECT; a truncated final member causes DEGRADED;
* segment checksums, including a comparison with an earlier report (a changed segment causes REJECT);
* receive-order regressions;
* gaps, reconnects per hour, backfills;
* availability per minute, event counts, duplicate facts;
* impossible future and non-epoch timestamps;
* asset / contract mapping;
* book validity share (Step 5);
* settlement provenance (trusted CF sources);
* label completeness;
* no label-name / feature-name overlap;
* OKX contract values.

All thresholds are in `QualityConfig`, which is fingerprinted. A REJECTed source is excluded, and its features
become `EXCLUDED_BY_QUALITY` (missing, never zero). A REJECTed session is skipped.

## 4. Coinbase sequence semantics (`feature_eval/coinbase_seq.py`)

Step 5 assumes that `sequence_num` is one counter per websocket connection, which heartbeats also advance.
Coinbase's wording also allows a per-product reading. Neither is assumed here.

The validator measures, on the stored raw envelopes:

* global / per-channel / per-product contiguity;
* whether heartbeats consume sequence numbers;
* multi-product envelopes;
* first sequence numbers per connection (reset behaviour);
* snapshot positions.

**Status:**

* `UNVERIFIED_REAL_FEED` until enough real evidence exists (5000 envelopes, 2 connections, 2 products, 30 heartbeats);
* `CONSISTENT_WITH_CURRENT_POLICY` only when connection-level contiguity holds (≥ 0.995);
* `CONTRADICTS_CURRENT_POLICY` when per-product or per-channel contiguity holds but connection-level does not;
* `SYNTHETIC_ONLY` for synthetic captures, which are never evidence.

Fail closed: until `CONSISTENT…`, the 112 Coinbase-book features are excluded from every model candidate set. On
`CONTRADICTS…`, the Coinbase books are also REJECTed by the quality validator. The required correction (track
`sequence_num` per product) is written into the report. Old sessions are never silently reinterpreted; they must be
re-derived from their raw envelopes after a deliberate, versioned fix.

## 5. Settlement labels (`feature_eval/labels.py`)

The label convention is selected **only by reconstruction agreement with official Kalshi expiration values**, never
by model performance. `settlement.resolution.verify_all` is run over all window policies. The declared label policy
(`cf_rti_60s_start_incl_asof_v1`) must reach:

* ≥ 50 compared markets (with a known official precision);
* ≥ 98 % EXACT expiration-value matches at the contract's official precision (Step 6.1: no universal tolerance);
* ≥ 99 % outcome agreement;
* and it must be the **only** passing convention. Tied or indistinguishable passing conventions →
  `SETTLEMENT_UNVERIFIED / AMBIGUOUS_CONVENTION`. The declared policy failing while another passes →
  `REQUIRES_VERSIONED_POLICY_MIGRATION`. Details in §18.

Label sources:

* `OFFICIAL_RESULT` (gold);
* `RECONSTRUCTED_VERIFIED` (gold, only when the gate is VERIFIED);
* `SETTLEMENT_UNVERIFIED` (never gold, excluded from promotion research);
* `LABEL_CONFLICT` (the official result and the reconstruction disagree, excluded);
* `UNLABELED`.

Synthetic labels are `SYNTHETIC_ONLY`.

## 6. Dataset, checkpoints, splits

**Frozen checkpoint grid:** 600, 480, 360, 300, 240, 180, 120, 90, 60, 30 s before the close.

**The market is the cluster unit.** Each row holds:

* market, asset, checkpoint;
* features and statuses from the one-pass joint Step 2–5 builder;
* the legacy probability, computed by the **unmodified** `kalshi_dashboard.evaluate()` inside the regression sandbox
  from captured data available at T;
* the Kalshi execution state (see below);
* the label.

The execution state is the Step-5 websocket book when READY. Otherwise it is the Step-3 depth-10 REST book, if that
book is at most 5 s old.

**Market-normalized weights.** Each market's rows sum to 1, so a market with more READY checkpoints gets no extra
influence. Checkpoint-specific results are reported too.

**Dataset fingerprint** (S10) covers:

* every session id and its raw-store checksum;
* the feature-universe fingerprint and version;
* the settlement-gate status, convention and label version;
* the checkpoint grid, assets, source exclusions and date range;
* include flags and the Coinbase status.

**Immutable feature-matrix cache.** Stored as gzip JSONL plus meta, keyed by the dataset fingerprint. An existing
matrix is never overwritten. The cache is refused (`CacheInvalid`) if the file or any raw-session checksum changed.

**Purged chronological splits** (never shuffled; all markets of one 15-minute slot stay together):

* TRAIN 60 % / DEVELOPMENT 20 % / FINAL_HOLDOUT 20 % by close time;
* exact boundaries are recorded.

**Purge.** An earlier market is dropped when its close plus the label horizon falls inside the later block's
information window:

* first checkpoint − max causal lookback;
* purge = lookback (1 500 000 ms) + label horizon (0 ms).

`purge_ms` is part of every experiment fingerprint.

**Walk-forward.** Expanding folds over the DEVELOPMENT blocks (4 by default). Each fold trains on every earlier
non-holdout market, purged. The FINAL_HOLDOUT never enters a fold. This is asserted, and `HoldoutLeak` is raised
otherwise.

## 7. Per-fold fitting (TRAIN rows only)

1. **Structural pruning** (`feature_eval/pruning.py`). It is label-independent: it never reads y (tested by flipping
   every label). It runs per family on the training rows and removes:
   * `NOT_MODEL_CANDIDATE`;
   * `NEVER_AVAILABLE`;
   * `CONSTANT`;
   * `NEAR_CONSTANT` (≥ 99.5 %);
   * `EXACT_DUPLICATE`;
   * `HIGHLY_CORRELATED` (|r| ≥ 0.995: algebraic identities, redundant horizons).

   Kept features, removed features and reasons are fingerprinted. Removed fields stay in the raw data and the rows.
   Nothing is deleted.
2. **Conservative screening.** Keep the top-k features by |market-weighted correlation with (y − legacy p)|, using
   training rows only (S3). k ≤ 12 and k ≤ the complexity budget.
3. **Complexity gate.** Effective parameters are:
   * B / C models: 2 + features + missing indicators;
   * D model: boosting rounds.

   The gate requires min(positives, negatives) ≥ 10 × parameters and training markets ≥ 20 × parameters. Otherwise
   the result is `INSUFFICIENT_DATA_FOR_COMPLEXITY`. The report lists independent training markets, positives,
   negatives, candidate count, post-pruning count and effective complexity.
4. **Missing data never becomes zero** (S9). Three strategies are compared:
   * `indicator`: the training mean plus an explicit missing indicator;
   * `complete_case`: rows dropped, with the sample loss per family reported;
   * `native`: the stumps learn a direction for missing values.
5. **Preprocessing** is fit on training rows only (S2).

**Models:**

| Model | Description |
|---|---|
| A | the frozen legacy probability |
| A2 | legacy recalibrated (logistic on the legacy logit): the fair same-class baseline |
| B | logistic |
| C | ridge logistic; λ from the predeclared grid {0.1, 1, 10, 100}, chosen on the latest 25 % of the fold's training rows (never test, never holdout; S4) |
| D | gradient-boosted stumps starting from the legacy logit |

The legacy logit is never shrunk. Everything is pure standard-library Python and deterministic.

## 8. Hypotheses, ablation, multiple comparisons

**Stage 1: predeclared layer hypotheses, each vs A2 on identical rows:**

* H01 +settlement
* H02 +spot
* H03 +perps
* H04 +micro
* H05 +settlement+spot
* H06 +spot+perps
* H07 +spot+micro
* H08 +perps+micro
* H09 +all validated layers

Each runs with models B, C and D.

**Stage 2: leave-one-family-out** on the best DEVELOPMENT configuration. "Best" means the lowest DEV out-of-fold log
loss; **the holdout is never consulted**.

**Stage 3: hierarchical families.** Families inside a layer are tested one by one only when that layer's stage-1
primary (C) hypothesis is `PROMISING_RESEARCH_ONLY`.

Each comparison reports the paired log-loss and Brier differences, with:

* market-level means;
* a **market-clustered bootstrap** CI (markets are resampled, never rows; S5);
* a market sign-flip p-value.

**Benjamini–Hochberg** q-values are computed per stage with the repo's `analyze_perp_predictive.bh_qvalues`.

**`PROMISING_RESEARCH_ONLY`** requires all of:

* the upper CI bound of the log-loss difference < 0;
* q ≤ 0.10;
* a practical improvement ≥ 0.002 log loss;
* stability.

**Stability.** Results are sliced by fold, asset, direction, checkpoint and volatility tercile. A dimension is
`UNSTABLE` when fewer than 2/3 of its evaluable slices (≥ 20 markets) improve. A promising result from a DEGRADED
session is `DEGRADED`.

**Regimes.** The research rows contain no pre-existing regime variable, so continuous volatility slices are used:
terciles of `vol.cf.rv.5m`, with cut points taken from TRAIN.

**Also reported for the best configuration, all on DEV only:**

* pooled vs pooled + asset indicators vs asset-specific;
* direction and checkpoint slices (time-to-close is **measurement only**: no window is optimized);
* the missing-data strategies;
* per-session generalization, and with / without `SESSION_NORMALIZED` features;
* TRAIN-only redundancy.

**Feature classes** (classification only; **no code or raw field is deleted on the basis of results**):

* `KEEP_CANDIDATE`
* `NO_INCREMENTAL_VALUE`
* `REDUNDANT`
* `INSUFFICIENT_DATA`
* `UNSTABLE`
* `UNAVAILABLE`

## 9. Metrics, calibration, confidence buckets

**Metrics:**

* Brier and log loss;
* calibration intercept and slope, reliability bins, ECE;
* ROC / PR AUC;
* accuracy and related figures (descriptive; never used for selection).

**Calibration** (`none` / `Platt` / `isotonic`). The calibration observations are the latest 25 % of a fold's
training markets. They are purged, **strictly later** than the model-fit data and earlier than the evaluated block.

Gates:

* Platt needs ≥ 40 calibration markets;
* isotonic needs ≥ 400.

Below its gate a method is `DISABLED`. The economics input is Platt when its gate passed in every fold, otherwise
uncalibrated. This is predeclared, not selected.

**Fixed confidence buckets:** 50–60, 60–70, 70–80, 80–85, 85–90, 90–92, 92–95, 95–97, 97–98, 98–99, 99+. Each bucket
reports:

* n and markets;
* predicted mean and observed frequency;
* calibration error and Brier contribution;
* average executable price and average net edge.

Buckets with fewer than 30 rows or 20 markets are `INSUFFICIENT_SAMPLE`. Wins are written as **"20 / 20"**, never as
a percentage. No 100 %-accuracy claim is possible.

## 10. Executable economics (`feature_eval/economics.py`, SIMULATED)

**Taker only (primary).** Immediate execution crosses the captured book with contemporaneous depth. For each
predeclared size (1, 10, 50 contracts):

* the VWAP walks the captured ask ladder (YES asks = 100 − NO bids);
* if the depth is insufficient the result is `NOT_EXECUTABLE`; depth is never extrapolated.

`net_edge = calibrated_prob − VWAP/100 − fee_per_contract` (probability units per $1 contract; unit conversions
tested). Any uncertainty buffer is reported separately.

**Fees** (`FeeModel` of time-versioned `FeeSchedule`s; exact `decimal.Decimal` arithmetic on exact price and
quantity; Step 6.2):

| Schedule | Effective | Formula | Rounding | Verified |
|---|---|---|---|---|
| `kalshi_general_2026_07_07` | from 2026-07-07 00:00 ET | taker M·0.07·C·P·(1−P), maker M·0.0175·C·P·(1−P) | **fee + position cost rounded UP to a centicent** ($0.0001) | terms as supplied by the project owner (PDF unreachable here) |
| `kalshi_general_pre_2026_07_07` | before 2026-07-07 (start unknown) | 0.07·C·P·(1−P) | fee rounded up to a cent (the earlier behaviour, preserved for old periods) | no |

* A trade always uses the schedule covering its **timestamp**. The July-2026 rounding is never applied to earlier
  trades.
* The multiplier M comes from captured Kalshi metadata. A market / event `fee_multiplier_override` beats the series /
  market `fee_multiplier`. `fee_type_override` must be one the schedule prices.
* The 15-minute crypto series are **not** hardcoded as general-fee markets: the schedule lists excepted products, and
  the captured fee type or override decides for each event.
* **`FEE_VERIFIED`** requires all of: a verified schedule version, the trade time inside an interval with a known
  start, a captured override state, a captured multiplier, and a known rounding rule.
* Otherwise the result is `FEE_UNVERIFIED`, or `FEE_UNKNOWN` when no schedule covers the trade.
* Every schedule, including its source hash and effective dates, is in the fee-model fingerprint.

**Maker EV is `NOT_EVALUATED`.** The fill probability of a resting order is unknown from the displayed book; a maker
EV needs a fill model.

Reported: the distribution of raw and net edge, executable vs not, hypothetical trades and markets, SIMULATED P&L,
return per contract, and grid-level MAE / MFE from later checkpoints' captured bids. Everything is labelled
SIMULATED.

## 11. Leakage (`feature_eval/leakage.py`), tested and mutation-tested

* **Name scan.** No `LABEL_FIELDS`, official result, expiration value, `micro_label.*` or `*fwd_*` column is allowed
  (S6, S7).
* **Value scan.** On TRAIN rows, a feature that separates the outcome almost perfectly (|2·AUC − 1| ≥ 0.999 over ≥ 30
  markets) is treated as leakage. This catches disguised post-event labels (S7).
* **Availability scan.** No checkpoint at or after the close; no unknown label source.
* **Counterfactual labels.** Flipping every settlement outcome changes no feature value (S6).
* **Other guards:** train-only scaler, selector and λ; the holdout never enters a fold; chronological folds.

## 12. Research ledger (`feature_eval/ledger.py`)

The ledger is append-only and hash-chained (`analysis_output/step6_research_ledger.jsonl`). Every dataset build,
development evaluation, synthetic self-test and **FINAL_HOLDOUT access** is recorded.

A holdout access is permanent. A second holdout evaluation of the same dataset fingerprint (for example after
re-tuning) is refused with `HoldoutBurned` (S11). Editing an entry breaks the chain, and `verify()` fails.

## 13. Reproducibility and fingerprints

The **experiment fingerprint** covers:

* the dataset fingerprint;
* the model and its hyperparameters / grids;
* the families;
* the missing-data strategy;
* the calibration method and gates;
* the split config including `purge_ms`, and the ridge inner-split specification (Step 6.1);
* pruning, complexity and decision thresholds;
* the fee-model fingerprint;
* the bootstrap specification;
* the seed (20260928).

All randomness uses seeded `random.Random` instances. The environment (Python, platform) is recorded in the
outputs. There are no third-party packages.

The **separate Step-6 fingerprint** (`config/step6_baseline.json`, `py -m feature_eval.fingerprint --verify`) pins:

* every `feature_eval` module and the three CLIs (canonical AST);
* the frozen-universe fingerprint;
* every predeclared default: split / purge, ablation, pruning, gates, calibration, fees, sizes, buckets, quality,
  Coinbase and label gates, hypotheses.

It also records the existing perp-veto and production-file hashes. Step 6.1 deliberately changed the settlement,
universe and Step-6 fingerprints; see §18 for OLD / NEW / WHY. No other fingerprint changed.

## 14. Synthetic self-test and benchmark (SYNTHETIC ONLY)

`feature_eval/synthetic.py` plants:

* a hidden driver the legacy stand-in does not know (`SPOT_PRICE_MOMENTUM`: signal, exact duplicate, algebraic
  duplicate, constant);
* noise families (settlement, perps, micro with 40 % missing values);
* a Coinbase-tagged feature;
* an execution-state column;
* thin books.

The self-test must find:

* the planted family `PROMISING_RESEARCH_ONLY` (stage 1 and stage 3);
* the noise layers not promising;
* the duplicates and constants structurally removed;
* the Coinbase feature gated;
* overall `SYNTHETIC_ONLY`.

It does. It uses lenient complexity gates for this code test only, and it never selects anything for real data.

`scripts/bench_step6.py` records synthetic throughput and the hardware in `analysis_output/step6_performance.json`.

## 15. Current real-data status

This repository contains **no real captured research sessions**. `py run_step6_research.py` therefore writes
`INSUFFICIENT_DATA` in every real output file. `py research_status.py` lists the unmet predeclared gates:

* ≥ 400 settled independent gold-label markets;
* ≥ 75 per asset;
* ≥ 100 per favoured direction;
* ≥ 200 per checkpoint;
* ≥ 50 per volatility slice;
* ≥ 100 positive and ≥ 100 negative outcomes.

It gives no ETA: data accrues only as fast as sessions are captured with `collect_research_data.py --all-research`.
The Coinbase sequence status is `UNVERIFIED_REAL_FEED` and the settlement convention is `SETTLEMENT_UNVERIFIED` until
real evidence exists.

## 16. Files changed outside `feature_eval/` (OLD / NEW / WHY)

| File | OLD | NEW | WHY |
|---|---|---|---|
| `run_all_tests.py` | `range(1, 22)` | `range(1, 23)` | runs the new stage 22 |
| `test_stage13.py` | `last = 21` | `last = 22` | the master-runner test counts stages |
| `test_stage17.py` | asserts `"range(1, 22)"` | asserts `"range(1, 23)"` | same |
| `.gitignore` | — | whitelists the Step-6 output files | outputs are delivered |
| docs | — | Step-6 sections | documentation |

Step 6 itself changed no production file, strategy constant, perp-veto file or Step 2–5 engine. Step 6.1 changed the
Step-2 settlement engine deliberately, for correctness; see §18.

## 17. Limitations (documented, not hidden)

* No real data yet. Every predictive and economic question is `INSUFFICIENT_DATA`.
* The fee schedule is unverified (`FEE_UNVERIFIED`), and maker economics are not evaluated.
* The legacy probability uses candles rebuilt from captured Coinbase trades. A capture gap makes a candle missing, as
  it would if Coinbase had no trade that minute.
* MAE / MFE are on the checkpoint grid only (not tick level).
* The value-based leakage scan can only flag near-perfect separation. Weaker leaks rely on the causal engines, the
  name scan and the counterfactual-label test.
* Pure-Python models (logistic, ridge, stumps) are deliberately small. The complexity gate keeps them honest; richer
  models need more data, not more code.
* Stability and regime slices need many markets. With little data they are reported as not evaluable.
* (6.1) SOL's current comparator and precision are unverified, so SOL reconstructed outcomes fail closed. Official SOL
  results stay usable.
* (6.1) The BTC / ETH / XRP rule text comes from the project owner's review of the current market pages (2026-09-29).
  kalshi.com could not be opened from the build environment. v1's effective start is unknown.
* (6.1) Exact .5 rounding ties are unresolved by design (tie rule undocumented). Empirical verification against
  official expiration values reports them separately.
* (6.1 → 6.2) The Step-3 synthetic world now issues its SYNTHETIC official results by the contract rule. It carries
  SYNTHETIC rule text; SOL deliberately has no precision sentence.
* (6.2) Real per-market rule text and fee metadata have never been observed from this environment (kalshi.com is
  blocked). The parser accepts only the documented wording; any real deviation fails closed until it is reviewed.
* (6.2) The July-2026 fee terms were supplied by the project owner; the PDF could not be read here. A fee is
  `FEE_VERIFIED` only with captured override and multiplier metadata.
* (6.2) A CF frame received DIRECTLY with a numeric (not string) value has no recoverable decimal text. The float's
  shortest repr is exact up to 15 significant digits.

## 18. Step 6.1 — correctness hardening

**Issue 1: contract settlement semantics** (`settlement/rules.py`; details in SETTLEMENT_ENGINE.md §9a).

* Outcomes come from versioned per-series rules. BTC / ETH / XRP use `GREATER_THAN_OR_EQUAL` ("at least"), so
  equality with the strike after official rounding is **YES**.
* Official precision is 2 dp for BTC and ETH and 4 dp for XRP, with exact decimal rounding.
* An unresolved .5 tie gives no outcome unless both candidates agree.
* SOL: UNVERIFIED, so it fails closed. An unknown series or operator also fails closed.
* The unrounded mean is kept, and labels carry the rule id and fingerprint.
* The rule-set fingerprint `49113ba7c27674b0…` enters the settlement and dataset fingerprints.

**Precision-aware verification** (`settlement/resolution.py`). `VALUE_TOLERANCE = 0.01` is gone. Rows report
separately:

* the exact match after official rounding;
* whether the value is within half an official unit;
* outcome agreement;
* the raw unrounded difference.

A BTC-sized tolerance can no longer validate a 4-dp XRP value (S20).

**Issue 2: ambiguous conventions** (`feature_eval/labels.convention_gate`). A window convention is `VERIFIED` only
when it is the unique passing policy and the verifier reports no tie. Other cases:

* tied / indistinguishable → `SETTLEMENT_UNVERIFIED / AMBIGUOUS_CONVENTION`. The settlement demo world with an exact
  1-s feed is exactly this case: ASOF, EXACT and BUCKET all pass. The old gate would have said VERIFIED.
* preferred fails, another passes → `REQUIRES_VERSIONED_POLICY_MIGRATION`;
* none passes → `NO_CONVENTION_PASSES`;
* too few markets → `INSUFFICIENT_MARKETS`;
* synthetic data → `SYNTHETIC_ONLY`.

Official Kalshi results remain gold independently of this gate.

**Issue 3: ridge λ selection** (`feature_eval/splits.inner_split`, `RidgeLogisticModel`):

* the split is market-level, by close-time group (all four assets of a slot together), and chronological;
* it is purged with the outer rule: lookback 1 500 000 ms + label horizon;
* zero market overlap, chronology and disjoint information windows are asserted;
* each candidate's scaler is fit on inner-training rows only;
* only the rows of the outer fold's training block are used;
* the selected λ is then fit on the full outer training block;
* without market metadata the model does not tune; it uses the most conservative λ;
* the inner-split specification is part of `AblationConfig` and the experiment fingerprint.

**Issue 4: rejected sources in labels** (`feature_eval/dataset.settlement_inputs`). A source/asset pair with a REJECT
quality verdict contributes nothing to any of these:

* convention verification;
* reconstructed values or outcomes;
* official resolution labels (a rejected or untrusted Kalshi resolution path is dropped).

A DEGRADED session keeps its surviving sources. The exclusion counts are in each session's metadata.

**Mutations S15–S20** (all caught; `analysis_output/step6_mutation_results.json`):

* S15: a tied convention becomes VERIFIED.
* S16: row-based inner split.
* S17: the inner split has no purge, and its internal guard is disabled, so the test itself must catch it.
* S18: a rejected CF source leaks back into labels.
* S19: equality under "at least" is not YES.
* S20: a universal 0.01 tolerance is used for values.

**Fingerprints (OLD → NEW, WHY)**

| Fingerprint | OLD | NEW | WHY |
|---|---|---|---|
| settlement | `3eba791cfe8163cc…` | `ba4e50c39ab59359…` | contract rules module; rule-based outcomes; precision-aware verification; engine v2; format v2 pins the rules |
| Step-6 feature universe | `e4c856ba2b63df06…` (v1) | `920c0a8aae81535f…` (v2) | settlement-layer records carry the new settlement source fingerprint |
| Step-6 baseline | `7469fc0fc46eef18…` | `bc7f1b47cae79b88…` | feature_eval modules (label gate, ridge split, rejected-source filter, fees), universe v2, defaults |
| dataset fingerprints | — | include label version `labels_v2` and the rule-set fingerprint | labels semantically changed (no real dataset exists yet) |

The previous baseline files are archived in `config/history/`. The legacy / extended strategy, market-data,
perp-data, microstructure and perp-veto fingerprints are unchanged.

**Earlier-stage test files changed (OLD / NEW / WHY)**

| File | OLD | NEW | WHY |
|---|---|---|---|
| `test_stage18.py` | equality → `AT_STRIKE`, no outcome; `expiration_value_abs_diff`, `within_tolerance` | equality → YES (`AT_STRIKE` informational); exact / half-unit / raw checks | the corrected contract semantics |
| `test_stage21.py` | pins settlement `3eba791c…` | pins `ba4e50c3…` | the deliberate settlement re-baseline |
| `scripts/settlement_validation_report.py` | "EV within tol" | "EV exact @ official precision" | the removed tolerance |

## 19. Step 6.2 — rule provenance, fee schedules, exact CF values

**Issue 1: no retroactive rules.** Details in `settlement/market_rules.py` and SETTLEMENT_ENGINE.md §9b.

* Every Kalshi market object's `rules_primary` / `rules_secondary` is retained verbatim as a snapshot, with:
  * its hash, series and event tickers;
  * capture and update times;
  * source, schema fingerprint and fee metadata.
* The Step-3 collector stores it in MARKET_STATE / RESOLUTION payloads, and the dataset attaches it to each market.
  A rejected `kalshi:<asset>` source contributes none.
* A narrow deterministic parser interprets the snapshot. Rule resolution:
  * the market's own text first; a contradiction with the static rule, the strike or another snapshot →
    `RULE_CONFLICT`;
  * unrecognised text → `RULE_TEXT_UNRECOGNIZED`, never overridden by the static table;
  * the static series rule only within its evidenced period: `RULE_CURRENT_OBSERVED` for closes at or after its
    observation date, `RULE_HISTORICALLY_UNVERIFIED` (diagnostic only) for earlier closes;
  * otherwise fail closed.
* `market_label()` gives `RECONSTRUCTED_VERIFIED` only with a gold rule status. Otherwise the result is
  `RULE_UNVERIFIED_FOR_MARKET` (non-gold). Official results are unaffected.
* Each dataset's fingerprint includes every market's rule-text hash, status and rule fingerprint. A changed snapshot
  changes the dataset, and the cache is invalidated through the raw-store checksums.
* SOL stays UNVERIFIED unless a SOL market's own text says "at least" and "nearest 4 decimal places".

**Issue 2: fee schedule** (§10). Exact Decimal fees. The July 7, 2026 general schedule rounds fee + position cost up
to a centicent; earlier trades keep cent rounding. Overrides take precedence. `FEE_VERIFIED` only with every condition
met.

**CF exact decimals.** The CF value's original text is carried to the official rounding. The regression case is
`99999.994999999999999`: exact → 99999.99 (NO against 100000); float → a .5 tie.

**Mutations S21–S27** (all caught):

* S21: a current rule is applied retroactively and gives a gold label.
* S22: the static rule silently beats a conflicting `rules_primary`.
* S23: the dataset fingerprint ignores the rule hash.
* S24: the July-2026 schedule rounds the fee to cents.
* S25: the fee-multiplier override is ignored.
* S26: the fee schedule is chosen without regard to the trade time.
* S27: settlement values take a lossy float round-trip.

**Fingerprints (OLD → NEW, WHY)**

| Fingerprint | OLD | NEW | WHY |
|---|---|---|---|
| settlement | `ba4e50c39ab59359…` | `4884524a795f0e2c…` | `market_rules.py`; rule-status-gated outcomes; `value_text`; snapshot retention in `parse_market`; checkpoint label fields |
| market-data | `969cec83e8b9912e…` | `609905956c9e6120…` | versioned metadata extension (`KALSHI_METADATA_VERSION` 2): contract snapshot in payloads, one event GET, synthetic rule text. No feature changed |
| feature universe | v2 `920c0a8aae81535f…` | v3 `edef198fb82913f2…` | the settlement source fingerprint changed |
| Step-6 baseline | `bc7f1b47cae79b88…` | `2344b12c3ab80360…` | label gating, rule hashes in the dataset fingerprint, fee model v3, labels v3, universe v3 |
| fee model | `fee_model_v2` | `fee_model_v3` `b1b0843cf51ae0cc…` | time-versioned schedules, centicent rounding, override handling |
| dataset fingerprints | — | now include `market_rules` and `labels_v3` | per-market rule provenance |

The previous files are archived in `config/history/`. The legacy / extended strategy, perp-data, microstructure and
perp-veto fingerprints are unchanged.
