# Market microstructure (Step 5, research only)

`microstructure/` reconstructs price-level order books for spot, perpetual and Kalshi binary markets. It derives
causal microstructure features from them and joins them with the Step 2–4 research layers. It is
**observation / research code only**:

* no production call, probability, confidence, threshold, entry window, stop, edge calculation, position size or
  execution path reads it;
* the existing Kalshi-perp veto chain never sees it;
* nothing here places an order;
* every request is a GET or a public / read-only websocket subscription.

Stage 21 (`test_stage21.py`) enforces all of this.

> Step 5 is the **last feature-building step**. After it, no more feature families are added. Step 6 must
> evaluate and ablate the families that now exist (see the end of this document).

---

## 1. Audit — what existed before Step 5

The codebase was audited before any implementation. Vocabulary:

* **TOP-OF-BOOK ONLY** — best bid / ask (+ sizes) only;
* **SNAPSHOT** — each message is a complete top-N book;
* **TRUE INCREMENTAL BOOK** — snapshot + deltas that must be applied in sequence;
* **POLLING** — periodic REST requests.

A top-of-book quote is never called an order book.

| Existing capability (Steps 3–4) | Classification | Sequence handling | Reconnect / backfill |
|---|---|---|---|
| Coinbase Exchange `ticker` (Step 3) | TOP-OF-BOOK ONLY | none (trade ids for trades) | reconnect; trades backfilled by REST |
| Kraken v2 `ticker` bbo (Step 3) | TOP-OF-BOOK ONLY | none | reconnect |
| Kalshi REST `/markets/{t}/orderbook?depth=10` every 1 s (Step 3) | POLLING of a SNAPSHOT (top 10 bids per side) | none (each poll complete) | next poll |
| Kalshi REST trades (Step 3) | POLLING | trade ids (dedup) | next poll |
| Binance `bookTicker` (Step 4) | TOP-OF-BOOK ONLY | `u` recorded | reconnect |
| Binance `depth{5,10,20}@100ms` (Step 4) | SNAPSHOT (partial book) | `u` / `pu` chain → SEQUENCE gap record | each message complete → self-healing |
| Bybit `orderbook.50` (Step 4) | TRUE INCREMENTAL BOOK, merged inside the adapter and emitted as top-N | `u` recorded, contiguity not documented | snapshot on (re)subscribe |
| Bybit `tickers` bid1 / ask1 (Step 4) | TOP-OF-BOOK ONLY | — | — |
| OKX `books5` (Step 4) | SNAPSHOT (5 levels) | none needed | each push complete |

Existing book features:

* Step 3: the Kalshi depth imbalance (from the polled top-10 snapshot).
* Step 4:
  * `book_top_imbalance`;
  * depth imbalance 5 / 10;
  * weighted mid;
  * spread / depth / pressure change.

All of these come from top-of-book or snapshot data. **No existing component maintained a local incremental
book, verified sequence continuity across a full book, or distinguished PRICE-LEVEL DEPTH from ORDER-LEVEL
QUEUE.**

**PRICE-LEVEL DEPTH vs ORDER-LEVEL QUEUE.** Every Step-5 book is a PRICE-LEVEL aggregate (size per price).
None of the public feeds used here identifies individual orders. So:

* no queue position is ever computed or implied;
* `LocalBook` exposes none;
* OKX's per-level order *count* is not a queue position and is not used.

## 2. Architecture

```
venue websockets (+ GET snapshots) ──► adapters (microstructure/sources/*) ──► MicroEvents
        │ raw text first (kind "raw", stream "ws#<connection>")                  │
        ▼                                                                        ▼
<session>/micro/ (Step-3 gzip JSONL store) ◄── MicroCollector ──► BookReconstructor (LIVE: gap → resnapshot)
        │
        ▼ replay (deterministic)
BookReconstructor (same code) ──► MicroFeatureEngine (+ Step-3 spot trades, Step-4 perp trades / contract values)
        │                                     │
        ▼                                     ▼
POST-EVENT labels (labels.py, separate)   joint dataset Step 2 + 3 + 4 + 5 (dataset.py)
```

Reused infrastructure:

* the event-time model (`event_ts`, `receive_ts`, `receive_mono_ns`, `ingest_seq`, LIVE / BACKFILLED);
* the append-only store, feed runners, backoff and feed health;
* gap records, the replayer and the status vocabulary;
* Step 4's GET poller.

Nothing in `market_data/`, `settlement/` or `perp_data/` was modified (their fingerprints verify unchanged).

## 3. Book venues and continuity

| Book | Channel | Classification | Sequence policy | Resnapshot |
|---|---|---|---|---|
| `coinbase_l2` | Coinbase Advanced Trade `level2` (+ `heartbeats`), public | TRUE INCREMENTAL BOOK | `chain_contiguous`: `sequence_num` +1 per message on the **connection** | reconnect |
| `kraken_book` | Kraken v2 `book` (+ `instrument`), depth 10/25/100/500/1000 | TRUE INCREMENTAL BOOK | `checksum`: CRC32 of the top 10 after every update (needs the pair's precisions); the book is truncated to the subscribed depth | reconnect |
| `binance_usdm_book` | `<s>@depth@100ms` + `GET /fapi/v1/depth` | TRUE INCREMENTAL BOOK | `binance_diff`: buffer → drop `u < lastUpdateId` → first `U ≤ L ≤ u` → `pu` = previous `u` | new GET snapshot |
| `bybit_linear_book` | `orderbook.{1,50,200,500}` | TRUE INCREMENTAL BOOK | `monotonic_only`: `u` must increase; `u = 1` restart snapshot resets. **A lost delta is not detectable** (contiguity undocumented) | reconnect |
| `okx_swap_book` | OKX `books` (400 levels) | TRUE INCREMENTAL BOOK | `prev_chain`: `prevSeqId` = previous `seqId` (the checksum is deprecated, always 0) | reconnect |
| `kalshi_ws` | Kalshi `orderbook_delta` + `trade` (read-only authenticated) | TRUE INCREMENTAL BOOK | `chain_contiguous`: `seq` +1 per message on the **subscription** (`sid`) | resubscribe (new sid) |

Chain ids are per connection or subscription (`coinbase_l2:c<conn>`, `kalshi_ws:c<conn>:sid<sid>`). The
adapter reports, for every book event, the previous sequence number on the chain — or the last one before a
discontinuity. So a message lost anywhere on the chain is detected at the next book event, even when:

* the lost message was a Coinbase heartbeat, or
* it belonged to another product or market.

A chain break invalidates **every** book on that chain.

## 4. Book states, gaps and resnapshots

`BookReconstructor` (the single implementation, used live and in replay) keeps each book in one of these states:

* `NO_BOOK`
* `AWAITING_SNAPSHOT`
* `WARMING_UP` — a snapshot arrived less than `warmup_ms` (1 s) ago
* `READY`
* `STALE` — no message from the source for longer than the venue threshold (5 s; Kalshi 120 s)
* `INVALID` — crossed or locked book, checksum mismatch, negative level, non-monotonic id, or `BOOK_RESET`
* `NEEDS_RESNAPSHOT` — a sequence gap

Once INVALID or NEEDS_RESNAPSHOT, a book is **never continued**; only a snapshot from a trustworthy chain
restores it.

Recovery path (live): invalid → READY statuses stop → resnapshot → WARMING_UP → READY.

* Binance books are queued for a GET snapshot (`SnapshotPoller`, ≤ 1 per book per second).
* Every other venue raises `ResnapshotRequired` after writing everything. The feed runner reconnects, and the
  venue sends fresh snapshots.
* On a disconnect the collector writes a `BOOK_RESET` event, so replay invalidates the book at the same
  receive time.

There is **no retroactive repair**:

* each book keeps an append-only list of VALID INTERVALS;
* a resnapshot opens a new interval at its own receive time;
* windowed features need the book to have been valid over the whole window, so a window that spans an outage
  stays NOT_READY until it lies entirely after the resnapshot.

## 5. Kalshi YES / NO conventions (`microstructure/kalshi.py`)

Kalshi lists **bids only** on both sides. Books are stored in YES terms, with the native NO side kept.

* YES bid = best YES bid
* YES ask = 100 − best NO bid
* NO bid = best NO bid
* NO ask = 100 − best YES bid
* YES spread = NO spread = 100 − YES bid − NO bid
* The size at the YES ask is the size of the best NO bid, and vice versa.

Unit conversions are explicit:

* `dollars_to_cents`
* `cents_to_prob` (price scaled to [0, 1] — not a calibrated probability)
* `prob_to_cents`

Out-of-range values are rejected, so cents and probabilities are never mixed implicitly.

Trades are expressed in YES terms: `taker_side` "yes" → aggressor buy; "no" → aggressor sell.

`executable_state()` returns YES / NO bid / ask prices and the size available at each.

## 6. Features (615 registered, ten families)

Every feature has a name, family, venue, window, unit and description (`microstructure/features/definitions.py`).
Values are None unless READY (Step 3's statuses). Counts per family:

| Family | Features |
|---|---|
| SPOT_BOOK | 82 |
| PERP_BOOK | 123 |
| KALSHI_BOOK | 31 |
| ORDER_FLOW | 78 |
| TRADE_INTENSITY | 56 |
| LIQUIDITY | 167 |
| SWEEP | 36 |
| REPLENISHMENT | 17 |
| TOXICITY | 17 |
| CROSS_VENUE_MICROSTRUCTURE | 8 |

**Time axis: RECEIVE time.** The book at T is the book after every book event received ≤ T. Windows are
(T − w, T] by receive time. Venue event times are only used for latency telemetry.

### Book state

Per exchange book (Coinbase, Kraken, Binance, Bybit, OKX):

* best bid / ask, mid, spread, relative spread (bps);
* microprice = (bid·ask_qty + ask·bid_qty) / (bid_qty + ask_qty), and microprice − mid;
* bid / ask depth over the best 1 / 5 / 10 / 20 levels, and imbalance_N = (B − A) / (B + A) (unit-free);
* total depth 10;
* distance to the next level;
* depth concentration (L1 / depth 10);
* depth slope: least-squares, through 0, of cumulative depth vs distance from mid;
* over 5 / 15 / 60 s: imbalance change, spread change and microprice change (velocity);
* microprice acceleration at 5 / 15 s;
* telemetry: median latency, update count, book age.

### Liquidity

* Notional within 10 / 25 bps of mid.
* Depth ADDED / REMOVED at levels ranked < 10 (rank taken before each change) over 1 / 5 / 15 / 30 / 60 s.
  Removed depth mixes cancellations and executions, so it is **not called cancellation**.
* Net depth-10 change over 5 / 60 s.
* depth10 / 5-min median and spread / 5-min median, from per-second as-of samples while the book was valid.
* `liquidity_vacuum` = 1 when depth ratio < 0.5 AND spread ratio > 2.

### Order-flow imbalance (OFI)

Cont, Kukanov and Stoikov (2014). For book update n, with best bid b (size qb) and best ask a (size qa):

```
e_n = qb_n·1[b_n ≥ b_{n−1}] − qb_{n−1}·1[b_n ≤ b_{n−1}] − qa_n·1[a_n ≤ a_{n−1}] + qa_{n−1}·1[a_n ≥ a_{n−1}]
ofi_l1.w = Σ e_n over updates received in (T − w, T]
```

Multi-level OFI (`mlofi_5`) applies the same term to the m-th best level (m = 1..5) and sums it: the aggregate
form of Xu, Gould and Howison (2018). Windows are 1 / 5 / 15 / 30 / 60 s.

### Cancel / execution decomposition — an ESTIMATE only

* `est_cancel_<side>.w = max(0, removed_<side>.w − same-side aggressive volume.w)`
* `est_exec_share_of_removed.60s` = traded volume / removed depth.

Trades and books arrive on different connections and trades may execute below the top 10, so these are
labelled ESTIMATE and never presented as exact.

### Trade intensity

Trades come from Step 3 (Coinbase / Kraken), Step 4 (Binance / Bybit / OKX) and the Kalshi websocket.

* counts over 1 / 5 / 15 / 60 s;
* rate ratio 5 s / 60 s;
* buy − sell volume imbalance over 5 / 60 s;
* median inter-arrival time;
* large trades.

A **large** trade is defined causally: above the 95th percentile of the venue's trade sizes received in
(T − 15 min, T − 60 s], with at least 50 trades (else NOT_READY). Future trades never move the threshold.

### Sweeps (evidence-based)

A sweep needs both:

* ≥ 2 levels better than the new best are cleared in **one** update, AND
* same-side trades received ≤ 1 s earlier reach the 2nd cleared level and account for ≥ 50% of the cleared size.

A repricing without trades is not a sweep. Reported:

* count over 60 s;
* for the last sweep: direction, levels cleared, notional, age, duration, and impact (mid change across the
  update).

### REPLENISHMENT_PATTERN

A best level is REPLENISHED when:

* its size dropped while same-side trades printed at that price within 1 s, and
* within 2 s it was restored at the **same price** to ≥ 80% of its pre-depletion size.

Reported: counts per side over 60 s and the longest run at one price. This is a pattern count. It is never
called a confirmed iceberg (hidden size is not observable in these feeds).

### Toxicity

* `vpin_style_50` — **VPIN-STYLE, not canonical VPIN.**
  * The bucket volume is session-relative: V = the venue's volume in its first 10 observed minutes / 100, then
    fixed.
  * Trades are classified by their AGGRESSOR flags, not bulk-volume classification.
  * VPIN = Σ |buy − sell| over the last 50 complete buckets / (50·V).
  * NOT_READY until V is set and 50 buckets are complete.
* Trade-sign lag-1 autocorrelation over 60 s (≥ 20 signed trades).
* Adverse move: the mean over trades received in [T − 65 s, T − 5 s] of sign·(mid(t + 5 s) − mid(t)) / mid(t)
  (bps; Kalshi: cents). Every mid used is received ≤ T.

### Kalshi book

For the row's market, in YES terms:

* executable state: YES / NO bid / ask and their sizes;
* spread, mid, microprice;
* YES / NO depth and imbalance over 1 / 5 / 10 levels;
* changes over 15 / 60 s;
* liquidity within 2 / 5 cents;
* depth added / removed;
* OFI;
* trades (counts, imbalance, VWAP, last-trade age, large trades);
* sweeps, replenishment, sign autocorrelation, adverse move (cents).

### Cross-venue microstructure

* spot mid dispersion (Coinbase vs Kraken, both USD);
* perp mid dispersion (all USDT);
* median imbalance_10 for spot and perp;
* imbalance-sign agreement;
* total ofi_l1;
* number of READY books.

USD and USDT prices are never compared inside this family.

### Missing data and units

* A missing, stale or invalid book gives **None + MISSING**, never 0; warm-up gives NOT_READY.
* OKX sizes are CONTRACTS. Coin / notional features are UNAVAILABLE until a VERIFIED contract value arrives
  from Step-4 INSTRUMENT events. Unit-free features (imbalance, microprice, spread) are READY regardless.

### Latency telemetry

`latency_median_ms.60s` = median of receive_ts − venue event_ts. It mixes transport delay with the offset
between the local clock and the venue's clock: receive_ts is the local wall clock, NTP quality at best. The
manifest records the per-venue distribution and the share of negative values, which reveal clock offsets.
Sub-10-ms differences are not interpretable.

### Feature-count control

* The windows are the brief's (1 / 5 / 15 / 30 / 60 s) only where it asks for them (deltas, OFI).
* Other features use two or three horizons.
* Kalshi and cross-venue features use a reduced set.
* The registry is capped by test (≤ 650).
* `names_by_family()` provides the grouping for family-level ablation.
* No feature selection has been done.

## 7. Causality, leakage and labels

A book update, trade, snapshot, resnapshot or gap notice is usable at T **iff `receive_ts ≤ T`**.

* The engines refuse out-of-order ingestion and `features_at(T)` with T earlier than the last receive time
  (`CausalityError`).
* The streaming path is tested equal to the batch path (`compute_at`) at many T.
* Canaries (an absurd book, trade or Kalshi trade received at T + 1 ms) never change features at T.

**Price impact is a POST-EVENT RESEARCH LABEL** (`microstructure/labels.py`), never a feature:

* per trade: signed impact at 250 ms / 500 ms / 1 s / 5 s;
* per checkpoint: forward mid moves.

Each label carries `available_at_ms`. It is written to `micro_labels.jsonl`, separate from `features.csv`.
Nothing in `microstructure/features/` imports the labels, lead-lag or dataset modules (tested).

## 8. Offline lead-lag (`microstructure/leadlag.py`, `scripts/lead_lag_analysis.py`)

Sources: CF RTI, Coinbase, Kraken, Binance, Bybit, OKX and the Kalshi YES mid, on a receive-time grid.
Cross-correlations of returns are computed at lags −5 s .. +5 s.

* The period is split **chronologically** into discovery (default 60%) and holdout (after an embargo of
  max |lag|).
* The best lag is chosen on discovery only; holdout correlations at that lag and at 0 are reported.
* The holdout never selects anything (tested: scrambling the holdout leaves the chosen lag unchanged).
* It is not a live feature.

## 9. Sub-second checkpoints (`microstructure/grid.py`)

Grids of 100 / 250 / 500 / 1000 ms are supported. A source is included only if its native update interval is
≤ the step. For example, the CF RTI (~1 s) and Kalshi REST polling (1 s) are **excluded** from the
100 / 250 / 500 ms grids and listed as excluded — never forward-filled. Event-driven books are eligible at
every step.

## 10. Joint dataset (Steps 2 + 3 + 4 + 5)

`microstructure.dataset.build_joint_dataset` makes ONE pass over the merged stream in
(receive_ts, ingest_seq, family) order.

* The Step-3, Step-4 and Step-5 engines see exactly the events received ≤ T at each checkpoint.
* The Step-3 / Step-4 values are identical to the Step-4 builder's (tested).

Files:

* `features.csv` (or jsonl);
* `labels.jsonl` — Step-2 settlement labels;
* `micro_labels.jsonl` — post-event labels;
* `provenance.json` — fingerprints, configs, feature definitions, family counts, inputs and
  `feeds_production: false`.

```
py scripts/build_micro_dataset.py market_data_sessions\<session> --assets BTC --out analysis_output\joint_btc
py scripts/build_micro_dataset.py <session> --assets BTC --grid-ms 250 --out analysis_output\grid_btc
py scripts/build_micro_dataset.py <session> --assets BTC --trade-labels binance_usdm_book --out analysis_output\impact
```

## 11. Collection, storage and retention

```
py collect_research_data.py --dry-run --all-research                  # plan + offline code checks, no network
py collect_research_data.py --assets BTC,ETH,SOL,XRP --all-research   # Steps 3 + 4 + 5 in one session
py collect_research_data.py --assets BTC --micro --micro-venues coinbase_l2,binance_usdm_book \
    --binance-book-depth 1000 --kraken-book-depth 25 --bybit-book-depth 200 \
    --compression-level 9 --segment-mb 64 --segment-minutes 60
py scripts/replay_microstructure.py market_data_sessions\<session> --digest --renormalize
py scripts/prune_sessions.py market_data_sessions --keep-days 14 [--delete]
```

Storage behaviour:

* **Depth and venues:** selectable per venue; `--micro-venues` limits bandwidth.
* **Compression:** gzip level 1–9.
* **Segment rotation:** by size and / or time.
* **Retention:** `scripts/prune_sessions.py` removes whole completed sessions older than N days; dry run by
  default.
* **Raw first:** every raw message is stored (≤ 4 MiB each; a longer message is counted in `drops`) with
  its connection number. Books can therefore be rebuilt exactly, and `--renormalize` proves the stored raw
  text regenerates the stored events.
* **No silent drops:** nothing is sampled or dropped. An overload is recorded with `record_overload`, which
  writes a `BOOK_RESET` plus a gap and invalidates the books. Venue-side losses surface as sequence gaps.
* The micro manifest reports storage bytes and a bytes-per-hour estimate.
* Kalshi `kalshi_ws` needs the read-only Kalshi API key already used for CF-via-Kalshi. Without it, that
  source is disabled and the others continue.

## 12. Benchmark (SYNTHETIC — see `analysis_output/microstructure_performance.json`)

Setup: 4 assets (BTC, ETH, SOL, XRP), 300 simulated seconds, all six books, with the Step-3 / Step-4
synthetic collectors in the same loop, on CPython 3.11.15. These are **synthetic** numbers: real message
rates and sizes differ.

Throughput and latency:

| Measure | Result |
|---|---|
| Collector (parse + raw store + live reconstruction, including the Step-3 / Step-4 collectors) | 263 raw msg/s, 262 events/s |
| Book-update latency (reconstructor apply) | p50 9.42 µs, p99 143.49 µs (80210 events) |
| Feature row, 615 features | p50 42.31 ms, p99 53.1 ms |
| Feature-engine ingest | 14176 events/s |
| Replay | load 17.87 s; rebuild 30169 events/s |
| Peak Python memory | collection 29.7 MiB; feature engine 9.5 MiB |

Disk:

* 452.3 B per raw message stored (raw + event, gzip level 6);
* compression ratio 7.0 / 8.6 / 9.2 at levels 1 / 6 / 9.

Projection with ASSUMED real rates (10–15 msg/s per symbol for the exchange books, 2 for Kalshi; 4 assets):
**about 19 GiB/day**.

* Coinbase dominates (the synthetic Coinbase messages carry a timestamp per level).
* Use `--micro-venues`, depth, `--compression-level 9` and `scripts/prune_sessions.py` to bound it.
* Measure the real bytes/hour from the first real session's `micro/manifest.json` (`store.estimate`).

## 13. Tests and mutations

Stage 21 (32 checks) covers:

* the audit vocabulary;
* the local book;
* states;
* every sequence policy;
* Kalshi units;
* all six adapters;
* the collector (raw first, resnapshot, BOOK_RESET, GET-only snapshots);
* injected faults per venue, with recovery;
* replay determinism and renormalization;
* every feature family's arithmetic;
* missing / warm-up;
* causality;
* no retroactive repair;
* leakage canaries;
* labels;
* lead-lag;
* grids;
* the joint dataset;
* the CLI;
* storage;
* the fingerprint;
* docs;
* performance bounds;
* all previous stages.

`scripts/mutation_test_microstructure.py` breaks eleven rules, one at a time, in a temporary copy. Each runs
as-is and with the engine guards disabled:

1. event time used instead of receive time
2. a future book update visible
3. sequence gaps ignored
4. a resnapshot retroactively repairs history
5. all removed liquidity labelled cancellation
6. future price impact leaks into the features
7. Kalshi YES / NO conversion reversed
8. a missing book becomes zero imbalance
9. a crossed book accepted as healthy
10. a Step-5 feature imported by production
11. a Step-5 feature imported by the existing perp veto

Result (`analysis_output/microstructure_mutation_results.json`): **11 / 11 caught**, both as-is and with the
guards disabled. Controls (unmutated; guards disabled only) pass.

The separate fingerprint lives in `config/microstructure_baseline.json` (`py -m microstructure.fingerprint --verify`).
It pins:

* the modules, feature registry, config and venue table;
* the hashes of the existing perp-veto chain and of the production files.

## 14. Limitations (documented, not hidden)

* **Bybit:** delta contiguity is not documented, so a lost delta cannot be detected from the stream. It
  surfaces only indirectly (e.g. a later crossed book). Flag: SEQUENCE_UNVERIFIABLE.
* **Kraken:** without the instrument precisions the checksum is not verified (flag CHECKSUM_UNVERIFIED).
* **Coinbase:** the level2 book comes from Advanced Trade, while Step 3's Coinbase trades come from the
  Exchange feed. They share a matching engine but not a sequence space.
* **Clocks:** latency and cross-venue timing include clock offsets (see §6).
* **Synthetic only:** all numbers here are synthetic. No real session has been captured in this environment,
  so nothing is claimed about predictive value.
* **Kalshi roll-over:** new markets are subscribed on the open connection (new sid). Their snapshots start a
  new chain.

## 15. Step 6 recommendation

**Do not add indicators.** Begin feature-family evaluation and ablation on real captured sessions:

1. Capture several weeks of Step 3 + 4 + 5 sessions (`--all-research`), with retention configured.
2. Build joint datasets with fixed, pre-declared checkpoints.
3. Evaluate families with walk-forward, chronologically split tests against the frozen legacy model as the
   baseline.
4. Treat each of the ten Step-5 families, plus the Step-3 / Step-4 families, as a unit of ablation:
   * pre-declare the metrics (log loss / Brier vs baseline, calibration);
   * correct for multiple comparisons;
   * report the families that add nothing.
5. Only families that show out-of-sample incremental value may proceed, through the existing shadow →
   validation → manual promotion discipline.

Nothing in Step 5 changes a production call.

## Sources (venue documentation verified for this step)

* Kalshi orderbook updates: https://docs.kalshi.com/websockets/orderbook-updates
* Kalshi public trades: https://docs.kalshi.com/websockets/public-trades
* Coinbase Advanced Trade websocket channels: https://docs.cloud.coinbase.com/advanced-trade/docs/ws-channels
* Kraken v2 book and checksum guide: https://docs.kraken.com/api/docs/guides/spot-ws-book-v2/
* Kraken book checksum v2: https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2
* Binance diff book depth stream: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams
* Binance local order book procedure: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
* OKX order book checksum deprecation: https://www.okx.com/en-us/help/okx-order-book-channels-checksum-field-deprecation
