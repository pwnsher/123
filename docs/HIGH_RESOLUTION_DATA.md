# High-resolution market data + causal research features (Step 3)

**Research / observation only.** Nothing in `market_data/` can create, veto, flip, size or delay a
production call. `kalshi_dashboard.py`, `kalshi_core/`, `run_local.py`, the perp code and the
settlement package never import `market_data` (stage 19 test 3). The production model's 60-minute
volatility, thresholds, entry window and stops are untouched (Step-1 fingerprints and 47 fixtures
verify, stage 19 test 1). No orders: every Kalshi request is a `GET`, and the only signed request is
the read-only websocket upgrade (`transport/kalshi_auth.check_request`).

```
RAW MESSAGES ──> NORMALIZATION ──> EVENT STORE ──> CAUSAL REPLAY ──> FEATURE ENGINE ──> FEATURE DATASET
 (exact text)    sources/*.py      storage.py      replay.py        features/engine.py  features/dataset.py
                 MarketEvent       gzip JSONL       arrival order    receive_ts <= T      features | labels | provenance
```

Raw data is stored **first** (kind `raw`, the exact text received), normalized events second, and
features are only ever computed from the stored events.

## 1. What exists and what was verified

| Item | State |
|---|---|
| Asset → CF index id (BTC→BRTI, ETH→ETHUSD_RTI, SOL→SOLUSD_RTI, XRP→XRPUSD_RTI) | **DOCUMENTED** (`settlement/assets.INDEX_ID_PROVENANCE`; logged re-baseline in SETTLEMENT_ENGINE.md) |
| Settlement **window** convention | **NOT verified.** The candidate framework of Step 2 is unchanged; `window_policy().verified is False`. Nothing here can mark it verified. |
| Parsers for CF (via Kalshi and direct), Coinbase, Kraken, Kalshi REST | built and tested on payloads **shaped like** the documented messages |
| A capture against the real endpoints | **not performed.** The build environment's network policy blocks kalshi.com, cfbenchmarks.com, coinbase.com and kraken.com. The first real run is the next step (§14). |

All test / benchmark data is synthetic (`market_data/synthetic.py`) and labelled so: session ids
start with `SYNTHETIC-`, the manifest carries a note, and the settlement-store session is marked
`synthetic: true` so the Step-2 validation report excludes it.

## 2. Sources

| Source | Module | Transport | Events | Auth |
|---|---|---|---|---|
| A. CF RTI via Kalshi | `sources/cf.py` `CfViaKalshiAdapter` | `wss://api.elections.kalshi.com/trade-api/ws/v2`, channel `cfbenchmarks_value`, `index_ids` | `INDEX_VALUE`, `PUBLISHED_AVERAGE` | Kalshi API key (RSA-PSS signed upgrade, read-only) |
| A. CF RTI direct | `sources/cf.py` `CfDirectAdapter` | `wss://www.cfbenchmarks.com/ws/v4`, `{"type":"subscribe","id":<index>,"stream":"value"}` | `INDEX_VALUE` | `CFB_API_ID` / `CFB_API_SECRET` |
| B. Coinbase Exchange | `sources/coinbase.py` | `wss://ws-feed.exchange.coinbase.com` channels `ticker`, `matches`, `heartbeat`; REST `/products/{p}/trades` backfill | `QUOTE`, `TRADE`, `HEARTBEAT` | none |
| C. Kraken (secondary) | `sources/kraken.py` | `wss://ws.kraken.com/v2` channels `trade` (snapshot) and `ticker` (`event_trigger: bbo`) | `TRADE`, `QUOTE`, `HEARTBEAT` | none |
| D. Kalshi markets | `sources/kalshi.py` + `kalshi_poller.py` | public REST (same base as the dashboard): `/markets`, `/markets/{t}/orderbook`, `/markets/trades`, `/markets/{t}` | `MARKET_STATE`, `BOOK`, `TRADE`, `RESOLUTION` | none |

CF data reuses the Step-2 parsers (`settlement.cf_live.parse_kalshi_cfb_message`, `parse_cfb_frame`)
and the Step-2 `SettlementObservation` type (carried inside each `INDEX_VALUE` payload). **Coinbase is
never substituted for CF**: only the two CF adapters feed the engine's CF stream (stage 19 test 8).
Without CF credentials the CF source is `DISCONNECTED` with the reason, and every CF-dependent
feature is `MISSING`/`NOT_READY`. It is never filled from spot.

### Semantics per venue (documented behaviour the parsers rely on)

* **Coinbase.** `match`: `side` is the **maker** order's side, so the aggressor is the opposite side
  (`INVERTED_MAKER_SIDE`). `last_match` (sent on subscribe) is a trade from before we connected, so it is
  stored as `BACKFILLED` + `SNAPSHOT`. `ticker` supplies best bid/ask (+ sizes) and is used as the BBO.
  A full `level2` book needs authentication and is not used. `heartbeat.last_trade_id` detects missed
  trades. Trade ids are contiguous per product, so id gaps are exact and recoverable via REST backfill.
  Times are RFC3339 with microseconds, UTC.
* **Kraken v2.** Trade `side` is the **taker** side (`TAKER_SIDE_FIELD`). The subscription snapshot (the
  last 50 trades) is `BACKFILLED` + `SNAPSHOT`. The `ticker` message has **no event timestamp** in
  the documented schema: quotes get `event_ts = None` + `EVENT_TIME_MISSING` and are ordered by
  receive time. A `timestamp` field is used if one is present. Heartbeats arrive about once per second
  and carry no symbol (`asset = "*"`, liveness only). Kraken was chosen because it is a long-running
  regulated USD venue with deep BTC/ETH/SOL/XRP books and explicit taker-side and µs-timestamp
  semantics.
* **Kalshi REST.** Market objects carry no update time, so `MARKET_STATE` / `BOOK` get `event_ts = None`
  and are ordered by receive time (the poll response). Prices are converted to YES **cents**. A
  missing ask is derived as 100 − opposite bid and flagged `DERIVED_ASK`. The book's YES asks are
  derived from NO bids. Trades: `taker_side` `yes` → `buy`, `no` → `sell` (YES terms). Trades returned
  by the first poll of a market (after a start or reconnect) are `BACKFILLED`. The settled result and
  `expiration_value` are fetched once, from close + 90 s (retried every 30 s, up to 30 min), and become a
  `RESOLUTION` event, which is a **label**.
* **Websocket behaviour.** One stdlib RFC 6455 client (`transport/ws.py`: TLS verification,
  `HTTPS_PROXY` CONNECT, automatic pong, fragments, close handshake). A read timeout never loses data:
  frames are consumed only when complete (a mid-frame timeout used to desync the stream; this was
  found and fixed in Step 3, see test 11).

## 3. The event envelope (`types.MarketEvent`)

`source, asset, event_type, symbol, event_ts_ms (source time or None), receive_ts_ms (local UTC wall
clock at arrival), receive_mono_ns (monotonic), ingest_seq (global arrival counter), source_seq,
mode (LIVE | BACKFILLED), flags, payload, session_id, schema_version`. Payload fields are fixed per
type (`PAYLOAD_FIELDS`). A missing field is a construction error. Missing **values** are `None`
(never 0). `series_ts_ms = min(event_ts, receive_ts)` orders values inside a stream. The clamp handles
a venue clock that is ahead of ours: an event cannot have happened after we received it.

Normalization (`normalization.py`): prices must be finite and > 0, sizes finite and ≥ 0, and anything
else is a recorded **parse failure** (kind `failure`), never a guessed value. ISO times must carry a
zone. Any number of fractional digits is accepted, parsed with exact integer arithmetic (Python 3.10
safe). Numeric epochs must be plausible milliseconds.

**Clocks.** Wall time is used for alignment. The monotonic clock (`clock.py`) is used for latency,
staleness and reconnect durations. `ClockMonitor` records `WALL_BACKWARDS` / `WALL_JUMP` anomalies
into the manifest.

## 4. The causal rule

```
an event is usable at T  ⇔  receive_ts_ms <= T          (alignment.available)
```

An event time before T does **not** make data available if it arrived after T. This covers a delayed
trade, an amended CF value, a late Kalshi book and a delayed venue quote. Cross-source features use,
per source, the newest value **received** by T (never aligned by event time). The engine enforces the
rule structurally:

* `FeatureEngine.ingest()` requires availability order (non-decreasing `receive_ts`), else `CausalityError`;
* `features_at(T)` refuses a T earlier than any ingested receive time (`CausalityError`);
* `RESOLUTION` events are never stored (they are labels);
* gaps become visible at the time they were detected (`Gap.known_at_ms`).

A late event lands in its **event-time place** inside its stream, but only from the moment it was
received (test 25).

## 5. Storage (why gzip JSONL)

`<output>/<session_id>/segment-NNNNNN.jsonl.gz` + `manifest.json`.

* One record per line: `{"k": kind, "d": data, "c": crc32}`. The kinds are `raw`, `event`, `failure`,
  `gap`, `feed` and `note`.
* Every flush (500 lines, or on tick/close) appends **one complete gzip member** and fsyncs it. A crash
  loses at most the unflushed tail. A torn last member is detected and reported, and the members before
  it stay readable.
* A new run never appends into an old segment. Segments rotate at 64 MB.
* Readers report CRC mismatches, invalid JSON, unknown kinds and truncation as corruption. Records are
  never silently dropped (test 14).

Why this format rather than SQLite / Parquet / Arrow:

* It is standard library only (no new dependency on Windows).
* It is append-only and crash-tolerant by construction.
* It is human-inspectable (`gzip -dc`).
* On synthetic data it compresses about 10.7× versus the uncompressed JSONL (≈ 82 bytes per event,
  including the raw text). Real payloads will compress differently.

The datasets derived from it are CSV/JSONL and can be converted to Parquet offline if that is ever
needed.

**Manifest** (`manifest.py`, written atomically every 5 s and at the end), with no secrets:

* session id, start/end and status;
* assets, and the sources with their settings;
* credential **presence** flags only (`kalshi_ws_auth_configured`, `cfb_direct_configured`,
  `cryptography_installed`);
* app / schema / feature-set versions and fingerprints (strategy, settlement, market data), plus the
  result of re-verifying the code at start;
* disconnects, reconnects, parse failures, duplicates, gaps and clock anomalies;
* store counters and the settlement-store path.

`assert_no_secrets` refuses secret-looking keys (test 15).

## 6. Feeds: states, reconnects, gaps, backfill

The feed states are `CONNECTED → WARMING_UP → HEALTHY`, `DEGRADED` (a parse failure, gap or stale spell
in the last 30 s), `STALE` (no message for 10 s, measured on the monotonic clock), `RECONNECTING` and
`DISCONNECTED` (stopped, retry limit reached, or credentials unavailable).

* **Reconnects** use exponential backoff: 1 s × 2ⁿ, capped at 60 s, with ±10 % deterministic jitter,
  reset after data flows.
* Each source runs in its own thread, so an optional source failing never blocks another (test 12).
* **Gaps** are recorded, never filled. There are four kinds:
  * `CADENCE`: CF values more than 2.5 s apart;
  * `TRADE_ID`: a Coinbase or Kraken id jump, with the exact missing count;
  * a Coinbase heartbeat whose `last_trade_id` was never received. It is checked one heartbeat later,
    so a heartbeat that overtakes its trade is not a false gap;
  * `DISCONNECT`: from the last message to the reconnect.

  A price grid across a gap is `MISSING`. A trade window overlapping a known gap is `MISSING`, even
  after a backfill, because completeness cannot be proven.
* **Backfill.** After a reconnect, Coinbase REST `/trades` is fetched. The results are `BACKFILLED`
  with their original event time and `receive_ts` = retrieval time, so they are not visible before
  they were retrieved (test 17). Duplicates of live trades collapse by trade id (first arrival wins),
  and the duplicate counts are recorded.

## 7. Replay

`replay.load_sessions(dirs)` returns events in original arrival order (`ingest_seq` within a session,
sessions by first arrival), plus gaps, failures and corruption. `Replayer(events, speed=None | 1.0 | 20.0)`
replays as fast as possible, in real time, or faster than real time. The same capture always gives
the same stream: the same digest from `py scripts/replay_market_data.py <session> --digest` (test 18).

## 8. Features (`features/definitions.py`: 452 named features, `hires_features_v1`)

Every value has a **status**; the value is `None` unless the status is `READY` or `PARTIAL`.

| Status | Meaning |
|---|---|
| READY | computed from complete inputs |
| NOT_READY | warm-up: collected history does not reach back far enough |
| MISSING | inputs should exist but do not (no data, stale feed, gap in the window) |
| UNAVAILABLE | the source cannot provide it (e.g. no documented aggressor side) |
| UNDEFINED | mathematically undefined (0/0), e.g. imbalance with no volume. **Not 0.** |
| PARTIAL | only in the named research mode `FeatureConfig(partial_windows=True)` (≥ 80 % grid coverage) |

The only value that is a true 0 without data is `xex.n_sources` (a count of fresh venues). A
window with no trades has volume 0 (observed, nothing traded), but its averages and imbalances are
`UNDEFINED`.

Windows are 1s, 5s, 15s, 30s, 60s, 3m and 5m. Any whole-second window is available through
`FeatureEngine.window_features(T, w_ms)`, which applies the same causal rules. Grid features sample the
price **as of** each 1-s instant T − k s, using a value at most 5 s old (no forward fill beyond that).
Trade windows are (T − w, T].

| Group | Features (per source / window) |
|---|---|
| price (`cf`, `coinbase`, `kraken`, `ref`) | last, mid, log/simple return, acceleration (return minus the previous return), OLS slope, distance travelled, directional persistence |
| vol (5s–5m) | realized vol, realized variance, mean absolute return, vol acceleration, short/long per-second ratios (15s/5m, 5s/60s, 60s/5m). The production 60-min vol is untouched. |
| volume (`coinbase`, `kraken`, `kalshi`; 5s–3m) | volume, count, average size, max size, volume rate vs the preceding 15-min baseline |
| flow | buy / sell / signed volume, imbalance, count imbalance, CVD since market open. Only when every trade in the window has a documented aggressor, otherwise `UNAVAILABLE`. |
| xex | Coinbase−Kraken last / mid / bps, return divergence, dispersion, number of fresh venues, lead-lag (±3 s) and its correlation |
| cfspot | CF − Coinbase / Kraken / reference (USD, bps), CF-vs-reference return divergence, basis velocity |
| strike | seconds remaining; raw, relative and vol-normalized distance for CF, Coinbase and the reference |
| settle | Step-2 `reconstruct(market, observations, as_of=T)` state: current index, accumulated mean, samples, coverage, last-observation age, seconds remaining, mean − strike, quality, phase |
| kalshi | YES/NO bid / ask / mid / spread, executable asks, implied probability, state age, mid velocity, spread change, top-of-book depth, imbalance, weighted mid; `model_vs_market` is a placeholder (always UNAVAILABLE) |

The **reference price** (`ref`) is the median of the fresh (≤ 5 s) venue mids. With one venue it is
that venue. There are no perp features.

## 9. Dataset, labels, provenance

`py scripts/build_market_features.py <session dirs> --assets BTC --out analysis_output/features_x`.
This makes **one pass** over the events in availability order. For every Kalshi market it emits a
checkpoint at close − s for s in (600, 480, 360, 300, 240, 180, 120, 90, 60, 30, 0), immediately
before the first event received after T. A checkpoint is skipped (and counted) when the market's
metadata had not arrived by T, or when T lies outside the captured data.

The batch path `compute_at(events, T)` is tested **equal** to the single pass at every checkpoint
(test 19). The writer produces three separate files:

* `features.csv|jsonl`: values plus a `<feature>__status` mask; a missing value is an empty cell;
* `labels.jsonl`: the final reconstruction plus the official Kalshi result, computed separately
  after the pass;
* `provenance.json`, which records:
  * the feature-set version, session ids and each session's recorded fingerprints;
  * the sources, feature definitions, windows, config and checkpoints;
  * the current strategy / settlement / market-data fingerprints;
  * the settlement policy ids with `verified` status, input counts, skipped checkpoints and a rows
    digest.

## 10. Leakage and mutation tests

Test 23 perturbs a real-shaped session only **after** T. The features at T must stay byte-identical,
through both the batch path and the single-pass dataset. The perturbations are:

* a delayed exchange trade (event before T, received after);
* an amended CF observation received after T;
* a post-checkpoint Kalshi book and market state;
* a future price (Coinbase and Kraken);
* post-close CF values and trades;
* a gap learned after T.

**Canaries:** each perturbation delivered by T *does* change the features, so the check is sensitive.
A deliberately leaky builder (event-time alignment with the guards off) is caught. A `RESOLUTION`
received before T, even a wrong one, never changes a feature (test 24). The settlement features equal
the Step-2 reconstruction **as of T**.

`py scripts/mutation_test_market_data.py` breaks one rule at a time in a temporary copy and runs the
relevant tests. Each mutation runs twice: once as-is, and once with the engine's causality guards
disabled, so the tests themselves must catch it. Results are in
`analysis_output/market_data_mutation_results.json`:

| # | Mutation | As-is | Guards disabled |
|---|---|---|---|
| M1 | receive timestamps ignored (availability decided by event time) | CAUGHT (test 9) | CAUGHT (test 23) |
| M2 | future visible (dataset checkpoints emitted after 5 s of later data were ingested) | CAUGHT (CausalityError guard) | CAUGHT (test 19) |
| M3 | missing values become zeros | CAUGHT (test 21) | CAUGHT (test 21) |
| M4 | backfilled data treated as live (visible from its event time instead of its retrieval time) | CAUGHT (test 5) | CAUGHT (test 5) |
| M5 | rolling windows include the future (upper bound T + 5 s) | CAUGHT (test 20) | CAUGHT (test 20) |
| M6 | cross-exchange quotes aligned by event time (a delayed venue quote used before it arrived) | CAUGHT (CausalityError guard) | CAUGHT (test 23) |
| M7 | settlement features computed in LABEL mode (final reconstruction instead of as-of T) | CAUGHT (test 24) | CAUGHT (test 24) |

Controls: unmutated copy PASS; guards disabled with no mutation PASS. Result: every mutation caught.

Found and fixed while writing these: the first M6 run with the guards disabled was **not caught**. Both sides of the cross-exchange comparison contained the same leaked quote (a real quote with event time T that arrived at T + 40 ms). Test 23 now also asserts the fundamental invariant, that deleting everything received after T leaves the features at T unchanged. That catches M6.

## 11. Performance (synthetic, 4 assets × 20 min; `py scripts/bench_market_data.py`)

| Measure | Result (CPython 3.11.15, this container) |
|---|---|
| Collector ingest (raw text → parse → store, fsync per flush) | 6,599 messages/s (151.5 µs/message) |
| Disk flush latency (500-line gzip member + fsync) | p50 3.268 ms, p99 6.099 ms, max 18.456 ms |
| Storage | 82.0 bytes/event including the raw text (4.5 MB for 57,392 events) |
| Replay: load + verify CRCs | 18,882 events/s (the raw records are read too) |
| Replay: iterate (event-by-event) | 3,918,177 events/s |
| Feature engine ingest + a 452-feature row every second | 57,392 events in 7.084 s |
| `features_at` latency (452 features) | p50 7.974 ms, p99 13.808 ms, max 21.376 ms |
| Single-pass dataset build (4 assets) | 60 checkpoint rows in 0.973 s |
| Memory (load session + one incremental engine over it) | peak 227.4 MB traced |

Full output: `analysis_output/market_data_performance.json`.

Real message rates were not measured here (no network access), so how much headroom these numbers
leave is still to be confirmed on a real capture. `features_at` builds all 452 features at once. A
consumer that needs only a few windows can use `window_features()`.

## 12. Running it (Windows PowerShell)

```powershell
py collect_market_data.py --dry-run                                   # plan + offline checks, no network, writes nothing
py collect_market_data.py --assets BTC,ETH,SOL,XRP --duration 3600    # all sources; Ctrl+C stops cleanly
py collect_market_data.py --assets BTC --coinbase --secondary --kalshi --duration 900   # no CF credentials needed
$env:KALSHI_API_KEY_ID = "<id>"; $env:KALSHI_PRIVATE_KEY_PATH = "kalshi_private_key.pem"
py -m pip install cryptography                                        # only for CF via Kalshi
py collect_market_data.py --cf --coinbase --kalshi                    # CF via Kalshi (read-only websocket)
py collect_market_data.py --cf --cf-source direct                     # CF direct ($env:CFB_API_ID / CFB_API_SECRET)
#  research status page (separate from the production dashboard):  http://127.0.0.1:8766/   JSON: /status
py scripts/replay_market_data.py market_data_sessions\<session> --digest
py scripts/build_market_features.py market_data_sessions\<session> --assets BTC --out analysis_output\features_btc
py scripts/bench_market_data.py
py scripts/mutation_test_market_data.py
py -m market_data.fingerprint --verify
```

Other flags: `--output`, `--status-port 0` (off), `--settlement-store <path|none>`,
`--kalshi-interval`, `--no-raw` (not recommended), `--no-live-features`.

The collector also appends CF observations, Kalshi-published averages, market metadata and official
results to `settlement_data/capture-<session>.jsonl` (a Step-2 settlement store). This means
`scripts/verify_settlement_resolution.py`, `compare_settlement_overlap.py` and
`settlement_validation_report.py` work on real captures unchanged. They report evidence and never
mark a convention verified.

The **status page** (`status_server.py`) is a separate localhost-only process/port showing:

* feed states and reconnects;
* CF RTI, the multi-exchange reference and cross-exchange dispersion;
* the settlement accumulator;
* high-resolution volatility;
* capture counters.

It has no control endpoints and cannot affect calls.

## 13. Integrity

* `config/market_data_baseline.json` pins every `market_data` module (canonical AST, so a cosmetic
  change does not count), the feature registry, the windows and the default config. Verify with
  `py -m market_data.fingerprint --verify`. Changing it requires
  `--write --i-intend-to-change-the-market-data-baseline` plus a row below.
* The strategy (`config/strategy_baseline.json`, 47 fixtures, legacy fingerprint `8d94f241…`) and the
  settlement baseline are independent of it and unchanged by Step 3, apart from the documented
  index-id re-baseline in SETTLEMENT_ENGINE.md.

### Market-data re-baseline log

| Date | What changed | Why |
|---|---|---|
| 2026-09-25 | initial baseline (`hires_features_v1`, 452 features) | Step 3 created the package |

## 14. Limitations and next steps

* **No real capture was possible here** (the network policy blocks every market-data host). Parsers
  follow the documented message shapes and are tested on synthetic look-alikes. The first real session
  may surface schema details. Unknown shapes become recorded parse failures, not wrong numbers.
* Kraken ticker quotes have no venue timestamp (receive-time ordered). Coinbase BBO comes from
  `ticker` (updates on trades), not a full book. Kalshi state comes from 1-s REST polling, not the
  authenticated orderbook websocket.
* Trade features overlapping a disconnect stay `MISSING` even after a backfill (conservative).
  Coinbase REST backfill returns at most 100 trades per product.
* Lead-lag is a simple max-correlation over ±3 s of 1-s returns. It is a descriptive feature, not
  evidence of causality.
* Nothing here has been shown to add predictive value. That needs real sessions and a pre-declared,
  walk-forward evaluation (ROADMAP phases 5 and 11) before anything goes near the live path.

Next: run `collect_market_data.py` locally for several days with CF credentials, then:

1. check parse failures and gaps in the manifests;
2. run the Step-2 resolution verification on the captured settlement store;
3. build feature datasets for offline research.

### Sources for the documented semantics

* Kalshi CF Benchmarks value channel: https://docs.kalshi.com/websockets/cfbenchmarks-value
* Kalshi CF REST passthrough: https://docs.kalshi.com/cfbenchmarks/rest-passthrough
* Kalshi market orderbook: https://docs.kalshi.com/api-reference/market/get-market-orderbook
* CF Benchmarks websocket value stream: https://docs.cfbenchmarks.com/api/websocket/value/
* CF Benchmarks indices: https://www.cfbenchmarks.com/data/indices/BRTI (and ETHUSD_RTI, SOLUSD_RTI, XRPUSD_RTI)
* Coinbase Exchange websocket channels: https://docs.cdp.coinbase.com/exchange/websocket-feed/channels
* Kraken websocket v2 trade / ticker: https://docs.kraken.com/api/docs/websocket-v2/trade , https://docs.kraken.com/api/docs/websocket-v2/ticker/
