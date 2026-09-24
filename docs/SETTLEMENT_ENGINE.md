# Settlement engine (Step 2) — research / observation only

The `settlement/` package models the value Kalshi 15-minute crypto markets **resolve against**,
causally and with provenance, so later steps can test a settlement-aware predictor. It never
creates, vetoes, flips or sizes a production call:
* nothing in `kalshi_dashboard`, `kalshi_core` or `run_local` imports it;
* it imports neither of them, nor any network, Discord or execution module.

`test_stage18.py` enforces both directions.

> **Status of real data: none.** This build environment could not reach Kalshi, CF Benchmarks or
> Coinbase (egress policy), so no real observation or settlement has been checked yet.
> `analysis_output/settlement_validation.json` reports `INSUFFICIENT_DATA` with 0 markets. Nothing in
> this document claims equivalence with Kalshi's official settlement.

## 1. What Kalshi resolves against

Sources consulted: public descriptions only (search-result summaries). The Kalshi and CF Benchmarks
documentation pages themselves were unreachable from the build environment.

* Kalshi crypto contracts, the 15-minute up/down series included (KXBTC15M, …), settle on a **simple
  average of the CF Benchmarks Real-Time Index (RTI) sampled once per second over the 60 seconds
  before the close**. For bitcoin the index is BRTI (published up to every 200 ms).
* The market's target ("price to beat", `floor_strike`) is set at the start of the window.
* A settled market object carries `result` (`yes`/`no`) and `expiration_value`, the settled index value.
* Kalshi offers an authenticated websocket channel, `cfbenchmarks_value`. It carries the raw upstream
  CF frame as a JSON string (`msg.data`) plus Kalshi-computed trailing-60-s and
  "last 60 s windowed average (15 min)" values. It also offers a REST passthrough to CF historical
  values.

**Not specified precisely anywhere I could read**, and therefore an explicit, testable assumption:
* which 60 instants are averaged;
* how a sample is taken from a sub-second feed;
* rounding;
* ties at the strike.

## 2. What existed before Step 2 (Step-1 system, verified in the code)

| Where | What it does |
|---|---|
| `kalshi_dashboard.evaluate` | Uses Kalshi `floor_strike` and Coinbase **spot**; P(UP) is spot vs strike over the minutes to close. No CF index, no averaging window. |
| `kalshi_dashboard.settle_calls` / `settle_pending` | Paper settlement from Kalshi `market.result` only; first attempt at close + 20 s, give up after 30 min / 10 min. |
| `label_binary_outcomes.py` | Research labels: `result` only (`expiration_value` discarded), 90 s grace, conflicts flagged. |
| `perp_telemetry.py` | Records the perp `reference_price` ("CF index **scaled per contract**") as `index_price`, about every 4 s. |
| Backtests (`kalshi_backtest.py`, `run_backtest`) | Coinbase 1-min candles keyed by bucket **start**: the "strike" is the close at open + 1 min and the "settlement" is the close at close + 1 min. Not a 60-s CF average before close. |
| Close time | From the market's `close_time` (UTC ISO); minutes left from the local clock. |

None of these is changed by Step 2. The findings are listed in the validation report.

## 3. Supported sources (parsers; all offline)

| Source name | Module | Payload | Trusted | Receive time |
|---|---|---|---|---|
| `cfb_ws_via_kalshi` | `cf_live.parse_kalshi_cfb_message` | Kalshi `cfbenchmarks_value` message | yes | from the capture |
| `cfb_ws` | `cf_live.parse_cfb_frame` | raw CF websocket value frame | yes | from the capture |
| `cfb_rest_history` / `cfb_rest_via_kalshi` | `cf_history.parse_cfb_historical` | CF REST historical values | yes | unknown (history) |
| `kalshi_market_api` | `kalshi_markets.parse_market` | Kalshi market object | metadata + labels | — |
| `kalshi_perp_reference_price` | `proxy.load_perp_reference_csv` | the existing telemetry CSV | **never** (PROXY) | — |
| `synthetic` | `synthetic.py` | test data | tests only | — |

Schemas (`settlement/schemas.py`) have required and optional fields, types, and constants such as
`type == "value"`. Each has a verification status:
* the CF / Kalshi-CF-feed schemas are `UNVERIFIED_FROM_PUBLIC_DOC_DESCRIPTIONS`;
* the Kalshi market schema is `VERIFIED_AGAINST_REPOSITORY_CODE`.

How unexpected payloads are handled:
* A missing required field or a wrong type → `SCHEMA_MISMATCH` issue, and the record is rejected.
* Extra fields in a strict spec → accepted, but flagged `SCHEMA_EXTRA_FIELDS`.
* Every record stores the **structure fingerprint** (keys and types, not values) of its payload, so
  a changed schema shows up in the store and in every reconstruction's provenance.
* Values must be finite and > 0. Timestamps must be integer epoch **ms** within 2009–2100, which
  catches seconds-vs-ms mistakes. ISO times must carry a timezone; naive times are refused.
* The engine never falls back to Coinbase spot. If CF data is missing, the quality is `MISSING`, or
  `PROXY_SOURCE` when only the perp proxy exists.

Asset → index ids: BTC→BRTI, ETH→ETHUSD_RTI, SOL→SOLUSD_RTI, XRP→XRPUSD_RTI (`assets.py`). The
non-BTC ids are assumed. A wrong id yields `MISSING`, never a wrong value.

## 4. Settlement-window convention (`settlement/policy.py`, `SettlementWindowPolicy`)

Default policy `cf_rti_60s_start_incl_asof_v1` (`verified=False`):

| Question | Answer |
|---|---|
| Sample instants | 60 instants, integer UTC epoch ms, 1 s apart |
| Beginning instant (close − 60 s) | **included** (`start_inclusive=True`) |
| Closing instant (close) | **excluded** (`end_inclusive=False`); instants are close−60 s … close−1 s |
| Sample at instant *g* | the latest observation with *g* − 1000 ms ≤ event time ≤ *g* (`ASOF`, `max_sample_age_ms=1000`); `OBSERVED_EXACT` if exactly at *g* |
| Final value | arithmetic mean of the 60 sample values (`math.fsum`), no rounding |
| Outcome | `yes` if final > strike, `no` if final < strike, **none + `AT_STRIKE` flag** if equal |
| Membership of an event time | `BEFORE_WINDOW` / `IN_WINDOW` / `AFTER_WINDOW`, per the inclusivity flags |

Candidate conventions, all evaluated by the resolution verifier:

| Policy | Convention |
|---|---|
| `cf_rti_60s_end_incl_asof_v1` | instants close−59 s … close |
| `cf_rti_60s_start_incl_exact_v1` | exact-instant values only |
| `cf_rti_60s_start_incl_bucket_v1` | the last value inside each 1-s bucket [k, k+1 s); known only when the bucket ends |

Grids are pure epoch arithmetic, identical in any process timezone and across DST changes (tested).

## 5. Reconstruction policy (`ReconstructionPolicy`)

| Case | `strict_v1` (default, fail closed) |
|---|---|
| Duplicates | Identical (event time, value) from any source are kept once and counted `DUPLICATE_DROPPED`. |
| Same instant, different values | `PREFER_AMENDED`: the value with the largest `amendTime` wins, but only once that amendment is available. Otherwise the instant is `CONFLICT` and the market gets no value. |
| Out of order | Values are ordered by **event** time, never by arrival. An arrival more than 2 s behind the newest event already received gives quality `OUT_OF_ORDER`: the value is still computed, but it is not `HEALTHY`. |
| Missing instants | `MISSING`. Never filled. |
| Coverage | A final value needs 60/60 observed samples; otherwise `INSUFFICIENT_COVERAGE` and **no value**. |
| Late data | Received after close + 5 s: flagged `LATE_OBSERVATION`. It is used for the label only (`include_late_in_final`) and never appears in an earlier checkpoint. |
| No receive time (history) | Availability = event time + `assumed_publication_lag_ms` (0), flagged `RECEIVE_TIME_MISSING`. |
| Malformed values / bad timestamps | Rejected at parse time and logged as issues. The resulting holes are `MISSING`. |
| Unsupported / mismatched schema | A relevant trusted-source `SCHEMA_MISMATCH` gives quality `SCHEMA_MISMATCH` and no value. |
| Proxy / unknown sources | Never sampled. |

Research policies (explicitly named, `research_only=True`, never `HEALTHY`):
* `research_partial_v1`: mean over the observed samples if at least 90 % are present. Quality
  `PARTIAL`, flag `PARTIAL_MEAN`.
* `research_interpolate_v1`: additionally linearly interpolates interior gaps. Samples are marked
  `INTERPOLATED` and counted separately from observed ones.

## 6. Quality states (`settlement/types.py`, `settlement/quality.py`)

The overall state is the worst condition present. A value merely existing never makes it `HEALTHY`.

| State | Measured condition |
|---|---|
| `HEALTHY` | Every elapsed sample observed, trusted source, fresh, no conflict / out-of-order / schema problem. For a final value: 60/60. |
| `PARTIAL` | Some elapsed samples missing but coverage still reachable, or a research policy's partial / interpolated value. |
| `STALE` | Before close: the newest available observation is more than 5 s old. |
| `INSUFFICIENT_COVERAGE` | Closed: fewer samples than the policy minimum. Open: enough already missing that the minimum is unreachable. |
| `SCHEMA_MISMATCH` | A relevant trusted payload failed validation. |
| `MISSING` | No trusted observation available. |
| `OUT_OF_ORDER` | An arrival beyond the reorder tolerance. |
| `CONFLICT` | An unresolvable conflict at an elapsed instant. |
| `PROXY_SOURCE` | Only proxy data. |
| `INVALID` | Unusable market spec. |
| `UNKNOWN` | Unexpected internal condition. |

Flags add detail: `GAP`, `ASOF_SAMPLES`, `AMENDMENT_APPLIED`, `LATE_OBSERVATION`,
`RECEIVE_TIME_MISSING`, `WINDOW_NOT_STARTED`, `WINDOW_OPEN`, `AT_STRIKE`,
`UNVERIFIED_WINDOW_CONVENTION`, and others.

## 7. Live accumulator (`settlement/accumulator.py`)

`SettlementAccumulator(market).ingest(obs)` runs as data arrives. `.state(now)` returns a read-only
`SettlementState`. `.result(now)` adds the final value once the window has closed.

The state exposes:
* `current_index` and its age;
* `observations_seen`;
* samples expected / elapsed / filled / interpolated / missing / remaining;
* `coverage_elapsed`;
* `accumulated_sum` and `accumulated_mean`;
* first and last included instants;
* `max_gap_s`, `phase`, `quality`, flags and sources.

It is incremental. `ingest` is O(log n) plus at most 2 re-sampled instants. Each instant is sampled
about once. The summary walks the 60 instants, never the history.

It refuses non-causal use: a state for a time earlier than an ingested observation's availability,
or an ingest out of arrival order, raises `CausalityError`. It shares `engine.ObservationBook` and
`engine.summarize` with batch reconstruction. `test_stage18` asserts **accumulator == batch** at
every 0.5 s across a stream with gaps, duplicates, amendments, conflicts and late arrivals.

A sample for instant *g* is provisional from *g* until the exact-time value arrives. Because of
publication latency it may first be an as-of value; any later revision is counted in
`sample_revisions`. This is causal: it uses only what had arrived.

Measured on this container (`py scripts/bench_settlement.py`, Python 3.11, 200 ms feed):

| Metric | Result |
|---|---|
| ingest | median 1.8 µs, p99 8 µs |
| `state()` | median 17 µs, p99 47 µs |
| 11 checkpoints per market | 12 ms |
| batch reconstruction | ~930 markets/s |
| resolution verification, 4 conventions | ~130 markets/s |

These numbers are machine-specific.

## 8. Historical reconstruction (`settlement/reconstruction.py`)

`reconstruct(market, observations, as_of_ms=T)` uses only observations with availability ≤ T **and**
event time ≤ T. With `as_of_ms=None` it runs in label mode and uses all data up to the close.

Inputs are processed in a total arrival order that doesn't depend on list order, so results are
deterministic. Shuffled input gives byte-identical output and provenance.

Provenance records:
* engine and reconstruction versions;
* window and reconstruction policy id / version / fingerprint, and whether the window policy is verified;
* the market's strike source and metadata schema fingerprint;
* sources, schema fingerprints, and the source event-time range;
* an **input digest** of exactly the observations used;
* counts (received, duplicates, conflicts, amendments, late, out-of-order, proxy, untrusted);
* coverage, quality, flags, and the issues considered.

## 9. Causal checkpoints and label separation (`settlement/checkpoints.py`)

Default checkpoints (seconds before close): 600, 480, 360, 300, 240, 180, 120, 90, 60, 30, 0.
Any value ≥ 0, including fractions, also works.

Each `CheckpointRecord` has:
* `key`: market, asset, index, strike, close, checkpoint, policies, engine.
* `features` (`FEATURE_FIELDS` only): the causal `SettlementState` at the checkpoint.
  `features_at()` takes **no resolution argument** and filters observations to those visible at the
  checkpoint before reconstructing, which also filters.
* `labels` (`LABEL_FIELDS`): `final_settlement_value`, `reconstructed_outcome`, `final_quality`,
  `final_coverage`, `official_result`, `official_expiration_value`, `reconstruction_matches_official`.
  These are computed separately, from the label-mode reconstruction and the official
  `OfficialResolution`.

Datasets are written atomically as `.csv` (`f_*` / `y_*` columns) or `.jsonl`.

**Leakage tests** (`test_stage18` test 13) perturb everything that becomes available after a
checkpoint, and each change must leave the checkpoint's features identical:
* scaled future values;
* dropped future values;
* injected late / future values;
* post-close values;
* a flipped official outcome;
* a removed official outcome.

This is checked both through `features_at` and **directly through `reconstruct`**. A test that only
went through `features_at` would miss a leak inside `reconstruct`, because the pre-filter masks it;
this was found and fixed during development.

Three deliberately leaky builders must be **detected**:
* one that uses the final value;
* one that uses the official outcome;
* one that uses post-checkpoint data.

Mutation checks confirmed that removing the receive-time filter from `reconstruct` is caught.

## 10. Resolution verification and overlap

* `scripts/verify_settlement_resolution.py` produces one row per market × convention:
  * reconstructed value and outcome;
  * strike;
  * official result and `expiration_value`, and the absolute difference;
  * agreement and category (`AGREE`, `DISAGREE`, `NO_RECONSTRUCTION`, `NO_OFFICIAL_RESULT`,
    `AT_STRIKE`, `NO_STRIKE`);
  * coverage, quality, flags and input digest;
  * a research **strike check**: the previous window's reconstructed average vs `floor_strike`.

  A convention verdict is issued only with ≥ 30 markets that have an `expiration_value`
  (`INSUFFICIENT_DATA` otherwise). Tied conventions are reported as tied. Outcome agreement alone
  rarely distinguishes conventions; `expiration_value` does.
* `scripts/compare_settlement_overlap.py`: live vs history.
  * Point level: exact matches, tolerance matches, max / mean / median error, missing-in-live,
    extra-in-live, and the worst mismatches listed.
  * Window level: per-instant membership and boundary disagreements, and which source instant was
    used.
  * Accumulated value at every checkpoint, and final values.
  * Kalshi-published final-minute averages vs our live reconstruction.

## 11. Store and offline replay

`settlement/cache.py` stores append-only JSON lines. Each line carries a store version, a kind, the
data, and a SHA-256-based checksum:
* A header is written once.
* Capture-session records hold provenance: tool, input file SHA-256, observed fingerprints, and a
  synthetic flag.
* Writes are fsync'ed whole lines. A truncated last line is reported `CORRUPT_RECORD` and skipped,
  and later appends start on a fresh line.
* Checksum or kind errors are reported, never interpreted.
* Secret-looking fields are refused.
* Loading is deterministic.

`test_stage18` test 17 runs the full cycle with the network disabled: capture files → import →
store → reconstruct → checkpoints → verify → overlap. It runs it twice and gets identical output.

## 12. Collecting validation data (what to do next, outside this environment)

1. **Kalshi markets.** Save `GET /trade-api/v2/markets?series_ticker=KXBTC15M&status=settled` pages
   (public) to JSON, then run
   `py scripts/settlement_import.py --kind kalshi-markets-json --input <file>`.
2. **Live CF values.** Subscribe to Kalshi's `cfbenchmarks_value` channel (authenticated, read-only)
   with any websocket client. Write one line per message:
   `{"receive_ts_ms": <local UTC ms at receipt>, "seq": <n>, "message": <raw message>}`.
   Import with `--kind kalshi-ws-jsonl`. Run `--inspect` on the first capture to confirm the schema
   fingerprint and fields.
3. **Historical CF values.** CF Benchmarks REST historical values (directly or via Kalshi's
   passthrough) for BRTI and the other indices, covering the same markets.
   Import with `--kind cf-rest-json --index-id BRTI`.
4. Run `py scripts/settlement_validation_report.py`. With at least 30 settled markets that have an
   `expiration_value` it names the matching convention. At that point, record it here, set the
   policy `verified=True` in a new policy version, and re-baseline with
   `py -m settlement.fingerprint --write --i-intend-to-change-the-settlement-baseline`.

No capture client is included: it would need Kalshi API credentials, which this step does not add.

## 13. Limitations (specific)

* No real data has been processed. The default convention, the non-BTC index ids and the CF / Kalshi
  feed schemas are **unverified**.
* 1-s as-of sampling of a 200 ms feed is an assumption. Bucket and exact variants are provided for
  comparison.
* Historical data has no receive times. Causality for history rests on the explicit
  `assumed_publication_lag_ms` (default 0, i.e. optimistic), which is flagged.
* The strike is taken from Kalshi metadata, not reconstructed (the strike check is research only).
* `OUT_OF_ORDER` detection needs receive times or capture sequence numbers.
* Checkpoint generation reconstructs from scratch per checkpoint (12 ms per market for 11
  checkpoints). The live path is incremental.
* The Kalshi-published averages are compared, never used as inputs.

## 14. Integrity

* Production code is unchanged. `kalshi_dashboard.py` is byte-identical.
* The Step-1 artifacts are byte-identical and asserted by `test_stage18` test 1:
  * `config/strategy_baseline.json`
  * `step5_baseline_manifest.json`
  * `regression/strategy_cases.json`
* All 47 strategy fixtures match. The legacy fingerprint is still `8d94f241…`.
* The settlement code has its own fingerprint in `config/settlement_baseline.json`
  (`py -m settlement.fingerprint --verify`).

### Settlement re-baseline log

| Date | What changed | Why |
|---|---|---|
| 2026-09-24 | initial settlement baseline | Step 2 |
