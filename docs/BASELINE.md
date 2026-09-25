# Strategy baseline (Step 1) — what must stay stable

Frozen on 2026-09-24 from branch `claude/magical-knuth-8ura2x`. That branch holds the supplied
`kalshi_optimized_final.zip` (commit `27ea381`, byte-identical to the ZIP) plus two earlier,
behaviour-preserving commits: volatility-regime research telemetry and the real-time dashboard.
This pass changed **no strategy behaviour** (see "Integrity" below).

## 1. Current model behaviour (verified against `kalshi_dashboard.evaluate`)

For each of BTC, ETH, SOL, XRP (Kalshi series `KX<COIN>15M`, Coinbase `<COIN>-USD`), every `POLL_SECONDS` = 4 s:

1. **Market**: the open market in the series with the nearest future `close_time`. The strike is
   `floor_strike`, else `cap_strike`, else `strike`. Book prices are in cents; a missing ask is
   derived as `100 − opposite bid`.
2. **Volatility**: the sample stdev of 1-minute log returns over the newest 61 **completed** Coinbase
   closes (the last, possibly partial, candle is dropped), times `VOL_MULT` = 1.15. Fewer than 20
   usable closes, a bad strike or spot, or σ ≤ 0 gives `status: stale` / `BLOCK_STALE_DATA`.
3. **Probability**: `z = ln(spot/strike) / (σ·√max(minutes_left, 1e-4))`, `P(UP) = Φ(z)`.
   There is no drift term and **no calibration step**.
4. **Direction**: UP if `P(UP) ≥ 50 %` (ties go UP), else DOWN. **Confidence** = `max(P(UP), 100 − P(UP))`.
5. **Edge**: `raw = conf − side_ask`; `net = raw − ENTRY_COST_CENTS` (2 ¢).
6. **Gates** (the first failure is the reported reason; a call needs all of them):

| Order | Gate | Constant | NO_CALL code |
|---|---|---|---|
| 1 | coin enabled (web toggle) | `ACTIVE_COINS` (all on) | `COIN_DISABLED` |
| 2 | direction allowed (web toggle) | `DIRECTION` = BOTH | `DIRECTION_DISABLED` |
| 3 | not too early: minutes_left ≤ 8 | `MAX_ENTRY_MIN` = 8.0 | `OUTSIDE_ENTRY_WINDOW_EARLY` |
| 4 | not too late: minutes_left ≥ 2 | `LAST_STOP_MIN` = 2.0 | `OUTSIDE_ENTRY_WINDOW_LATE` |
| 5 | not choppy: ≤ 3 strike crossings across the last 16 candle closes (15 completed + the current partial minute) | `CHOP_MAX` = 3, window 15 | `CHOPPY_MARKET` |
| 6 | confidence ≥ 80 % (unrounded) | `MIN_CONF` = 80.0 | `CONFIDENCE_BELOW_THRESHOLD` |
| 7 | favoured-side ask ≥ 85 ¢ | `MIN_PRICE` = 85.0 | `PRICE_BELOW_THRESHOLD` |
| 8 | net edge > 0 and ≥ `EDGE_THRESH` | `EDGE_THRESH` = 0.0 | `INSUFFICIENT_EDGE` |
| – | data guard (before the gates) | – | `DATA_STALE` |
| – | poller: first signal per ticker only | – | `ALREADY_CALLED_THIS_MARKET` |
| – | poller: live perp veto BLOCK (default **inactive**) | `PERP_LIVE_VETO_ENABLED` = 0 | `PERP_VETO_SUPPRESSED` |
| – | poller: watcher paused | – | `WATCHER_PAUSED` |
| – | no open market / network error / exception | – | `MARKET_UNAVAILABLE` / `DATA_UNAVAILABLE` / `EVALUATION_ERROR` |

**Entry window**: 2.0 ≤ minutes_left ≤ 8.0, inclusive at both ends (fixtures E06/E07).

7. **Stop** (display/paper): `rec_stop = round(side_ask × sl_pct)` with `sl_pct` BTC 0.75, ETH 0.60,
   SOL 0.75, XRP 0.65. In paper settlement a stop triggers when the lowest observed bid of the held
   side is ≤ stop; the exit is `min(bid_lo, stop)` and counts as a loss.
8. **Settlement**: Kalshi `GET /markets/{ticker}` → `result` (`yes` = UP won). Settlement is
   attempted from close + 20 s; unsettled calls are dropped after 30 min (stats after 10 min). Fee
   per side is `ceil(0.07·C·P·(1−P)·100)` ¢ (minimum 1, 0 for zero contracts). A maker order whose
   ask never traded down to the limit is a non-trade.
9. **Sizing** (informational, never enforced): quarter-Kelly on `(conf − 2) %` vs the ask, capped at
   5 % of `BANKROLL` = $1000, and 0 when net edge ≤ 0. Default `ENTRY_MODE` is `taker`.
10. **Perp overlay**: telemetry and shadow journals are on by default and observational only. The
    suppress-only live veto requires a validated manual promotion **and**
    `PERP_LIVE_VETO_ENABLED=1` **and** an unchanged legacy fingerprint. None exists, so it is
    `INACTIVE_NO_PROMOTION`.

Machine-readable form: `config/strategy_baseline.json` (generated from the source; do not edit by hand).

## 2. Inputs

Kalshi public REST (markets, results) and Coinbase Exchange public REST (ticker, 1-minute candles,
cached 30 s). There are no websockets and no API keys. Perp telemetry uses Kalshi's public perps
endpoints and is not a model input.

## 3. Evaluation methodology (unchanged)

* Paper record from real Kalshi quotes and results (`settle_calls`).
* Backtests (`kalshi_backtest.py`, `run_backtest`) use Coinbase 1-minute closes as a strike and
  settlement proxy. Entry is priced at the model confidence. **MIN_PRICE, edge and chop gates are
  not applied.** Stops are model-priced. Calibration is reported as Brier, log loss and ECE on the
  newest 30 % of settled trades. These are idealised numbers, not achievable P&L.
* The perp research uses chronological walk-forward with train-only preprocessing, BH-FDR,
  clustered bootstrap, and prospective frozen-threshold shadow validation (see `SETUP.txt`).

## 4. Strategy fingerprints

| Fingerprint | Value | Protects |
|---|---|---|
| legacy (`strategy_fingerprint.py`, format v2) | `8d94f241e8fc8edadc76058e1f12f430b6e4f499f4c0a30fba1cb5cf07dad82a` | 14 constants + 10 functions of `kalshi_dashboard.py`; required by the Step 6 live gate; pinned in `step5_baseline_manifest.json` (unchanged) |
| extended (`kalshi_core/baseline.py`, new) | `784141876a1b7f4c9f605036ef1358a6a3ea51ef32fced1f80f74045a6adef8c` | the legacy set **plus** market/book/strike parsing, poller first-signal + veto gating, web filters, poll cadence, settlement/stat helpers, both backtests + calibration metrics, perp overlay maths, frozen live-veto limits |

Both use canonical-AST hashing: comments, docstrings, whitespace and line numbers are ignored;
semantic changes are not. The manifest is byte-identical on Python 3.10, 3.11, 3.12 and 3.13.

```powershell
py -m kalshi_core.baseline --verify
py strategy_fingerprint.py --verify step5_baseline_manifest.json
```

## 5. Behavioural contract (regression fixtures)

`regression/strategy_cases.json` holds 47 deterministic cases. Outputs came from the unmodified
code, with a frozen clock (2026-09-21T14:00Z), no network and temporary files:
29 `evaluate()` scenarios, 5 poller cycles, 7 settlement/journal/fee cases, 3 backtest cases and
3 perp-overlay/veto cases. `test_stage16.py` replays them. Representative pinned outputs:

| Case | Min left | P(UP) % | Side | Conf % | Ask ¢ | Net edge ¢ | Stop ¢ | Legacy reason | Decision |
|---|---|---|---|---|---|---|---|---|---|
| E01 above strike | 5.0 | 95.8587 | UP | 95.9 | 90 | 3.9 | 68 | ENTER | CALL |
| E02 below strike | 5.0 | 4.0877 | DOWN | 95.9 | 90 | 3.9 | 68 | ENTER | CALL |
| E03 near strike | 5.0 | 78.4447 | UP | 78.4 | 52 | 24.4 | 39 | WAIT_LOW_CONFIDENCE | NO_CALL |
| E04 high vol | 5.0 | 64.1596 | UP | 64.2 | 62 | 0.2 | 46 | WAIT_LOW_CONFIDENCE | NO_CALL |
| E05 low vol | 5.0 | 100.0 | UP | 100.0 | 97 | 1.0 | 73 | ENTER | CALL |
| E06 8.0 min (early edge) | 8.0 | 99.8258 | UP | 99.8 | 90 | 7.8 | 68 | ENTER | CALL |
| E07 2.0 min (late edge) | 2.0 | 99.6951 | UP | 99.7 | 93 | 4.7 | 70 | ENTER | CALL |
| E08 too early | 8.5 | 90.8293 | UP | 90.8 | 90 | −1.2 | 68 | WAIT_TOO_EARLY | NO_CALL |
| E09 too late | 1.5 | 99.9229 | UP | 99.9 | 98 | −0.1 | 74 | WAIT_LATE | NO_CALL |
| E12 no edge | 5.0 | 95.8587 | UP | 95.9 | 94 | −0.1 | 70 | WAIT_NO_EDGE | NO_CALL |
| E13 price < 85 | 5.0 | 95.8587 | UP | 95.9 | 80 | 13.9 | 60 | WAIT_BAD_PRICE | NO_CALL |
| E14 choppy (5 crossings) | 5.0 | 95.231 | UP | 95.2 | 90 | 3.2 | 68 | BLOCK_CHOP | NO_CALL |
| E26 exactly 3 crossings | 5.0 | 95.0856 | UP | 95.1 | 90 | 3.1 | 68 | ENTER | CALL |
| E27 4 crossings | 5.0 | 96.1911 | UP | 96.2 | 90 | 4.2 | 68 | BLOCK_CHOP | NO_CALL |
| E28 conf 80.045 | 5.0 | 80.0452 | UP | 80.0 | 86 | −8.0 | 64 | WAIT_NO_EDGE | NO_CALL |
| E29 ask exactly 85 | 5.0 | 88.2984 | UP | 88.3 | 85 | 1.3 | 64 | ENTER | CALL |
| E15–E18 stale data | – | – | – | – | – | – | – | BLOCK_STALE_DATA | NO_CALL |
| E19 no ask, no opposite bid | – | – | – | – | – | – | – | raises TypeError | NO_CALL (EVALUATION_ERROR) |
| E25 spot == strike | 5.0 | 50.0 | UP | 50.0 | 51 | −3.0 | 38 | WAIT_LOW_CONFIDENCE | NO_CALL |

Settlement fixture S01 pins win, loss, stop, gap-through-stop (exit 20 ¢), stop exactly at the bid,
default stop (`entry × 0.7`), unfilled maker, filled maker, too-early, unresolved-and-kept, and
given-up-after-30-min. Backtest fixtures pin both backtesters' trade lists, Brier/log-loss/ECE and the
full `run_backtest()` output. V01–V03 pin the Step 5 overlay decisions and the inactive live gate.

Mutation check (done by hand during Step 1): changing `VOL_MULT`, `CHOP_MAX`, `MIN_CONF` by 0.1,
`MIN_PRICE`, `ENTRY_COST_CENTS`, either window bound, the ≥ 50 % direction tie rule, a coin's
`sl_pct`, the book derivation or the stop rule each makes the fixtures report `DIFFER`. Two edits are
caught only by the fingerprints: `conf_side`'s tie rule (only reachable at price == strike inside a
backtest) and the backtest's `break`/`continue` on a missing settlement close (behaviour-equivalent on
the stored histories).

## 6. Test baseline

Measured on this container with `run_all_tests.py` (per-suite result) and a per-check count of
`PASS`/`FAIL`/`SKIP` lines (each stage run once, master mode):

| Interpreter | Before (15 suites) | After (17 suites) |
|---|---|---|
| CPython 3.12.3 (documented validated version) | 15/15 suites, 249 checks passed, 0 failed | 17/17 suites, 275 checks passed, 0 failed, 0 skipped |
| CPython 3.13.12 | 15/15 suites passed (per-check count not taken) | 17/17 suites, 275 passed, 0 failed, 0 skipped |
| CPython 3.11.15 | 11/15 suites; 223 passed, 3 failed (stages 3, 7, 8, 12 red) | 17/17 suites, 275 passed, 0 failed, 1 skipped (stage 12 6.1a: no `type_params` field before 3.12) |
| CPython 3.10.20 | 11/15 suites (same 4 red) | 17/17 suites, 275 passed, 0 failed, 1 skipped (same) |

Step 2 (settlement layer added): 18/18 suites, 297 checks passed, 0 failed on CPython 3.10.20, 3.11.15, 3.12.3 and 3.13.12
(1 skip on 3.10/3.11, the same version-guarded AST check). Before Step 2 on 3.11/3.12: 17/17 suites, 275 checks.

Step 3 (market-data layer added): before, 18/18 suites and 297 checks (re-verified on 3.11 and 3.12). After,
19/19 suites, 329 checks passed, 0 failed on CPython 3.10.20, 3.11.15, 3.12.3 and 3.13.12 (the same 1 skip on 3.10/3.11).
`scripts/mutation_test_market_data.py`: 7/7 causal-rule mutations caught (as-is and with the engine guards disabled).

Step 4 (perp-data layer added): before, 19/19 suites and 329 checks (re-verified on 3.11 from the delivered Step-3 ZIP). After,
20/20 suites, 355 checks passed, 0 failed on CPython 3.10.20, 3.11.15, 3.12.3 and 3.13.12 (the same 1 skip on 3.10/3.11).
`scripts/mutation_test_perp_data.py`: 9/9 mutations caught (as-is and with the perp engine guards disabled).

Why 3.10/3.11 were red before: `kalshi_backtest.py` used a backslash inside an f-string expression
(legal only from Python 3.12, PEP 701). It could not even be imported, and stages 7/8 failed because
they re-run stage 3. Stage 12 test 6.1 assumed `FunctionDef.type_params`, an AST field that only
exists from 3.12. Both are fixed without changing behaviour (see the completion report).
Stage 15's browser checks need `node` (present here, so nothing was skipped).
Lint (`ruff`, default rules): 459 findings before, all pre-existing style. The new files add only
E702 (`fn(); print(...)`, the repository's own test idiom). `mypy` on `kalshi_core` + `run_local.py`:
no issues. `compileall`: clean on 3.10–3.13.

## 7. Known limitations (unchanged behaviour, documented, not fixed)

* **No calibration** in the live path; confidence is the raw log-normal probability.
* The model has **no drift** and a volatility estimate that ignores intraminute structure. Minutes
  left comes from the local clock (no clock-skew handling).
* `evaluate()` raises `TypeError` when the favoured side has no ask and no opposite bid (eager
  f-string in the verdict table). The poller records `error: …` and makes no call. Fixing it
  requires editing a fingerprinted function, so it is deferred to a deliberate change.
* Backtests: the strike and settlement are Coinbase proxies, not the CF Benchmarks RTI; entries are
  priced at model confidence; MIN_PRICE/edge/chop gates are not applied. Treat them as calibration
  checks, not P&L.
* `GATE_PRICE` differs: 90.0 in `kalshi_dashboard.py`, 85.0 in `kalshi_backtest.py`
  (expectancy display only).
* Time-of-day stats use `America/New_York` via `zoneinfo`. On Windows without the `tzdata` package
  they silently fall back to UTC (`TZ_OFFSET_HOURS` = 0). Fixtures pin the UTC fallback.
* `kalshi_dashboard.main()` and `run_local.py` do not restore a persisted "paused" state at startup
  (only the Discord bot does); the watcher starts running.
* All four coins are evaluated sequentially in one thread, so a slow endpoint delays the others.
* Legacy `print()` output remains on stdout alongside the new structured log on stderr.

## 8. What must remain stable in later phases

1. `config/strategy_baseline.json` and `regression/strategy_cases.json` verify, unless a phase
   deliberately re-baselines (`--i-intend-to-change-the-baseline`) and records here **what changed,
   why, and the out-of-sample evidence**.
2. The legacy fingerprint `8d94f241…` and `step5_baseline_manifest.json`, while the Step 6 live
   veto exists. Changing them invalidates every promotion by design.
3. Fail-closed data guards; causal (no-lookahead) perp alignment; the suppress-only scope of the
   veto; `get_execution_engine(LIVE)` raising; no core import of `discord`, `kalshi_api_learn` or
   `kalshi_core.execution` from prediction code.

### Re-baseline log

| Date | Phase | What changed | Evidence |
|---|---|---|---|
| 2026-09-24 | Step 1 | initial baseline (no behaviour change) | stages 1–17 green; fixtures identical on 3.10–3.13 |
| 2026-09-24 | Step 2 | **none**: settlement research layer added beside the strategy; no Step-1 artifact rewritten | Step-1 artifacts byte-identical (stage 18 test 1); 47 fixtures MATCH; stages 1–18 green on 3.10–3.13 (297 checks) |
| 2026-09-25 | Step 3 | **none** for the strategy. The separate settlement baseline was rewritten only to record the documented CF index-id provenance (OLD/NEW/WHY in SETTLEMENT_ENGINE.md); a new, separate `config/market_data_baseline.json` | Step-1 artifacts and perp/production files byte-identical (stage 19 test 1); 47 fixtures MATCH; legacy `8d94f241…` / extended `784141876…` unchanged; stages 1–19 green on 3.10–3.13 (329 checks) |
| 2026-09-25 | Step 4 | **none** for the strategy, the settlement engine, the Step-3 engine and the EXISTING perp veto chain (14 files byte-identical; fingerprint `499c1e16…`). New, separate `config/perp_data_baseline.json` | Step-1 artifacts identical (stage 20 test 1); 47 fixtures MATCH; legacy `8d94f241…` / extended `784141876…` unchanged; settlement and market-data fingerprints verify unchanged; stages 1–20 green on 3.10–3.13 (355 checks) |

### Step 2 notes

* Added `settlement/`, `scripts/`, `config/settlement_baseline.json` (its own fingerprint) and
  `test_stage18.py`. The only edits to existing files are the runner range (1–18), stage 13's `last = 18`,
  stage 17's runner assertion, `.gitignore`, and docs. `kalshi_dashboard.py`,
  `config/strategy_baseline.json`, `step5_baseline_manifest.json` and `regression/strategy_cases.json`
  are unchanged.
* **Missing-ask defect (fixture E19) intentionally NOT fixed.** The `TypeError` is raised inside the
  fingerprinted `evaluate()` and caught inside the fingerprinted `poller()`. No outer adapter or input
  normalisation can turn it into a clean production NO_CALL without fabricating an ask or editing pinned
  code. It still fails closed (`status: error: …`, no call). Fix it only in a deliberate re-baseline.

### Step 3 notes

* Added `market_data/` (collector, sources, transports, storage, replay, causal feature engine,
  dataset), `collect_market_data.py`, four scripts (`replay_market_data.py`, `build_market_features.py`,
  `bench_market_data.py`, `mutation_test_market_data.py`), `config/market_data_baseline.json`,
  `test_stage19.py` and `docs/HIGH_RESOLUTION_DATA.md`. Results: `analysis_output/market_data_performance.json`
  and `analysis_output/market_data_mutation_results.json`.
* Edits to existing files:
  * the runner range (1–19), stage 13's `last = 19` and stage 17's runner assertion;
  * `.gitignore` (session data ignored; the two result files kept) and `.env.example` (collector
    variable names, empty);
  * docs;
  * `settlement/assets.py`: the index-id provenance (constants only; mapping values unchanged), with
    its logged settlement re-baseline.
* `kalshi_dashboard.py`, the production probability, calls, thresholds, entry window, stops and the
  60-minute volatility are untouched. No new feature can create, veto or change a call. Nothing imports
  `market_data` except its own tools and tests.
* The Missing-ask defect (fixture E19) is still intentionally not fixed (see Step 2 notes).

### Step 4 notes

* Added:
  * `perp_data/` (venue adapters, collector, poller, store/replay on Step 3's infrastructure, causal perp
    feature engine with families, joint Step-3 + Step-4 dataset, synthetic generator, fingerprint);
  * `collect_research_data.py`;
  * four scripts (`replay_perp_data.py`, `build_research_dataset.py`, `bench_perp_data.py`,
    `mutation_test_perp_data.py`);
  * `config/perp_data_baseline.json`, `test_stage20.py` and `docs/PERP_HIGH_RESOLUTION_DATA.md`.

  Results are in `analysis_output/perp_data_performance.json` and
  `analysis_output/perp_data_mutation_results.json`.
* Edits to existing files:
  * the runner range (1–20), stage 13's `last = 20` and stage 17's runner assertion;
  * `.gitignore` (the two new result files are kept);
  * docs.
* No existing module changed: `market_data/` and `settlement/` are untouched (their fingerprints verify), and
  so are all perp-veto chain files and `kalshi_dashboard.py`. No OLD/NEW/WHY re-baseline was needed.
* The new perp features never enter the existing veto. The veto keeps its own features, statistics,
  thresholds, promotion state (INACTIVE) and strategy fingerprint binding.

