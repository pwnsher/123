# Risk architecture (Step 6.6 + 6.6.1 + 6.6.2: risk manager foundation, PAPER / SHADOW ONLY, zero live orders)

Package: `risk/` (pure stdlib, Python 3.10-3.13, exact `Decimal`). Tests: `test_stage24.py`. Mutations:
`scripts/mutation_test_risk.py` (R1-R35). Demo: `scripts/risk_restart_demo.py`. Fingerprint:
`config/risk_baseline.json`. Shadow policy: `config/risk_policy_shadow_v1.json` (TEST / SHADOW DEFAULT, NOT
RESEARCH-VALIDATED, NOT PRODUCTION-APPROVED).

Step 6.6 builds infrastructure, not strategy. It generates no prediction, creates no CALL, never overrides NO_CALL and
never places or cancels an order. Risk may only approve, reduce size, tighten price, expire an approval, veto, or
latch a safety breaker. It never increases size, widens price, flips direction, changes a probability or EV, creates
a signal, overrides missing / failed data, or manufactures an approval.

## 1. System boundary

```
  future signal engine (NOT built here)
            |  RiskCandidate (a proposed trade: side, size / price upper bounds, provenance)
            v
  +----------------------------- risk/ (Step 6.6) ------------------------------+
  |  RiskSnapshot (passed IN: portfolio / health / quote / execution facts)      |
  |  RiskPolicy   (versioned, fingerprinted, configurable limits)                |
  |        |                                                                     |
  |  evaluate()  (PURE: no clock read, no I/O, no network)                       |
  |        |                                                                     |
  |  RiskManager -> risk journal (SQLite, append-only): decision, approval,      |
  |                 breaker transitions, trade results, consumption              |
  |        |                                                                     |
  |  RiskDecision  APPROVE | REDUCE | VETO                                       |
  |        | (APPROVE / REDUCE only)                                             |
  |  RiskApproval  (bound + sealed, finite TTL, single use)                      |
  +--------|---------------------------------------------------------------------+
           v  risk.shadow.intent_from_approval (shadow / demo only)
  OrderIntent -> Step 6.5 ExecutionEngine (paper): verifies the approval binding (execution.risk), consumes it once
              -> PaperExecutionAdapter -> PaperVenue (simulated; no network)
```

Nothing in production imports `risk` (the structural test checks every production, data, research and execution
module, and `sys.modules` after importing the production entry points). Execution does not import `risk` either:
the dependency only points downward.

## 2. RiskCandidate

`risk/types.py`, frozen, schema version 1. Fields: `candidate_id, created_at, market_ticker, asset, side,
signal_status (CALL | NO_CALL), predicted_direction, requested_contracts, requested_max_limit_price, decision_ts,
expires_at, raw_probability, calibrated_probability, market_bid, market_ask, current_executable_price,
available_depth, estimated_fee, estimated_slippage, estimated_net_ev, model_id, model_fingerprint,
calibration_fingerprint, signal_fingerprint, checkpoint, market_close_ts`.

* Financial values are `Decimal` or the explicit `UNKNOWN`. Binary floats, bools and None are refused
  (`RiskInputError`); a missing value must be stated as UNKNOWN and is never coerced to 0 or to "safe".
* The requested size and price are UPPER BOUNDS. The side is immutable.
* `estimated_fee` / `estimated_slippage` are TOTALS for the requested size.
* Semantic validity (side / direction consistency, ranges, timestamps, known probabilities and hex fingerprints for a
  CALL, asset in the policy group map) is checked by the evaluation. A violation becomes an `INVALID_CANDIDATE` hard
  veto, so even a bad input yields an auditable decision.

## 3. RiskSnapshot

Frozen, schema version 1. It captures the exact state a decision used: equity, cash, day start / peak equity, daily
realized / unrealized PnL, open position count, portfolio / asset open risk and gross notional, the candidate group's
same- and opposite-direction risk, consecutive losses, the market / asset / side, current market position and open
risk, the five health fields, quote / feature / model-decision ages, spread, depth, minutes remaining, the
`exposure_group` the provider aggregated, and the execution facts (`market_execution_state`, `asset_execution_state`
from `EXEC_STATES`).

`risk_snapshot_hash` is a versioned sha256 of canonical JSON (sorted keys, Decimal canonical text, UNKNOWN as
"UNKNOWN"). It is stable across processes and machines, and field order and "1000" vs "1000.00" don't matter.
Every DECISION event stores the full snapshot. Risk never fetches or refreshes a snapshot: it is always passed in.

## 4. Health status types

`PASS, DEGRADED, FAIL, UNKNOWN, UNAVAILABLE` for feed, model, calibration, settlement and execution health. Risk does
not infer source-level health; it acts on the normalized statuses provided. FAIL, UNKNOWN and UNAVAILABLE always
veto (`*_HEALTH_FAIL / _UNKNOWN / _UNAVAILABLE`). DEGRADED vetoes (`*_HEALTH_DEGRADED`) unless the policy's
`allow_degraded_<x>` flag is true; DEGRADED is never allowed automatically.

## 5. RiskPolicy

`risk/policy.py`: versioned, immutable, every limit configurable. That includes the per-trade, asset, portfolio,
group and same-direction limits; the daily loss, drawdown and consecutive-loss limits; spread, depth and staleness
caps; minimum equity; the DEGRADED flags; the known fee / slippage / EV requirements; the approval TTL; and the
`asset_group_map`. There are also optional explicit reserves for unknown fee / slippage, an extra per-contract
reserve, and an optional `max_entry_price` (tighten only).

`risk_policy_fingerprint` is a sha256 over every policy value plus the rules version and the evaluation order. Any
semantic change (a limit, health behaviour, a staleness threshold, the TTL, a group mapping) changes it; mapping order
does not. Every RiskDecision stores it.

**RiskPolicy object vs ACTIVE policy (Step 6.6.2).** A `RiskPolicy` is a configuration value. The ACTIVE policy is
DURABLE, ordered state of the risk journal: `POLICY_ACTIVATED` events, append-only, each storing `event_id`,
`previous_policy_fingerprint`, `new_policy_fingerprint`, `policy_id`, `policy_version`, the full policy, the timestamp
and a mandatory `reason`.
* **Changing it:** `RiskManager.activate_policy(policy, reason)` (`set_policy` is an alias) appends one activation
  inside a risk-store transaction (`BEGIN IMMEDIATE`). It is therefore totally ordered with every evaluation and
  every final authorization, across processes, RiskManager instances and SQLite connections. The moment it commits,
  approvals carrying the previous fingerprint are rejected (`RISK_POLICY_CHANGED`).
* **Construction** validates the supplied policy and reconciles it with the journal:
  * a fresh store activates it (audited "initial activation");
  * the same fingerprint continues;
  * a different fingerprint raises `RiskPolicyMismatchError` unless an explicit `activate_reason` is given. There is
    never a silent overwrite or reversion.
* **Reopening:** `RiskManager.from_store(store, clock)` reopens with the persisted active policy, after re-verifying
  its fingerprint (a mismatch fails closed).
* **One policy per transaction:** every risk transaction resolves ONE `(policy, fingerprint)` pair from the journal
  and uses it for everything in that transaction: day-reset metadata, breaker thresholds and transitions, the
  evaluation's caps, the decision id, and the RiskDecision / RiskApproval fingerprint. Thresholds and identity can
  never mix policies.

**Why thresholds are not research-validated.** No value in this repository is claimed to be optimal or profitable.
Tests build explicit fixture policies, and `config/risk_policy_shadow_v1.json` is labelled TEST / SHADOW DEFAULT,
NOT RESEARCH-VALIDATED, NOT PRODUCTION-APPROVED, and is loaded by nothing in production. Final thresholds will be
chosen later from research and shadow / live evidence.

**Boundary semantics.** value <= configured maximum is allowed and value > maximum is capped or vetoed; value >=
configured minimum is allowed and value < minimum is vetoed. By definition, `max_consecutive_losses = N` latches at the
N-th consecutive loss (count >= N). Loss / drawdown breakers trip strictly above their maximum.

## 6. Worst-case loss (long binary contracts only)

Step 6.6 supports long Kalshi-style binary contracts only, with no naked short exposure. The worst-case loss uses the
MAXIMUM authorized price, never the bid, mid, model probability or EV:

```
per_contract   = approved_max_limit_price + extra_reserve_per_contract
                 (+ unknown_fee_reserve_per_contract / unknown_slippage_reserve_per_contract, only when that
                  estimate is UNKNOWN and the policy explicitly allows a reserve instead of a veto)
fixed_reserve  = estimated_fee_total + estimated_slippage_total     (kept in full when the size is reduced)
worst_case(q)  = q * per_contract + fixed_reserve                   10 * 0.40 + 0.20 = 4.20
```

Expected value and worst-case risk are separate. If a required fee or slippage is UNKNOWN, the result is VETO; risk
is never understated because accounting is incomplete. Exact Decimal arithmetic is used throughout, with no
floating-point off-by-one approvals.

## 7. Hard veto vs size reduction

**Hard veto** (always VETO, never a reduction): `INVALID_POLICY`, `INVALID_CANDIDATE`, `INVALID_SNAPSHOT`, `NO_CALL`, `EXPIRED_CANDIDATE`, `CANDIDATE_ALREADY_EXECUTED`, `STALE_RISK_SNAPSHOT`, `FEED_HEALTH_FAIL`, `FEED_HEALTH_UNKNOWN`, `FEED_HEALTH_UNAVAILABLE`, `FEED_HEALTH_DEGRADED`, `MODEL_HEALTH_FAIL`, `MODEL_HEALTH_UNKNOWN`, `MODEL_HEALTH_UNAVAILABLE`, `MODEL_HEALTH_DEGRADED`, `CALIBRATION_HEALTH_FAIL`, `CALIBRATION_HEALTH_UNKNOWN`, `CALIBRATION_HEALTH_UNAVAILABLE`, `CALIBRATION_HEALTH_DEGRADED`, `SETTLEMENT_HEALTH_FAIL`, `SETTLEMENT_HEALTH_UNKNOWN`, `SETTLEMENT_HEALTH_UNAVAILABLE`, `SETTLEMENT_HEALTH_DEGRADED`, `EXECUTION_HEALTH_FAIL`, `EXECUTION_HEALTH_UNKNOWN`, `EXECUTION_HEALTH_UNAVAILABLE`, `EXECUTION_HEALTH_DEGRADED`, `DAILY_REALIZED_LOSS_BREAKER`, `DAILY_TOTAL_LOSS_BREAKER`, `ROLLING_DRAWDOWN_BREAKER`, `CONSECUTIVE_LOSS_BREAKER`, `ACCOUNT_EQUITY_UNKNOWN`, `ACCOUNT_EQUITY_TOO_LOW`, `ACCOUNT_CASH_UNKNOWN`, `DAILY_PNL_UNKNOWN`, `EQUITY_STATE_UNKNOWN`, `CONSECUTIVE_LOSS_STATE_UNKNOWN`, `STALE_QUOTE`, `STALE_FEATURES`, `STALE_MODEL_DECISION`, `EXECUTION_UNRESOLVED`, `EXECUTION_MISMATCH`, `EXISTING_MARKET_EXPOSURE`, `EXPOSURE_STATE_UNKNOWN`, `PRICE_UNKNOWN`, `PRICE_ABOVE_CAP`, `SPREAD_UNKNOWN`, `SPREAD_TOO_WIDE`, `DEPTH_UNKNOWN`, `INSUFFICIENT_DEPTH`, `FEE_UNKNOWN`, `SLIPPAGE_UNKNOWN`, `EV_UNKNOWN`, `NON_POSITIVE_EV`.

**Size caps** (the approved size is the MINIMUM of the requested size and every cap; never an average, a median,
the first match or the most permissive): `PER_TRADE_CONTRACT_LIMIT`, `PER_TRADE_NOTIONAL_LIMIT`, `PER_TRADE_LOSS_LIMIT`, `PER_TRADE_EQUITY_FRACTION_LIMIT`, `INSUFFICIENT_CASH`, `ASSET_OPEN_RISK_LIMIT`, `ASSET_NOTIONAL_LIMIT`, `PORTFOLIO_OPEN_RISK_LIMIT`, `PORTFOLIO_NOTIONAL_LIMIT`, `CRYPTO_GROUP_RISK_LIMIT`, `SAME_DIRECTION_CRYPTO_LIMIT`, `DEPTH_CAP`.

Each cap is the largest q whose worst case (or notional `q * price` for notional caps) stays within the remaining
budget, floored to the contract quantum 0.01 (the repository's fixed-point contract granularity), so rounding only
ever reduces size. A cap of <= 0 means VETO. REDUCE reports every cap below the requested size (the binding
limits). No Kelly or dynamic sizing exists, and risk never increases size because a trade looks attractive.

**Price.** `approved_max_limit_price = min(requested, policy.max_entry_price if set)`. It is tightened only, never
widened, and no fixed "+0.01 / +0.02 entryDiff" exists anywhere. An executable price above the approved cap means
VETO (`PRICE_ABOVE_CAP`); an UNKNOWN executable price means VETO.

**EV.** UNKNOWN EV means VETO when the policy requires known EV; EV <= 0 always means VETO. Risk never changes EV or
recomputes a probability. The final EV signal semantics belong to the later EV signal engine.

## 8. Evaluation order (deterministic)

```
1 input validity (policy, candidate, snapshot, candidate/snapshot scope)
2 signal status (NO_CALL)
3 candidate expiry / market close
4 risk-snapshot freshness
5 system health (feed, model, calibration, settlement, execution)
6 breakers (latched state + triggers observed in this snapshot)
7 required account information (equity, cash, daily PnL, consecutive-loss state)
8 quote / feature / model-decision staleness
9 market: execution state, existing exposure, executable price, spread, depth
10 fee / slippage / EV known; EV > 0
11 per-trade caps (contracts, notional, worst-case loss, equity fraction, cash)
12 asset caps (open risk, gross notional)
13 portfolio caps (open risk, gross notional)
14 exposure-group cap (crypto group open risk)
15 same-direction group cap
16 executable-depth cap
17 decision: any hard veto -> VETO; else min(all caps) floored to the contract quantum: == requested -> APPROVE, 0 < q < requested -> REDUCE, <= 0 -> VETO
```

All applicable reason codes are collected and reported in the fixed `REASON_CODES` order, never in dict or set
iteration order. Structural invalidity (stage 1) stops further stages.

## 9. Exposure

* **Portfolio exposure:** total open risk and total gross notional.
* **Asset exposure:** the candidate asset's open risk and gross notional.
* **Crypto group:** `asset_group_map` (BTC, ETH, SOL, XRP to CRYPTO, configurable) is a conservative concentration
  bucket, NOT a statistical correlation estimate. The group's gross open risk counts BOTH directions.
* **Same-direction:** risk in the candidate's direction only. BTC UP 50 + ETH UP 40 + candidate SOL UP 30 = 120;
  BTC UP + ETH DOWN is not directional concentration, but both count in the group's gross exposure.
* **Existing same-market exposure** (an active entry, open position, non-zero position or open risk, or UNKNOWN
  exposure) means VETO; risk never assumes execution will net it. Exits come later.
* `risk.shadow.exposure_fields` / `exposures_from_engine` show how a provider can aggregate them (shadow only).

## 10. Breakers

Types: DAILY_REALIZED_LOSS, DAILY_TOTAL_LOSS, ROLLING_DRAWDOWN, CONSECUTIVE_LOSS. State machine:

```
CLEAR --TRIGGER--> TRIGGERED --LATCH--> LATCHED --RESET--> CLEAR      (any other transition fails closed)
```

* **Daily loss breakers:** the daily realized-loss breaker trips when the loss exceeds `max_daily_realized_loss`.
  The daily total-loss breaker uses realized + unrealized PnL only when both are authoritative. UNKNOWN PnL vetoes
  (`DAILY_PNL_UNKNOWN`) but never trips or latches a breaker (no PnL is invented).
* **Drawdown breaker:** drawdown = day peak equity - current equity. The peak is the maximum of the snapshot's
  `day_peak_equity`, its equity and every persisted snapshot of the same UTC day. UNKNOWN equity state vetoes.
* **Consecutive-loss breaker:** a closed trade with PnL < 0 increments the count; PnL > 0 resets it to 0; PnL == 0
  does neither (documented and tested). An UNKNOWN result is never a win: it is recorded as unresolved, and the
  loss-streak state is UNKNOWN (veto) until a final result arrives. The count used is the larger of the persisted
  streak and the snapshot's.
* **Observation is independent of the candidate (Step 6.6.1).** Every `RiskManager.evaluate()` first OBSERVES
  the snapshot (`_observe_breakers`), whatever the candidate is: CALL, NO_CALL or even an invalid candidate. Only then
  does it AUTHORIZE. A NO_CALL still never yields an approval, but an authoritative snapshot showing a breach latches
  the breaker. `observe()` uses the same path.
* **Breaker-authoritative snapshot** (`risk.evaluate.breaker_snapshot_problems` is empty):
  * it has a snapshot id;
  * `captured_at <= now` (never a future snapshot) and it was captured on the SAME UTC day as now (yesterday's
    figures never latch today's daily breakers);
  * its breaker fields are safely representable (exact Decimal / int or explicit UNKNOWN, guaranteed by RiskSnapshot
    construction, which refuses floats and None);
  * the consecutive-loss count, when known, is >= 0.

  UNKNOWN breaker fields never trip anything (they veto the candidate instead). Candidate properties (signal status,
  market scope, size) play no part.
* **Breaker persistence:** TRIGGER and LATCH are written in one transaction, and each transition stores
  `breaker_id, breaker_type, previous_state, new_state, timestamp, reason, snapshot_hash, policy_fingerprint` and the
  UTC day. Current state is the replay of that history, so a restart never clears a breaker, and better intraday PnL
  never unlatches it.
* **Reset semantics:** the daily realized, daily total and drawdown breakers reset at the first evaluation or
  observation on a LATER UTC calendar day than the day they latched (UTC from the millisecond timestamp, never the
  local time zone). CONSECUTIVE_LOSS resets only by an explicit operator reset with a reason. Every reset is logged.
* **RESET_RULE is enforced (Step 6.6.1).** `RiskManager.reset_breaker` allows an operator reset ONLY for a
  breaker whose rule is OPERATOR (CONSECUTIVE_LOSS), and only with a non-empty reason. A reset of DAILY_REALIZED_LOSS,
  DAILY_TOTAL_LOSS or ROLLING_DRAWDOWN (rule UTC_DAY_BOUNDARY), an unknown breaker or a reasonless reset raises
  `RiskBreakerResetError`. Breaker history is untouched; the refused attempt is recorded as an append-only
  `BREAKER_RESET_REJECTED` audit event.
* **The CONSECUTIVE_LOSS reset is effective (Step 6.6.2).** The operator reset appends a `LOSS_STREAK_RESET` event
  (`reset_id`, timestamp, `operator_reason`, `previous_consecutive_losses`, `new_baseline = 0`, `epoch`,
  `policy_fingerprint`) and the breaker LATCHED -> CLEAR transition in ONE risk-store transaction. A crash between
  or after them rolls BOTH back. `RiskStore.loss_streak()` replays the journal in order:
  * a reset sets the streak to 0, and earlier losses no longer count;
  * later losses count normally; a win resets; zero PnL keeps the current post-reset streak;
  * an UNKNOWN result stays unresolved (fail closed, never a win), whenever it was recorded;
  * TRADE_RESULT history is never deleted;
  * a second reset while CLEAR is an invalid breaker transition, so it is never applied twice;
  * restart reconstructs everything.
* **Consecutive-loss source of truth (Step 6.6.2).** The persisted, post-reset streak of the risk journal is
  canonical. Each reset starts a new consecutive-loss EPOCH (the count of LOSS_STREAK_RESET events). A snapshot's
  `consecutive_losses` is taken into account (the maximum of it and the journal's) ONLY when its `loss_streak_epoch`
  equals the journal's current epoch:
  * a count from an older epoch predates the operator reset and is not applied, so an old external "3" cannot
    immediately undo the reset;
  * a newer or UNKNOWN epoch is inconsistent evidence, which means CONSECUTIVE_LOSS_STATE_UNKNOWN (veto).

  Snapshot loss information is therefore still used, just never across a reset it does not know about.

## 11. RiskDecision

Fields: `risk_decision_id, candidate_id, candidate_hash, decision, decision_ts, expires_at, market_ticker, asset,
side, signal_status, requested_contracts, approved_contracts, requested_max_limit_price, approved_max_limit_price,
calculated_worst_case_loss, risk_snapshot_hash, risk_policy_fingerprint, reason_codes, human_readable_reasons`.

The constructor re-checks every invariant as a second line of defence:
* approved >= 0 and approved <= requested;
* approved price <= requested price;
* APPROVE means approved == requested; REDUCE means 0 < approved < requested; VETO means approved == 0;
* only a CALL can be approved.

**Deterministic id.** `"rd-" + sha256(canonical {v: risk_decision_v1, candidate_id, candidate_hash, snapshot hash,
policy fingerprint, decision, approved contracts, approved price, reason codes})`. There is no random UUID. The same
candidate + snapshot + policy gives the same decision, and the store returns the recorded one, so re-evaluation is
idempotent. A changed snapshot or policy gives a new id. Reusing a `candidate_id` with different content means
`INVALID_CANDIDATE`.

## 12. RiskApproval, approval TTL and binding

Only APPROVE / REDUCE produce a RiskApproval: `risk_decision_id, candidate_id, approved_contracts,
approved_max_limit_price, approved_side, market_ticker, asset, issued_at, expires_at, risk_snapshot_hash,
risk_policy_fingerprint, signal_fingerprint, model_fingerprint, calibration_fingerprint`.

* **Approval TTL:** `expires_at = min(decision time + approval_ttl_ms, candidate.expires_at, market_close_ts)`, which
  is finite. An expired approval is rejected by execution. It is never silently renewed: re-evaluating the same
  snapshot returns the same decision (or a stale-snapshot VETO), and only a NEW snapshot yields a new approval.
* **Binding:** `to_execution()` seals every binding field with `execution.risk.approval_binding_hash`. Execution
  rejects a tampered approval, another candidate, another side, market or asset, a larger size, a worse price, an
  expired approval, different provenance, and an approval issued under another policy (`RISK_POLICY_CHANGED`: the
  durable book requires the CURRENT policy fingerprint).
* **Provenance:** copied from the candidate, never manufactured. A candidate without hex signal / model /
  calibration fingerprints is vetoed as INVALID_CANDIDATE, and a RiskApproval cannot even be constructed without
  them.
* **Breakers stop outstanding approvals:** the durable book rejects every approval while any breaker is
  latched (`RISK_BREAKER_LATCHED`). A breaker that latches AFTER an approval was issued therefore still stops that
  entry, before consumption and after a restart.
* **One valid approval per candidate:** a newer approval SUPERSEDES an unexpired, unconsumed older one
  (`RISK_APPROVAL_SUPERSEDED`). A candidate whose approval was consumed is never approved again
  (`CANDIDATE_ALREADY_EXECUTED`).

## 13. Final authorization, approval consumption and replay (Step 6.6.1 / 6.6.2)

`StoreApprovalBook` exposes two operations:

* `verify(intent, now_ms)` is **read-only**. Execution calls it during PRECHECK and READY; it never consumes.
* `verify_and_consume(intent, execution_key, now_ms)` is the **final authorization**. Execution calls it exactly once,
  immediately before READY -> SUBMITTING (after the order-vs-intent invariants). In ONE risk-store transaction
  (`BEGIN IMMEDIATE`, so it is serialized with every breaker / approval / supersession write) it re-checks
  everything against the CURRENT clock (the later of the engine's freshly read time and the risk clock, read inside
  the transaction) and the persisted state:
  * the approval exists and is not superseded;
  * no breaker is TRIGGERED or LATCHED;
  * binding hash, candidate, the DURABLE active policy fingerprint (read from the journal inside the transaction),
    provenance and snapshot hash;
  * expiry: `now >= expires_at` means EXPIRED;
  * market / asset / side, size and price;
  * prior consumption: the same intent_id + execution_key is an idempotent replay, anything else is rejected.

  Then comes the **final critical section (Step 6.6.2)**, with no callback, hook or fault point between its checks
  and the append:
  * `authorized_at` = a FINAL read of the current clock (the later of the clock and the earlier `now`);
  * the durable active policy fingerprint is re-read and must equal the approval's;
  * `authorized_at < expires_at` is re-checked (`>=` means RISK_APPROVAL_EXPIRED);
  * the breakers are re-read;
  * then the CONSUMPTION event is appended, carrying exactly that `authorized_at` and the approval's policy
    fingerprint.

  An exact replay of the same intent re-runs this section too (an expired / re-policied replay is rejected).
* **The linearization point has two parts (Step 6.6.2):**
* **Time authorization point:** the final clock read `authorized_at`, immediately before the append. Time cannot be
  locked, so expiry is decided at that exact, recorded instant, never at a stale earlier read.
* **Durability / state-ordering point:** the COMMIT of the CONSUMPTION event. A breaker transition, supersession or
  policy activation committed BEFORE it makes the authorization fail (it is read inside the serialized transaction).
  One committed AFTER it cannot affect an execution that is already authorized; it governs only later
  authorizations (exits / position management come later). A policy activation attempted from another connection
  while the final transaction holds the write lock waits for it and is ordered after the consumption.
* **A final-check failure** (breaker, expiry, supersession, policy, binding, consumed by another intent) means
  READY -> REJECTED with the exact risk reason. No venue request was made, so it is never EXECUTION_UNKNOWN, and
  there are zero submit attempts.
* **A crash before consumption** rolls back: the approval stays unconsumed and a replay authorizes normally.
* **A crash after consumption but before SUBMITTING** (execution fault point
  `after_final_risk_consumption_before_submitting`) leaves the approval durably bound to that intent / execution key:
  * no other intent can use it, and the candidate is never approved again (`CANDIDATE_ALREADY_EXECUTED`);
  * on restart the execution journal still shows READY. Replaying the SAME intent re-runs every final check and
    replays the consumption idempotently, then submits exactly once;
  * if the approval has expired (or a breaker latched) in the meantime, the final check fails: REJECTED, with no order
    and no second authorization. The spent approval stays spent (fail closed).

## 14. Execution integration

The Step 6.5 manual `RiskApprovalBook` still works (in-memory single use). The durable book is
`RiskManager.approval_book()`. Execution still verifies `risk_decision_id` and `risk_snapshot_hash`, and now also
the binding, candidate, policy, provenance, side, market, asset, maximum size, maximum price and expiry. Only approval
validation changed in execution; the execution fingerprint OLD / NEW / WHY is in `EXECUTION_ARCHITECTURE.md` §20.

**Fail closed on accounting uncertainty.** EXECUTION_UNKNOWN, HALTED_FOR_RECONCILIATION, an unsafe
ACCOUNTING_INCOMPLETE or an UNKNOWN execution state for the market or the asset means VETO `EXECUTION_UNRESOLVED`;
POSITION_MISMATCH / FILL_MISMATCH means VETO `EXECUTION_MISMATCH`. Risk never repairs execution state:
reconciliation owns it. `risk.shadow.execution_facts` derives these facts from an execution journal (shadow only).

## 15. Risk journal

`risk/store.py`, SQLite, schema version 2 (Step 6.6.2: + POLICY_ACTIVATED, LOSS_STREAK_RESET). The table and trigger
DDL is unchanged, so a version-1 store is migrated meta-only (`migrated_from = 1`; the manager then records an audited
initial policy activation) and a newer or unknown version is refused. It is append-only (triggers abort UPDATE /
DELETE). Writes need a transaction
(`BEGIN IMMEDIATE`; any exception, including a simulated crash, rolls back). Event kinds: DECISION (the candidate, the
full snapshot, policy fingerprint, decision, sizes, prices, worst-case loss, caps, triggers, reason codes, approval
id, timestamp, expiry), APPROVAL, APPROVAL_SUPERSEDED, CONSUMPTION (with `authorized_at`), BREAKER, TRADE_RESULT,
OBSERVATION, BREAKER_RESET_REJECTED, POLICY_ACTIVATED, LOSS_STREAK_RESET.

Deterministic event ids make an identical re-append a no-op; differing content raises. The journal answers: why was
this trade allowed or vetoed, why was its size reduced, and what exact state did risk see?

## 16. Restart / crash recovery

All risk state is derived by replaying the journal. The crash points are `before_decision_write`, `during_decision_transaction`, `after_decision_write`, `before_approval_creation`, `after_approval_creation`, `during_breaker_transition`, `after_breaker_trigger`, `during_approval_consumption`, `after_approval_consumption`. The tests crash at each one and
restart, and confirm:
* no approval disappears incorrectly, and no veto becomes an approval;
* no breaker clears, and decision identity is unchanged;
* an approval cannot be reused improperly (consumption replays idempotently).

`scripts/risk_restart_demo.py` shows both required flows in separate processes:
* an approval is reconstructed after restart, and the same candidate gets the same decision with no conflicting
  approval;
* a loss breaker latches, is still latched after restart, and a new candidate is vetoed.

## 17. UNKNOWN semantics and fail closed

UNKNOWN is a singleton that refuses arithmetic, comparison and truthiness, so it can never silently become 0. UNKNOWN
health, ages, PnL, equity, cash, exposure, depth (when required), spread (when required), price, fee, slippage
(without an explicit policy reserve) and EV (when required) all VETO. Risk never invents data and never refreshes
anything.

## 18. Risk fingerprint

`py -m risk.fingerprint --verify` covers the five schemas and their versions, the evaluation order, the hard-veto and
cap codes, the worst-case formula, the contract quantum, breaker states, transitions and reset rules, the approval
binding fields, the persistence DDL hash and event kinds, the fault points, the vocabularies, the invariants,
`live_execution_available`, and the canonical AST of every `risk/` module. It never includes database contents.
Rewriting requires `--write --i-intend-to-change-the-risk-baseline` and an OLD / NEW / WHY entry here.

| Step | OLD | NEW | Why |
|---|---|---|---|
| Step 6.6 | (none) | `c44e6b1850d645b6f3dc65c3754079b9d6ba4aad807c14a8b52edf060e469693` | initial risk manager foundation |
| Step 6.6.2 | `4f791073991178a99664173ba2e6f15900e904460e53fd71af49ffb9c36a1664` | `d489039cae8495dbe3a2614ee17cfd6b13f13e95e0e9e6d3728a6d690a8b694f` | final authorization linearization and streak reset hardening. `risk/manager.py`: a durable, ordered active policy (`activate_policy`, `from_store`, construction reconciliation, `RiskPolicyMismatchError`); one (policy, fingerprint) per transaction; a final critical section in `verify_and_consume` (final clock read `authorized_at`, durable-policy / expiry / breaker re-check, then the append); the CONSECUTIVE_LOSS reset appends LOSS_STREAK_RESET atomically with the breaker clear. `risk/store.py`: schema 2 (POLICY_ACTIVATED, LOSS_STREAK_RESET, meta-only v1 migration), streak relative to the latest reset, epoch. `risk/types.py`: RiskSnapshot schema 2 (+ `loss_streak_epoch`). `risk/evaluate.py`: the epoch rule for consecutive losses. `risk/faults.py`: two reset fault points. The old baseline is archived in `config/history/risk_baseline_step6.6.1.json`. |
| Step 6.6.1 | `c44e6b1850d645b6f3dc65c3754079b9d6ba4aad807c14a8b52edf060e469693` | `4f791073991178a99664173ba2e6f15900e904460e53fd71af49ffb9c36a1664` | audit hardening. `risk/evaluate.py`: `breaker_snapshot_problems` (authority criteria). `risk/manager.py`: breaker observation independent of the candidate's signal; RESET_RULE enforced (`RiskBreakerResetError`); `set_policy`; `StoreApprovalBook` with read-only `verify` and atomic `verify_and_consume` (all checks + consumption in one transaction at the current time). `risk/store.py`: the unchecked `consume` primitive removed; new audit event kind `BREAKER_RESET_REJECTED`. The old baseline is archived in `config/history/risk_baseline_step6.6.json`. |

## 19. Why Step 6.6 cannot place live orders

* The risk package imports no network, crypto or credential module, contains no URL, endpoint, HTTP-verb or
  credential literal, calls no `post / put / patch / delete / request / urlopen / create_connection / sign / getenv /
  submit_order / cancel_order`, and never reads `os.environ`. The structural test 63 enforces this, and mutation R34
  injects a network path to prove the test catches it.
* Risk never calls an adapter. Orders exist only through the Step 6.5 paper path, whose live adapter is a stub that
  refuses every call; the repository-wide LIVE refusal is unchanged and tested.
* Nothing in production imports `risk` or `execution`.

## 20. Mutation tests (R1-R55)

`py scripts/mutation_test_risk.py` (R1-R55) applies each mutation to a temporary copy and runs the behavioural Stage-24 tests
(never the fingerprint test). The mutations:
* **Signal, side, size, price:** R1 NO_CALL approved; R2 side flipped; R3 size increased; R4 price widened;
  R5 hard veto downgraded to a reduction.
* **Health and staleness:** R6 feed FAIL ignored; R7 UNKNOWN health treated as PASS; R8 expired candidate approved;
  R9 stale quote approved; R10 stale snapshot approved.
* **EV, fees, worst case:** R11 non-positive EV approved; R12 / R13 UNKNOWN fee / slippage treated as zero; R14
  worst case computed at the current price.
* **Caps:** R15-R19 per-trade / asset / portfolio / group / same-direction cap ignored; R20 the largest cap used
  instead of the smallest.
* **Breakers:** R21 breaker not latched; R22 breaker lost on restart; R23 drawdown ignored; R24 consecutive loss
  ignored.
* **Approvals:** R25 approval expiry ignored; R26-R29 approval reusable for another candidate / the opposite side /
  a larger size / a worse price.
* **Determinism and safety:** R30 non-deterministic decision id; R31 policy change not fingerprinted; R32 non-atomic
  journal; R33 execution UNKNOWN not vetoed; R34 network path introduced; R35 approval manufactures missing
  provenance.
* **Extra:** R36 the read-only verify ignores a breaker latched after issuance.
* **Step 6.6.1:** R37 NO_CALL skips breaker observation; R38 / R39 / R40 a same-day operator reset clears
  DAILY_REALIZED_LOSS / DAILY_TOTAL_LOSS / ROLLING_DRAWDOWN; R41 / R42 / R43 / R44 the final consume ignores a newly
  latched breaker / approval expiration / supersession / a policy change (since Step 6.6.2 R41 / R42 / R44 disable
  both the authorize check and the final critical-section re-check, which is otherwise defence in depth); R45 consumption during PRECHECK instead of
  READY; R46 the final consume skips revalidation (non-atomic); R47 after a crash past consumption, another intent
  can use the approval.
* **Step 6.6.2:**
  * R48 policy changed after the final check, old approval still consumed;
  * R49 a stale time lets an approval that expired inside the critical section through;
  * R50 the CONSECUTIVE_LOSS reset clears the breaker but not the streak;
  * R51 the streak reset is not restart persistent;
  * R52 the streak reset and the breaker clear are non-atomic;
  * R53 a pre-reset snapshot count defeats the reset;
  * R54 a decision evaluated under policy A is persisted with policy B's fingerprint;
  * R55 a policy change is not durable.

Results are in `analysis_output/risk_mutation_results.json`.

## 21. Deferred work

**Step 6.7 (next):** a realistic execution simulator (order-book replay, slippage, latency, maker queue, fill
probability), feeding realistic fee / slippage estimates into candidates.

**Later:** research-derived thresholds, sizing beyond the fixed caps (Kelly / dynamic sizing remain out of scope
until validated), the final EV signal engine, exit / stop strategy, real account reconciliation, and wiring a real
signal engine to RiskCandidate in shadow mode.

**Eventual live trading (separate, explicitly approved step):** a real execution adapter, real balances, transfers and
operational controls. None of this exists today; LIVE remains refused.
