# Execution architecture (Step 6.5: execution foundation, ZERO live orders)

Status: **paper / shadow only.** This step adds the infrastructure a future live executor will need: identity,
state, persistence, accounting, reconciliation and failure handling. It **cannot place a real order**, it is **not
wired into any production decision**, and it changes nothing in the prediction, NO_CALL, PUP / confidence, threshold,
settlement, feature, collector, calibration, perp-veto or research-gate code.

Package: `execution/` (pure stdlib, Python 3.10-3.13, exact `Decimal` arithmetic).
Tests: `test_stage23.py`. Mutations: `scripts/mutation_test_execution.py` (E1-E20).
Demo: `scripts/execution_restart_demo.py`. Fingerprint: `config/execution_baseline.json`.

## 1. System boundary

```
   (future, Step 6.6+)                       Step 6.5 (this package)                         outside world
 calibrated signal + risk  --OrderIntent-->  ExecutionEngine --OrderRequest-->  ExecutionAdapter
 manager (NOT built here)                      |  ^                              |-- PaperExecutionAdapter -> PaperVenue (simulated)
                                               v  |                              '-- FutureKalshiExecutionAdapter (stub: every
                                         Journal (SQLite, append-only)                 method raises LiveExecutionUnavailable)
                                         Ledger (Decimal, rebuilt from the journal)
                                         Reconciliation (pure function: assess)
```

* Inputs: a validated, immutable `OrderIntent` plus a `RiskApprovalBook` (lookup only: execution never manufactures,
  widens or overrides an approval).
* Outputs: journal events, ledger snapshots, market-lock status, audit views. Nothing in production reads them.
* No module outside `execution/` imports `execution` (the structural test scans every production, data and research
  module and also checks `sys.modules` after importing the production entry points).

## 2. OrderIntent

`execution/intent.py`: a frozen dataclass (`ORDER_INTENT_SCHEMA_VERSION = 1`). Fields: `intent_id, candidate_id,
created_at, market_ticker, asset, side (YES|NO), requested_contracts, max_limit_price, time_in_force
(IMMEDIATE_OR_CANCEL|FILL_OR_KILL|GOOD_TIL_EXPIRY), decision_ts, expires_at, raw_probability, calibrated_probability,
market_price, estimated_fee, estimated_slippage, estimated_net_ev, model_id, model_fingerprint,
calibration_fingerprint, signal_fingerprint, risk_decision_id, risk_snapshot_hash, schema_version`.

Validation (raises `IntentError`): non-empty ids; side / time-in-force enumerations; quantities and prices are
`Decimal` (floats are refused; price in (0, 1] with at most 4 decimals; contracts > 0 with at most 2 decimals);
probabilities in [0, 1]; `decision_ts <= created_at < expires_at`; fingerprints are hex; the risk reference is
present. An unknown estimate stays the `UNKNOWN` sentinel and is never coerced to zero. `content_hash()` hashes the
canonical JSON of every field.

## 3. Execution key (deterministic idempotency)

`execution/identity.py`, version `exec_key_v1`:

```
execution_key = sha256( canonical_json({"v": "exec_key_v1", <EXECUTION_KEY_FIELDS>}) )
EXECUTION_KEY_FIELDS = intent_id, market_ticker, asset, side, requested_contracts, max_limit_price,
                       time_in_force, expires_at, risk_decision_id
canonical_json       = json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=True)
Decimal text         = canonical (no exponent, no trailing zeros: "5.00" -> "5", "0.5000" -> "0.5")
```

Field order, Decimal formatting and the process do not matter (tested across fresh processes). The key and the
client order id are persisted with the intent row.

## 4. client_order_id

`client_order_id = "x1" + sha256("coid_v1|" + execution_key)[:30]` (32 characters, `[0-9a-fx]`). It is a pure
function of the execution key: the same intent after any number of restarts produces the same id, and the engine
never submits a request whose id differs (invariant). Idempotency is enforced in three layers: the engine's
recorded-intent check, the journal's unique `intent_id` (same content: no-op; other content: `JournalConflict`), and
the venue / adapter keyed by `client_order_id`.

## 5. State diagram

```
                     (start)
                        |
                     CREATED ----------------------------.
                        |                                 |
                    VALIDATED ------------------.         |
                        |                        |        |
                     PRECHECK ---------------.   |        |
                        |                     |   |        |
                      READY ----------.       |   |        |
                        |  (persisted |       v   v        v
                        |   first)    '---> EXPIRED     REJECTED  (terminal)
                    SUBMITTING ------------------------------^
                     |       \
                     |        '--> EXECUTION_UNKNOWN --> HALTED_FOR_RECONCILIATION
                     v                    ^   ^                 |  (only RECONCILIATION
                ACKNOWLEDGED -------------'   |                 |   with evidence leaves)
                 |   |    \                   |                 v
                 |   |     '--> CANCEL_PENDING-+     ACKNOWLEDGED / PARTIALLY_FILLED / FILLED /
                 |   v              |  |            CANCELLED / EXPIRED / REJECTED / SETTLEMENT_PENDING
                 | PARTIALLY_FILLED<'  |
                 |   |  (repeats)      v
                 v   v              CANCELLED
                FILLED                 |
                   \                   |       EXPIRED
                    '----> SETTLEMENT_PENDING <-'
                                |
                              CLOSED (terminal)

  Any of ACKNOWLEDGED, PARTIALLY_FILLED, FILLED, CANCEL_PENDING, CANCELLED, EXPIRED, SETTLEMENT_PENDING
  --RECONCILIATION (POSITION_MISMATCH / FILL_MISMATCH)--> HALTED_FOR_RECONCILIATION
```

State groups: TERMINAL = {REJECTED, CLOSED}; PRE_SUBMIT = {CREATED, VALIDATED, PRECHECK, READY};
OUTSTANDING = {SUBMITTING, ACKNOWLEDGED, PARTIALLY_FILLED, CANCEL_PENDING}; UNSAFE = {EXECUTION_UNKNOWN,
HALTED_FOR_RECONCILIATION}.

## 6. Complete transition table

`execution/states.py` holds the ONLY authoritative table. `check_transition(prev, new, actor, evidence)` raises
`InvalidTransition` for any pair that is not listed, for the wrong actor, and for a RECONCILIATION transition without
evidence (a reconciliation event id). The journal re-validates the whole chain on every `reconstruct()`, so a forged
history is refused. The only way into SUBMITTING is from READY: no path re-submits.

| Previous state | New state | Allowed actor(s) |
|---|---|---|
| (start) | CREATED | ENGINE |
| ACKNOWLEDGED | CANCELLED | ENGINE, RECONCILIATION |
| ACKNOWLEDGED | CANCEL_PENDING | ENGINE |
| ACKNOWLEDGED | EXECUTION_UNKNOWN | ENGINE |
| ACKNOWLEDGED | EXPIRED | ENGINE, RECONCILIATION |
| ACKNOWLEDGED | FILLED | ENGINE, RECONCILIATION |
| ACKNOWLEDGED | HALTED_FOR_RECONCILIATION | RECONCILIATION |
| ACKNOWLEDGED | PARTIALLY_FILLED | ENGINE, RECONCILIATION |
| CANCELLED | HALTED_FOR_RECONCILIATION | RECONCILIATION |
| CANCELLED | SETTLEMENT_PENDING | ENGINE |
| CANCEL_PENDING | CANCELLED | ENGINE, RECONCILIATION |
| CANCEL_PENDING | EXECUTION_UNKNOWN | ENGINE |
| CANCEL_PENDING | FILLED | ENGINE, RECONCILIATION |
| CANCEL_PENDING | HALTED_FOR_RECONCILIATION | RECONCILIATION |
| CANCEL_PENDING | PARTIALLY_FILLED | ENGINE, RECONCILIATION |
| CREATED | REJECTED | ENGINE |
| CREATED | VALIDATED | ENGINE |
| EXECUTION_UNKNOWN | HALTED_FOR_RECONCILIATION | ENGINE, RECONCILIATION |
| EXPIRED | HALTED_FOR_RECONCILIATION | RECONCILIATION |
| EXPIRED | SETTLEMENT_PENDING | ENGINE |
| FILLED | HALTED_FOR_RECONCILIATION | RECONCILIATION |
| FILLED | SETTLEMENT_PENDING | ENGINE |
| HALTED_FOR_RECONCILIATION | ACKNOWLEDGED | RECONCILIATION |
| HALTED_FOR_RECONCILIATION | CANCELLED | RECONCILIATION |
| HALTED_FOR_RECONCILIATION | EXPIRED | RECONCILIATION |
| HALTED_FOR_RECONCILIATION | FILLED | RECONCILIATION |
| HALTED_FOR_RECONCILIATION | PARTIALLY_FILLED | RECONCILIATION |
| HALTED_FOR_RECONCILIATION | REJECTED | RECONCILIATION |
| HALTED_FOR_RECONCILIATION | SETTLEMENT_PENDING | RECONCILIATION |
| PARTIALLY_FILLED | CANCELLED | ENGINE, RECONCILIATION |
| PARTIALLY_FILLED | CANCEL_PENDING | ENGINE |
| PARTIALLY_FILLED | EXECUTION_UNKNOWN | ENGINE |
| PARTIALLY_FILLED | EXPIRED | ENGINE, RECONCILIATION |
| PARTIALLY_FILLED | FILLED | ENGINE, RECONCILIATION |
| PARTIALLY_FILLED | HALTED_FOR_RECONCILIATION | RECONCILIATION |
| PARTIALLY_FILLED | PARTIALLY_FILLED | ENGINE, RECONCILIATION |
| PARTIALLY_FILLED | SETTLEMENT_PENDING | ENGINE |
| PRECHECK | EXPIRED | ENGINE |
| PRECHECK | READY | ENGINE |
| PRECHECK | REJECTED | ENGINE |
| READY | EXPIRED | ENGINE |
| READY | REJECTED | ENGINE |
| READY | SUBMITTING | ENGINE |
| SETTLEMENT_PENDING | CLOSED | ENGINE |
| SETTLEMENT_PENDING | HALTED_FOR_RECONCILIATION | RECONCILIATION |
| SUBMITTING | ACKNOWLEDGED | ENGINE |
| SUBMITTING | EXECUTION_UNKNOWN | ENGINE |
| SUBMITTING | REJECTED | ENGINE |
| VALIDATED | EXPIRED | ENGINE |
| VALIDATED | PRECHECK | ENGINE |
| VALIDATED | REJECTED | ENGINE |

## 7. Persistent journal

`execution/journal.py`, SQLite, `JOURNAL_SCHEMA_VERSION = 1`, tables `meta`, `intents`, `events`.

* **Append-only:** triggers abort any UPDATE / DELETE on `events` and `intents`.
* **Transactional:** every write runs inside `journal.transaction()` (`BEGIN IMMEDIATE ... COMMIT`; any exception,
  including a simulated process death, rolls back). Multi-event writes (EXECUTION_UNKNOWN + HALTED, fills + fees,
  settlement + SETTLEMENT_PENDING) are one atomic unit. Writes outside a transaction raise `JournalError`.
* **Deterministic event ids:** `t:<key>:<n>` transitions, `r:<key>:<n>` reconciliations, `fill:<adapter>:<fill_id>`,
  `fee:<adapter>:<fee_id>`, `m:<key>:<n>` marks, `s:<key>` settlement, `a:<intent_id>:intent` audit. The same id with
  identical content is a no-op (`DUPLICATE`); with different content it raises `JournalConflict`.
* **Persist first:** SUBMITTING is committed before the adapter is called; CANCEL_PENDING before a cancel is sent.
* **Replay:** `reconstruct()` rebuilds every execution's state, order id, history and last reconciliation verdict;
  the ledger, locks and positions are all derived from it. Nothing authoritative lives in memory.
* Event kinds: TRANSITION, ORDER_REF, FILL, FEE, MARK, RECONCILIATION, SETTLEMENT, AUDIT.

## 8. Position ledger

`execution/ledger.py` (`ledger_v1`), one ledger per execution, rebuilt from journal events:

* fills keyed by `fill_id` (a duplicate notification is ignored; the same id with other content raises
  `FillConflict`); fees keyed by `fee_id` the same way;
* `average_entry_price = sum(q * p) / sum(q)` exactly (2 @ 0.40 + 3 @ 0.50 = 0.46);
* a fill without a fee record, or a fee reported as UNKNOWN, makes `fees_paid` **UNKNOWN** (never zero);
* a missing mark is **UNKNOWN** and unrealized PnL is UNKNOWN; `pnl_authoritative` is true only when accounting is
  complete;
* settlement pays 1 per contract to the winning side and 0 otherwise, on the FILLED quantity only.

`UNKNOWN` (`execution/money.py`) is a singleton that refuses arithmetic, comparison and truthiness, so it can never be
silently treated as 0.

## 9. Paper adapter

`execution/adapter.py` defines `ExecutionAdapter` (`submit_order, cancel_order, get_order, get_fills, get_positions,
get_balance, reconcile`). `execution/paper.py` implements `PaperExecutionAdapter` over a `PaperVenue`: the simulated
outside world, deterministic, saved and loaded as JSON so it survives a process restart. Scenarios:

* submit modes: `ACCEPT`, `REJECT`, `LOST_ACK` (accepted, response lost), `TIMEOUT_NOT_RECEIVED` (nothing happened,
  outcome unknown), `CRASH_AFTER_ACCEPT` (accepted, process dies);
* cancel modes: `OK`, `LOST_ACK`, `AMBIGUOUS_NOT_DONE`, `FILL_THEN_CANCEL` (a full or partial fill races the cancel);
* immediate / partial / multiple / delayed / no fills, duplicate fill notifications, fills above the cap (forced, to
  test detection), UNKNOWN fees;
* order lookup authoritative or not; query failures (timeouts); position `LIVE`, `STALE`, `FAIL` or `CONFLICT`;
* adapter restart (a new adapter over the same venue sees the same truth).

`PaperExecutionAdapter.writable_venue = False`: it only mutates the in-memory simulation.

## 10. Reconciliation engine

`execution/reconcile.py`: `assess(state, intent, client_order_id, snapshot, ledger, other_exposure)` is a pure
function over the adapter snapshot (order lookup, fills, position, each collected independently so one failure does
not hide another). Verdicts: CONSISTENT, RECOVERABLE_DIFFERENCE, EXECUTION_UNKNOWN, POSITION_MISMATCH, FILL_MISMATCH,
ACCOUNTING_INCOMPLETE. Rules, in order:

1. pre-submit executions are never queried and are CONSISTENT (nothing was sent)
2. order lookup failed while a request was in flight (SUBMITTING / CANCEL_PENDING / UNKNOWN / HALTED) -> EXECUTION_UNKNOWN; otherwise -> ACCOUNTING_INCOMPLETE (unsafe)
3. order not found: authoritative absence + no fills + flat position -> RECOVERABLE_DIFFERENCE to REJECTED; otherwise EXECUTION_UNKNOWN (never assume the submit failed)
4. order parameters differing from the recorded intent -> FILL_MISMATCH
5. fills query failed -> ACCOUNTING_INCOMPLETE (unsafe)
6. fills deduplicated by fill_id; the same id with different content -> FILL_MISMATCH
7. a journaled fill the venue no longer reports -> FILL_MISMATCH
8. fill total > requested, a fill price > max_limit_price, or fill total != the order's filled size -> FILL_MISMATCH
9. position query failed or stale -> ACCOUNTING_INCOMPLETE (unsafe)
10. reported position != ledger exposure of the market (both sides) -> POSITION_MISMATCH
11. venue-implied state unreachable from the journal state by a reconciliation transition -> FILL_MISMATCH
12. venue-implied state != journal state and reachable -> RECOVERABLE_DIFFERENCE; equal -> CONSISTENT (or ACCOUNTING_INCOMPLETE, safe, when a fee is UNKNOWN)

Examples from the brief, each covered by a Stage-23 test:

* **A** - journal ACKNOWLEDGED, venue FILLED, fills prove the full size, position matches: RECOVERABLE_DIFFERENCE,
  recovered to FILLED (tests 14 and 18).
* **B** - journal SUBMITTING, venue has no order, position zero, no fills: the submit is NOT assumed to have failed.
  It stays EXECUTION_UNKNOWN / HALTED unless an authoritative lookup proves absence; only then does it become
  REJECTED (tests 10 and 12).
* **C** - journal FILLED size 5, fills total 5, venue position 3: POSITION_MISMATCH, halt, market locked, ledger not
  "repaired" (test 18).
* **D** - a duplicate fill notification: deduplicated by `fill_id`, never double-counted (test 3).

Contradictory evidence is never silently repaired, and every reconciliation is persisted as a RECONCILIATION event.
A mismatch moves the execution to HALTED_FOR_RECONCILIATION; only a later
reconciliation with consistent, non-stale evidence releases it.

## 11. Market locks

`ExecutionEngine.market_lock_status(asset, market_ticker)` is **derived from the journal on every call** (the in-memory
cache is never consulted), so locks survive restarts. A market is locked while any execution on it is
EXECUTION_UNKNOWN or HALTED_FOR_RECONCILIATION; pre-submit or outstanding (only one active entry intent per asset +
market_ticker); has an open filled position; last reconciled to POSITION_MISMATCH or FILL_MISMATCH; or is
ACCOUNTING_INCOMPLETE with an unsafe position. A locked market rejects new intents with `MARKET_LOCKED`.

## 12. Restart recovery

A restart is simply a new `ExecutionEngine` over the same journal file. `recover()`:

1. turns every in-flight request (SUBMITTING, CANCEL_PENDING, EXECUTION_UNKNOWN) into EXECUTION_UNKNOWN then
   HALTED_FOR_RECONCILIATION (its outcome is unproven);
2. reconciles every outstanding, unsafe or FILLED execution against the adapter;
3. leaves pre-submit executions alone. A replayed intent resumes them; a replayed intent that was ever sent is never
   submitted again.

Covered: a crash before / after SUBMITTING is persisted, after the venue accepted but before the ACK, after the ACK,
after a partial fill, before fill persistence, before the settlement update and during recovery itself.

## 13. Failure injection

`execution/faults.py`: `FaultInjector.arm(point, nth)` raises `SimulatedCrash` (a `BaseException`, so no `except
Exception` can swallow a simulated death) at: `before_journal_write`, `journal_mid_transaction`, `after_journal_write`, `before_submit`, `during_submit`, `after_submit_before_ack`, `after_ack`, `before_fill_persist`, `after_partial_fill`, `during_cancel`, `after_cancel`, `before_settlement_update`, `during_recovery`. The harness test crashes at every point, restarts, recovers and
replays, then asserts that the journal is a valid chain and that no client order id was ever submitted twice.

## 14. EXECUTION_UNKNOWN semantics

When the outcome of a submit or cancel cannot be proven (a lost acknowledgement, a timeout, any unexpected adapter
exception, or a restart during an in-flight request), the engine writes EXECUTION_UNKNOWN and
HALTED_FOR_RECONCILIATION in ONE transaction. From then on there is no automatic retry, no new intent for the same
asset + market_ticker, no assumption that nothing happened or that the order filled, no size guessing, and no second
submit on replay. Only reconciliation with evidence releases the halt: the order found, or an authoritative absence
proof with no fills and a flat position.

## 15. Risk boundary

Every intent carries `risk_decision_id` and `risk_snapshot_hash`. Before READY to SUBMITTING the engine requires a
matching approval in the `RiskApprovalBook`. It rejects RISK_APPROVAL_MISSING, RISK_VETO, RISK_SNAPSHOT_MISMATCH,
RISK_APPROVAL_EXPIRED, RISK_APPROVAL_SCOPE_MISMATCH (ticker / asset / side), SIZE_EXCEEDS_APPROVAL and
PRICE_EXCEEDS_APPROVAL. The book is lookup-only: execution can never manufacture an approval, raise the approved size,
worsen the approved price or override a veto. The risk manager itself is Step 6.6.

Pre-submit checks (`_precheck`, paper / local state only, never a live call): intent not expired; market not locked
(an existing local intent, unresolved execution, outstanding order or open position); risk approval valid; identity
unchanged (execution key, client order id); provenance unchanged (ticker, asset, side, fingerprints, risk reference);
the venue position is verifiable (not failed or stale) and fully explained by journaled exposure.

## 16. Execution may only reduce risk

`execution/invariants.py`, checked on every order request and every fill:

- order side == intent side (never flipped: UP stays UP, DOWN stays DOWN)
- order market_ticker == intent market_ticker
- 0 < order count <= intent requested_contracts (never increased)
- 0 < order limit_price <= intent max_limit_price (never widened)
- order time_in_force == intent time_in_force; order expiry <= intent expires_at
- order client_order_id == the deterministic id of the intent's execution key
- filled quantity <= requested_contracts; every fill price <= max_limit_price
- an intent is never submitted at or after its expires_at
- provenance (model / calibration / signal fingerprints, risk decision id / snapshot) is never rewritten

## 17. Settlement handling

`ExecutionEngine.settle(SettlementReference)` takes an official result (`SettlementReference.from_official` accepts
the settlement engine's `OfficialResolution`; an unsettled result is refused). FILLED, PARTIALLY_FILLED, CANCELLED or
EXPIRED executions with filled quantity move to SETTLEMENT_PENDING (with a SETTLEMENT event recording
`settled_size` = the filled quantity) and then CLOSED. Never-filled orders create no position. A cancelled partial
fill settles only its filled quantity. Halted executions are not settled until reconciled. Settlement is idempotent
(`s:<key>`) and crash-safe. Settlement logic itself is unchanged; execution only references its result.

## 18. Prediction / trade audit record

`execution/audit.py` keeps three questions apart. Was the probability right (`prediction_correct`, None unless both
direction and result are known)? Was the decision right (EV, risk approval)? Was the execution right (submitted /
filled size, average price, fees)? `ExecutionEngine.audit_view(intent_id)` returns them as separate sections with the
provenance fingerprints. A correct prediction can still be a bad trade, and a losing trade does not prove the model
was wrong.

## 19. Why Step 6.5 cannot place live orders

* `FutureKalshiExecutionAdapter` is a stub: its constructor and every method raise `LiveExecutionUnavailable`
  (imported from `kalshi_core.execution`); `adapter.py` asserts `LIVE_EXECUTION_AVAILABLE is False` at import.
* The structural test (`test_stage23.py`, test 35) parses every module in `execution/` and fails on any network or
  crypto import (`requests, http, urllib, socket, ssl, httpx, aiohttp, websocket(s), asyncio, cryptography, hmac,
  subprocess`), any import of the data collectors or the dashboard, any URL / endpoint / credential literal (`http(s)://,
  ws(s)://, /portfolio, /orders, /transfer, trade-api, kalshi.com, KALSHI-ACCESS, api_key, private_key`), any HTTP verb
  literal (`POST, PUT, PATCH, DELETE`) and any call named `post, put, patch, delete, request, urlopen,
  create_connection, sign`. The forbidden strings themselves live only in the test file.
* `cancel_order` exists only as the paper interface method required by the adapter protocol; it mutates the in-memory
  `PaperVenue` and nothing else. There is no transfer function anywhere in the package.
* The repository-wide LIVE refusal (`kalshi_core.execution.get_execution_engine("LIVE")`) is unchanged and tested.
* Mutation E15 injects a POST path into `adapter.py` and is caught.

## 20. Fingerprint

`py -m execution.fingerprint --verify` covers the OrderIntent schema, transition graph, execution key / client order
id spec, journal schema / DDL hash, reconciliation rules and verdicts, adapter and paper protocols, ledger version,
invariants, fault points, `live_execution_available = False`, and the canonical AST of every `execution/` module. It
never includes database contents. Rewriting requires `--write --i-intend-to-change-the-execution-baseline` and an
OLD / NEW / WHY entry below.

| Date | OLD | NEW | Why |
|---|---|---|---|
| Step 6.5 | (none) | `d67c7c7ff001befbe86f96006fd8d996177bb0b813b648e38cc5ebdcb8e1a650` | initial execution foundation |

## 21. Mutation tests (E1-E20)

`py scripts/mutation_test_execution.py` applies each mutation to a temporary copy and runs the behavioural Stage-23
tests there (never the fingerprint test, which would catch any edit). The unmutated control must pass.
E1 duplicate intent allowed; E2 random idempotency key; E3 side flip; E4 size increase; E5 worse max price;
E6 ambiguous submit retried; E7 duplicate fill double counted; E8 duplicate fee double counted; E9 unknown fee to
zero; E10 invalid transition accepted; E11 crash recovery loses the active order; E12 reconciliation mismatch
ignored; E13 unknown market accepts a new intent; E14 non-atomic journal; E15 writable live endpoint; E16 expired
intent submits; E17 memory-only lock lost on restart; E18 partial treated as full; E19 cancelled partial discards
the filled position; E20 a second client_order_id after restart. Results: `analysis_output/execution_mutation_results.json`.

## 22. Deferred work

**Step 6.6:** the portfolio risk manager that produces `RiskApproval`s (exposure limits, per-asset and correlated caps,
daily loss limits, kill switch), dynamic sizing / Kelly, and wiring a calibrated candidate through risk into an
`OrderIntent` in shadow mode.

**Step 6.7:** realistic paper simulation (slippage, latency, maker-queue and fill-probability models from recorded
order books), exit / hold policies, and a shadow run of the full intent pipeline against recorded sessions.

**Eventual live execution (separate, explicitly approved step):** a real `FutureKalshiExecutionAdapter` (signed order
submission, cancellation, read-only order / fill / position queries), real exchange reconciliation, balance and
transfer handling, operational controls, and lifting the repository-wide LIVE refusal. None of this exists today.
