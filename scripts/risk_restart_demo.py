#!/usr/bin/env python3
"""
Deterministic CLEAN-RESTART demonstration of the Step 6.6 risk layer (PAPER / SHADOW ONLY - no network, no orders).

    py scripts/risk_restart_demo.py            (runs in a temporary directory and removes it afterwards)

Each phase runs in its OWN process; only the risk journal, the execution journal (SQLite) and the paper venue (JSON)
survive between processes.

A.  A1: candidate -> risk snapshot -> RiskDecision + RiskApproval -> persisted -> process exits
    A2: restart -> approval + breaker state reconstructed -> the same candidate re-evaluated -> the SAME decision, no
        conflicting approval -> the approval authorises one paper OrderIntent -> exits
    A3: restart -> the same intent replays idempotently (one order, one consumption) -> a different intent cannot
        reuse the approval
B.  B1: a daily-loss breaker triggers -> latched -> process exits
    B2: restart -> the breaker is still LATCHED -> a new, otherwise safe candidate is VETOED
Exit code 0 only if every expectation holds.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)

T0 = 1_790_000_000_000
NOW = T0 + 5
TICKER = "KXBTC15M-26SEP292045-45"
GROUPS = {"BTC": "CRYPTO", "ETH": "CRYPTO", "SOL": "CRYPTO", "XRP": "CRYPTO"}


def _policy():
    from risk.policy import RiskPolicy
    return RiskPolicy(policy_id="demo", policy_version=1, label="DEMO FIXTURE - NOT RESEARCH-VALIDATED",
                      max_contracts_per_trade="6", max_notional_per_trade="100", max_loss_per_trade="100",
                      max_fraction_equity_per_trade="1", max_asset_open_risk="100", max_asset_gross_notional="100",
                      max_portfolio_open_risk="1.80", max_portfolio_gross_notional="100",
                      max_crypto_group_open_risk="100", max_same_direction_crypto_risk="100",
                      max_daily_realized_loss="50", max_daily_total_loss="80", max_rolling_drawdown="100",
                      max_consecutive_losses=3, max_spread="0.05", min_executable_depth="1", max_quote_age_ms=5000,
                      max_feature_age_ms=5000, max_decision_age_ms=10_000, max_snapshot_age_ms=5000,
                      minimum_account_equity="100", allow_degraded_feed=False, allow_degraded_model=False,
                      allow_degraded_calibration=False, allow_degraded_settlement=False,
                      allow_degraded_execution=False, require_known_fee=True, require_known_slippage=True,
                      require_known_ev=True, approval_ttl_ms=60_000, asset_group_map=GROUPS)


def _candidate(cid):
    from risk.types import RiskCandidate
    return RiskCandidate(candidate_id=cid, created_at=T0, market_ticker=TICKER, asset="BTC", side="YES",
                         signal_status="CALL", predicted_direction="UP", requested_contracts="10",
                         requested_max_limit_price="0.40", decision_ts=T0 - 10, expires_at=T0 + 300_000,
                         raw_probability="0.61", calibrated_probability="0.58", market_bid="0.38", market_ask="0.40",
                         current_executable_price="0.40", available_depth="5", estimated_fee="0.20",
                         estimated_slippage="0", estimated_net_ev="0.05", model_id="demo-model",
                         model_fingerprint="ab" * 16, calibration_fingerprint="cd" * 16, signal_fingerprint="ef" * 16,
                         checkpoint="T-300", market_close_ts=T0 + 600_000)


def _snapshot(sid, **kw):
    from risk.types import RiskSnapshot
    d = dict(snapshot_id=sid, captured_at=T0, account_equity="1000", available_cash="1000", day_start_equity="1000",
             day_peak_equity="1000", daily_realized_pnl="0", daily_unrealized_pnl="0", open_positions_count=0,
             total_open_risk="0", total_gross_notional="0", asset_open_risk="0", asset_gross_notional="0",
             same_direction_crypto_risk="0", opposite_direction_crypto_risk="0", consecutive_losses=0,
             market_ticker=TICKER, asset="BTC", side="YES", current_position_size="0", current_market_open_risk="0",
             feed_health="PASS", model_health="PASS", calibration_health="PASS", settlement_health="PASS",
             execution_health="PASS", quote_age_ms=100, feature_age_ms=100, model_decision_age_ms=100, spread="0.02",
             available_depth="5", market_minutes_remaining="10", exposure_group="CRYPTO",
             market_execution_state="CLEAR", asset_execution_state="CLEAR")
    d.update(kw)
    return RiskSnapshot(**d)


def phase(name, workdir):
    from dataclasses import replace

    from execution.engine import ExecutionEngine, FixedClock
    from execution.journal import Journal
    from execution.money import canon
    from execution.paper import PaperExecutionAdapter, PaperVenue
    from risk.manager import RiskManager
    from risk.shadow import intent_from_approval
    from risk.store import RiskStore
    clock = FixedClock(NOW)
    mgr = RiskManager(RiskStore(os.path.join(workdir, "risk.sqlite")), _policy(), clock)
    vpath = os.path.join(workdir, "venue.json")
    out = {"phase": name, "pid": os.getpid()}

    def engine(venue):
        return ExecutionEngine(Journal(os.path.join(workdir, "journal.sqlite")), PaperExecutionAdapter(venue),
                               mgr.approval_book(), clock)

    if name == "A1":
        d, a = mgr.evaluate(_candidate("demo-A"), _snapshot("snap-A"))
        out.update(decision=d.decision, approved=canon(d.approved_contracts), reasons=list(d.reason_codes),
                   decision_id=d.risk_decision_id, approval=a is not None, approvals=mgr.store.count("APPROVAL"))
    elif name == "A2":
        d, a = mgr.evaluate(_candidate("demo-A"), _snapshot("snap-A"))        # the same candidate after a restart
        out.update(decision_id=d.risk_decision_id, approvals=mgr.store.count("APPROVAL"),
                   decisions=mgr.store.count("DECISION"), breakers=mgr.breakers())
        venue = PaperVenue()
        out["submit"] = engine(venue).submit(intent_from_approval(_candidate("demo-A"), a, "demo-intent")).value
        out["order_count"] = [o["count"] for o in venue.orders.values()]
        venue.save(vpath)
    elif name == "A3":
        venue = PaperVenue.load(vpath)
        d, a = mgr.decision(sys.argv[4])
        it = intent_from_approval(_candidate("demo-A"), a, "demo-intent")
        out["replay"] = engine(venue).submit(it).value
        out["submit_attempts"] = list(venue.submit_attempts.values())
        out["consumptions"] = mgr.store.count("CONSUMPTION")
        ok, why = mgr.approval_book().verify_and_consume(replace(it, intent_id="other-intent"), "other-key", NOW)
        out["reuse"] = [ok, why.split(" ")[0]]
    elif name == "B1":
        d, a = mgr.evaluate(_candidate("demo-B1"), _snapshot("snap-B1", daily_realized_pnl="-75"))
        out.update(decision=d.decision, reasons=list(d.reason_codes), breakers=mgr.breakers())
    elif name == "B2":
        out["breakers_after_restart"] = mgr.breakers()
        d, a = mgr.evaluate(_candidate("demo-B2"), _snapshot("snap-B2", daily_realized_pnl="25"))   # PnL improved
        out.update(decision=d.decision, reasons=list(d.reason_codes), approval=a is not None)
    print(json.dumps(out, sort_keys=True))


def _run(name, workdir, *extra):
    p = subprocess.run([sys.executable, os.path.abspath(__file__), "--phase", name, workdir, *extra],
                       capture_output=True, text=True, cwd=HERE)
    if p.returncode != 0:
        raise SystemExit(f"phase {name} failed: {p.stderr[-800:]}")
    return json.loads(p.stdout.strip().splitlines()[-1])


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["--phase"]:
        phase(argv[1], argv[2])
        return 0
    root = tempfile.mkdtemp(prefix="risk-demo-")
    try:
        a_dir, b_dir = os.path.join(root, "A"), os.path.join(root, "B")
        os.makedirs(a_dir)
        os.makedirs(b_dir)
        a1 = _run("A1", a_dir)
        a2 = _run("A2", a_dir)
        a3 = _run("A3", a_dir, a1["decision_id"])
        b1, b2 = _run("B1", b_dir), _run("B2", b_dir)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    checks = {
        "A: every phase ran in its own process": len({a1["pid"], a2["pid"], a3["pid"]}) == 3,
        "A: candidate reduced to the binding caps (10 -> 4)": a1["decision"] == "REDUCE" and a1["approved"] == "4",
        "A: binding limits reported": a1["reasons"] == ["PER_TRADE_CONTRACT_LIMIT", "PORTFOLIO_OPEN_RISK_LIMIT",
                                                         "DEPTH_CAP"],
        "A: approval persisted": a1["approval"] and a1["approvals"] == 1,
        "A: after restart the same candidate gets the SAME decision": a2["decision_id"] == a1["decision_id"],
        "A: no conflicting approval after restart": a2["approvals"] == 1 and a2["decisions"] == 1,
        "A: the approval authorises exactly 4 contracts": a2["submit"] == "ACKNOWLEDGED" and a2["order_count"] == ["4"],
        "A: replay after restart is idempotent": a3["replay"] == "ACKNOWLEDGED" and a3["submit_attempts"] == [1]
                                                 and a3["consumptions"] == 1,
        "A: a different intent cannot reuse the approval": a3["reuse"] == [False, "RISK_APPROVAL_ALREADY_CONSUMED"],
        "B: the loss breaker triggers and latches": b1["decision"] == "VETO"
                                                    and b1["breakers"]["DAILY_REALIZED_LOSS"] == "LATCHED",
        "B: after restart the breaker is still LATCHED": b2["breakers_after_restart"]["DAILY_REALIZED_LOSS"] == "LATCHED",
        "B: a new candidate is vetoed (PnL improvement does not unlatch)": b2["decision"] == "VETO"
                                                                           and "DAILY_REALIZED_LOSS_BREAKER" in b2["reasons"]
                                                                           and not b2["approval"],
    }
    print(json.dumps({"A": [a1, a2, a3], "B": [b1, b2]}, sort_keys=True, indent=1))
    for k, v in checks.items():
        print(("PASS  " if v else "FAIL  ") + k)
    ok = all(checks.values())
    print("DEMO: " + ("every expectation holds" if ok else "an expectation FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
