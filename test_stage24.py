#!/usr/bin/env python3
"""
Stage 24 - Step 6.6 RISK MANAGER FOUNDATION (paper / shadow only, zero live orders).

Run:  py test_stage24.py                    (or py run_all_tests.py for stages 1-24)
      py test_stage24.py --only nocall,caps  (a subset; used by scripts/mutation_test_risk.py)
Every test uses fake ids, explicit fixture policies and temporary directories; no network, no real order, no
database is left behind.
"""
import ast
import dataclasses
import json
import os
import shutil
import subprocess
import sys
import tempfile
from decimal import Decimal as D

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from execution.engine import ExecutionEngine, FixedClock  # noqa: E402
from execution.journal import Journal  # noqa: E402
from execution.money import UNKNOWN  # noqa: E402
from execution.paper import PaperExecutionAdapter, PaperVenue  # noqa: E402
from execution.risk import approval_binding_hash  # noqa: E402
from execution.states import ExecState  # noqa: E402
from kalshi_core.execution import LiveExecutionUnavailable  # noqa: E402
from risk.breakers import BREAKER_TYPES, InvalidBreakerTransition, check_breaker_transition, utc_day  # noqa: E402
from risk.decision import RiskDecision, RiskInvariantViolation  # noqa: E402
from risk.evaluate import RiskState, evaluate, worst_case_loss  # noqa: E402
from risk.faults import RISK_FAULT_POINTS, RiskFaultInjector, SimulatedCrash  # noqa: E402
from risk.manager import RiskManager, RiskPolicyMismatchError  # noqa: E402
from risk.policy import RiskPolicy, load_policy, risk_policy_fingerprint  # noqa: E402
from risk.reasons import REASON_CODES  # noqa: E402
from risk.shadow import Exposure, execution_facts, exposure_fields, intent_from_approval  # noqa: E402
from risk.store import RiskStore  # noqa: E402
from risk.types import RiskCandidate, RiskInputError, RiskSnapshot, risk_snapshot_hash  # noqa: E402

S = ExecState
T0 = 1_790_000_000_000
NOW = T0 + 5
TK = "KXBTC15M-26SEP292045-45"
TK2 = "KXBTC15M-26SEP292100-45"
TK_ETH = "KXETH15M-26SEP292045-45"
DAY = 86_400_000
_DIRS = []


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


def tmpdir():
    d = tempfile.mkdtemp(prefix="stage24-")
    _DIRS.append(d)
    return d


GROUPS = {"BTC": "CRYPTO", "ETH": "CRYPTO", "SOL": "CRYPTO", "XRP": "CRYPTO"}


def policy(**kw):
    """Explicit FIXTURE policy: generous caps so each test isolates the limit it exercises (not a real threshold)."""
    d = dict(policy_id="fixture", policy_version=1, label="TEST FIXTURE", max_contracts_per_trade="1000",
             max_notional_per_trade="1000", max_loss_per_trade="1000", max_fraction_equity_per_trade="1",
             max_asset_open_risk="1000", max_asset_gross_notional="1000", max_portfolio_open_risk="1000",
             max_portfolio_gross_notional="1000", max_crypto_group_open_risk="1000",
             max_same_direction_crypto_risk="1000", max_daily_realized_loss="50", max_daily_total_loss="80",
             max_rolling_drawdown="100", max_consecutive_losses=3, max_spread="0.05", min_executable_depth="1",
             max_quote_age_ms=5000, max_feature_age_ms=5000, max_decision_age_ms=10_000, max_snapshot_age_ms=5000,
             minimum_account_equity="100", allow_degraded_feed=False, allow_degraded_model=False,
             allow_degraded_calibration=False, allow_degraded_settlement=False, allow_degraded_execution=False,
             require_known_fee=True, require_known_slippage=True, require_known_ev=True, approval_ttl_ms=60_000,
             asset_group_map=GROUPS)
    d.update(kw)
    return RiskPolicy(**d)


def cand(cid="c1", **kw):
    d = dict(candidate_id=cid, created_at=T0, market_ticker=TK, asset="BTC", side="YES", signal_status="CALL",
             predicted_direction="UP", requested_contracts="10", requested_max_limit_price="0.40", decision_ts=T0 - 10,
             expires_at=T0 + 300_000, raw_probability="0.61", calibrated_probability="0.58", market_bid="0.38",
             market_ask="0.40", current_executable_price="0.40", available_depth="100", estimated_fee="0.20",
             estimated_slippage="0", estimated_net_ev="0.05", model_id="model-1", model_fingerprint="ab" * 16,
             calibration_fingerprint="cd" * 16, signal_fingerprint="ef" * 16, checkpoint="T-300",
             market_close_ts=T0 + 600_000)
    d.update(kw)
    return RiskCandidate(**d)


def snap(sid="s1", **kw):
    d = dict(snapshot_id=sid, captured_at=T0, account_equity="1000", available_cash="1000", day_start_equity="1000",
             day_peak_equity="1000", daily_realized_pnl="0", daily_unrealized_pnl="0", open_positions_count=0,
             total_open_risk="0", total_gross_notional="0", asset_open_risk="0", asset_gross_notional="0",
             same_direction_crypto_risk="0", opposite_direction_crypto_risk="0", consecutive_losses=0,
             market_ticker=TK, asset="BTC", side="YES", current_position_size="0", current_market_open_risk="0",
             feed_health="PASS", model_health="PASS", calibration_health="PASS", settlement_health="PASS",
             execution_health="PASS", quote_age_ms=100, feature_age_ms=100, model_decision_age_ms=100, spread="0.02",
             available_depth="100", market_minutes_remaining="10", exposure_group="CRYPTO",
             market_execution_state="CLEAR", asset_execution_state="CLEAR")
    d.update(kw)
    return RiskSnapshot(**d)


def ev(c=None, s=None, p=None, state=None, now=NOW):
    return evaluate(c or cand(), s or snap(), p or policy(), state or RiskState(), now)


class REnv:
    """A risk store file + manager factory; restart() = a new store connection + a new manager (process restart)."""

    def __init__(self, p=None, now=NOW):
        self.dir = tmpdir()
        self.path = os.path.join(self.dir, "risk.sqlite")
        self.jpath = os.path.join(self.dir, "journal.sqlite")
        self.p = p or policy()
        self.clock = FixedClock(now)
        self.faults = RiskFaultInjector()
        self.venue = PaperVenue()
        self.mgr = self.new()

    def new(self):
        return RiskManager(RiskStore(self.path, self.faults), self.p, self.clock)

    def restart(self, p=None):
        self.faults.disarm()
        if p is not None:
            self.p = p
        self.mgr = self.new()
        return self.mgr

    def engine(self, jpath=None):
        return ExecutionEngine(Journal(jpath or self.jpath), PaperExecutionAdapter(self.venue),
                               self.mgr.approval_book(), self.clock)


def reasons_of(eng, intent):
    from execution.identity import execution_key
    return [e["reason"] for e in eng.j.events(execution_key(intent), "TRANSITION")][-1]


def crash(fn, *a):
    try:
        fn(*a)
    except SimulatedCrash as e:
        return str(e)
    raise AssertionError("expected a simulated crash")


def assert_veto(e, *codes):
    assert e.decision == "VETO" and e.approved_contracts == 0, (e.decision, e.reason_codes)
    for c in codes:
        assert c in e.reason_codes, (c, e.reason_codes)


# ═══════════════════ 1-5 signal, side, size, price ═══════════════════
def test_call_approved():
    e = ev()
    assert e.decision == "APPROVE" and e.approved_contracts == D("10") and e.reason_codes == ()
    assert e.approved_max_limit_price == D("0.40") and e.worst_case_loss == D("4.20")
    r = REnv()
    d, a = r.mgr.evaluate(cand(), snap())
    assert d.decision == "APPROVE" and a is not None and a.approved_contracts == D("10")
    assert a.expires_at == NOW + 60_000 and a.issued_at == NOW


def test_no_call_vetoed():
    e = ev(cand(signal_status="NO_CALL"))
    assert_veto(e, "NO_CALL")
    e = ev(cand(signal_status="NO_CALL", predicted_direction=UNKNOWN, raw_probability=UNKNOWN))
    assert_veto(e, "NO_CALL")
    r = REnv()
    d, a = r.mgr.evaluate(cand(signal_status="NO_CALL"), snap())
    assert d.decision == "VETO" and a is None and r.mgr.store.count("APPROVAL") == 0
    try:
        RiskDecision(risk_decision_id="x", candidate_id="c", candidate_hash="h", decision="APPROVE", decision_ts=NOW,
                     expires_at=NOW + 1, market_ticker=TK, asset="BTC", side="YES", signal_status="NO_CALL",
                     requested_contracts=D("1"), approved_contracts=D("1"), requested_max_limit_price=D("0.4"),
                     approved_max_limit_price=D("0.4"), calculated_worst_case_loss=D("0.4"), risk_snapshot_hash="h",
                     risk_policy_fingerprint="f", reason_codes=(), human_readable_reasons=())
        raise AssertionError("a NO_CALL approval was constructed")
    except RiskInvariantViolation:
        pass


def test_side_immutable():
    for side, direction in (("YES", "UP"), ("NO", "DOWN")):
        r = REnv()
        c = cand(side=side, predicted_direction=direction)
        d, a = r.mgr.evaluate(c, snap(side=side))
        assert d.side == side and a.approved_side == side and intent_from_approval(c, a, "i1").side == side
    assert_veto(ev(cand(side="YES", predicted_direction="DOWN")), "INVALID_CANDIDATE")    # inconsistent direction
    try:
        setattr(cand(), "side", "NO"); raise AssertionError("candidate mutated")
    except dataclasses.FrozenInstanceError:
        pass


def test_size_never_increases():
    for req in ("1", "3.5", "10", "20"):
        e = ev(cand(requested_contracts=req), p=policy(max_contracts_per_trade="100000"))
        assert e.approved_contracts <= D(req) and e.approved_contracts == D(req)
    e = ev(cand(requested_contracts="10"), p=policy(max_contracts_per_trade="7"))
    assert e.decision == "REDUCE" and e.approved_contracts == D("7")
    try:
        RiskDecision(risk_decision_id="x", candidate_id="c", candidate_hash="h", decision="APPROVE", decision_ts=NOW,
                     expires_at=NOW + 1, market_ticker=TK, asset="BTC", side="YES", signal_status="CALL",
                     requested_contracts=D("5"), approved_contracts=D("6"), requested_max_limit_price=D("0.4"),
                     approved_max_limit_price=D("0.4"), calculated_worst_case_loss=D("2.4"), risk_snapshot_hash="h",
                     risk_policy_fingerprint="f", reason_codes=(), human_readable_reasons=())
        raise AssertionError("approved > requested constructed")
    except RiskInvariantViolation:
        pass


def test_price_never_widens():
    e = ev(p=policy(max_entry_price="0.90"))
    assert e.approved_max_limit_price == D("0.40")                                  # never widened
    e = ev(cand(current_executable_price="0.35"), p=policy(max_entry_price="0.38"))
    assert e.approved_max_limit_price == D("0.38")                                  # tightened
    assert_veto(ev(p=policy(max_entry_price="0.38")), "PRICE_ABOVE_CAP")            # executable 0.40 > cap 0.38
    assert_veto(ev(cand(current_executable_price="0.41")), "PRICE_ABOVE_CAP")
    assert_veto(ev(cand(current_executable_price=UNKNOWN)), "PRICE_UNKNOWN")


# ═══════════════════ 6-8 deterministic identities ═══════════════════
def _child(code):
    p = subprocess.run([sys.executable, "-c", "import sys; sys.path.insert(0, %r); sys.argv=['x']; "
                        "import test_stage24 as t; " % HERE + code], capture_output=True, text=True, cwd=HERE)
    assert p.returncode == 0, p.stderr[-800:]
    return p.stdout.strip()


def test_decision_id_deterministic():
    r1, r2 = REnv(), REnv()
    d1, _ = r1.mgr.evaluate(cand(), snap())
    d2, _ = r2.mgr.evaluate(cand(), snap())
    assert d1 == d2 and d1.risk_decision_id.startswith("rd-")
    code = ("from risk.manager import RiskManager; from risk.store import RiskStore; import os, tempfile; "
            "from execution.engine import FixedClock; "
            "m = RiskManager(RiskStore(os.path.join(tempfile.mkdtemp(), 'r.sqlite')), t.policy(), FixedClock(t.NOW)); "
            "print(m.evaluate(t.cand(), t.snap())[0].risk_decision_id)")
    assert {_child(code) for _ in range(2)} == {d1.risk_decision_id}                 # identical in fresh processes
    assert REnv().mgr.evaluate(cand(), snap(account_equity="999"))[0].risk_decision_id != d1.risk_decision_id
    assert REnv(policy(max_contracts_per_trade="999")).mgr.evaluate(cand(), snap())[0].risk_decision_id \
        != d1.risk_decision_id
    assert r1.mgr.evaluate(cand(), snap()) == (d1, r1.mgr.store.approval(d1.risk_decision_id))  # re-evaluation
    assert r1.mgr.store.count("DECISION") == 1 and r1.mgr.store.count("APPROVAL") == 1


def test_snapshot_hash_deterministic():
    h = risk_snapshot_hash(snap())
    rev = RiskSnapshot(**{k: v for k, v in reversed(list(dataclasses.asdict(snap()).items()))})
    assert risk_snapshot_hash(rev) == h and risk_snapshot_hash(snap(account_equity="1000.00")) == h
    assert risk_snapshot_hash(snap(spread="0.03")) != h and risk_snapshot_hash(snap(feed_health="DEGRADED")) != h
    assert _child("from risk.types import risk_snapshot_hash; print(risk_snapshot_hash(t.snap()))") == h
    try:
        snap(account_equity=1000.0); raise AssertionError("float accepted")
    except RiskInputError:
        pass
    try:
        snap(account_equity=None); raise AssertionError("None accepted (missing must be UNKNOWN)")
    except RiskInputError:
        pass


def test_policy_fingerprint_deterministic():
    fp = risk_policy_fingerprint(policy())
    assert risk_policy_fingerprint(policy()) == fp
    assert _child("from risk.policy import risk_policy_fingerprint; print(risk_policy_fingerprint(t.policy()))") == fp
    changes = [dict(max_contracts_per_trade="999"), dict(max_daily_realized_loss="49"), dict(max_rolling_drawdown="99"),
               dict(allow_degraded_feed=True), dict(max_quote_age_ms=4999), dict(approval_ttl_ms=59_999),
               dict(asset_group_map=dict(GROUPS, XRP="OTHER")), dict(require_known_ev=False),
               dict(max_entry_price="0.5"), dict(extra_reserve_per_contract="0.01"), dict(policy_version=2),
               dict(min_executable_depth="2"), dict(max_spread="0.04"), dict(max_consecutive_losses=4)]
    fps = {risk_policy_fingerprint(policy(**c)) for c in changes}
    assert fp not in fps and len(fps) == len(changes)
    assert risk_policy_fingerprint(policy(asset_group_map={"XRP": "CRYPTO", "SOL": "CRYPTO", "ETH": "CRYPTO",
                                                           "BTC": "CRYPTO"})) == fp      # mapping order irrelevant
    try:
        policy(max_loss_per_trade=5.0); raise AssertionError("float policy value accepted")
    except RiskInputError:
        pass


# ═══════════════════ 9-13 expiry / staleness ═══════════════════
def test_expired_candidate():
    assert_veto(ev(cand(expires_at=T0 + 1000), now=T0 + 1000), "EXPIRED_CANDIDATE")      # fresh data, expired
    assert ev(cand(expires_at=T0 + 1001), now=T0 + 1000).decision == "APPROVE"
    assert_veto(ev(now=T0 + 300_000), "EXPIRED_CANDIDATE")
    assert_veto(ev(cand(expires_at=T0 + 900_000), now=T0 + 600_000, s=snap(captured_at=T0 + 599_000)),
                "EXPIRED_CANDIDATE")                                            # market closed
    assert ev(now=T0 + 299_999, s=snap(captured_at=T0 + 299_000), c=cand(decision_ts=T0 + 298_000,
                                                                          created_at=T0 + 298_000)).decision == "APPROVE"


def test_stale_snapshot():
    assert_veto(ev(now=T0 + 5001), "STALE_RISK_SNAPSHOT")
    assert ev(now=T0 + 5000).decision == "APPROVE"                                  # == max allowed
    assert_veto(ev(s=snap(captured_at=NOW + 1)), "INVALID_SNAPSHOT")                # from the future


def test_stale_quote():
    assert_veto(ev(s=snap(quote_age_ms=5001)), "STALE_QUOTE")
    assert_veto(ev(s=snap(quote_age_ms=UNKNOWN)), "STALE_QUOTE")
    assert ev(s=snap(quote_age_ms=5000)).decision == "APPROVE"


def test_stale_features():
    assert_veto(ev(s=snap(feature_age_ms=5001)), "STALE_FEATURES")
    assert_veto(ev(s=snap(feature_age_ms=UNKNOWN)), "STALE_FEATURES")


def test_stale_model_decision():
    assert_veto(ev(s=snap(model_decision_age_ms=10_001)), "STALE_MODEL_DECISION")
    assert_veto(ev(cand(decision_ts=NOW - 10_001)), "STALE_MODEL_DECISION")
    assert_veto(ev(s=snap(model_decision_age_ms=UNKNOWN)), "STALE_MODEL_DECISION")


# ═══════════════════ 14-20 health ═══════════════════
def test_health_gates():
    for f in ("feed_health", "model_health", "calibration_health", "settlement_health", "execution_health"):
        pre = f.split("_")[0].upper()
        for h in ("FAIL", "UNKNOWN", "UNAVAILABLE"):
            assert_veto(ev(s=snap(**{f: h})), f"{pre}_HEALTH_{h}")
        assert_veto(ev(s=snap(**{f: "DEGRADED"})), f"{pre}_HEALTH_DEGRADED")
        allow = {"feed_health": "allow_degraded_feed", "model_health": "allow_degraded_model",
                 "calibration_health": "allow_degraded_calibration",
                 "settlement_health": "allow_degraded_settlement", "execution_health": "allow_degraded_execution"}[f]
        assert ev(s=snap(**{f: "DEGRADED"}), p=policy(**{allow: True})).decision == "APPROVE"
        assert_veto(ev(s=snap(**{f: "FAIL"}), p=policy(**{allow: True})), f"{pre}_HEALTH_FAIL")
    assert_veto(ev(s=snap(feed_health="BROKEN")), "INVALID_SNAPSHOT")


def test_feed_fail():
    assert_veto(ev(s=snap(feed_health="FAIL")), "FEED_HEALTH_FAIL")


def test_feed_unknown():
    assert_veto(ev(s=snap(feed_health="UNKNOWN")), "FEED_HEALTH_UNKNOWN")
    assert_veto(ev(s=snap(model_health="UNKNOWN")), "MODEL_HEALTH_UNKNOWN")


# ═══════════════════ 21-27 account / fee / EV / spread ═══════════════════
def test_equity():
    assert_veto(ev(s=snap(account_equity=UNKNOWN)), "ACCOUNT_EQUITY_UNKNOWN")
    assert_veto(ev(s=snap(account_equity="99.99", day_peak_equity="99.99")), "ACCOUNT_EQUITY_TOO_LOW")
    assert ev(s=snap(account_equity="100", day_peak_equity="100", available_cash="100")).decision == "APPROVE"
    assert_veto(ev(s=snap(available_cash=UNKNOWN)), "ACCOUNT_CASH_UNKNOWN")


def test_unknown_fee():
    assert_veto(ev(cand(estimated_fee=UNKNOWN)), "FEE_UNKNOWN")
    assert_veto(ev(cand(estimated_fee=UNKNOWN), p=policy(require_known_fee=False)), "FEE_UNKNOWN")  # no reserve
    e = ev(cand(estimated_fee=UNKNOWN), p=policy(require_known_fee=False, unknown_fee_reserve_per_contract="0.07"))
    assert e.decision == "APPROVE" and e.worst_case_loss == D("10") * D("0.47")       # reserved, never zero


def test_unknown_slippage():
    assert_veto(ev(cand(estimated_slippage=UNKNOWN)), "SLIPPAGE_UNKNOWN")
    e = ev(cand(estimated_slippage=UNKNOWN), p=policy(require_known_slippage=False,
                                                      unknown_slippage_reserve_per_contract="0.02"))
    assert e.decision == "APPROVE" and e.worst_case_loss == D("10") * D("0.42") + D("0.20")


def test_ev_rules():
    assert_veto(ev(cand(estimated_net_ev=UNKNOWN)), "EV_UNKNOWN")
    for v in ("0", "-0.01"):
        assert_veto(ev(cand(estimated_net_ev=v)), "NON_POSITIVE_EV")
    assert_veto(ev(cand(estimated_net_ev="0"), p=policy(require_known_ev=False)), "NON_POSITIVE_EV")
    assert ev(cand(estimated_net_ev="0.0001")).decision == "APPROVE"


def test_spread():
    assert_veto(ev(s=snap(spread="0.051")), "SPREAD_TOO_WIDE")
    assert ev(s=snap(spread="0.05")).decision == "APPROVE"
    assert_veto(ev(s=snap(spread=UNKNOWN)), "SPREAD_UNKNOWN")


def test_depth():
    e = ev(cand(available_depth="7"))
    assert e.decision == "REDUCE" and e.approved_contracts == D("7") and "DEPTH_CAP" in e.reason_codes
    e = ev(s=snap(available_depth="6"))                                             # min(candidate, snapshot)
    assert e.approved_contracts == D("6")
    assert_veto(ev(cand(available_depth="0.99")), "INSUFFICIENT_DEPTH")
    assert ev(cand(available_depth="1", requested_contracts="1")).decision == "APPROVE"   # == minimum allowed
    assert_veto(ev(cand(available_depth=UNKNOWN)), "DEPTH_UNKNOWN")


# ═══════════════════ 30-40 caps ═══════════════════
def test_per_trade_caps():
    e = ev(p=policy(max_contracts_per_trade="6"))
    assert e.approved_contracts == D("6") and e.reason_codes == ("PER_TRADE_CONTRACT_LIMIT",)
    e = ev(p=policy(max_notional_per_trade="2"))                                    # 2 / 0.40 = 5
    assert e.approved_contracts == D("5") and "PER_TRADE_NOTIONAL_LIMIT" in e.reason_codes
    e = ev(p=policy(max_loss_per_trade="2.60"))                                     # (2.60 - 0.20) / 0.40 = 6
    assert e.approved_contracts == D("6") and e.worst_case_loss == D("2.60") and "PER_TRADE_LOSS_LIMIT" in e.reason_codes
    e = ev(p=policy(max_fraction_equity_per_trade="0.0022"))                        # 0.0022 * 1000 = 2.2 -> 5
    assert e.approved_contracts == D("5") and "PER_TRADE_EQUITY_FRACTION_LIMIT" in e.reason_codes
    e = ev(s=snap(available_cash="1.40"))                                           # (1.40 - 0.20) / 0.40 = 3
    assert e.approved_contracts == D("3") and "INSUFFICIENT_CASH" in e.reason_codes


def test_asset_caps():
    e = ev(s=snap(asset_open_risk="3"), p=policy(max_asset_open_risk="5"))           # (5 - 3 - 0.2) / 0.4 = 4.5
    assert e.approved_contracts == D("4.5") and "ASSET_OPEN_RISK_LIMIT" in e.reason_codes
    e = ev(s=snap(asset_gross_notional="1"), p=policy(max_asset_gross_notional="2"))   # 1 / 0.4 = 2.5
    assert e.approved_contracts == D("2.5") and "ASSET_NOTIONAL_LIMIT" in e.reason_codes


def test_portfolio_caps():
    e = ev(s=snap(total_open_risk="8"), p=policy(max_portfolio_open_risk="10"))      # (10 - 8 - 0.2) / 0.4 = 4.5
    assert e.approved_contracts == D("4.5") and "PORTFOLIO_OPEN_RISK_LIMIT" in e.reason_codes
    e = ev(s=snap(total_gross_notional="9"), p=policy(max_portfolio_gross_notional="10"))   # 1 / 0.4 = 2.5
    assert e.approved_contracts == D("2.5") and "PORTFOLIO_NOTIONAL_LIMIT" in e.reason_codes


def test_crypto_group_cap():
    e = ev(s=snap(same_direction_crypto_risk="1", opposite_direction_crypto_risk="2"),
           p=policy(max_crypto_group_open_risk="4.2"))                               # (4.2 - 3 - 0.2) / 0.4 = 2.5
    assert e.approved_contracts == D("2.5") and "CRYPTO_GROUP_RISK_LIMIT" in e.reason_codes


def test_same_direction_cap():
    """brief example: BTC UP 50 + ETH UP 40 + candidate SOL UP 30 -> 120 same-direction."""
    exps = [Exposure("BTC", "KXBTC-A", "UP", D("50"), D("50")), Exposure("ETH", "KXETH-A", "UP", D("40"), D("40")),
            Exposure("ETH", "KXETH-B", "DOWN", D("25"), D("25"))]
    f = exposure_fields(exps, policy(), "SOL", "KXSOL-A", "UP")
    assert f["same_direction_crypto_risk"] == D("90") and f["opposite_direction_crypto_risk"] == D("25")
    assert f["total_open_risk"] == D("115") and f["asset_open_risk"] == 0
    c = cand(market_ticker="KXSOL-A", asset="SOL", requested_contracts="75", requested_max_limit_price="0.40",
             estimated_fee="0", available_depth="1000")
    s = snap(market_ticker="KXSOL-A", asset="SOL", **{k: v for k, v in f.items() if k != "exposure_group"})
    assert ev(c, s, policy(max_same_direction_crypto_risk="120")).approved_contracts == D("75")      # 90 + 30 = 120
    e = ev(c, s, policy(max_same_direction_crypto_risk="119.99"))
    assert e.decision == "REDUCE" and e.approved_contracts == D("74.97") and "SAME_DIRECTION_CRYPTO_LIMIT" in e.reason_codes
    e = ev(cand(side="NO", predicted_direction="DOWN", market_ticker="KXSOL-A", asset="SOL", requested_contracts="75",
                estimated_fee="0", available_depth="1000"),
           snap(market_ticker="KXSOL-A", asset="SOL", side="NO",
                **{k: v for k, v in exposure_fields(exps, policy(), "SOL", "KXSOL-A", "DOWN").items()
                   if k != "exposure_group"}), policy(max_same_direction_crypto_risk="60"))
    assert e.approved_contracts == D("75")                     # DOWN: only 25 same-direction (BTC/ETH UP is opposite)
    e = ev(c, s, policy(max_crypto_group_open_risk="130"))     # but the group's gross risk counts both directions
    assert e.approved_contracts == D("37.5") and "CRYPTO_GROUP_RISK_LIMIT" in e.reason_codes


def test_multiple_caps_smallest():
    """brief §37: requested 20; contract 15, notional 12, trade loss 11, asset 8, portfolio 7, group 5, depth 4 -> 4."""
    c = cand(requested_contracts="20", requested_max_limit_price="0.50", current_executable_price="0.50",
             estimated_fee="0", available_depth="4")
    p = policy(max_contracts_per_trade="15", max_notional_per_trade="6", max_loss_per_trade="5.5",
               max_asset_open_risk="4", max_portfolio_open_risk="3.5", max_crypto_group_open_risk="2.5")
    e = ev(c, p=p)
    assert e.caps["PER_TRADE_CONTRACT_LIMIT"] == 15 and e.caps["PER_TRADE_NOTIONAL_LIMIT"] == 12
    assert e.caps["PER_TRADE_LOSS_LIMIT"] == 11 and e.caps["ASSET_OPEN_RISK_LIMIT"] == 8
    assert e.caps["PORTFOLIO_OPEN_RISK_LIMIT"] == 7 and e.caps["CRYPTO_GROUP_RISK_LIMIT"] == 5 and e.caps["DEPTH_CAP"] == 4
    assert e.decision == "REDUCE" and e.approved_contracts == D("4")              # never average / median / largest
    assert ev(cand(requested_contracts="20", requested_max_limit_price="0.50", current_executable_price="0.50",
                   estimated_fee="0", available_depth="100"), p=p).approved_contracts == D("5")


def test_cap_zero_veto():
    e = ev(s=snap(total_open_risk="1000"))
    assert_veto(e, "PORTFOLIO_OPEN_RISK_LIMIT")
    e = ev(p=policy(max_loss_per_trade="0.20"))                                   # only the fixed fee fits
    assert_veto(e, "PER_TRADE_LOSS_LIMIT")
    e = ev(p=policy(max_contracts_per_trade="0.004"))                            # floors to 0
    assert_veto(e, "PER_TRADE_CONTRACT_LIMIT")


# ═══════════════════ 41-49 breakers ═══════════════════
def test_daily_realized_breaker():
    r = REnv()
    assert r.mgr.evaluate(cand("c1"), snap(daily_realized_pnl="-50"))[0].decision == "APPROVE"     # == max allowed
    d, a = r.mgr.evaluate(cand("c2"), snap(daily_realized_pnl="-50.01"))
    assert d.decision == "VETO" and "DAILY_REALIZED_LOSS_BREAKER" in d.reason_codes and a is None
    assert r.mgr.breakers()["DAILY_REALIZED_LOSS"] == "LATCHED"
    hist = [(e["payload"]["previous_state"], e["payload"]["new_state"]) for e in r.mgr.store.events("BREAKER")]
    assert hist == [("CLEAR", "TRIGGERED"), ("TRIGGERED", "LATCHED")]


def test_daily_total_breaker():
    r = REnv()
    d, _ = r.mgr.evaluate(cand(), snap(daily_realized_pnl="-40", daily_unrealized_pnl="-40.01"))
    assert "DAILY_TOTAL_LOSS_BREAKER" in d.reason_codes and r.mgr.breakers()["DAILY_TOTAL_LOSS"] == "LATCHED"
    assert_veto(ev(s=snap(daily_unrealized_pnl=UNKNOWN)), "DAILY_PNL_UNKNOWN")       # needs authoritative PnL
    assert_veto(ev(s=snap(daily_realized_pnl=UNKNOWN)), "DAILY_PNL_UNKNOWN")
    assert REnv().mgr.breakers()["DAILY_TOTAL_LOSS"] == "CLEAR"


def test_drawdown_breaker():
    assert ev(s=snap(account_equity="900", day_peak_equity="1000")).decision == "APPROVE"   # 100 == max allowed
    r = REnv()
    d, _ = r.mgr.evaluate(cand(), snap(account_equity="899.99", day_peak_equity="1000"))
    assert "ROLLING_DRAWDOWN_BREAKER" in d.reason_codes and r.mgr.breakers()["ROLLING_DRAWDOWN"] == "LATCHED"
    r2 = REnv()                                                       # the internally tracked day peak is used too
    r2.mgr.observe(snap(account_equity="1200", day_peak_equity="1200"))
    d, _ = r2.mgr.evaluate(cand(), snap(account_equity="1050", day_peak_equity="1050"))
    assert "ROLLING_DRAWDOWN_BREAKER" in d.reason_codes                               # 1200 - 1050 = 150 > 100
    assert_veto(ev(s=snap(day_peak_equity=UNKNOWN)), "EQUITY_STATE_UNKNOWN")


def test_consecutive_loss_breaker():
    r = REnv()
    for i, pnl in enumerate(("-1", "-2")):
        r.mgr.store.record_trade_result(f"t{i}", pnl, NOW)
    assert r.mgr.evaluate(cand("c1"), snap())[0].decision == "APPROVE"              # 2 < 3
    r.mgr.store.record_trade_result("t2", "-0.5", NOW)
    d, _ = r.mgr.evaluate(cand("c2"), snap())
    assert "CONSECUTIVE_LOSS_BREAKER" in d.reason_codes and r.mgr.breakers()["CONSECUTIVE_LOSS"] == "LATCHED"
    assert_veto(ev(s=snap(consecutive_losses=3)), "CONSECUTIVE_LOSS_BREAKER")      # snapshot-reported count too
    assert_veto(ev(s=snap(consecutive_losses=UNKNOWN)), "CONSECUTIVE_LOSS_STATE_UNKNOWN")


def test_breaker_blocks_outstanding_approval():
    r, d, a = _approved()
    it = intent_from_approval(cand(), a, "i1")
    assert r.mgr.approval_book().verify(it, NOW)[0]
    r.mgr.observe(snap("s-loss", daily_realized_pnl="-60"))                     # a breaker latches after issuance
    ok, why = r.mgr.approval_book().verify(it, NOW)
    assert not ok and why.startswith("RISK_BREAKER_LATCHED") and "DAILY_REALIZED_LOSS" in why
    eng = r.engine()
    assert eng.submit(it) == S.REJECTED and reasons_of(eng, it).startswith("RISK_BREAKER_LATCHED")
    assert r.venue.submit_attempts == {} and r.mgr.store.count("CONSUMPTION") == 0
    assert not r.restart().approval_book().verify(it, NOW)[0]                   # also after restart


def test_breaker_persists_restart():
    r = REnv()
    r.mgr.evaluate(cand("c1"), snap(daily_realized_pnl="-60"))
    for _ in range(2):
        m = r.restart()
        assert m.breakers()["DAILY_REALIZED_LOSS"] == "LATCHED"
        d, a = m.evaluate(cand(f"c{_ + 5}"), snap(daily_realized_pnl="0"))
        assert d.decision == "VETO" and "DAILY_REALIZED_LOSS_BREAKER" in d.reason_codes and a is None


def test_breaker_stays_latched():
    r = REnv()
    r.mgr.evaluate(cand("c1"), snap(daily_realized_pnl="-60"))
    for pnl in ("-10", "0", "500"):                                            # PnL improves intraday
        d, _ = r.mgr.evaluate(cand(f"c-{pnl}"), snap(daily_realized_pnl=pnl))
        assert d.decision == "VETO" and "DAILY_REALIZED_LOSS_BREAKER" in d.reason_codes
    try:
        check_breaker_transition("DAILY_REALIZED_LOSS", "TRIGGERED", "CLEAR"); raise AssertionError("bad transition")
    except InvalidBreakerTransition:
        pass
    try:
        with r.mgr.store.transaction():
            r.mgr.store.breaker_transition("DAILY_REALIZED_LOSS", "TRIGGERED", NOW, "x")
        raise AssertionError("LATCHED -> TRIGGERED accepted")
    except InvalidBreakerTransition:
        pass


def test_utc_day_reset():
    day_start = (NOW // DAY + 1) * DAY                                       # next UTC midnight
    r = REnv()
    r.clock.t = day_start - 10_000
    r.mgr.evaluate(cand("c1", created_at=day_start - 20_000, decision_ts=day_start - 20_000, expires_at=day_start + DAY,
                        market_close_ts=day_start + DAY), snap(captured_at=day_start - 10_000, daily_realized_pnl="-60"))
    r.mgr.store.record_trade_result("a", "-1", day_start - 9000)
    r.mgr.store.record_trade_result("b", "-1", day_start - 8000)
    r.mgr.store.record_trade_result("c", "-1", day_start - 7000)
    r.clock.t = day_start - 7000                                                # never observe a future snapshot
    r.mgr.observe(snap(captured_at=day_start - 7000))
    assert r.mgr.breakers() == {"DAILY_REALIZED_LOSS": "LATCHED", "DAILY_TOTAL_LOSS": "CLEAR",
                                "ROLLING_DRAWDOWN": "CLEAR", "CONSECUTIVE_LOSS": "LATCHED"}
    r.clock.t = day_start - 1                                                   # still the same UTC day
    r.mgr.observe(snap(captured_at=day_start - 1))
    assert r.mgr.breakers()["DAILY_REALIZED_LOSS"] == "LATCHED"
    r.clock.t = day_start + 1                                                   # the next UTC day
    m = r.restart()
    m.observe(snap(captured_at=day_start + 1))
    assert m.breakers()["DAILY_REALIZED_LOSS"] == "CLEAR" and m.breakers()["CONSECUTIVE_LOSS"] == "LATCHED"
    resets = [e for e in m.store.events("BREAKER") if e["payload"]["action"] == "RESET"]
    assert len(resets) == 1 and "UTC day boundary" in resets[0]["payload"]["reason"]
    m.reset_breaker("CONSECUTIVE_LOSS", "operator reviewed the losing streak")
    assert m.breakers()["CONSECUTIVE_LOSS"] == "CLEAR"
    assert utc_day(day_start) == utc_day(day_start + DAY - 1) != utc_day(day_start - 1)
    for tz in ("Pacific/Kiritimati", "America/Los_Angeles"):                   # never the local time zone
        p = subprocess.run([sys.executable, "-c", f"import sys; sys.path.insert(0, {HERE!r}); import time; "
                            f"time.tzset(); from risk.breakers import utc_day; print(utc_day({day_start - 1}))"],
                           capture_output=True, text=True, env=dict(os.environ, TZ=tz))
        assert p.stdout.strip() == utc_day(day_start - 1), (tz, p.stdout, p.stderr)


def test_zero_pnl():
    st = RiskStore(os.path.join(tmpdir(), "r.sqlite"))
    st.record_trade_result("a", "-1", NOW)
    st.record_trade_result("b", "-1", NOW)
    st.record_trade_result("z", "0", NOW)                                        # neither increments nor resets
    assert st.loss_streak() == (2, 0)
    st.record_trade_result("w", "0.01", NOW)                                     # a win resets
    assert st.loss_streak() == (0, 0)


def test_unknown_pnl_not_win():
    r = REnv()
    r.mgr.store.record_trade_result("a", "-1", NOW)
    r.mgr.store.record_trade_result("b", "-1", NOW)
    r.mgr.store.record_trade_result("u", UNKNOWN, NOW)
    assert r.mgr.store.loss_streak() == (2, 1)                                   # not reset (never a win)
    d, _ = r.mgr.evaluate(cand("c1"), snap())
    assert d.decision == "VETO" and "CONSECUTIVE_LOSS_STATE_UNKNOWN" in d.reason_codes
    r.mgr.store.record_trade_result("u", "-3", NOW)                              # resolved as a loss
    assert r.mgr.store.loss_streak() == (3, 0)
    d, _ = r.mgr.evaluate(cand("c2"), snap())
    assert "CONSECUTIVE_LOSS_BREAKER" in d.reason_codes


# ═══════════════════ 50, 68-69 persistence ═══════════════════
def test_decision_persists():
    r = REnv()
    d, a = r.mgr.evaluate(cand(), snap(total_open_risk="8"), )
    rec = r.mgr.store.decision_record(d.risk_decision_id)
    assert rec["candidate"] == cand().to_dict() and rec["snapshot"] == snap(total_open_risk="8").to_dict()
    assert rec["policy_fingerprint"] == risk_policy_fingerprint(r.p) and rec["approval_id"] == d.risk_decision_id
    assert rec["decision"]["reason_codes"] == list(d.reason_codes) and rec["caps"]
    m = r.restart()
    assert m.decision(d.risk_decision_id) == (d, a)
    v, _ = m.evaluate(cand("v1", signal_status="NO_CALL"), snap())
    assert m.store.decision_record(v.risk_decision_id)["approval_id"] is None


def test_risk_rollback():
    r = REnv()
    st = r.mgr.store
    n = st.count()
    try:
        with st.transaction():
            st.append("OBSERVATION", "o:x", NOW, {"snapshot": {}})
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert st.count() == n
    r.faults.arm("during_decision_transaction")
    crash(r.mgr.evaluate, cand(), snap())
    m = r.restart()
    assert m.store.count("DECISION") == 0 and m.store.count("APPROVAL") == 0           # nothing half-written
    r.faults.arm("before_approval_creation")
    crash(m.evaluate, cand(), snap())
    m = r.restart()
    assert m.store.count("DECISION") == 0 and m.store.count("APPROVAL") == 0
    m.evaluate(cand(), snap())
    st = m.store
    assert st.count() > 0
    try:
        st.conn.execute("DELETE FROM events"); raise AssertionError("risk journal edited")
    except Exception as e:                                                         # noqa: BLE001
        assert "append-only" in str(e)
    try:
        st.append("OBSERVATION", "o:y", NOW, {}); raise AssertionError("write outside a transaction")
    except Exception as e:                                                         # noqa: BLE001
        assert "transaction" in str(e)


def test_risk_journal_restart():
    r = REnv()
    d1, a1 = r.mgr.evaluate(cand("c1"), snap())
    d2, _ = r.mgr.evaluate(cand("c2", signal_status="NO_CALL"), snap())
    r.mgr.store.record_trade_result("t1", "-1", NOW)
    before = (r.mgr.store.count(), r.mgr.breakers(), r.mgr.store.loss_streak())
    for _ in range(2):
        m = r.restart()
        assert (m.store.count(), m.breakers(), m.store.loss_streak()) == before
        assert m.decision(d1.risk_decision_id) == (d1, a1) and m.decision(d2.risk_decision_id)[0] == d2
        assert m.evaluate(cand("c1"), snap()) == (d1, a1)                           # no conflicting approval


# ═══════════════════ 51-60 approvals ═══════════════════
def _approved(r=None, c=None, s=None):
    r = r or REnv()
    d, a = r.mgr.evaluate(c or cand(), s or snap())
    assert a is not None
    return r, d, a


def test_approval_expiry():
    r, d, a = _approved()
    eng = r.engine()
    r.clock.t = a.expires_at
    it = intent_from_approval(cand(), a, "i1")
    assert eng.submit(it) in (S.REJECTED, S.EXPIRED)
    assert reasons_of(eng, it) in ("RISK_APPROVAL_EXPIRED", "EXPIRED", "EXPIRED_ON_ARRIVAL"), reasons_of(eng, it)
    r2, d2, a2 = _approved()
    eng2 = r2.engine()
    r2.clock.t = a2.expires_at - 1
    it2 = dataclasses.replace(intent_from_approval(cand(), a2, "i1"), expires_at=a2.expires_at + 10)
    ok, why = r2.mgr.approval_book().verify(it2, a2.expires_at)
    assert not ok and why == "RISK_APPROVAL_EXPIRED"
    assert r2.mgr.approval_book().verify(intent_from_approval(cand(), a2, "i1"), a2.expires_at - 1)[0]
    r2.clock.t = NOW + 6000                                                      # re-evaluation never renews silently
    d3, a3 = r2.mgr.evaluate(cand(), snap())
    assert d3.decision == "VETO" and "STALE_RISK_SNAPSHOT" in d3.reason_codes and a3 is None
    d4, a4 = r2.mgr.evaluate(cand(), snap("s2", captured_at=NOW + 6000))          # a NEW snapshot -> a new approval
    assert a4 is not None and a4.risk_decision_id != a2.risk_decision_id and a4.expires_at > a2.expires_at
    assert a.expires_at == min(NOW + 60_000, cand().expires_at, cand().market_close_ts)
    a_short = REnv().mgr.evaluate(cand(expires_at=NOW + 1000), snap())[1]
    assert a_short.expires_at == NOW + 1000                                       # never beyond the candidate


def _verify(r, a, **changes):
    it = dataclasses.replace(intent_from_approval(cand(), a, "i1"), **changes)
    return r.mgr.approval_book().verify(it, NOW)


def test_approval_candidate_binding():
    r, d, a = _approved()
    assert _verify(r, a) == (True, "RISK_APPROVED")
    assert _verify(r, a, candidate_id="other-candidate")[1] == "RISK_APPROVAL_CANDIDATE_MISMATCH"
    ex = a.to_execution()
    assert ex.binding_hash == approval_binding_hash(dataclasses.replace(ex, binding_hash=None))
    from execution.risk import check_intent_against_approval
    it = intent_from_approval(cand(), a, "i1")
    assert check_intent_against_approval(it, dataclasses.replace(ex, max_contracts=D("50")), NOW)[1] \
        == "RISK_APPROVAL_TAMPERED"
    assert check_intent_against_approval(it, dataclasses.replace(ex, candidate_id="c9"), NOW)[1] \
        == "RISK_APPROVAL_TAMPERED"


def test_approval_side_binding():
    r, d, a = _approved()
    assert _verify(r, a, side="NO")[1] == "RISK_APPROVAL_SCOPE_MISMATCH"


def test_approval_market_binding():
    r, d, a = _approved()
    assert _verify(r, a, market_ticker=TK2)[1] == "RISK_APPROVAL_SCOPE_MISMATCH"
    assert _verify(r, a, market_ticker=TK_ETH, asset="ETH")[1] == "RISK_APPROVAL_SCOPE_MISMATCH"


def test_approval_size_binding():
    r, d, a = _approved(c=cand(requested_contracts="4"))
    assert _verify(r, a, requested_contracts=D("4.01"))[1] == "SIZE_EXCEEDS_APPROVAL"
    assert _verify(r, a, requested_contracts=D("3"))[0]                          # smaller is fine


def test_approval_price_binding():
    r, d, a = _approved()
    assert _verify(r, a, max_limit_price=D("0.41"))[1] == "PRICE_EXCEEDS_APPROVAL"
    assert _verify(r, a, max_limit_price=D("0.39"))[0]


def test_approval_policy_binding():
    r, d, a = _approved()
    try:                                                                         # never a silent policy swap
        r.restart(policy(max_contracts_per_trade="999")); raise AssertionError("policy silently overwritten")
    except RiskPolicyMismatchError:
        pass
    m = r.restart(policy())
    m.activate_policy(policy(max_contracts_per_trade="999"), "test: policy change")   # explicit, durable
    ok, why = m.approval_book().verify(intent_from_approval(cand(), a, "i1"), NOW)
    assert not ok and why == "RISK_POLICY_CHANGED"
    d2, a2 = m.evaluate(cand(), snap())
    assert a2.risk_decision_id != a.risk_decision_id and a2.risk_policy_fingerprint != a.risk_policy_fingerprint
    assert m.approval_book().verify(intent_from_approval(cand(), a, "i1"), NOW)[1] in ("RISK_POLICY_CHANGED",
                                                                                     "RISK_APPROVAL_SUPERSEDED")
    assert m.approval_book().verify(intent_from_approval(cand(), a2, "i2"), NOW)[0]
    assert _verify(r, a2, signal_fingerprint="99" * 16)[1] == "RISK_APPROVAL_PROVENANCE_MISMATCH"


def test_approval_single_use():
    r, d, a = _approved()
    it = intent_from_approval(cand(), a, "i1")
    eng = r.engine()
    assert eng.submit(it) == S.ACKNOWLEDGED
    other = dataclasses.replace(it, intent_id="i2")
    eng2 = r.engine(os.path.join(r.dir, "journal2.sqlite"))                      # no market lock in this journal
    assert eng2.submit(other) == S.REJECTED and reasons_of(eng2, other).startswith("RISK_APPROVAL_ALREADY_CONSUMED")
    assert r.mgr.store.count("CONSUMPTION") == 1
    d2, a2 = r.mgr.evaluate(cand(), snap("s2"))                                   # the candidate was executed
    assert d2.decision == "VETO" and "CANDIDATE_ALREADY_EXECUTED" in d2.reason_codes and a2 is None


def test_replay_idempotent():
    r, d, a = _approved()
    it = intent_from_approval(cand(), a, "i1")
    assert r.engine().submit(it) == S.ACKNOWLEDGED
    for _ in range(2):
        r.restart()
        eng = r.engine()
        assert eng.submit(it) == S.ACKNOWLEDGED
    assert r.mgr.store.count("CONSUMPTION") == 1 and len(r.venue.orders) == 1
    assert list(r.venue.submit_attempts.values()) == [1]


def test_second_intent_cannot_reuse():
    r, d, a = _approved()
    it = intent_from_approval(cand(), a, "i1")
    r.engine().submit(it)
    m = r.restart()
    ok, why = m.approval_book().verify_and_consume(dataclasses.replace(it, intent_id="i9"), "some-key", NOW)
    assert not ok and why.startswith("RISK_APPROVAL_ALREADY_CONSUMED")
    r2, d2, a2 = _approved()
    a_ok, _w = r2.mgr.approval_book().verify_and_consume(intent_from_approval(cand(), a2, "x1"), "k1", NOW)
    assert a_ok and not r2.mgr.approval_book().verify_and_consume(intent_from_approval(cand(), a2, "x2"), "k2", NOW)[0]


def test_supersede_and_candidate_conflict():
    r = REnv()
    d1, a1 = r.mgr.evaluate(cand(), snap())
    d2, a2 = r.mgr.evaluate(cand(), snap("s2", account_equity="1001"))           # a new snapshot
    assert a2.risk_decision_id != a1.risk_decision_id
    ok, why = r.mgr.approval_book().verify(intent_from_approval(cand(), a1, "i1"), NOW)
    assert not ok and why == "RISK_APPROVAL_SUPERSEDED"                           # at most one valid approval
    assert r.mgr.approval_book().verify(intent_from_approval(cand(), a2, "i1"), NOW)[0]
    d3, a3 = r.mgr.evaluate(cand(requested_contracts="11"), snap())               # same id, other content
    assert d3.decision == "VETO" and "INVALID_CANDIDATE" in d3.reason_codes and a3 is None


# ═══════════════════ 61-62 execution facts ═══════════════════
def test_unresolved_execution_veto():
    for st in ("EXECUTION_UNKNOWN", "HALTED_FOR_RECONCILIATION", "ACCOUNTING_INCOMPLETE_UNSAFE", "UNKNOWN"):
        assert_veto(ev(s=snap(market_execution_state=st)), "EXECUTION_UNRESOLVED")
        assert_veto(ev(s=snap(asset_execution_state=st)), "EXECUTION_UNRESOLVED")
    from execution.risk import RiskApproval as XA, RiskApprovalBook
    from execution.identity import execution_key
    from execution.intent import OrderIntent
    venue = PaperVenue().script_submit("TIMEOUT_NOT_RECEIVED")
    book = RiskApprovalBook([XA("rk", "a" * 64, True, TK, "BTC", "YES", "5", "0.5", T0 + 600_000)])
    eng = ExecutionEngine(Journal(os.path.join(tmpdir(), "j.sqlite")), PaperExecutionAdapter(venue), book,
                          FixedClock(NOW))
    it = OrderIntent(intent_id="e1", candidate_id="x", created_at=T0, market_ticker=TK, asset="BTC", side="YES",
                     requested_contracts="5", max_limit_price="0.5", time_in_force="GOOD_TIL_EXPIRY", decision_ts=T0,
                     expires_at=T0 + 300_000, raw_probability="0.6", calibrated_probability="0.6", market_price="0.5",
                     estimated_fee="0", estimated_slippage="0", estimated_net_ev="0.1", model_id="m",
                     model_fingerprint="ab" * 16, calibration_fingerprint="cd" * 16, signal_fingerprint="ef" * 16,
                     risk_decision_id="rk", risk_snapshot_hash="a" * 64)
    assert eng.submit(it) == S.HALTED_FOR_RECONCILIATION and execution_key(it)
    assert execution_facts(eng, "BTC", TK) == ("HALTED_FOR_RECONCILIATION", "CLEAR")
    assert execution_facts(eng, "BTC", TK2) == ("CLEAR", "HALTED_FOR_RECONCILIATION")
    m, a_ = execution_facts(eng, "BTC", TK2)
    assert_veto(ev(cand(market_ticker=TK2), snap(market_ticker=TK2, market_execution_state=m,
                                                 asset_execution_state=a_)), "EXECUTION_UNRESOLVED")
    assert execution_facts(eng, "ETH", TK_ETH) == ("CLEAR", "CLEAR")


def test_execution_mismatch_veto():
    for st in ("POSITION_MISMATCH", "FILL_MISMATCH"):
        assert_veto(ev(s=snap(market_execution_state=st)), "EXECUTION_MISMATCH")
        assert_veto(ev(s=snap(asset_execution_state=st)), "EXECUTION_MISMATCH")
    for st in ("ACTIVE_ENTRY", "OPEN_POSITION"):                                  # existing same-market exposure
        assert_veto(ev(s=snap(market_execution_state=st)), "EXISTING_MARKET_EXPOSURE")
        assert ev(s=snap(asset_execution_state=st)).decision == "APPROVE"        # other market: exposure caps apply
    assert_veto(ev(s=snap(current_position_size="1")), "EXISTING_MARKET_EXPOSURE")
    assert_veto(ev(s=snap(current_market_open_risk="0.5")), "EXISTING_MARKET_EXPOSURE")
    assert_veto(ev(s=snap(current_position_size=UNKNOWN)), "EXPOSURE_STATE_UNKNOWN")
    assert_veto(ev(s=snap(total_open_risk=UNKNOWN)), "EXPOSURE_STATE_UNKNOWN")


# ═══════════════════ 38, 39 hard veto overrides reduction; provenance ═══════════════════
def test_hard_veto_overrides_reduction():
    p = policy(max_contracts_per_trade="2")
    assert ev(p=p).decision == "REDUCE"
    for c, s, code in ((cand(), snap(feed_health="FAIL"), "FEED_HEALTH_FAIL"),
                       (cand(signal_status="NO_CALL"), snap(), "NO_CALL"),
                       (cand(estimated_net_ev="0"), snap(), "NON_POSITIVE_EV"),
                       (cand(), snap(market_execution_state="EXECUTION_UNKNOWN"), "EXECUTION_UNRESOLVED")):
        e = ev(c, s, p)
        assert_veto(e, code)
        assert "PER_TRADE_CONTRACT_LIMIT" not in e.reason_codes                   # never "REDUCE 2"


def test_provenance_untouched():
    r = REnv()
    c = cand()
    d, a = r.mgr.evaluate(c, snap())
    for f in ("signal_fingerprint", "model_fingerprint", "calibration_fingerprint"):
        assert getattr(a, f) == getattr(c, f)
    it = intent_from_approval(c, a, "i1")
    for f in ("raw_probability", "calibrated_probability", "estimated_net_ev", "signal_fingerprint",
              "model_fingerprint", "calibration_fingerprint"):
        assert getattr(it, f) == getattr(c, f), f
    assert c == cand()                                                           # the candidate is never modified
    for missing in (dict(signal_fingerprint=UNKNOWN), dict(model_fingerprint=""), dict(calibration_fingerprint="zz")):
        d, a = REnv().mgr.evaluate(cand(**missing), snap())
        assert d.decision == "VETO" and "INVALID_CANDIDATE" in d.reason_codes and a is None


# ═══════════════════ §36 example paper flow ═══════════════════
def test_paper_flow():
    """BTC UP requested 10; per-trade cap 6; portfolio capacity 4; depth 5 -> REDUCE 4; execution can never send 5 / 10."""
    p = policy(max_contracts_per_trade="6", max_portfolio_open_risk="1.80")       # (1.80 - 0.20) / 0.40 = 4
    r = REnv(p)
    d, a = r.mgr.evaluate(cand(available_depth="5"), snap())
    assert d.decision == "REDUCE" and d.approved_contracts == D("4")
    assert d.reason_codes == ("PER_TRADE_CONTRACT_LIMIT", "PORTFOLIO_OPEN_RISK_LIMIT", "DEPTH_CAP")
    assert d.calculated_worst_case_loss == D("1.80")
    for n in ("5", "10"):
        eng = r.engine(os.path.join(r.dir, f"j{n}.sqlite"))
        big = dataclasses.replace(intent_from_approval(cand(), a, f"big{n}"), requested_contracts=D(n))
        assert eng.submit(big) == S.REJECTED and reasons_of(eng, big) == "SIZE_EXCEEDS_APPROVAL"
    assert r.venue.submit_attempts == {}
    it = intent_from_approval(cand(), a, "i4")
    eng = r.engine()
    assert eng.submit(it) == S.ACKNOWLEDGED
    order = list(r.venue.orders.values())[0]
    assert order["count"] == "4" and order["side"] == "YES" and order["limit_price"] == "0.4"


# ═══════════════════ §44-46 properties, worst case, boundaries ═══════════════════
def test_property_matrix():
    n = 0
    for req in ("1", "3.5", "10", "20"):
        for price in ("0.01", "0.10", "0.33", "0.99"):
            for loss in ("0.5", "2", "100"):
                for depth in ("0.5", "4", "100"):
                    for health in ("PASS", "FAIL"):
                        for status in ("CALL", "NO_CALL"):
                            for evv in ("0.05", "0", "-0.1"):
                                c = cand(requested_contracts=req, requested_max_limit_price=price,
                                         current_executable_price=price, available_depth=depth,
                                         signal_status=status, estimated_net_ev=evv, estimated_fee="0.01")
                                e = ev(c, snap(feed_health=health), policy(max_loss_per_trade=loss))
                                n += 1
                                a, rq = e.approved_contracts, D(req)
                                assert a >= 0 and a <= rq
                                assert e.approved_max_limit_price is None or e.approved_max_limit_price <= D(price)
                                if e.decision == "VETO":
                                    assert a == 0
                                elif e.decision == "REDUCE":
                                    assert 0 < a < rq
                                else:
                                    assert a == rq
                                if status == "NO_CALL" or health == "FAIL" or D(evv) <= 0:
                                    assert e.decision == "VETO", (req, price, status, health, evv)
                                if e.decision != "VETO":
                                    assert e.worst_case_loss <= D(loss) and a <= D(depth)
                                    assert e.worst_case_loss == a * D(price) + D("0.01")
    assert n == 1728


def test_worst_case_exact():
    assert worst_case_loss(D("10"), D("0.40"), D("0.20"), D("0")) == D("4.20")
    e = ev(p=policy(max_loss_per_trade="4.20"))
    assert e.decision == "APPROVE" and e.worst_case_loss == D("4.20")               # exactly at the limit
    e = ev(p=policy(max_loss_per_trade="4.1999"))                                 # one increment above the limit
    assert e.decision == "REDUCE" and e.approved_contracts == D("9.99") and e.worst_case_loss <= D("4.1999")
    for price, req, fee in (("0.01", "100000", "1"), ("0.10", "33", "0"), ("0.33", "3", "0.03"),
                            ("0.99", "7", "0.07")):
        e = ev(cand(requested_contracts=req, requested_max_limit_price=price, current_executable_price=price,
                    estimated_fee=fee, available_depth="1000000"),
               p=policy(max_contracts_per_trade="1000000", max_notional_per_trade="1000000",
                        max_loss_per_trade="1000000", max_asset_open_risk="1000000",
                        max_asset_gross_notional="1000000", max_portfolio_open_risk="1000000",
                        max_portfolio_gross_notional="1000000", max_crypto_group_open_risk="1000000",
                        max_same_direction_crypto_risk="1000000"),
               s=snap(account_equity="1000000", day_start_equity="1000000", day_peak_equity="1000000",
                      available_cash="1000000", available_depth="1000000"))
        assert e.worst_case_loss == D(req) * D(price) + D(fee), (price, e.worst_case_loss)
    e = ev(cand(requested_max_limit_price="0.33", current_executable_price="0.33", estimated_fee="0"),
           p=policy(max_loss_per_trade="1"))                                         # 1 / 0.33 = 3.0303.. -> 3.03
    assert e.approved_contracts == D("3.03") and e.worst_case_loss == D("0.9999") <= D("1")
    e = ev(cand(current_executable_price="0.30"), p=policy(max_loss_per_trade="2.60"))
    assert e.approved_contracts == D("6") and e.worst_case_loss == D("2.60")     # the MAX price, never the current


def test_boundaries():
    assert ev(s=snap(spread="0.05")).decision == "APPROVE" and ev(s=snap(spread="0.0501")).decision == "VETO"
    assert ev(cand(available_depth="1", requested_contracts="1")).decision == "APPROVE"
    assert ev(cand(available_depth="0.99", requested_contracts="1")).decision == "VETO"
    assert ev(s=snap(quote_age_ms=5000)).decision == "APPROVE" and ev(s=snap(quote_age_ms=5001)).decision == "VETO"
    assert ev(s=snap(account_equity="100", day_peak_equity="100", available_cash="100")).decision == "APPROVE"
    assert ev(s=snap(daily_realized_pnl="-50")).decision == "APPROVE"
    assert ev(s=snap(daily_realized_pnl="-50.0001")).decision == "VETO"
    assert ev(s=snap(consecutive_losses=2)).decision == "APPROVE" and ev(s=snap(consecutive_losses=3)).decision == "VETO"
    assert ev(p=policy(max_contracts_per_trade="10")).decision == "APPROVE"
    assert ev(p=policy(max_contracts_per_trade="9.99")).approved_contracts == D("9.99")


def test_reason_order_and_validity():
    e1 = ev(s=snap(feed_health="FAIL", quote_age_ms=99_999, spread="0.9"), c=cand(estimated_net_ev="0"))
    e2 = ev(s=snap(spread="0.9", quote_age_ms=99_999, feed_health="FAIL"), c=cand(estimated_net_ev="0"))
    assert e1.reason_codes == e2.reason_codes == tuple(sorted(e1.reason_codes, key=REASON_CODES.index))
    assert e1.reason_codes == ("FEED_HEALTH_FAIL", "STALE_QUOTE", "SPREAD_TOO_WIDE", "NON_POSITIVE_EV")
    assert_veto(ev(p=policy(approval_ttl_ms=0)), "INVALID_POLICY")
    assert_veto(ev(p=policy(max_fraction_equity_per_trade="1.5")), "INVALID_POLICY")
    assert_veto(ev(cand(asset="DOGE")), "INVALID_CANDIDATE")
    assert_veto(ev(s=snap(market_ticker=TK2)), "INVALID_SNAPSHOT")
    assert_veto(ev(s=snap(exposure_group="OTHER")), "INVALID_SNAPSHOT")
    assert_veto(ev(cand(requested_contracts="0")), "INVALID_CANDIDATE")
    assert_veto(ev(cand(requested_max_limit_price="1.01")), "INVALID_CANDIDATE")
    p = load_policy(os.path.join(HERE, "config", "risk_policy_shadow_v1.json"))
    assert p.problems() == [] and "NOT RESEARCH-VALIDATED" in p.label and "NOT PRODUCTION-APPROVED" in p.label


# ═══════════════════ §53 crash points ═══════════════════
def test_crash_points():
    reference, _ = REnv().mgr.evaluate(cand(), snap())
    for point in ("before_decision_write", "during_decision_transaction", "before_approval_creation"):
        r = REnv()
        r.faults.arm(point)
        crash(r.mgr.evaluate, cand(), snap())
        m = r.restart()
        assert m.store.count("DECISION") == 0 and m.store.count("APPROVAL") == 0, point
        d, a = m.evaluate(cand(), snap())
        assert d == reference and a is not None, point                            # same identity after restart
    for point in ("after_decision_write", "after_approval_creation"):
        r = REnv()
        r.faults.arm(point)
        crash(r.mgr.evaluate, cand(), snap())
        m = r.restart()
        assert m.store.count("APPROVAL") == 1, point                              # never disappears
        assert m.evaluate(cand(), snap())[0] == reference and m.store.count("APPROVAL") == 1
    r = REnv()                                                                    # a veto never becomes an approval
    r.faults.arm("during_decision_transaction")
    crash(r.mgr.evaluate, cand(signal_status="NO_CALL"), snap())
    assert r.restart().evaluate(cand(signal_status="NO_CALL"), snap())[1] is None
    r = REnv()                                                                    # a breaker never clears
    r.faults.arm("during_breaker_transition")
    crash(r.mgr.evaluate, cand(), snap(daily_realized_pnl="-60"))
    m = r.restart()
    assert m.breakers()["DAILY_REALIZED_LOSS"] == "CLEAR" and m.store.count("DECISION") == 0   # rolled back
    d, a = m.evaluate(cand(), snap(daily_realized_pnl="-60"))
    assert d.decision == "VETO" and m.breakers()["DAILY_REALIZED_LOSS"] == "LATCHED"
    r = REnv()
    r.faults.arm("after_breaker_trigger")
    crash(r.mgr.observe, snap(daily_realized_pnl="-60"))
    assert r.restart().breakers()["DAILY_REALIZED_LOSS"] == "LATCHED"
    for point in ("during_approval_consumption", "after_approval_consumption"):
        r, d, a = _approved()
        it = intent_from_approval(cand(), a, "i1")
        r.faults.arm(point)
        crash(r.engine().submit, it)
        r.restart()
        eng = r.engine()
        assert eng.submit(it) == S.ACKNOWLEDGED, point
        assert r.mgr.store.count("CONSUMPTION") == 1 and list(r.venue.submit_attempts.values()) == [1]
        other = dataclasses.replace(it, intent_id="i2")
        assert not r.mgr.approval_book().verify_and_consume(other, "k", NOW)[0]                    # still single use
    assert set(RISK_FAULT_POINTS) == {"before_decision_write", "during_decision_transaction", "after_decision_write",
                                      "before_approval_creation", "after_approval_creation",
                                      "during_breaker_transition", "after_breaker_trigger",
                                      "during_approval_consumption", "after_approval_consumption",
                                      "after_streak_reset_before_breaker_clear",
                                      "after_breaker_clear_before_commit"}


# ═══════════════════ Step 6.6.1: breaker observation, reset rule, atomic final authorisation ═══════════════════
from execution.adapter import AdapterUnavailable  # noqa: E402
from execution.faults import FaultInjector  # noqa: E402
from risk.manager import RiskBreakerResetError  # noqa: E402


def test_no_call_latches_breaker():
    """brief §3: a NO_CALL with an authoritative loss beyond the limit latches the breaker; a later same-day CALL with
    PnL back at 0 is VETOED - with and without a restart in between."""
    for restart in (False, True):
        r = REnv()
        d, a = r.mgr.evaluate(cand("n1", signal_status="NO_CALL"), snap("s1", daily_realized_pnl="-100"))
        assert d.decision == "VETO" and "NO_CALL" in d.reason_codes and a is None
        assert r.mgr.breakers()["DAILY_REALIZED_LOSS"] == "LATCHED"
        m = r.restart() if restart else r.mgr
        r.clock.t = NOW + 1000                                                  # later, same UTC day
        d, a = m.evaluate(cand("c2"), snap("s2", captured_at=NOW + 1000, daily_realized_pnl="0"))
        assert d.decision == "VETO" and "DAILY_REALIZED_LOSS_BREAKER" in d.reason_codes and a is None, restart
    # an INVALID candidate (any candidate property) cannot suppress observation of an authoritative snapshot either
    r = REnv()
    r.mgr.evaluate(cand("bad", requested_contracts="0"), snap(daily_realized_pnl="-100"))
    assert r.mgr.breakers()["DAILY_REALIZED_LOSS"] == "LATCHED"
    # non-authoritative snapshots never latch: future, or captured on another UTC day
    r = REnv()
    r.mgr.evaluate(cand("f1", signal_status="NO_CALL"), snap(captured_at=NOW + 1, daily_realized_pnl="-100"))
    r.mgr.observe(snap("yday", captured_at=NOW - DAY, daily_realized_pnl="-100"))
    assert r.mgr.breakers()["DAILY_REALIZED_LOSS"] == "CLEAR"
    from risk.evaluate import breaker_snapshot_problems
    assert breaker_snapshot_problems(snap(), NOW) == []
    assert breaker_snapshot_problems(snap(captured_at=NOW + 1), NOW) and breaker_snapshot_problems(snap(sid=""), NOW)


def _trip(kind):
    r = REnv()
    s = {"DAILY_REALIZED_LOSS": snap(daily_realized_pnl="-60"),
         "DAILY_TOTAL_LOSS": snap(daily_realized_pnl="-40", daily_unrealized_pnl="-41"),
         "ROLLING_DRAWDOWN": snap(account_equity="850", day_peak_equity="1000")}[kind]
    r.mgr.observe(s)
    assert r.mgr.breakers()[kind] == "LATCHED"
    return r


def _refused_reset(kind):
    r = _trip(kind)
    n = r.mgr.store.count("BREAKER")
    try:
        r.mgr.reset_breaker(kind, "manual override"); raise AssertionError(f"{kind} cleared intraday by an operator")
    except RiskBreakerResetError:
        pass
    assert r.mgr.breakers()[kind] == "LATCHED" and r.mgr.store.count("BREAKER") == n      # history untouched
    assert r.mgr.store.count("BREAKER_RESET_REJECTED") == 1
    assert r.restart().breakers()[kind] == "LATCHED"
    d, _ = r.mgr.evaluate(cand(), snap("s9"))
    assert d.decision == "VETO"


def test_reset_rule_realized():
    _refused_reset("DAILY_REALIZED_LOSS")


def test_reset_rule_total():
    _refused_reset("DAILY_TOTAL_LOSS")


def test_reset_rule_drawdown():
    _refused_reset("ROLLING_DRAWDOWN")


def test_reset_rule_consecutive_and_day_boundary():
    r = REnv()
    for i in range(3):
        r.mgr.store.record_trade_result(f"t{i}", "-1", NOW)
    r.mgr.observe(snap())
    assert r.mgr.breakers()["CONSECUTIVE_LOSS"] == "LATCHED"
    for bad in ("", "   ", None):                                                 # E. a reason is mandatory
        try:
            r.mgr.reset_breaker("CONSECUTIVE_LOSS", bad); raise AssertionError("reset without a reason")
        except RiskBreakerResetError:
            pass
    assert r.mgr.breakers()["CONSECUTIVE_LOSS"] == "LATCHED"
    r.mgr.reset_breaker("CONSECUTIVE_LOSS", "operator reviewed the streak")       # D. allowed
    assert r.mgr.breakers()["CONSECUTIVE_LOSS"] == "CLEAR"
    try:
        r.mgr.reset_breaker("NOT_A_BREAKER", "x"); raise AssertionError("unknown breaker reset")
    except RiskBreakerResetError:
        pass
    r = _trip("DAILY_REALIZED_LOSS")                                              # F. day N -> day N+1
    nxt = (NOW // DAY + 1) * DAY + 5
    r.clock.t = nxt
    d, a = r.mgr.evaluate(cand("c-next", created_at=nxt - 10, decision_ts=nxt - 20, expires_at=nxt + 300_000,
                               market_close_ts=nxt + 600_000), snap("s-next", captured_at=nxt))
    assert r.mgr.breakers()["DAILY_REALIZED_LOSS"] == "CLEAR" and d.decision == "APPROVE"
    assert [e["payload"]["action"] for e in r.mgr.store.events("BREAKER")][-1] == "RESET"


class _Hooked(PaperExecutionAdapter):
    """Paper adapter that runs a hook during the n-th position query (call 1 = PRECHECK, call 2 = final READY)."""

    def __init__(self, venue, hooks):
        super().__init__(venue)
        self.hooks, self.calls = hooks, 0

    def get_positions(self, market_ticker):
        self.calls += 1
        h = self.hooks.get(self.calls)
        if h:
            h()
        return super().get_positions(market_ticker)


def _race(hook2=None, hook1=None, c=None, faults=None, jname="race.sqlite"):
    r, d, a = _approved(c=c)
    it = intent_from_approval(c or cand(), a, "i1")
    hooks = {k: v for k, v in ((1, hook1), (2, hook2)) if v}
    eng = ExecutionEngine(Journal(os.path.join(r.dir, jname)), _Hooked(r.venue, {k: (lambda f=f: f(r, a, it))
                                                                                  for k, f in hooks.items()}),
                          r.mgr.approval_book(), r.clock, faults=faults)
    return r, a, it, eng


def test_race_breaker():
    r, a, it, eng = _race(lambda r, a, it: r.mgr.observe(snap("loss", daily_realized_pnl="-100")))
    assert eng.submit(it) == S.REJECTED and reasons_of(eng, it).startswith("RISK_BREAKER_LATCHED")
    assert r.venue.submit_attempts == {} and r.mgr.store.count("CONSUMPTION") == 0


def test_race_expiry():
    r, a, it, eng = _race(lambda r, a, it: setattr(r.clock, "t", a.expires_at))     # now == expires_at
    assert eng.submit(it) == S.REJECTED and reasons_of(eng, it) == "RISK_APPROVAL_EXPIRED"
    assert r.venue.submit_attempts == {} and r.mgr.store.count("CONSUMPTION") == 0


def test_race_supersede():
    r, a, it, eng = _race(lambda r, a, it: r.mgr.evaluate(cand(), snap("s-new", account_equity="1001")))
    assert eng.submit(it) == S.REJECTED and reasons_of(eng, it) == "RISK_APPROVAL_SUPERSEDED"
    assert r.venue.submit_attempts == {} and r.mgr.store.count("CONSUMPTION") == 0


def test_race_policy():
    r, a, it, eng = _race(lambda r, a, it: r.mgr.set_policy(policy(max_contracts_per_trade="999")))
    assert eng.submit(it) == S.REJECTED and reasons_of(eng, it) == "RISK_POLICY_CHANGED"
    assert r.venue.submit_attempts == {}


def test_race_other_intent():
    def steal(r, a, it):
        ok, _ = r.mgr.approval_book().verify_and_consume(dataclasses.replace(it, intent_id="thief"), "k-thief", NOW)
        assert ok
    r, a, it, eng = _race(steal)
    assert eng.submit(it) == S.REJECTED and reasons_of(eng, it).startswith("RISK_APPROVAL_ALREADY_CONSUMED by thief")
    assert r.venue.submit_attempts == {} and r.mgr.store.count("CONSUMPTION") == 1


def test_final_consume_once_and_replay():
    seen = {}
    r, a, it, eng = _race(hook2=lambda r, a, it: seen.setdefault("during_ready", r.mgr.store.count("CONSUMPTION")),
                          hook1=lambda r, a, it: seen.setdefault("during_precheck", r.mgr.store.count("CONSUMPTION")))
    assert eng.submit(it) == S.ACKNOWLEDGED
    assert seen == {"during_precheck": 0, "during_ready": 0}                     # 9: consumed only at the end
    ev = r.mgr.store.events("CONSUMPTION")
    assert len(ev) == 1 and ev[0]["payload"]["intent_id"] == "i1" and list(r.venue.submit_attempts.values()) == [1]
    for _ in range(3):                                                           # 6: exact replay is idempotent
        assert eng.submit(it) == S.ACKNOWLEDGED
    assert r.mgr.store.count("CONSUMPTION") == 1 and list(r.venue.submit_attempts.values()) == [1]


def test_precheck_does_not_consume():
    def fail_ready(r, a, it):
        raise AdapterUnavailable("position query timed out")
    r, a, it, eng = _race(hook2=fail_ready)
    assert eng.submit(it) == S.REJECTED and reasons_of(eng, it).startswith("PRECHECK_UNVERIFIABLE")
    assert r.mgr.store.count("CONSUMPTION") == 0                                 # 8: PRECHECK never consumed it
    assert r.mgr.approval_book().verify_and_consume(dataclasses.replace(it, intent_id="i2"), "k2", NOW)[0]


def test_crash_before_final_consume():
    r, a, it, eng = _race()
    r.faults.arm("during_approval_consumption")
    crash(eng.submit, it)
    m = r.restart()
    assert m.store.count("CONSUMPTION") == 0                                     # 10: still unconsumed
    eng2 = ExecutionEngine(Journal(os.path.join(r.dir, "race.sqlite")), PaperExecutionAdapter(r.venue),
                           m.approval_book(), r.clock)
    assert eng2.state(execution_key_of(it)) == S.READY
    assert eng2.submit(it) == S.ACKNOWLEDGED and list(r.venue.submit_attempts.values()) == [1]
    assert m.store.count("CONSUMPTION") == 1


def execution_key_of(it):
    from execution.identity import execution_key
    return execution_key(it)


def test_crash_after_final_consume():
    """11: the risk consumption is durable but SUBMITTING was never journaled. Recovery semantics: the approval stays
    bound to that intent; no other intent can use it, no new approval is issued for the candidate, and the SAME
    intent replays idempotently (final checks re-run, consumption replays) and submits exactly once."""
    xf = FaultInjector().arm("after_final_risk_consumption_before_submitting")
    r, a, it, eng = _race(faults=xf)
    crash(eng.submit, it)
    m = r.restart()
    assert m.store.count("CONSUMPTION") == 1 and r.venue.submit_attempts == {}
    eng2 = ExecutionEngine(Journal(os.path.join(r.dir, "race.sqlite")), PaperExecutionAdapter(r.venue),
                           m.approval_book(), r.clock)
    assert eng2.state(execution_key_of(it)) == S.READY
    eng2.recover()
    other = dataclasses.replace(it, intent_id="i-other")
    ok, why = m.approval_book().verify_and_consume(other, "k-other", NOW)
    assert not ok and why.startswith("RISK_APPROVAL_ALREADY_CONSUMED")
    eng3 = ExecutionEngine(Journal(os.path.join(r.dir, "other.sqlite")), PaperExecutionAdapter(r.venue),
                           m.approval_book(), r.clock)
    assert eng3.submit(other) == S.REJECTED
    d2, a2 = m.evaluate(cand(), snap("s-again"))
    assert d2.decision == "VETO" and "CANDIDATE_ALREADY_EXECUTED" in d2.reason_codes and a2 is None
    assert eng2.submit(it) == S.ACKNOWLEDGED
    assert list(r.venue.submit_attempts.values()) == [1] and m.store.count("CONSUMPTION") == 1
    assert eng2.submit(it) == S.ACKNOWLEDGED and list(r.venue.submit_attempts.values()) == [1]


# ═══════════════════ Step 6.6.2: durable active policy, final time point, effective streak reset ═══════════════════
import threading  # noqa: E402
import time as _time  # noqa: E402


def _inside_final(r, cb):
    """Run cb exactly once INSIDE verify_and_consume: after the full _authorize() passed and the prior-consumption
    lookup ran, before the final critical section (the same point the audit used)."""
    st = r.mgr.store
    orig, fired = st.consumption, []

    def hooked(rid):
        if not fired:
            fired.append(1)
            cb()
        return orig(rid)
    st.consumption = hooked
    return fired


def test_policy_race_separate_connection():
    """§5: policy A approval passes the read-only checks; policy B is activated (separate RiskStore connection /
    RiskManager) and COMMITTED before the final authorisation -> REJECTED RISK_POLICY_CHANGED, nothing consumed."""
    def activate_b(r, a, it):
        other = RiskManager(RiskStore(r.path), r.p, r.clock)                       # another connection / manager
        other.activate_policy(policy(max_contracts_per_trade="999"), "operator: tighter limits")
    r, a, it, eng = _race(activate_b)
    assert eng.submit(it) == S.REJECTED and reasons_of(eng, it) == "RISK_POLICY_CHANGED"
    assert r.venue.submit_attempts == {} and r.mgr.store.consumption(a.risk_decision_id) is None
    assert r.mgr.store.count("CONSUMPTION") == 0


def test_policy_race_inside_final():
    """The audited ordering: _authorize under policy A already succeeded inside verify_and_consume; policy B is then
    activated before the CONSUMPTION append -> the final critical section re-reads the durable active policy."""
    r, d, a = _approved()
    it = intent_from_approval(cand(), a, "i1")
    fired = _inside_final(r, lambda: r.mgr.activate_policy(policy(max_contracts_per_trade="999"), "mid-flight"))
    eng = r.engine()
    assert eng.submit(it) == S.REJECTED and reasons_of(eng, it) == "RISK_POLICY_CHANGED" and fired
    assert r.venue.submit_attempts == {} and r.mgr.store.count("CONSUMPTION") == 0


def test_policy_reverse_order():
    """Consumption commits FIRST; a policy activation from another thread / connection blocks on the risk-store write
    lock and commits AFTERWARDS: the execution stays authorised (idempotent replay), policy B governs what follows."""
    r, d, a = _approved()
    it = intent_from_approval(cand(), a, "i1")
    errs, started = [], threading.Event()

    def other_process():
        try:
            started.set()
            m2 = RiskManager(RiskStore(r.path), r.p, r.clock)                  # blocks until the consume commits
            m2.activate_policy(policy(max_contracts_per_trade="999"), "operator: policy B")
        except Exception as e:                                                   # noqa: BLE001
            errs.append(e)
    th = threading.Thread(target=other_process)

    def start_and_wait():
        th.start()
        started.wait(5)
        _time.sleep(0.3)                                                         # the activation is now blocked
    _inside_final(r, start_and_wait)
    eng = r.engine()
    assert eng.submit(it) == S.ACKNOWLEDGED
    th.join(10)
    assert not errs and not th.is_alive(), errs
    evs = r.mgr.store.events()
    seq = {e["kind"]: e["seq"] for e in evs if e["kind"] in ("CONSUMPTION",)}
    act = [e for e in evs if e["kind"] == "POLICY_ACTIVATED"][-1]
    assert act["seq"] > seq["CONSUMPTION"] and act["payload"]["reason"] == "operator: policy B"
    cons = r.mgr.store.consumption(a.risk_decision_id)
    assert cons["policy_fingerprint"] == a.risk_policy_fingerprint
    assert r.mgr.policy_fingerprint == risk_policy_fingerprint(policy(max_contracts_per_trade="999"))
    for _ in range(2):
        assert eng.submit(it) == S.ACKNOWLEDGED                                  # idempotent, never re-authorised
    assert list(r.venue.submit_attempts.values()) == [1] and r.mgr.store.count("CONSUMPTION") == 1
    d2, a2 = r.mgr.evaluate(cand("c-next"), snap("s-next"))                      # policy B applies from now on
    assert a2.risk_policy_fingerprint == risk_policy_fingerprint(policy(max_contracts_per_trade="999"))


def test_policy_durable_restart():
    pa, pb = policy(), policy(max_contracts_per_trade="7")
    fa, fb = risk_policy_fingerprint(pa), risk_policy_fingerprint(pb)
    r = REnv(pa)
    acts = r.mgr.store.events("POLICY_ACTIVATED")
    assert len(acts) == 1 and acts[0]["payload"]["previous_policy_fingerprint"] is None
    assert acts[0]["payload"]["new_policy_fingerprint"] == fa and "initial activation" in acts[0]["payload"]["reason"]
    try:
        r.mgr.activate_policy(pb, "  "); raise AssertionError("activation without a reason")
    except ValueError:
        pass
    r.mgr.activate_policy(pb, "operator: smaller size cap")
    p = r.mgr.store.events("POLICY_ACTIVATED")[-1]
    assert p["event_id"] == "pa:1" and p["payload"]["previous_policy_fingerprint"] == fa
    assert (p["payload"]["new_policy_fingerprint"], p["payload"]["policy_id"], p["payload"]["policy_version"],
            p["payload"]["reason"]) == (fb, "fixture", 1, "operator: smaller size cap") and p["ts_ms"] == NOW
    r.faults.disarm()
    m = RiskManager.from_store(RiskStore(r.path), r.clock)                       # restart: B is reconstructed
    assert m.policy_fingerprint == fb and risk_policy_fingerprint(m.policy) == fb
    try:                                                                         # no silent reversion to A
        RiskManager(RiskStore(r.path), pa, r.clock); raise AssertionError("silent policy reversion")
    except RiskPolicyMismatchError:
        pass
    m2 = RiskManager(RiskStore(r.path), pa, r.clock, activate_reason="operator: revert to A")
    assert m2.policy_fingerprint == fa and m2.store.count("POLICY_ACTIVATED") == 3
    try:
        with m2.store.transaction():
            m2.store.conn.execute("UPDATE events SET payload_json='{}' WHERE kind='POLICY_ACTIVATED'")
        raise AssertionError("policy history rewritten")
    except Exception as e:                                                       # noqa: BLE001
        assert "append-only" in str(e)
    # a Step-6.6.1 (schema 1) store migrates meta-only and gets an audited initial activation; newer is refused
    d = tmpdir()
    st = RiskStore(os.path.join(d, "old.sqlite"))
    st.conn.execute("UPDATE meta SET value='1' WHERE key='schema_version'")
    st.close()
    st = RiskStore(os.path.join(d, "old.sqlite"))
    meta = dict(st.conn.execute("SELECT key, value FROM meta").fetchall())
    assert meta["schema_version"] == "2" and meta["migrated_from"] == "1" and st.active_policy() is None
    assert RiskManager(st, pa, r.clock).policy_fingerprint == fa
    st.conn.execute("UPDATE meta SET value='3' WHERE key='schema_version'")
    st.close()
    try:
        RiskStore(os.path.join(d, "old.sqlite")); raise AssertionError("a newer risk store was opened")
    except Exception as e:                                                       # noqa: BLE001
        assert "not supported" in str(e)


def test_expiry_inside_final():
    r, d, a = _approved()
    it = intent_from_approval(cand(), a, "i1")
    fired = _inside_final(r, lambda: setattr(r.clock, "t", a.expires_at))       # exactly expires_at
    eng = r.engine()
    assert eng.submit(it) == S.REJECTED and reasons_of(eng, it) == "RISK_APPROVAL_EXPIRED" and fired
    assert r.venue.submit_attempts == {} and r.mgr.store.count("CONSUMPTION") == 0
    r2, d2, a2 = _approved()                                                     # expires_at - 1 may authorise
    it2 = intent_from_approval(cand(), a2, "i1")
    _inside_final(r2, lambda: setattr(r2.clock, "t", a2.expires_at - 1))
    eng2 = r2.engine()
    assert eng2.submit(it2) == S.ACKNOWLEDGED
    c = r2.mgr.store.consumption(a2.risk_decision_id)
    assert c["authorized_at"] == a2.expires_at - 1                              # the exact final time read
    assert r2.mgr.store.events("CONSUMPTION")[0]["ts_ms"] == a2.expires_at - 1


def _three_losses(r):
    for i in range(3):
        r.mgr.store.record_trade_result(f"old{i}", "-1", NOW)
    r.mgr.observe(snap("s-loss"))
    assert r.mgr.breakers()["CONSECUTIVE_LOSS"] == "LATCHED" and r.mgr.store.loss_streak() == (3, 0)


def test_streak_reset_effective():
    r = REnv()
    _three_losses(r)
    r.mgr.reset_breaker("CONSECUTIVE_LOSS", "operator reviewed the streak")           # A
    assert r.mgr.breakers()["CONSECUTIVE_LOSS"] == "CLEAR" and r.mgr.store.loss_streak() == (0, 0)
    rs = r.mgr.store.events("LOSS_STREAK_RESET")
    assert len(rs) == 1 and rs[0]["payload"]["previous_consecutive_losses"] == 3
    assert rs[0]["payload"]["new_baseline"] == 0 and rs[0]["payload"]["operator_reason"]
    assert rs[0]["payload"]["policy_fingerprint"] == r.mgr.policy_fingerprint
    assert r.mgr.store.count("TRADE_RESULT") == 3                                  # history intact
    d, a = r.mgr.evaluate(cand("c1"), snap("s1"))
    assert "CONSECUTIVE_LOSS_BREAKER" not in d.reason_codes and d.decision == "APPROVE" and a is not None
    try:                                                                            # never applied twice
        r.mgr.reset_breaker("CONSECUTIVE_LOSS", "again"); raise AssertionError("double reset")
    except InvalidBreakerTransition:
        pass
    assert r.mgr.store.count("LOSS_STREAK_RESET") == 1
    m = r.restart()                                                                  # B
    assert m.breakers()["CONSECUTIVE_LOSS"] == "CLEAR" and m.store.loss_streak() == (0, 0)
    assert m.evaluate(cand("c2"), snap("s2"))[0].decision == "APPROVE"
    m.store.record_trade_result("new0", "-1", NOW)                                  # C
    assert m.store.loss_streak() == (1, 0)
    m.store.record_trade_result("z", "0", NOW)                                      # zero keeps the post-reset streak
    assert m.store.loss_streak() == (1, 0)
    for i in (1, 2):                                                                 # D
        m.store.record_trade_result(f"new{i}", "-1", NOW)
    assert m.store.loss_streak() == (3, 0)
    m.observe(snap("s3"))
    assert m.breakers()["CONSECUTIVE_LOSS"] == "LATCHED"


def test_streak_reset_snapshot_epoch():
    """§13: the journal's post-reset streak is canonical; a snapshot count counts only in the CURRENT epoch."""
    r = REnv()
    _three_losses(r)
    r.mgr.reset_breaker("CONSECUTIVE_LOSS", "operator reviewed the streak")
    d, _ = r.mgr.evaluate(cand("c1"), snap("s1", consecutive_losses=3, loss_streak_epoch=0))   # pre-reset count
    assert d.decision == "APPROVE" and r.mgr.breakers()["CONSECUTIVE_LOSS"] == "CLEAR"
    d, _ = r.mgr.evaluate(cand("c2"), snap("s2", consecutive_losses=2, loss_streak_epoch=1))   # new info, epoch 1
    assert d.decision == "APPROVE"
    assert_veto(ev(s=snap(consecutive_losses=0, loss_streak_epoch=2), state=RiskState(loss_streak_epoch=1)),
                "CONSECUTIVE_LOSS_STATE_UNKNOWN")                                  # an epoch the journal never saw
    assert_veto(ev(s=snap(loss_streak_epoch=UNKNOWN)), "CONSECUTIVE_LOSS_STATE_UNKNOWN")
    d, _ = r.mgr.evaluate(cand("c3"), snap("s3", consecutive_losses=3, loss_streak_epoch=1))   # 3 NEW losses
    assert "CONSECUTIVE_LOSS_BREAKER" in d.reason_codes and r.mgr.breakers()["CONSECUTIVE_LOSS"] == "LATCHED"


def test_streak_reset_atomic():
    for point in ("after_streak_reset_before_breaker_clear", "after_breaker_clear_before_commit"):   # E, F
        r = REnv()
        _three_losses(r)
        r.faults.arm(point)
        crash(r.mgr.reset_breaker, "CONSECUTIVE_LOSS", "operator reviewed")
        m = r.restart()
        assert m.breakers()["CONSECUTIVE_LOSS"] == "LATCHED" and m.store.loss_streak() == (3, 0), point
        assert m.store.count("LOSS_STREAK_RESET") == 0
        d, _ = m.evaluate(cand("c1"), snap("s1"))
        assert d.decision == "VETO" and "CONSECUTIVE_LOSS_BREAKER" in d.reason_codes


def test_streak_reset_unknown_result():
    r = REnv()
    _three_losses(r)
    r.mgr.reset_breaker("CONSECUTIVE_LOSS", "operator reviewed")
    r.mgr.store.record_trade_result("n1", "-1", NOW)
    r.mgr.store.record_trade_result("n2", "-1", NOW)
    r.mgr.store.record_trade_result("u", UNKNOWN, NOW)                              # G
    assert r.mgr.store.loss_streak() == (2, 1)                                      # not reset: never a win
    d, _ = r.mgr.evaluate(cand("c1"), snap("s1"))
    assert d.decision == "VETO" and "CONSECUTIVE_LOSS_STATE_UNKNOWN" in d.reason_codes
    r.mgr.store.record_trade_result("u", "-2", NOW)
    assert r.mgr.store.loss_streak() == (3, 0)


def test_evaluate_one_policy():
    """§15/16: one (policy, fingerprint) pair per evaluation. Policy B activated in the middle of evaluate() (after
    the pure evaluation, before persistence) never produces a decision computed under A but labelled B."""
    pa, pb = policy(max_contracts_per_trade="6"), policy(max_contracts_per_trade="999")
    r = REnv(pa)
    st = r.mgr.store
    orig, fired = st.decision_record, []

    def hooked(rid):
        if not fired:
            fired.append(1)
            r.mgr.activate_policy(pb, "mid-evaluation")
        return orig(rid)
    st.decision_record = hooked
    d, a = r.mgr.evaluate(cand(), snap())
    st.decision_record = orig
    assert fired and d.approved_contracts == D("6") and d.decision == "REDUCE"         # caps of A ...
    assert d.risk_policy_fingerprint == risk_policy_fingerprint(pa)                  # ... labelled A
    assert a.risk_policy_fingerprint == risk_policy_fingerprint(pa)
    assert st.decision_record(d.risk_decision_id)["policy_fingerprint"] == risk_policy_fingerprint(pa)
    ok, why = r.mgr.approval_book().verify(intent_from_approval(cand(), a, "i1"), NOW)
    assert not ok and why == "RISK_POLICY_CHANGED"                                   # B is active now
    d2, _ = r.mgr.evaluate(cand(), snap("s2"))
    assert d2.approved_contracts == D("10") and d2.risk_policy_fingerprint == risk_policy_fingerprint(pb)


# ═══════════════════ 63-67, 70 isolation / structure ═══════════════════
FORBIDDEN_IMPORTS = {"requests", "http", "urllib", "urllib3", "socket", "ssl", "httpx", "aiohttp", "websocket",
                     "websockets", "asyncio", "cryptography", "hmac", "subprocess", "kalshi_dashboard", "kalshi_bot",
                     "kalshi_api_learn", "market_data", "perp_data", "microstructure", "feature_eval", "perp_live",
                     "discord", "settlement"}
FORBIDDEN_LITERALS = ("http://", "https://", "wss://", "ws://", "/portfolio", "/orders", "/transfer", "trade-api",
                      "kalshi.com", "KALSHI-ACCESS", "api_key", "private_key", "api_secret", "PRIVATE KEY")
FORBIDDEN_CALLS = {"post", "put", "patch", "delete", "request", "urlopen", "create_connection", "sign", "getenv",
                   "submit_order", "cancel_order"}


def _risk_files():
    d = os.path.join(HERE, "risk")
    return sorted(os.path.join(d, f) for f in os.listdir(d) if f.endswith(".py"))


def test_no_network_path():
    for f in _risk_files():
        tree = ast.parse(open(f, encoding="utf-8").read())
        mods = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
               {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        assert not {m for m in mods if m.split(".")[0] in FORBIDDEN_IMPORTS}, (f, mods)
        for s in [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]:
            assert not any(x.lower() in s.lower() for x in FORBIDDEN_LITERALS), (f, s[:60])
            assert s.strip().upper() not in ("POST", "PUT", "PATCH", "DELETE", "GET"), (f, s)
        called = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
        called |= {n.func.id for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
        assert not called & FORBIDDEN_CALLS, (f, called & FORBIDDEN_CALLS)
        names = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        assert "environ" not in names, f                                         # no credential loading
    prod = ["kalshi_dashboard.py", "kalshi_backtest.py", "kalshi_bot.py", "run_local.py", "strategy_fingerprint.py",
            "collect_market_data.py", "collect_research_data.py", "perp_live.py", "perp_shadow.py",
            "perp_probability.py"]
    for d in ("kalshi_core", "settlement", "market_data", "perp_data", "microstructure", "feature_eval", "execution"):
        for root, dirs, files in os.walk(os.path.join(HERE, d)):
            dirs[:] = [x for x in dirs if x != "__pycache__"]
            prod += [os.path.relpath(os.path.join(root, x), HERE) for x in files if x.endswith(".py")]
    for f in prod:                                       # nothing below / beside risk imports it (execution included)
        tree = ast.parse(open(os.path.join(HERE, f), encoding="utf-8").read())
        mods = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
               {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        assert not any(m == "risk" or m.startswith("risk.") for m in mods), f"{f} imports risk"
    code = ("import sys; sys.path.insert(0, %r); import run_local, kalshi_core.signal, kalshi_core.adapter, perp_live; "
            "print(any(m == 'risk' or m.startswith('risk.') for m in sys.modules))") % HERE
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=HERE)
    assert p.returncode == 0 and p.stdout.strip() == "False", p.stdout + p.stderr


def test_live_refused():
    from kalshi_core import execution as kx
    from execution.adapter import FutureKalshiExecutionAdapter
    assert kx.LIVE_EXECUTION_AVAILABLE is False
    try:
        kx.get_execution_engine("LIVE"); raise AssertionError("LIVE engine returned")
    except LiveExecutionUnavailable:
        pass
    try:
        FutureKalshiExecutionAdapter(); raise AssertionError("live adapter constructed")
    except LiveExecutionUnavailable:
        pass


def _stage23(keys):
    p = subprocess.run([sys.executable, "test_stage23.py", "--only", keys], cwd=HERE, capture_output=True, text=True,
                       env=dict(os.environ, KALSHI_MASTER_TEST_RUN="1"), timeout=900)
    assert p.returncode == 0 and "Selected Stage 23 tests passed" in p.stdout, p.stdout[-1500:] + p.stderr[-800:]


def test_production_isolation():
    _stage23("isolation,nolivewrite,secrets")                                     # 47 fixtures + every fingerprint


def test_execution_restart_unchanged():
    _stage23("crashbefore,crashafter,crashack,crashpartial,rebuild,noduprestart,unknownrestart,lockunknown,demo")


def test_execution_fee_reconciliation_unchanged():
    _stage23("fillidentity,feecontra,feeresolve,feeremap,ackabsence,unreachable,dupintent,reducerisk,risk")


def test_risk_restart_demo():
    p = subprocess.run([sys.executable, os.path.join("scripts", "risk_restart_demo.py")], capture_output=True,
                       text=True, cwd=HERE, timeout=300)
    assert p.returncode == 0 and "DEMO: every expectation holds" in p.stdout, p.stdout[-1500:] + p.stderr[-800:]


def test_risk_fingerprint():
    from risk import fingerprint as RF
    ok, problems = RF.verify()
    assert ok, problems
    d = tmpdir()
    pkg = os.path.join(d, "risk")
    shutil.copytree(os.path.join(HERE, "risk"), pkg, ignore=shutil.ignore_patterns("__pycache__"))
    assert RF.verify(pkg_dir=pkg)[0]
    with open(os.path.join(pkg, "evaluate.py"), "a", encoding="utf-8") as f:
        f.write("\nMUTATED = 1\n")
    assert not RF.verify(pkg_dir=pkg)[0]
    p = subprocess.run([sys.executable, "-m", "risk.fingerprint", "--write"], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 2
    from execution import fingerprint as XF
    assert XF.verify()[0], XF.verify()[1]


def test_docs():
    doc = open(os.path.join(HERE, "docs", "RISK_ARCHITECTURE.md"), encoding="utf-8").read()
    low = doc.lower()
    for s in ("system boundary", "riskcandidate", "risksnapshot", "riskpolicy", "riskdecision", "riskapproval",
              "worst-case loss", "hard veto", "size reduction", "portfolio exposure", "asset exposure",
              "crypto group", "same-direction", "daily loss", "drawdown", "consecutive-loss", "breaker persistence",
              "approval ttl", "approval consumption", "risk journal", "fingerprint", "restart", "fail closed",
              "unknown", "not research-validated", "cannot place live orders", "step 6.7", "r35"):
        assert s in low, s
    rm = open(os.path.join(HERE, "docs", "ROADMAP.md"), encoding="utf-8").read()
    for s in ("Step 6.5.1", "Step 6.6", "Step 6.7", "CURRENT", "APPROVED"):
        assert s in rm, s
    mut = json.load(open(os.path.join(HERE, "analysis_output", "risk_mutation_results.json")))
    assert mut["all_caught_and_controls_pass"] is True
    assert {m["id"] for m in mut["mutations"]} >= {f"R{i}" for i in range(1, 56)}
    for s in ("policy_activated", "activate_policy", "authorized_at", "loss_streak_reset", "loss_streak_epoch",
              "durability / state-ordering point", "time authorization point", "r55"):
        assert s in low, s
    for s in ("linearization point", "verify_and_consume", "breaker-authoritative", "reset_rule is enforced",
              "after_final_risk_consumption_before_submitting", "r47"):
        assert s in low, s


def test_previous_stages():
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 24):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-600:], p.stderr[-600:])


TESTS = [
    ("call", "1 a CALL candidate can be approved (APPROVE, full size, finite approval)", test_call_approved),
    ("nocall", "2 NO_CALL is always vetoed; no approval can exist for NO_CALL", test_no_call_vetoed),
    ("side", "3 side can never change (decision, approval, intent)", test_side_immutable),
    ("size", "4 size can never increase", test_size_never_increases),
    ("price", "5 price can never widen (tighten only)", test_price_never_widens),
    ("decisionid", "6 deterministic risk_decision_id (processes, restart); snapshot / policy change -> new id", test_decision_id_deterministic),
    ("snaphash", "7 deterministic risk_snapshot_hash; floats / None refused", test_snapshot_hash_deterministic),
    ("policyfp", "8 deterministic policy fingerprint; every semantic change changes it", test_policy_fingerprint_deterministic),
    ("expired", "9 expired candidate / closed market -> VETO", test_expired_candidate),
    ("stalesnap", "10 stale (or future) risk snapshot -> VETO", test_stale_snapshot),
    ("stalequote", "11 stale / UNKNOWN quote age -> VETO", test_stale_quote),
    ("stalefeat", "12 stale features -> VETO", test_stale_features),
    ("stalemodel", "13 stale model decision -> VETO", test_stale_model_decision),
    ("feedfail", "14 feed FAIL -> VETO", test_feed_fail),
    ("feedunknown", "15 feed / model UNKNOWN -> VETO", test_feed_unknown),
    ("health", "16-20 every health FAIL / UNKNOWN / UNAVAILABLE vetoes; DEGRADED follows policy", test_health_gates),
    ("equity", "21-22 missing equity / cash fail closed; minimum equity", test_equity),
    ("fee", "23 UNKNOWN required fee -> VETO (never zero)", test_unknown_fee),
    ("slippage", "24 UNKNOWN required slippage -> VETO (never zero)", test_unknown_slippage),
    ("ev", "25-26 UNKNOWN EV / non-positive EV -> VETO", test_ev_rules),
    ("spread", "27 spread too wide / UNKNOWN -> VETO", test_spread),
    ("depth", "28-29 depth reduction; insufficient / UNKNOWN depth -> VETO", test_depth),
    ("pertrade", "30-33 per-trade contract / notional / worst-case loss / equity-fraction / cash caps", test_per_trade_caps),
    ("asset", "34 per-asset open-risk and notional caps", test_asset_caps),
    ("portfolio", "35-36 portfolio open-risk and notional caps", test_portfolio_caps),
    ("group", "37 crypto-group cap (both directions count)", test_crypto_group_cap),
    ("samedir", "38 same-direction crypto cap (BTC UP + ETH UP + SOL UP = 120)", test_same_direction_cap),
    ("mincaps", "39 simultaneous caps -> the smallest safe size (20 -> 4)", test_multiple_caps_smallest),
    ("capzero", "40 a cap leaving <= 0 -> VETO", test_cap_zero_veto),
    ("realized", "41 daily realized-loss breaker (latched, logged)", test_daily_realized_breaker),
    ("total", "42 daily total-loss breaker (authoritative PnL only)", test_daily_total_breaker),
    ("drawdown", "43 drawdown breaker (day peak incl. internally tracked)", test_drawdown_breaker),
    ("consecutive", "44 consecutive-loss breaker", test_consecutive_loss_breaker),
    ("breakerrestart", "45 breakers persist through restart", test_breaker_persists_restart),
    ("breakerapproval", "46b a breaker latched after issuance blocks an outstanding approval (also after restart)", test_breaker_blocks_outstanding_approval),
    ("latched", "46 a breaker stays latched when PnL improves; invalid transitions fail closed", test_breaker_stays_latched),
    ("dayreset", "47 deterministic UTC day reset (never local time); operator-only consecutive reset", test_utc_day_reset),
    ("zeropnl", "48 zero PnL neither increments nor resets the loss streak", test_zero_pnl),
    ("unknownpnl", "49 UNKNOWN PnL is never a win; the loss state is UNKNOWN until resolved", test_unknown_pnl_not_win),
    ("persist", "50 every RiskDecision persists with candidate, exact snapshot, policy, caps, reasons", test_decision_persists),
    ("expiry", "51 approval expiration; no silent renewal; new snapshot -> new approval", test_approval_expiry),
    ("bindcand", "52 approval bound to its candidate; tampering detected", test_approval_candidate_binding),
    ("bindside", "53 approval bound to its side", test_approval_side_binding),
    ("bindmarket", "54 approval bound to its market / asset", test_approval_market_binding),
    ("bindsize", "55 approval bound to its size", test_approval_size_binding),
    ("bindprice", "56 approval bound to its price", test_approval_price_binding),
    ("bindpolicy", "57 approval bound to its policy (changed policy -> rejected) and provenance", test_approval_policy_binding),
    ("singleuse", "58 single logical use; a consumed candidate is never approved again", test_approval_single_use),
    ("replay", "59 replaying the same intent after restart is idempotent", test_replay_idempotent),
    ("reuse", "60 a second different intent cannot reuse an approval (also after restart)", test_second_intent_cannot_reuse),
    ("supersede", "60b at most one valid approval per candidate; candidate_id reuse refused", test_supersede_and_candidate_conflict),
    ("unresolved", "61 unresolved execution (incl. from a real engine journal) -> VETO", test_unresolved_execution_veto),
    ("mismatch", "62 execution mismatch / existing same-market exposure -> VETO", test_execution_mismatch_veto),
    ("hardveto", "38b a hard veto always overrides a possible reduction", test_hard_veto_overrides_reduction),
    ("provenance", "39b provenance is copied, never changed or manufactured", test_provenance_untouched),
    ("paperflow", "36 example paper flow: REDUCE 4; execution cannot submit 5 or 10", test_paper_flow),
    ("property", "44 invariant matrix (1728 deterministic cases)", test_property_matrix),
    ("worstcase", "45 exact Decimal worst-case loss; boundary and one increment above", test_worst_case_exact),
    ("boundaries", "46 boundary semantics (<= max allowed, >= min allowed)", test_boundaries),
    ("validity", "31 deterministic reason order; invalid policy / candidate / snapshot; shadow config labelled", test_reason_order_and_validity),
    ("crash", "53 crash at every risk fault point -> restart: nothing lost, no veto -> approval, no breaker cleared", test_crash_points),
    ("rollback", "68 risk transaction rollback; append-only; writes need a transaction", test_risk_rollback),
    ("journal", "69 the risk journal survives restart (decisions, approvals, breakers, streak)", test_risk_journal_restart),
    ("nocallbreaker", "651-3 [6.6.1] a NO_CALL still latches an authoritative breaker breach (restart-safe)", test_no_call_latches_breaker),
    ("resetrealized", "651-6A [6.6.1] same-day operator reset of DAILY_REALIZED_LOSS refused", test_reset_rule_realized),
    ("resettotal", "651-6B [6.6.1] same-day operator reset of DAILY_TOTAL_LOSS refused", test_reset_rule_total),
    ("resetdrawdown", "651-6C [6.6.1] same-day operator reset of ROLLING_DRAWDOWN refused", test_reset_rule_drawdown),
    ("resetconsec", "651-6DEF [6.6.1] CONSECUTIVE_LOSS operator reset (reason required); UTC day-boundary reset", test_reset_rule_consecutive_and_day_boundary),
    ("racebreaker", "651-18.1 [6.6.1] breaker latched between verify and final consume -> REJECTED, 0 submits", test_race_breaker),
    ("raceexpiry", "651-18.2 [6.6.1] approval expires before final consume -> REJECTED, 0 submits", test_race_expiry),
    ("racesupersede", "651-18.3 [6.6.1] approval superseded before final consume -> REJECTED", test_race_supersede),
    ("racepolicy", "651-18.4 [6.6.1] policy changes before final consume -> REJECTED", test_race_policy),
    ("raceother", "651-18.5 [6.6.1] approval consumed by another intent before final consume -> REJECTED", test_race_other_intent),
    ("finalonce", "651-18.6/7/9 [6.6.1] final consume once at READY; exact replay idempotent", test_final_consume_once_and_replay),
    ("precheckconsume", "651-18.8 [6.6.1] PRECHECK completion does not consume the approval", test_precheck_does_not_consume),
    ("crashbeforeconsume", "651-18.10 [6.6.1] crash before final consumption -> approval unconsumed", test_crash_before_final_consume),
    ("crashafterconsume", "651-18.11 [6.6.1] crash after consumption, before SUBMITTING -> no second intent / order", test_crash_after_final_consume),
    ("policyrace", "662-5a [6.6.2] policy B committed (other connection) before final consume -> RISK_POLICY_CHANGED", test_policy_race_separate_connection),
    ("policyinside", "662-5b [6.6.2] policy B activated inside the final authorisation -> RISK_POLICY_CHANGED", test_policy_race_inside_final),
    ("policyreverse", "662-5c [6.6.2] consumption first, activation after (thread + connection): stays authorised", test_policy_reverse_order),
    ("policydurable", "662-2/3/4 [6.6.2] active policy durable, audited, restart-safe, no silent overwrite; migration", test_policy_durable_restart),
    ("expiryinside", "662-9 [6.6.2] expiry reached inside the final authorisation -> rejected; expires_at-1 ok", test_expiry_inside_final),
    ("streakreset", "662-14ABCD [6.6.2] operator reset restores eligibility; restart; new losses count again", test_streak_reset_effective),
    ("streakepoch", "662-13 [6.6.2] snapshot loss count only in the current epoch (pre-reset count ignored)", test_streak_reset_snapshot_epoch),
    ("streakatomic", "662-14EF [6.6.2] streak reset + breaker clear are one transaction", test_streak_reset_atomic),
    ("streakunknown", "662-14G [6.6.2] UNKNOWN result after reset fails closed, never a win", test_streak_reset_unknown_result),
    ("onepolicy", "662-15 [6.6.2] one policy snapshot per evaluation (caps and fingerprint never mixed)", test_evaluate_one_policy),
    ("nonetwork", "63 risk package has no network / live / credential path; production never imports risk", test_no_network_path),
    ("live", "64 LIVE remains refused", test_live_refused),
    ("isolation", "65 47 production fixtures + every existing fingerprint unchanged", test_production_isolation),
    ("execrestart", "66 existing execution restart tests unchanged", test_execution_restart_unchanged),
    ("execfees", "67 execution fee / fill reconciliation unchanged", test_execution_fee_reconciliation_unchanged),
    ("demo", "70 risk restart demo (separate processes)", test_risk_restart_demo),
    ("fingerprint", "71 risk architecture fingerprint (+ execution fingerprint verifies)", test_risk_fingerprint),
    ("docs", "72 docs/RISK_ARCHITECTURE.md, ROADMAP, R1-R35 results", test_docs),
    ("previous", "73 all previous stage suites", test_previous_stages),
]


if __name__ == "__main__":
    only = None
    if "--only" in sys.argv:
        only = set(sys.argv[sys.argv.index("--only") + 1].split(","))
    try:
        for key, name, fn in TESTS:
            if only is None or key in only:
                run(name, fn)
    finally:
        for d in _DIRS:
            shutil.rmtree(d, ignore_errors=True)
    if only is None:
        print("\nAll Stage 24 tests passed.")
    else:
        print(f"\nSelected Stage 24 tests passed: {','.join(sorted(only))}")
