#!/usr/bin/env python3
"""
Mutation tests for the Step-6.6 risk manager foundation (paper / shadow only, offline, no network).

    py scripts/mutation_test_risk.py [--out analysis_output/risk_mutation_results.json] [--only R1,R7]

Each mutation breaks ONE risk-safety rule (R1-R35 of the Step-6.6 brief, R36 an extra, R37-R47 of Step 6.6.1, R48-R55 of Step 6.6.2) in a TEMPORARY copy of the repository and
runs the relevant BEHAVIOURAL Stage-24 tests there; CAUGHT = those tests fail. The fingerprint test is never used to
catch a mutation. Control: the unmutated copy must PASS the same tests. The working tree is never modified.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IGNORE = shutil.ignore_patterns(".git", "__pycache__", "analysis_output", "market_data_sessions", "settlement_data",
                                ".venv", "venv*", "*.zip", ".mypy_cache", ".ruff_cache", "*.sqlite")
EVL = "risk/evaluate.py"
MGR = "risk/manager.py"
DEC = "risk/decision.py"
POL = "risk/policy.py"
STO = "risk/store.py"
XRK = "execution/risk.py"
# the injected R34 endpoint, assembled so that THIS file never contains the literal (stages 17 / 18 scan scripts/)
LIVE_PATH = "/".join(("", "portfolio", "orders"))
CAPLINE = "    q = min([req] + list(caps.values()))\n"


def _cap(code, expr):
    return (EVL, f'        "{code}": {expr},\n', f'        "{code}": req,\n', 1)


MUTATIONS = [
    ("R1", "NO_CALL can be approved (the NO_CALL veto is removed)",
     [(EVL, '    if c.signal_status == "NO_CALL":\n        veto("NO_CALL",', '    if False:\n        veto("NO_CALL",', 1)],
     ["nocall", "hardveto"]),
    ("R2", "risk can flip side (the approval carries the opposite side)",
     [(DEC, "approved_side=decision.side,", 'approved_side={"YES": "NO", "NO": "YES"}[decision.side],', 1)],
     ["side"]),
    ("R3", "risk can increase size (the largest of requested and caps is approved)",
     [(EVL, CAPLINE, "    q = max([req] + list(caps.values()))\n", 1)],
     ["size", "pertrade"]),
    ("R4", "risk can widen price (max instead of min against the policy entry price)",
     [(EVL, "        price_cap = min(price_cap, policy.max_entry_price)",
       "        price_cap = max(price_cap, policy.max_entry_price)", 1)],
     ["price"]),
    ("R5", "a hard veto is downgraded into a size reduction (sizing runs despite hard-veto codes)",
     [(EVL, "    if codes:\n        return _veto_result(codes, now_ms, triggers)\n\n    # 11-16",
       "    # 11-16", 1)],
     ["hardveto"]),
    ("R6", "feed FAIL ignored (feed health is skipped)",
     [(EVL, "    for f in HEALTH_FIELDS:\n        h, prefix", "    for f in HEALTH_FIELDS[1:]:\n        h, prefix", 1)],
     ["feedfail"]),
    ("R7", "UNKNOWN health treated as PASS",
     [(EVL, '        if h == "PASS" or (h == "DEGRADED"', '        if h in ("PASS", "UNKNOWN") or (h == "DEGRADED"', 1)],
     ["feedunknown"]),
    ("R8", "expired candidate approved",
     [(EVL, "    if now_ms >= c.expires_at:\n", "    if False:\n", 1)],
     ["expired"]),
    ("R9", "stale quote approved",
     [(EVL, "        if age is UNKNOWN or age > cap:\n",
       '        if code != "STALE_QUOTE" and (age is UNKNOWN or age > cap):\n', 1)],
     ["stalequote"]),
    ("R10", "stale snapshot approved",
     [(EVL, "    if now_ms - s.captured_at > policy.max_snapshot_age_ms:\n", "    if False:\n", 1)],
     ["stalesnap"]),
    ("R11", "non-positive EV approved (zero EV passes)",
     [(EVL, "    elif ev <= 0:\n", "    elif ev < 0:\n", 1)],
     ["ev"]),
    ("R12", "UNKNOWN required fee treated as zero",
     [(EVL, '            veto("FEE_UNKNOWN", "estimated_fee UNKNOWN (never treated as zero)")', "            pass", 1)],
     ["fee"]),
    ("R13", "UNKNOWN required slippage treated as zero",
     [(EVL, '            veto("SLIPPAGE_UNKNOWN", "estimated_slippage UNKNOWN (never treated as zero)")', "            pass",
       1)],
     ["slippage"]),
    ("R14", "worst-case loss uses the current price instead of the maximum authorised price",
     [(EVL, "    P = price_cap\n", "    P = c.current_executable_price\n", 1)],
     ["worstcase"]),
    ("R15", "per-trade cap ignored", [_cap("PER_TRADE_CONTRACT_LIMIT", "policy.max_contracts_per_trade")],
     ["pertrade"]),
    ("R16", "asset exposure cap ignored",
     [_cap("ASSET_OPEN_RISK_LIMIT", "by_risk(policy.max_asset_open_risk - s.asset_open_risk)")], ["asset"]),
    ("R17", "portfolio exposure cap ignored",
     [_cap("PORTFOLIO_OPEN_RISK_LIMIT", "by_risk(policy.max_portfolio_open_risk - s.total_open_risk)")],
     ["portfolio"]),
    ("R18", "crypto-group exposure cap ignored",
     [_cap("CRYPTO_GROUP_RISK_LIMIT", "by_risk(policy.max_crypto_group_open_risk - group_risk)")], ["group"]),
    ("R19", "same-direction exposure cap ignored",
     [_cap("SAME_DIRECTION_CRYPTO_LIMIT", "by_risk(policy.max_same_direction_crypto_risk - s.same_direction_crypto_risk)")],
     ["samedir"]),
    ("R20", "multiple caps use the largest instead of the smallest",
     [(EVL, CAPLINE, "    q = min(req, max(caps.values()))\n", 1)],
     ["mincaps"]),
    ("R21", "daily loss breaker not latched (the trigger is never persisted)",
     [(MGR, "        for t, reason in triggers:\n",
       '        for t, reason in [x for x in triggers if x[0] != "DAILY_REALIZED_LOSS"]:\n', 1)],
     ["latched"]),
    ("R22", "breaker lost on restart (latched only in process memory)",
     [(MGR, "        self.store, self.clock = store, clock\n",
       "        self.store, self.clock = store, clock\n        self._mem = {}\n", 1),
      (MGR, '            self.store.breaker_transition(t, "TRIGGERED", now_ms, reason, snap_hash, fp)\n'
            '            self.store.faults.hit("during_breaker_transition")\n'
            '            self.store.breaker_transition(t, "LATCHED", now_ms, reason, snap_hash, fp)\n',
       '            self._mem[t] = "LATCHED"\n', 1),
      (MGR, "breakers=tuple((t, s) for t, (s, _d) in sorted(self.store.breaker_states().items())),",
       "breakers=tuple((t, self._mem.get(t, s)) for t, (s, _d) in sorted(self.store.breaker_states().items())),", 1)],
     ["breakerrestart"]),
    ("R23", "drawdown breaker ignored",
     [(EVL, "        if peak - eq > policy.max_rolling_drawdown:\n", "        if False:\n", 1)],
     ["drawdown"]),
    ("R24", "consecutive-loss breaker ignored",
     [(EVL, "        if n >= policy.max_consecutive_losses:\n", "        if False:\n", 1)],
     ["consecutive"]),
    ("R25", "approval expiry ignored by execution",
     [(XRK, "    if now_ms >= approval.expires_at:\n", "    if False:\n", 1)],
     ["expiry"]),
    ("R26", "approval reusable for a different candidate",
     [(XRK, "    if approval.candidate_id is not None and approval.candidate_id != intent.candidate_id:\n",
       "    if False:\n", 1)],
     ["bindcand"]),
    ("R27", "approval reusable for the opposite side",
     [(XRK, "    if (approval.market_ticker, approval.asset, approval.side) != (intent.market_ticker, intent.asset, "
            "intent.side):\n",
       "    if (approval.market_ticker, approval.asset) != (intent.market_ticker, intent.asset):\n", 1)],
     ["bindside"]),
    ("R28", "approval reusable for a larger size",
     [(XRK, "    if intent.requested_contracts > approval.max_contracts:\n", "    if False:\n", 1)],
     ["bindsize"]),
    ("R29", "approval reusable for a worse price",
     [(XRK, "    if intent.max_limit_price > approval.max_limit_price:\n", "    if False:\n", 1)],
     ["bindprice"]),
    ("R30", "risk decision id non-deterministic",
     [(DEC, '    return "rd-" + hashlib.sha256(canonical_json(body).encode("ascii")).hexdigest()[:40]',
       '    return "rd-" + hashlib.sha256((canonical_json(body) + __import__("uuid").uuid4().hex).encode("ascii"))'
       '.hexdigest()[:40]', 1)],
     ["decisionid"]),
    ("R31", "a policy change does not change the fingerprint (values excluded)",
     [(POL, '            "policy": policy.to_dict()}',
       '            "policy": {"policy_id": policy.policy_id, "policy_version": policy.policy_version}}', 1)],
     ["policyfp"]),
    ("R32", "risk journal write non-atomic (no BEGIN / COMMIT / ROLLBACK)",
     [(STO, '        self.conn.execute("BEGIN IMMEDIATE")\n', "        pass\n", 1),
      (STO, '            self.conn.execute("COMMIT")\n', "            pass\n", 1),
      (STO, '            self.conn.execute("ROLLBACK")\n', "            pass\n", 1)],
     ["rollback"]),
    ("R33", "execution UNKNOWN / HALTED does not veto",
     [(EVL, "        if st in EXEC_UNRESOLVED:\n", "        if False:\n", 1)],
     ["unresolved"]),
    ("R34", "the risk package introduces a network / live API path",
     [(MGR, "class StoreApprovalBook:",
       "import urllib.request  # noqa: E402\n\n\ndef _live_order(body):\n"
       f'    return urllib.request.urlopen(urllib.request.Request("https://trading-api.example.invalid/trade-api/v2'
       f'{LIVE_PATH}", data=body, method="POST"))\n\n\nclass StoreApprovalBook:', 1)],
     ["nonetwork"]),
    ("R35", "a risk approval can manufacture missing signal provenance",
     [(EVL, "        if not _is_hex(getattr(c, n)):\n", "        if False:\n", 1),
      (DEC, "signal_fingerprint=candidate.signal_fingerprint,",
       'signal_fingerprint=candidate.signal_fingerprint if isinstance(candidate.signal_fingerprint, str) '
       'and candidate.signal_fingerprint else "0" * 32,', 1)],
     ["provenance"]),
    ("R36", "(extra) the read-only verify() ignores a breaker latched after issuance",
     [(MGR, "    def verify(self, intent, now_ms):\n        return self._authorize(intent, now_ms)\n",
       "    def verify(self, intent, now_ms):\n        return check_intent_against_approval(intent, self.get("
       "intent.risk_decision_id), now_ms, self.required_policy_fingerprint)\n", 1)],
     ["breakerapproval"]),
    ("R37", "NO_CALL causes breaker observation to be skipped",
     [(MGR, "            observed = self._observe_breakers(snapshot, now, snap_hash, pol, fp)   # A. observation",
       '            observed = self._observe_breakers(snapshot, now, snap_hash, pol, fp) if candidate.signal_status '
       '== "CALL" else []  # A. observation', 1)],
     ["nocallbreaker"]),
    ("R38", "a same-day operator reset clears DAILY_REALIZED_LOSS",
     [(MGR, '        if RESET_RULE.get(breaker_type) != "OPERATOR" or not reason_ok:',
       '        if (RESET_RULE.get(breaker_type) != "OPERATOR" and breaker_type != "DAILY_REALIZED_LOSS") or not reason_ok:', 1)],
     ["resetrealized"]),
    ("R39", "a same-day operator reset clears DAILY_TOTAL_LOSS",
     [(MGR, '        if RESET_RULE.get(breaker_type) != "OPERATOR" or not reason_ok:',
       '        if (RESET_RULE.get(breaker_type) != "OPERATOR" and breaker_type != "DAILY_TOTAL_LOSS") or not reason_ok:', 1)],
     ["resettotal"]),
    ("R40", "a same-day operator reset clears ROLLING_DRAWDOWN",
     [(MGR, '        if RESET_RULE.get(breaker_type) != "OPERATOR" or not reason_ok:',
       '        if (RESET_RULE.get(breaker_type) != "OPERATOR" and breaker_type != "ROLLING_DRAWDOWN") or not reason_ok:', 1)],
     ["resetdrawdown"]),
    ("R41", "the final consume ignores a newly latched breaker (both the authorise check and the final re-check)",
     [(MGR, "        if latched:                                  # a breaker latched AFTER issuance still stops the "
            "entry\n", "        if False:\n", 1),
      (MGR, '            if any(s != "CLEAR" for s, _d in st.breaker_states().values()):\n'
            '                return False, "RISK_BREAKER_LATCHED"\n', "", 1)],
     ["racebreaker"]),
    ("R42", "the final consume ignores approval expiration (a stale time is used at both checks)",
     [(MGR, "            now = max(now_ms, self.clock.now_ms())          # the CURRENT time",
       "            now = intent.created_at                          # the CURRENT time", 1),
      (MGR, "            authorized_at = max(now, self.clock.now_ms())          # FINAL time read",
       "            authorized_at = now                                    # FINAL time read", 1)],
     ["raceexpiry"]),
    ("R43", "the final consume ignores approval supersession",
     [(MGR, "        if intent.risk_decision_id and self.store.superseded(intent.risk_decision_id):\n",
       "        if False:\n", 1)],
     ["racesupersede"]),
    ("R44", "the final consume ignores a current policy mismatch (both the authorise check and the final re-check)",
     [(MGR, "                                             self.required_policy_fingerprint)\n",
       "                                             None)\n", 1),
      (MGR, '            if st.active_policy_fingerprint() != appr.risk_policy_fingerprint:\n'
            '                return False, "RISK_POLICY_CHANGED"\n', "", 1)],
     ["racepolicy", "policyrace"]),
    ("R45", "the approval is consumed during the preliminary PRECHECK instead of the final READY authorisation",
     [("execution/engine.py", "        return None, None                            # read-only: the approval is NOT "
                              "consumed here",
       "        ok, reason = self.risk_book.verify_and_consume(intent, key, now)\n"
       "        return (None, None) if ok else (S.REJECTED, reason)", 1)],
     ["precheckconsume"]),
    ("R46", "final verify and consume are non-atomic: consume skips the revalidation",
     [(MGR, "            ok, reason = self._authorize(intent, now)\n", '            ok, reason = True, "unchecked"\n', 1)],
     ["racebreaker", "raceexpiry", "racesupersede", "racepolicy"]),
    ("R47", "after a crash past consumption another logical intent can use the approval",
     [(MGR, "                return False, f\"RISK_APPROVAL_ALREADY_CONSUMED by {prior['intent_id']}\"\n",
       '                return True, "RISK_APPROVAL_REPLAY"\n', 1)],
     ["crashafterconsume"]),
    ("R48", "policy can change after the final authorisation check and the old approval is still consumed",
     [(MGR, '            if st.active_policy_fingerprint() != appr.risk_policy_fingerprint:\n'
            '                return False, "RISK_POLICY_CHANGED"\n', "", 1)],
     ["policyinside"]),
    ("R49", "the final consume uses a stale time and permits an approval that expired inside the critical section",
     [(MGR, "            authorized_at = max(now, self.clock.now_ms())          # FINAL time read",
       "            authorized_at = now                                    # FINAL time read", 1)],
     ["expiryinside"]),
    ("R50", "the CONSECUTIVE_LOSS operator reset clears the breaker but not the effective streak",
     [(MGR, "                self.store.append_loss_streak_reset(now, operator_reason, fp)\n", "                pass\n", 1)],
     ["streakreset"]),
    ("R51", "the loss-streak reset is not restart persistent (kept in process memory only)",
     [(MGR, "                self.store.append_loss_streak_reset(now, operator_reason, fp)\n",
       "                self._mem_streak_reset = True\n", 1),
      (MGR, "        n, unresolved = self.store.loss_streak()\n        epoch = self.store.loss_streak_epoch()\n",
       '        n, unresolved = (0, 0) if getattr(self, "_mem_streak_reset", False) else self.store.loss_streak()\n'
       '        epoch = self.store.loss_streak_epoch() + (1 if getattr(self, "_mem_streak_reset", False) else 0)\n', 1)],
     ["streakreset"]),
    ("R52", "the loss-streak reset and the breaker clear are non-atomic (two transactions)",
     [(MGR, '                self.store.faults.hit("after_streak_reset_before_breaker_clear")\n',
       '                self.store.conn.execute("COMMIT"); self.store.conn.execute("BEGIN IMMEDIATE")\n'
       '                self.store.faults.hit("after_streak_reset_before_breaker_clear")\n', 1)],
     ["streakatomic"]),
    ("R53", "a pre-reset snapshot consecutive-loss count immediately defeats the operator reset",
     [(EVL, "        n = max(s.consecutive_losses, state.consecutive_losses) if se == state.loss_streak_epoch \\\n"
            "            else state.consecutive_losses\n",
       "        n = max(s.consecutive_losses, state.consecutive_losses)\n", 1)],
     ["streakepoch"]),
    ("R54", "a RiskDecision evaluated under policy A can be persisted with the policy-B fingerprint",
     [(MGR, "                             risk_policy_fingerprint=fp, reason_codes=ev.reason_codes,",
       "                             risk_policy_fingerprint=self.policy_fingerprint, reason_codes=ev.reason_codes,", 1)],
     ["onepolicy"]),
    ("R55", "an active-policy change is not durable (silently lost on restart)",
     [(MGR, "                self.store.append_policy_activation(self.clock.now_ms(), policy.to_dict(), fp, reason)\n",
       "                self._pending_policy = (policy, fp)\n", 1)],
     ["policydurable"]),
]


def apply(root, edits):
    for rel, old, new, count in edits:
        p = os.path.join(root, rel)
        s = open(p, encoding="utf-8").read()
        n = s.count(old)
        if n != count:
            raise RuntimeError(f"mutation anchor not found as expected in {rel}: {old[:60]!r} ({n} != {count})")
        open(p, "w", encoding="utf-8", newline="\n").write(s.replace(old, new))


def run_variant(edits, tests, timeout=1800):
    tmp = tempfile.mkdtemp(prefix="mut-risk-")
    root = os.path.join(tmp, "repo")
    try:
        shutil.copytree(REPO, root, ignore=IGNORE)
        apply(root, edits)
        t0 = time.time()
        p = subprocess.run([sys.executable, "test_stage24.py", "--only", ",".join(tests)], cwd=root, capture_output=True,
                           text=True, timeout=timeout, env=dict(os.environ, KALSHI_MASTER_TEST_RUN="1"))
        fails = [ln for ln in p.stdout.splitlines() if ln.startswith("FAIL")]
        return {"caught": p.returncode != 0, "returncode": p.returncode, "seconds": round(time.time() - t0, 1),
                "failed_test": fails[0][:300] if fails else None,
                "passed_tests": [ln[6:70] for ln in p.stdout.splitlines() if ln.startswith("PASS")]}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(REPO, "analysis_output", "risk_mutation_results.json"))
    ap.add_argument("--only", default=None)
    a = ap.parse_args(argv)
    only = set(a.only.split(",")) if a.only else None
    res = {"note": "Each mutation breaks one Step-6.6 risk-safety rule in a temporary copy; CAUGHT = the behavioural "
                   "Stage-24 tests fail (never the fingerprint test).", "controls": {}, "mutations": []}
    all_tests = sorted({t for m in MUTATIONS for t in m[3]})
    res["controls"]["unmutated"] = run_variant([], all_tests)
    for mid, desc, edits, tests in MUTATIONS:
        if only and mid not in only:
            continue
        r = {"id": mid, "mutation": desc, "tests": tests, "as_is": run_variant(edits, tests)}
        res["mutations"].append(r)
        print(f"{mid} {desc}\n   {'CAUGHT' if r['as_is']['caught'] else 'NOT CAUGHT'}  ({r['as_is']['failed_test']})",
              flush=True)
    c = res["controls"]
    ok = not c["unmutated"]["caught"] and all(m["as_is"]["caught"] for m in res["mutations"])
    res["all_caught_and_controls_pass"] = ok
    print(f"control: unmutated {'PASS' if not c['unmutated']['caught'] else 'FAIL ' + str(c['unmutated']['failed_test'])}")
    print("RESULT: " + ("every mutation caught; control passes" if ok else "NOT every mutation caught / the control failed"))
    if a.out:
        os.makedirs(os.path.dirname(a.out), exist_ok=True)
        with open(a.out, "w", encoding="utf-8", newline="\n") as f:
            json.dump(res, f, indent=1)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
