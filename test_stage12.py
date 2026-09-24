#!/usr/bin/env python3
"""Stage 12 tests: Step 6 production hardening, manual promotion, fail-safe live veto.
No network. Run:  py test_stage12.py"""
import ast
import csv
import datetime as dt
import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import types

import analyze_perp_integration as ai
import build_perp_integration_experiment as bx
import check_perp_deployment as cd
import kalshi_dashboard as k
import perp_live as pl
import perp_probability as pp
import perp_shadow as ps
import perp_telemetry as pt
import promote_perp_integration as promote
import step5_fixture as fx
import strategy_fingerprint as sf
import test_stage7 as t7
import test_stage8 as t8
import test_stage10 as t10
import test_stage11 as t11

HERE = os.path.dirname(os.path.abspath(__file__))
DASH = os.path.join(HERE, "kalshi_dashboard.py")
BASELINE = os.path.join(HERE, "step5_baseline_manifest.json")
K = {"MIN_CONF": 80.0, "MIN_PRICE": 85.0, "EDGE_THRESH": 0.0, "ENTRY_COST_CENTS": 2.0}
FIX = {}
# Trusted V2 legacy strategy fingerprint, generated from the EXACT Step 5 kalshi_dashboard.py
# (sha256 6ff7e622...670c). It depends only on strategy semantics, not on the Python version.
TRUSTED_STEP5_V2_FINGERPRINT = "8d94f241e8fc8edadc76058e1f12f430b6e4f499f4c0a30fba1cb5cf07dad82a"


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


def man():
    with open(BASELINE) as f:
        return json.load(f)


# ═══════════════════ 1-7 baseline manifest / fingerprint ═══════════════════
def test_baseline_manifest():
    m = man()
    assert m["manifest_schema_version"] == 1
    assert m["step5_dashboard_sha256"] == "6ff7e6225f55aa4c706a57c0ad45c0ce564c57fcaa6a120252ace04a6c31670c"   # 1
    assert m["fingerprint_format_version"] == 2 and m["fingerprint_algorithm"] == "canonical_ast_v2_sha256"
    assert m["legacy_strategy_fingerprint"] == TRUSTED_STEP5_V2_FINGERPRINT                                    # exact Step 5
    assert sf.current_fingerprint(DASH)[0] == TRUSTED_STEP5_V2_FINGERPRINT                                      # Step 6 == Step 5
    for old in ({k: v for k, v in m.items() if k not in ("fingerprint_format_version", "fingerprint_algorithm")},
                dict(m, fingerprint_format_version=1), dict(m, fingerprint_algorithm="ast_dump_sha256")):
        ok, why = sf.verify(DASH, old)
        assert not ok and why[0].startswith("UNSUPPORTED_FINGERPRINT_FORMAT"), why                             # v1 refused
    assert len(m["strategy_config_values"]) == 14 and len(m["strategy_function_hashes"]) == 10
    a = sf.build_manifest(DASH)["legacy_strategy_fingerprint"]
    b = sf.build_manifest(DASH)["legacy_strategy_fingerprint"]
    assert a == b and len(a) == 64                                                                  # 2
    assert a == m["legacy_strategy_fingerprint"]                                                    # 7


def _variant(edit):
    src = open(DASH).read()
    old, new = edit
    assert src.count(old) >= 1, old[:40]
    p = os.path.join(tempfile.mkdtemp(), "d.py")
    open(p, "w").write(src.replace(old, new, 1))
    return p


def test_fingerprint_sensitivity():
    m = man()
    same = [("    conf = max(p_up, 100 - p_up)", "\n    # a fresh comment\n\n    conf = max(p_up, 100 - p_up)\n"),  # 3
            ("Kalshi 15m watcher started", "Kalshi 15m watcher has started"),                                      # 6
            ("<div class=\"wrap\">", "<div class=\"wrap\" data-x=\"1\">")]
    for e in same:
        ok, why = sf.verify(_variant(e), m)
        assert ok, (e[0][:30], why)
    diff = [("MIN_CONF      = 80.0", "MIN_CONF      = 79.0"),                                                      # 4
            ("    conf = max(p_up, 100 - p_up)", "    conf = max(p_up, 100 - p_up) * 1.01"),                       # 5
            ("EDGE_THRESH   = 0.0", "EDGE_THRESH   = 0.5"),
            ("def _contracts(r):", "def _contracts(r):\n    pass")]
    for e in diff:
        ok, why = sf.verify(_variant(e), m)
        assert not ok and why, e[0][:30]
    for bad in (("MIN_CONF      = 80.0", "MIN_CONF      = float('80')"),
                ("MIN_CONF      = 80.0", "MIN_CONF      = 80.0\nMIN_CONF = 81.0")):
        try:
            sf.extract(_variant(bad)); raise AssertionError("accepted unverifiable constant")
        except sf.FingerprintError:
            pass
    assert sf.MUTABLE_CONSTANTS == ("BANKROLL", "ENTRY_MODE")


def _fn(src):
    return ast.parse(src).body[0]


def _strip_empty_fields(node):
    """Simulate Python 3.13's view: every None/[] field simply absent (ast.dump show_empty=False)."""
    for n in ast.walk(node):
        for f in list(n._fields):
            v = getattr(n, f, None)
            if v is None or (isinstance(v, list) and not v):
                try:
                    delattr(n, f)
                except AttributeError:
                    pass
    return node


def _schema_sensitive_repr(value):
    """TEST-ONLY model of the defective v1 property, independent of the running interpreter's
    ast.dump() defaults: a field that is MISSING is recorded differently from a field that is
    PRESENT but empty. (v1 hashed interpreter-specific dump text, which had exactly this flaw
    between Python versions.)"""
    if isinstance(value, ast.AST):
        fields = {}
        for field in value._fields:
            fields[field] = _schema_sensitive_repr(getattr(value, field)) if hasattr(value, field) \
                else {"__missing__": True}
        return {"_type": type(value).__name__, "_fields": fields}
    if isinstance(value, list):
        return [_schema_sensitive_repr(v) for v in value]
    return value if isinstance(value, (str, int, float, bool)) or value is None else repr(value)


def test_fingerprint_ignores_empty_version_specific_ast_fields():
    """Regression for the Python 3.13 portability defect. FAILS with the old ast.dump()-based v1."""
    src = "def f(x, y=2):\n    if x > y:\n        return x * 1.5\n    return None\n"
    a, b = _fn(src), _fn(src)
    if hasattr(a, "type_params"):
        del a.type_params                              # pre-3.12 FunctionDef: no type_params field at all
    b.type_params = []                                 # 3.12+/3.13 FunctionDef: type_params=[]
    assert sf.canonicalize_ast(a) == sf.canonicalize_ast(b)
    assert sf.function_hash(a) == sf.function_hash(b)
    # Step 1 baseline note: the next two checks need FunctionDef.type_params to be an AST FIELD, which
    # it is only on Python >= 3.12 (PEP 695). On 3.10/3.11 the attribute set above is not a field, so
    # neither the schema-sensitive view nor canonicalize_ast can see it, and PEP 695 syntax cannot be
    # parsed at all there. They run unchanged on 3.12+; on older interpreters they are reported as SKIP.
    if "type_params" in ast.FunctionDef._fields:
        # the legacy/schema-sensitive view distinguishes them (the v1 flaw) — on EVERY interpreter
        assert sf.canonical_json(_schema_sensitive_repr(a)) != sf.canonical_json(_schema_sensitive_repr(b))
        # non-empty type_params is real syntax -> must NOT be hidden
        c = _fn(src)
        c.type_params = [ast.Name(id="T", ctx=ast.Load())]
        assert sf.canonicalize_ast(c) != sf.canonicalize_ast(b) and sf.function_hash(c) != sf.function_hash(b)
    else:
        print("SKIP  6.1a type_params checks (FunctionDef has no type_params field before Python 3.12)")
    # optional field: missing == None, but a real value differs
    d, e, g = _fn(src), _fn(src), _fn(src)
    del d.returns
    e.returns = None
    g.returns = ast.Name(id="float", ctx=ast.Load())
    assert sf.canonicalize_ast(d) == sf.canonicalize_ast(e) != sf.canonicalize_ast(g)
    # Python 3.13 simulation on the REAL strategy functions: identical v2 hashes, different v1 text
    tree_full = ast.parse(open(DASH).read())
    tree_313 = ast.parse(open(DASH).read())
    fn_full = {n.name: n for n in ast.walk(tree_full) if isinstance(n, ast.FunctionDef) and n.name in sf.STRATEGY_FUNCTIONS}
    fn_313 = {n.name: n for n in ast.walk(tree_313) if isinstance(n, ast.FunctionDef) and n.name in sf.STRATEGY_FUNCTIONS}
    for name in sf.STRATEGY_FUNCTIONS:
        before = sf.function_hash(fn_full[name])
        _strip_empty_fields(fn_313[name])
        assert sf.function_hash(fn_313[name]) == before, name     # V2 unchanged by empty-field deletion
    # canonical output uses only JSON primitives and is deterministic
    cj = sf.canonical_json(sf.canonicalize_ast(fn_full["evaluate"]))
    assert json.loads(cj) == sf.canonicalize_ast(fn_full["evaluate"]) and cj == sf.canonical_json(json.loads(cj))
    for field in ("lineno", "col_offset", "end_lineno", "end_col_offset"):
        assert f'"{field}"' not in cj


def _ast_variant(mutator):
    tree = ast.parse(open(DASH).read())
    mutator(tree)
    p = os.path.join(tempfile.mkdtemp(), "d.py")
    open(p, "w").write(ast.unparse(tree))
    return p


def _fn_in(tree, name):
    return next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)


def test_fingerprint_semantic_and_nonsemantic():
    m = man()
    assert sf.verify(_ast_variant(lambda t: None), m)[0]                  # full reformat (unparse): still same
    def gt_to_gte(t):
        cmp_ = next(c for c in ast.walk(_fn_in(t, "evaluate")) if isinstance(c, ast.Compare) and isinstance(c.ops[0], ast.Gt))
        cmp_.ops[0] = ast.GtE()
    def bump_const(name):
        def f(t):
            c = next(x for x in ast.walk(_fn_in(t, name)) if isinstance(x, ast.Constant)
                     and isinstance(x.value, (int, float)) and not isinstance(x.value, bool))
            c.value = c.value + 1
        return f
    for label, mut in (("evaluate > -> >=", gt_to_gte), ("conf_side constant", bump_const("conf_side")),
                       ("_size_fraction constant", bump_const("_size_fraction")),
                       ("crossings constant", bump_const("crossings"))):
        ok, why = sf.verify(_ast_variant(mut), m)
        assert not ok and any("strategy function changed" in w for w in why), label
    ok, why = sf.verify(_variant(("MIN_CONF      = 80.0", "MIN_CONF      = 81")), m)
    assert not ok and any("MIN_CONF" in w for w in why)
    # non-semantic: relocate a strategy function to the end of the file + add an unrelated helper
    src = open(DASH).read()
    tree = ast.parse(src)
    fn = _fn_in(tree, "norm_cdf")
    lines = src.splitlines(keepends=True)
    block = "".join(lines[fn.lineno - 1:fn.end_lineno])
    moved = "".join(lines[:fn.lineno - 1] + lines[fn.end_lineno:]) + "\n\n" + block + \
        "\n\ndef brand_new_unrelated_helper(x):\n    return str(x) + '!'\n"
    p = os.path.join(tempfile.mkdtemp(), "d.py"); open(p, "w").write(moved)
    ok, why = sf.verify(p, m)
    assert ok, why


def test_deployment_check_drift():
    d = tempfile.mkdtemp()
    s = cd.status(os.path.join(d, "none.json"), os.path.join(d, "p.json"), BASELINE, DASH,
                  os.path.join(d, "c.json"), env_enabled=False)
    assert s["strategy_fingerprint_valid"] is True and s["final_status"] == "INACTIVE"
    assert not any("strategy drift" in x for x in s["problems"]), s["problems"]            # no FALSE drift
    assert s["runtime_python"] and s["baseline_fingerprint_format"] == 2
    drift = _variant(("MIN_CONF      = 80.0", "MIN_CONF      = 81"))
    s2 = cd.status(os.path.join(d, "none.json"), os.path.join(d, "p.json"), BASELINE, drift,
                   os.path.join(d, "c.json"), env_enabled=False)
    assert s2["strategy_fingerprint_valid"] is False and any("strategy drift" in x for x in s2["problems"])  # TRUE drift
    f = chain()
    the_promotion()
    s3 = cd.status(f["promo"], f["reg"], BASELINE, drift, f["candidate"], env_enabled=True)
    assert s3["final_status"] == "REFUSED"
    v1 = os.path.join(d, "v1.json")
    json.dump(json.load(open(BASELINE)) | {"fingerprint_format_version": 1}, open(v1, "w"))
    s4 = cd.status(os.path.join(d, "none.json"), os.path.join(d, "p.json"), v1, DASH, os.path.join(d, "c.json"),
                   env_enabled=False)
    assert s4["strategy_fingerprint_valid"] is False and any("UNSUPPORTED_FINGERPRINT_FORMAT" in x for x in s4["problems"])
    g = pl.LiveVetoGate.from_files(f["promo"], f["reg"], v1, DASH, os.path.join(d, "v.csv"), env_enabled=True,
                                   allow_synthetic=True)
    assert g.active is False and any("UNSUPPORTED_FINGERPRINT_FORMAT" in x for x in g.problems)
    p = json.load(open(f["promo"]))
    old = {kk: v for kk, v in p.items() if kk != "fingerprint_format_version"}
    old["promotion_hash"] = pl.compute_promotion_hash(old)
    assert pl.validate_promotion(old, 2.0e9, allow_synthetic=True)                          # v1 promotion refused
    old2 = dict(p, fingerprint_format_version=1); old2["promotion_hash"] = pl.compute_promotion_hash(old2)
    assert any("UNSUPPORTED_FINGERPRINT_FORMAT" in e for e in pl.validate_promotion(old2, 2.0e9, allow_synthetic=True))


# ═══════════════════ synthetic promotion chain (tests only) ═══════════════════
def chain():
    """Step 4 policy -> Step 4 VALIDATED report -> Step 5 experiment -> Step 5 VALIDATED candidate."""
    if FIX:
        return FIX
    c = t11.chain()
    t11.build(c, allow_synthetic=True)
    out = os.path.join(c["dir"], "out5")
    rep = ai.run(c["exp_path"], c["journal"], c["labels"], c["calls"], c["reg"], c["report"], out,
                 allow_synthetic=True, gates=t11.G, reps=300, log=None)
    assert rep["experiments"][0]["status"] == ai.S_VALIDATED, rep["experiments"][0]["status_reasons"]
    cand_path = os.path.join(out, "perp_integration_candidate.json")
    doc = json.load(open(cand_path))
    doc["candidates"][0]["dashboard_sha256"] = man()["step5_dashboard_sha256"]    # synthetic exp used this dashboard
    json.dump(doc, open(cand_path, "w"), indent=1, sort_keys=True)
    FIX.update(c, candidate=cand_path, promo=os.path.join(c["dir"], "promotion.json"), out5=out)
    return FIX


def promote_ok(**kw):
    f = chain()
    out = kw.pop("output", f["promo"])
    return promote.build(f["candidate"], f["exp_path"], f["reg"], f["report"], BASELINE, DASH, out,
                         confirm=kw.pop("confirm", pl.CONFIRM_PHRASE), allow_synthetic=kw.pop("allow_synthetic", True),
                         log=None, **kw)


def the_promotion():
    f = chain()
    if not os.path.exists(f["promo"]):
        promote_ok()
    return json.load(open(f["promo"]))


# ═══════════════════ 8-20 promotion ═══════════════════
def test_no_candidate():
    d = tempfile.mkdtemp()
    lines = []
    s = promote.build(os.path.join(d, "none.json"), os.path.join(d, "e.json"), os.path.join(d, "p.json"),
                      os.path.join(d, "r.json"), BASELINE, DASH, os.path.join(d, "promo.json"),
                      confirm=pl.CONFIRM_PHRASE, log=lines.append)
    assert s["promotions_created"] == 0 and not os.path.exists(os.path.join(d, "promo.json"))       # 8
    txt = "\n".join(lines)
    assert "Eligible real Step 5 candidates: 0" in txt and "Live veto remains unavailable." in txt
    p = subprocess.run([sys.executable, "promote_perp_integration.py", "--candidate", os.path.join(d, "none.json"),
                        "--output", os.path.join(d, "p2.json")], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "Promotions created: 0" in p.stdout


def _cand_variant(mutate):
    f = chain()
    doc = json.load(open(f["candidate"]))
    mutate(doc["candidates"][0])
    p = os.path.join(tempfile.mkdtemp(), "cand.json")
    json.dump(doc, open(p, "w"))
    return p


def test_promotion_refusals():
    f = chain()
    out = os.path.join(f["dir"], "never.json")
    cases = {
        "not validated": _cand_variant(lambda c: c.update(status="PROBABILITY_OVERLAY_FAILED")),            # 9
        "activation flag flipped": _cand_variant(lambda c: c.update(activation_allowed=True)),
        "policy hash mismatch": _cand_variant(lambda c: c.update(source_policy_hash="0" * 64)),             # 14
        "dashboard baseline mismatch": _cand_variant(lambda c: c.update(dashboard_sha256="0" * 64)),        # 15
    }
    for name, cp in cases.items():
        try:
            promote.build(cp, f["exp_path"], f["reg"], f["report"], BASELINE, DASH, out,
                          confirm=pl.CONFIRM_PHRASE, allow_synthetic=True, log=None)
            if name in ("not validated", "activation flag flipped"):
                assert not os.path.exists(out), name          # no eligible candidate -> nothing written
            else:
                raise AssertionError(f"{name} accepted")
        except promote.PromotionRefused:
            pass
        assert not os.path.exists(out), name
    for confirm in (None, "", "live_veto_only", "LIVE VETO ONLY", "YES"):                                  # 11, 12
        try:
            promote_ok(confirm=confirm, output=out); raise AssertionError(f"confirm {confirm!r} accepted")
        except promote.PromotionRefused as e:
            assert "confirmation phrase" in str(e)
    try:
        promote_ok(allow_synthetic=False, output=out); raise AssertionError("synthetic promoted")           # 10
    except promote.PromotionRefused as e:
        assert "synthetic" in str(e)
    for days in (0, -1, 31, 90):                                                                            # 20
        try:
            promote_ok(expiry_days=days, output=out); raise AssertionError(f"{days} days accepted")
        except promote.PromotionRefused:
            pass
    # 16: strategy drift refuses promotion
    drift = _variant(("MIN_CONF      = 80.0", "MIN_CONF      = 79.0"))
    try:
        promote.build(f["candidate"], f["exp_path"], f["reg"], f["report"], BASELINE, drift, out,
                      confirm=pl.CONFIRM_PHRASE, allow_synthetic=True, log=None)
        raise AssertionError("strategy drift promoted")
    except promote.PromotionRefused as e:
        assert "strategy drift" in str(e)
    assert not os.path.exists(out)


def test_promotion_success_and_identity():
    f = chain()
    s = promote_ok(now=2.0e9)                                                                               # 13
    p = json.load(open(f["promo"]))
    assert s["promotions_created"] == 1 and p["mode"] == "LIVE_VETO_ONLY" and p["synthetic"] is True
    assert p["scope"] == pl.REQUIRED_SCOPE and p["selected_alpha"] in pp.ALPHA_GRID
    assert p["legacy_strategy_fingerprint"] == man()["legacy_strategy_fingerprint"]
    assert p["step5_baseline_dashboard_sha256"] == man()["step5_dashboard_sha256"]
    assert not any(w in kk.lower() for kk in p for w in ("enable", "force", "bypass", "activate"))
    p2 = os.path.join(f["dir"], "p2.json")
    promote_ok(now=2.0e9, output=p2)
    assert json.load(open(p2))["promotion_id"] == p["promotion_id"]                                          # 17
    p3 = os.path.join(f["dir"], "p3.json")
    promote_ok(now=2.0e9, expiry_days=7, output=p3)
    assert json.load(open(p3))["promotion_id"] != p["promotion_id"]
    assert pl.compute_promotion_hash(p) == p["promotion_hash"]                                               # 18
    for edit in ({"conflict_threshold": -0.5}, {"selected_alpha": 1.0}, {"synthetic": False}):
        bad = dict(p, **edit)
        assert pl.compute_promotion_hash(bad) != p["promotion_hash"]
        assert any("hash" in e for e in pl.validate_promotion(bad, 2.0e9, allow_synthetic=True))
    now = pl._parse_iso(p["expires_at_utc"])
    assert "promotion expired" in pl.validate_promotion(p, now + 1, allow_synthetic=True)                    # 19
    assert pl.validate_promotion(p, now - 10, allow_synthetic=True) == []


# ═══════════════════ 21-26 activation ═══════════════════
def gate(env=True, promo=None, **kw):
    f = chain()
    the_promotion()
    d = kw.pop("dir", None) or tempfile.mkdtemp(dir=f["dir"])
    return pl.LiveVetoGate.from_files(promo or f["promo"], f["reg"], BASELINE, DASH,
                                      os.path.join(d, "veto.csv"), env_enabled=env,
                                      kill_file=kw.pop("kill_file", os.path.join(d, "KILL")),
                                      allow_synthetic=kw.pop("allow_synthetic", True), **kw)


def test_activation_matrix():
    f = chain()
    assert gate(env=False).active is False                                                                   # 21
    assert gate(env=False).status == "READY_BUT_DISABLED"
    d = tempfile.mkdtemp()
    g = pl.LiveVetoGate.from_files(os.path.join(d, "none.json"), f["reg"], BASELINE, DASH,
                                   os.path.join(d, "v.csv"), env_enabled=True)
    assert g.active is False and g.status == "INACTIVE_NO_PROMOTION"                                         # 22
    bad = os.path.join(d, "bad.json")
    open(bad, "w").write("{not json")
    assert pl.LiveVetoGate.from_files(bad, f["reg"], BASELINE, DASH, os.path.join(d, "v2.csv"),
                                      env_enabled=True).active is False                                      # 23
    p = json.load(open(f["promo"]))
    tampered = dict(p, conflict_threshold=-0.9)
    tp = os.path.join(d, "tampered.json"); json.dump(tampered, open(tp, "w"))
    gt = gate(env=True, promo=tp)
    assert gt.active is False and gt.status == "REFUSED" and any("hash" in x for x in gt.problems)
    assert gate(env=True).active is True                                                                     # 24
    assert gate(env=True, allow_synthetic=False).active is False                                             # 25
    expired = dict(p, created_at_utc="2020-01-01T00:00:00+00:00", expires_at_utc="2020-01-10T00:00:00+00:00")
    expired["promotion_hash"] = pl.compute_promotion_hash(expired)
    ep = os.path.join(d, "expired.json"); json.dump(expired, open(ep, "w"))
    assert gate(env=True, promo=ep).active is False                                                          # 26
    # runtime strategy settings drifting from the validated baseline refuse activation (spec 60)
    dr = pl.LiveVetoGate.from_files(f["promo"], f["reg"], BASELINE, DASH,
                                    os.path.join(d, "v3.csv"), env_enabled=True, allow_synthetic=True,
                                    runtime_config={"BANKROLL": 999999, "ENTRY_MODE": "maker"})
    assert dr.active is False and any("BANKROLL" in x for x in dr.problems)


# ═══════════════════ 27-35 preview ═══════════════════
def _tel_with_history(minutes=6):
    tel = t8._sampler_tel(prefill=t8.MODES["normal"])
    now = time.time()
    for c in k.COINS:
        for back in range(minutes * 60, 0, -4):
            tel.spot_long[c].append(now - back, 100.3)
    return tel, now


def _r(now, **kw):
    r = {"status": "ok", "spot_raw": 100.4, "ticker": "TKP", "fav": "UP", "p_up": 93.0, "conf": 93.0,
         "side_ask": 88.0, "raw_edge": 5.0, "net_edge": 3.0, "signal": True, "spot_observed_ts": now,
         "close": "2026-09-01T12:06:00Z", "remain": 6.0, "rec_stop": 66}
    r.update(kw)
    return r


def test_preview_is_read_only():
    tel, now = _tel_with_history()
    d = tempfile.mkdtemp()
    tel.logger = pt.TelemetryCSVLogger(os.path.join(d, "t.csv"))
    calls = []
    tel.on_rows = calls.append
    snap = (tel.store.count("BTC"), len(tel.spot_long["BTC"]), len(tel.perp_hist["BTC"]), tel.processed_cycles_total,
            tel.submitted_cycles_total, tel.dropped_cycles_total, tel._q.qsize(), tel.session_id,
            [s.avail for s in tel.store.upto("BTC", 1e12)])
    for _ in range(3):
        row = tel.preview_row("BTC", _r(now), now)
    after = (tel.store.count("BTC"), len(tel.spot_long["BTC"]), len(tel.perp_hist["BTC"]), tel.processed_cycles_total,
             tel.submitted_cycles_total, tel.dropped_cycles_total, tel._q.qsize(), tel.session_id,
             [s.avail for s in tel.store.upto("BTC", 1e12)])
    assert row and snap == after                                                # 29, 30, 31
    assert os.listdir(d) == [] and calls == []                                  # 28 no CSV, no callbacks
    assert row["preview"] is True and row["analysis_ready"] is True
    tel2, now2 = _tel_with_history()
    def boom(*a, **kw):
        raise AssertionError("preview made a network call")
    tel2.provider.snapshot = boom
    tel2.provider.snapshots = boom
    assert tel2.preview_row("BTC", _r(now2), now2) is not None                  # 27 (provider never called)


def test_preview_parity_and_causality():
    tel, now = _tel_with_history()
    prev = tel.preview_row("BTC", _r(now), now)
    fut = t8._snap("BTC", ts=now + 2.0, mid=999.0, idx=100.0)                   # future snapshot
    tel.store.add(fut)
    prev2 = tel.preview_row("BTC", _r(now), now)
    keys = [c for c in pt.STEP2_COLUMNS if not c.startswith(("submitted", "processed", "dropped", "drops"))]
    assert all(prev[c] == prev2[c] for c in keys)                               # 32 future snapshot ignored
    rows = tel.record_cycle({"BTC": _r(now)}, {"BTC": now}, now=now + 0.2)
    rec = rows["BTC"]
    for c in keys:                                                              # 34 parity
        assert prev[c] == rec[c], (c, prev[c], rec[c])
    for c in ("base_p_up", "fav", "side_ask"):
        assert prev[c] == rec[c] if c in rec else True
    stale, sn = _tel_with_history()
    stale.store = pt.SnapshotStore(list(k.COINS))
    stale.store.add(t8._snap("BTC", ts=sn - 600, mid=100.05, idx=100.0))        # far too old
    p3 = stale.preview_row("BTC", _r(sn), sn)
    assert p3["analysis_ready"] is False and "PERP_TOO_OLD" in (p3["quality_flags"] or "")   # 33
    irr, ni = _tel_with_history()
    for j, back in enumerate((181, 151, 119, 87, 61, 33, 17, 5)):               # 35 irregular spacing
        irr.store.add(t8._snap("BTC", ts=ni - back, mid=100.0 + j * 0.01, idx=100.0))
    pi = irr.preview_row("BTC", _r(ni), ni)
    ri = irr.record_cycle({"BTC": _r(ni)}, {"BTC": ni}, now=ni + 0.1)["BTC"]
    assert all(pi[c] == ri[c] for c in keys)


# ═══════════════════ 36-47 live math ═══════════════════
def live_gate_for(alpha=0.25, threshold=-0.1, gamma=-3.0, env=True, scorer=None, kill=None, group="ALL"):
    """A gate wired to a hand-made frozen policy + promotion (tests only)."""
    d = tempfile.mkdtemp()
    pol = t10.make_policy(gamma=gamma, threshold=threshold, group=group)
    promo = {"promotion_schema_version": 1, "promotion_id": "TESTPROMO", "mode": pl.MODE,
             "experiment_id": "EXP", "candidate_sha256": "c" * 64, "source_policy_id": pol["policy_id"],
             "source_policy_hash": pol["policy_hash"], "feature_name": pol["feature_name"],
             "coin_or_group": pol["coin_or_group"], "selected_alpha": alpha,
             "max_abs_perp_delta": pp.MAX_ABS_PERP_DELTA, "conflict_threshold": pol["conflict_threshold"],
             "step5_baseline_dashboard_sha256": man()["step5_dashboard_sha256"],
             "legacy_strategy_fingerprint": man()["legacy_strategy_fingerprint"],
             "strategy_config_hash": man()["strategy_config_hash"],
             "fingerprint_format_version": man()["fingerprint_format_version"],
             "created_at_utc": "2026-09-01T00:00:00+00:00", "expires_at_utc": "2099-01-01T00:00:00+00:00",
             "synthetic": True, "scope": dict(pl.REQUIRED_SCOPE), "source_hashes": {}}
    promo["promotion_hash"] = pl.compute_promotion_hash(promo)
    g = pl.LiveVetoGate(promo, ps.FrozenPolicy(pol, allow_synthetic=True), pl.LiveVetoJournal(os.path.join(d, "v.csv")),
                        env_enabled=env, status="ACTIVE" if env else "READY_BUT_DISABLED",
                        kill_file=kill or os.path.join(d, "KILL"), baseline=man(), scorer=scorer)
    return g, d, pol


def prow(**kw):
    row = {"coin": "BTC", "analysis_ready": True, "causal_premium_bps": 2.0, "causal_spot_ret_60s_bps": 0.5,
           "telemetry_session_id": "S", "cycle_id": 1, "spot_observed_ts_epoch_ms": 2_000_000_000_000,
           "quality_flags": ""}
    row.update(kw)
    return row


def test_live_math():
    g, d, pol = live_gate_for(alpha=0.25, gamma=-3.0)
    r = _r(time.time(), p_up=93.0, conf=93.0, side_ask=88.0, fav="UP")
    row = prow(base_p_up=93.0, fav="UP", causal_premium_bps=0.2)
    res = g.evaluate("BTC", r, row)
    fp = ps.FrozenPolicy(pol, allow_synthetic=True).score(dict(row, base_p_up=93.0, fav="UP"))
    assert abs(res["p_b_up"] - fp["p_b_up"]) < 1e-15 and abs(res["conflict_score"] - fp["conflict"]) < 1e-15   # 36
    assert res["step4_decision"] == ps.ALLOW and res["decision"] in (pl.ALLOW, pl.BLOCK)                        # 38
    assert abs(res["delta_raw"] - (fp["p_c_up"] - fp["p_b_up"])) < 1e-15                                        # 39
    g2, _, _ = live_gate_for(alpha=0.25, gamma=-3.0, threshold=-0.001)
    res2 = g2.evaluate("BTC", r, prow(base_p_up=93.0, fav="UP", causal_premium_bps=5.0, binary_ticker="T2"))
    assert res2["decision"] == pl.BLOCK and res2["reason"] == pl.B_STEP4                                        # 37
    # 40/41: cap and alpha applied exactly
    o = pp.overlay(0.93, 0.70, 0.90, 0.25, "UP", 88.0, K)          # delta_raw = +0.20 -> capped
    assert o["delta_capped"] == 0.05 and abs(o["integrated_p_up"] - (0.93 + 0.25 * 0.05)) < 1e-15
    assert abs(pp.favored(0.61, "UP") - 0.61) < 1e-15 and abs(pp.favored(0.61, "DOWN") - 0.39) < 1e-15           # 42, 43
    # 44-47 via decide on the legacy side
    assert pp.decide(0.49, "UP", 85.0, K)["decision"] == pp.BLOCK_FLIP                                           # 44
    assert pp.decide(0.51, "DOWN", 85.0, K)["decision"] == pp.BLOCK_FLIP
    assert pp.decide(0.79, "UP", 70.0, K)["decision"] == pp.BLOCK_LOW_CONF                                       # 45
    assert pp.decide(0.90, "UP", 88.0, K)["decision"] == pp.BLOCK_NO_EDGE                                        # 46
    assert pp.decide(0.93, "UP", 88.0, K)["decision"] == pp.WOULD_ALLOW                                          # 47
    for reason, kwargs in ((pl.B_FLIP, dict(gamma=-60.0, alpha=1.0)), (pl.B_CONF, dict(gamma=-9.0, alpha=1.0))):
        gg, _, _ = live_gate_for(threshold=-0.99, **kwargs)
        rr = gg.evaluate("BTC", _r(time.time(), p_up=81.0, conf=81.0, side_ask=78.0),
                         prow(base_p_up=81.0, fav="UP", causal_premium_bps=9.0))
        assert rr["decision"] == pl.BLOCK, (reason, rr["reason"])


def test_gate_cannot_upgrade():
    g, d, _ = live_gate_for(alpha=1.0, gamma=+3.0)
    res = g.evaluate("BTC", _r(time.time(), signal=False, p_up=77.0, conf=77.0),
                     prow(base_p_up=77.0, fav="UP", causal_premium_bps=9.0))
    assert res["decision"] == pl.INACTIVE and res["reason"] == pl.R_NO_LEGACY and res["legacy_signal"] is False  # 48
    # 48 (defence in depth): the pure overlay refuses to score a non-signal at all
    for p_up in (0.99, 0.5, 0.01):
        assert pp.decide(p_up, "UP", 50.0, K, legacy_signal=False)["decision"] == pp.NO_NEW_CALL
        assert pp.overlay(0.6, 0.6, 0.9, 1.0, "UP", 50.0, K, legacy_signal=False)["decision"] == pp.NO_NEW_CALL
    assert not os.path.exists(os.path.join(d, "v.csv"))
    r = _r(time.time())
    res2 = g.evaluate("BTC", r, prow(base_p_up=93.0, fav="UP", causal_premium_bps=9.0))
    assert res2["decision"] == pl.ALLOW and res2["integrated_conf"] > res2["legacy_conf"]
    for key in ("contracts", "rec_stop", "side", "entry_mode", "side_ask", "stop"):
        assert key not in res2 or key in ("side_ask",)                                                # 49-53 audit only
    assert res2["legacy_conf"] == r["conf"] and res2["legacy_net_edge"] == r["net_edge"]              # 54, 55


# ═══════════════════ 56-64 call path ═══════════════════
def _poller(gate_obj, fixture_idx=0):
    """One real poller() cycle with the live gate wired in; records legacy call actions."""
    tel, now = _tel_with_history()
    done = threading.Event()
    def publish(rows):
        k._perp_publish(rows); done.set()
    tel.on_rows = publish
    calls, logged, papered = [], [], []
    margs, px, closes = t8.POLLER_FIXTURES[fixture_idx]
    mkt = t7._market(*margs)
    d = tempfile.mkdtemp()
    real_log, real_paper = k.log_call, k._append_paper
    def log_call(coin, r):
        logged.append(r["ticker"]); real_log(coin, r)
    def append_paper(coin, r):
        papered.append(r["ticker"]); real_paper(coin, r)
    def stop_sleep(_s):
        raise t7._StopPoller()
    saved = (set(k._alerted), dict(k._pending), dict(k._call_pending), dict(k.STATE))
    k._alerted.clear(); k._pending.clear(); k._call_pending.clear()
    try:
        with t7.binary_inputs(mkt, px, closes), \
             t7.patched(k, current_market=lambda s: dict(mkt, ticker=f"{s}-T1"),
                        time=types.SimpleNamespace(time=time.time, sleep=stop_sleep), _perp=tel, _shadow=None,
                        _gate=gate_obj, log_call=log_call, _append_paper=append_paper,
                        ON_CALL=lambda coin, r, crec: calls.append(r["ticker"]),
                        PERP_TELEMETRY_ENABLED=True, PERP_SHADOW_ENABLED=False, POST_EMBED=None,
                        TELEGRAM_BOT_TOKEN="", RESULTS_FILE=os.path.join(d, "r.json"),
                        CALLS_FILE=os.path.join(d, "c.json"), TRADES_CSV=os.path.join(d, "t.csv"),
                        PAPER_ORDERS_CSV=os.path.join(d, "p.csv"), EXPLAIN_MARK_FILE=os.path.join(d, "e")):
            k.RUNNING.set()
            try:
                k.poller()
            except t7._StopPoller:
                pass
            done.wait(5)
            out = {"on_call": calls, "log_call": logged, "paper": papered,
                   "pending": dict(k._call_pending), "alerted": sorted(k._alerted),
                   "coins": json.loads(json.dumps(k.STATE["coins"])),
                   "health": json.loads(json.dumps(k.STATE.get("perp_live_veto"), default=str)),
                   "calls_json": json.load(open(os.path.join(d, "c.json"))) if os.path.exists(os.path.join(d, "c.json")) else [],
                   "paper_rows": list(csv.DictReader(open(os.path.join(d, "p.csv")))) if os.path.exists(os.path.join(d, "p.csv")) else []}
            return out
    finally:
        k._alerted.clear(); k._alerted.update(saved[0]); k._pending.clear(); k._pending.update(saved[1])
        k._call_pending.clear(); k._call_pending.update(saved[2])
        with k.LOCK:
            k.STATE.clear(); k.STATE.update(saved[3])


class FakeGate:
    """Deterministic gate stand-in for call-path tests."""
    def __init__(self, decision, reason="X", fail_open=False, journal=None):
        self.decision, self.reason, self.fail_open, self.journal = decision, reason, fail_open, journal
        self.seen = []
        self.configured, self.active = True, decision != pl.INACTIVE
    def evaluate(self, coin, r, preview):
        self.seen.append((coin, r.get("ticker"), preview is not None and preview.get("preview") is True))
        if self.journal is not None:
            self.journal.append({"promotion_id": "P", "binary_ticker": r.get("ticker"),
                                 "live_decision": self.decision, "live_reason": self.reason})
        return {"decision": self.decision, "reason": self.reason, "fail_open": self.fail_open,
                "active": self.active, "legacy_signal": True, "latency_ms": 1.0}
    def health(self):
        return {"configured": True, "active": self.active, "status": "ACTIVE", "blocked": 0}
    def banner(self):
        return "test"


def test_call_path():
    base = _poller(None)
    assert len(base["on_call"]) == 4 and base["log_call"] == base["on_call"] and base["paper"] == base["on_call"]
    for decision, reason, fo in ((pl.ALLOW, pl.R_ALLOW, False), (pl.FAIL_OPEN, pl.FO_FEATURE, True)):
        g = FakeGate(decision, reason, fo)
        out = _poller(g)
        assert out["on_call"] == base["on_call"] and out["log_call"] == base["log_call"]                  # 56, 57
        assert out["paper"] == base["paper"] and len(out["on_call"]) == 4
        assert sorted(out["pending"]) == sorted(base["pending"]) and out["alerted"] == base["alerted"]
        assert all(s[2] for s in g.seen), "gate did not receive the causal preview row"
        for c in k.COINS:                                                                                 # 52-55
            for f in ("conf", "net_edge", "raw_edge", "rec_stop", "side_ask", "fav", "signal", "p_up"):
                assert out["coins"][c][f] == base["coins"][c][f]
        for tk, cp in out["pending"].items():
            for f in ("contracts", "stop", "entry", "entry_mode", "side"):
                assert cp[f] == base["pending"][tk][f]                                                     # 49-52
    blocked = _poller(FakeGate(pl.BLOCK, pl.B_STEP4))
    assert blocked["on_call"] == [] and blocked["log_call"] == [] and blocked["paper"] == []              # 58
    assert blocked["pending"] == {} and blocked["calls_json"] == [] and blocked["paper_rows"] == []       # 60
    assert len(blocked["alerted"]) == 4                                                                   # first signal handled
    for c in k.COINS:
        assert blocked["coins"][c]["signal"] is True and blocked["coins"][c]["conf"] == base["coins"][c]["conf"]


def test_first_signal_and_restart():
    g, d, _ = live_gate_for(threshold=-0.001, gamma=-3.0)
    r = _r(time.time(), ticker="TKX")
    row = prow(base_p_up=93.0, fav="UP", causal_premium_bps=5.0, binary_ticker="TKX")
    first = g.evaluate("BTC", r, row)
    assert first["decision"] == pl.BLOCK                                                                  # 59 journal row
    rows = list(csv.DictReader(open(g.journal.path)))
    assert len(rows) == 1 and rows[0]["live_decision"] == pl.BLOCK and rows[0]["binary_ticker"] == "TKX"
    for _ in range(3):
        again = g.evaluate("BTC", r, prow(base_p_up=93.0, fav="UP", causal_premium_bps=0.0, binary_ticker="TKX"))
        assert again["decision"] == pl.BLOCK                                                              # 61
    assert len(list(csv.DictReader(open(g.journal.path)))) == 1
    g2 = pl.LiveVetoGate(g.promotion, g.policy, pl.LiveVetoJournal(g.journal.path), env_enabled=True,
                         status="ACTIVE", kill_file=os.path.join(d, "NOKILL"), baseline=man())
    assert g2.evaluate("BTC", r, row)["decision"] == pl.BLOCK                                             # 63
    ga, da, _ = live_gate_for(threshold=-0.99)
    ra = _r(time.time(), ticker="TKA")
    a1 = ga.evaluate("BTC", ra, prow(base_p_up=93.0, fav="UP", binary_ticker="TKA"))
    assert a1["decision"] == pl.ALLOW
    ga2 = pl.LiveVetoGate(ga.promotion, ga.policy, pl.LiveVetoJournal(ga.journal.path), env_enabled=True,
                          status="ACTIVE", kill_file=os.path.join(da, "NOKILL"), baseline=man())
    a2 = ga2.evaluate("BTC", ra, prow(base_p_up=93.0, fav="UP", binary_ticker="TKA"))
    assert a2["decision"] == pl.ALLOW and len(list(csv.DictReader(open(ga.journal.path)))) == 1           # 64
    gf, df, _ = live_gate_for(threshold=-0.99)
    rf = _r(time.time(), ticker="TKF")
    f1 = gf.evaluate("BTC", rf, prow(base_p_up=93.0, fav="UP", analysis_ready=False, binary_ticker="TKF"))
    assert f1["decision"] == pl.FAIL_OPEN                                                                 # 62
    f2 = gf.evaluate("BTC", rf, prow(base_p_up=93.0, fav="UP", binary_ticker="TKF"))
    assert f2["decision"] == pl.ALLOW and f2["reason"].startswith("REPLAY")
    assert len(list(csv.DictReader(open(gf.journal.path)))) == 1


# ═══════════════════ 65-70 fail-open ═══════════════════
def test_fail_open():
    cases = {pl.FO_FEATURE: prow(base_p_up=93.0, fav="UP", causal_premium_bps=None),                       # 65
             pl.FO_CONTROL: prow(base_p_up=93.0, fav="UP", causal_spot_ret_60s_bps=None),                  # 66
             pl.FO_NOT_READY: prow(base_p_up=93.0, fav="UP", analysis_ready=False),                        # 67
             pl.FO_PREVIEW: None}
    for i, (reason, row) in enumerate(cases.items()):
        g, _, _ = live_gate_for()
        res = g.evaluate("BTC", _r(time.time(), ticker=f"T{i}"), row)
        assert res["decision"] == pl.FAIL_OPEN and res["reason"] == reason and res["fail_open"] is True
    gs, _, _ = live_gate_for(group="ETH")                        # policy scoped to one coin
    out_of_scope = gs.evaluate("BTC", _r(time.time(), ticker="TS"), prow(base_p_up=93.0, fav="UP"))
    assert out_of_scope["decision"] == pl.ALLOW and out_of_scope["reason"] == pl.R_NOT_APPLICABLE
    assert not os.path.exists(gs.journal.path)                   # out-of-scope calls are not journalled
    gsc, _, _ = live_gate_for()
    no_scaler = gsc.evaluate("BTC", _r(time.time(), ticker="TZ"), prow(base_p_up=93.0, fav="UP", coin="ZZZ"))
    assert no_scaler["decision"] == pl.FAIL_OPEN and no_scaler["reason"] == pl.FO_STALE      # 68
    g2, _, _ = live_gate_for(threshold=-0.99)        # Step 4 allows, so Layer 2 is reached
    num = g2.evaluate("BTC", _r(time.time(), ticker="TN", p_up=None), prow(base_p_up=93.0, fav="UP"))
    assert num["decision"] == pl.FAIL_OPEN and num["reason"] in (pl.FO_FEATURE, pl.FO_NUMERIC)             # 69
    def boom(row):
        raise RuntimeError("scorer exploded")
    g3, _, _ = live_gate_for(scorer=boom)
    r3 = g3.evaluate("BTC", _r(time.time(), ticker="TE"), prow(base_p_up=93.0, fav="UP"))
    assert r3["decision"] == pl.FAIL_OPEN and r3["reason"] == pl.FO_INTERNAL and g3.consecutive_errors == 1  # 70


# ═══════════════════ 71-75 circuit breakers ═══════════════════
def test_circuit_breakers():
    def boom(row):
        raise RuntimeError("x")
    g, _, _ = live_gate_for(scorer=boom)
    for i in range(pl.MAX_CONSECUTIVE_GATE_ERRORS):
        g.evaluate("BTC", _r(time.time(), ticker=f"E{i}"), prow(base_p_up=93.0, fav="UP"))
    assert g.breaker_latched and g.breaker_reason == "CIRCUIT_BREAKER_GATE_ERRORS" and g.active is False    # 71
    g.scorer = None
    good = g.evaluate("BTC", _r(time.time(), ticker="OK"), prow(base_p_up=93.0, fav="UP"))
    assert good["decision"] == pl.INACTIVE and g.active is False                                            # 72, 75
    gb, _, _ = live_gate_for(threshold=-0.001, gamma=-3.0)
    for i in range(40):                                                                                      # 73
        row = prow(base_p_up=93.0, fav="UP", causal_premium_bps=5.0 if i % 2 else 0.0, binary_ticker=f"B{i}")
        gb.evaluate("BTC", _r(time.time(), ticker=f"B{i}"), row)
    assert gb.breaker_latched and gb.breaker_reason == "CIRCUIT_BREAKER_BLOCK_RATE"
    assert gb.health()["rolling_block_fraction"] > pl.LIVE_VETO_MAX_BLOCK_FRACTION
    gf, _, _ = live_gate_for()
    for i in range(35):                                                                                      # 74
        gf.evaluate("BTC", _r(time.time(), ticker=f"F{i}"), prow(base_p_up=93.0, fav="UP", analysis_ready=False,
                                                                  binary_ticker=f"F{i}"))
    assert gf.breaker_latched and gf.breaker_reason == "CIRCUIT_BREAKER_FAIL_OPEN_RATE"
    # 72: a latch is permanent for the process — recording good outcomes cannot clear it
    for _ in range(pl.LIVE_VETO_ROLLING_WINDOW + 5):
        gf._record(pl.ALLOW)
    assert gf.breaker_latched and gf.active is False
    gb._record(pl.ALLOW)
    assert gb.breaker_latched and gb.active is False
    out = _poller(gf)
    assert len(out["on_call"]) == 4                                                                          # 75


# ═══════════════════ 76-79 kill switch ═══════════════════
def test_kill_switch():
    g, d, _ = live_gate_for(threshold=-0.001, gamma=-3.0)
    kill = g.kill_file
    row = lambda i: prow(base_p_up=93.0, fav="UP", causal_premium_bps=5.0, binary_ticker=f"K{i}")
    assert g.evaluate("BTC", _r(time.time(), ticker="K0"), row(0))["decision"] == pl.BLOCK                  # 76
    open(kill, "w").write("stop")
    assert g.evaluate("BTC", _r(time.time(), ticker="K1"), row(1))["decision"] == pl.INACTIVE               # 77
    assert g.kill_latched and g.active is False
    os.remove(kill)
    assert g.evaluate("BTC", _r(time.time(), ticker="K2"), row(2))["decision"] == pl.INACTIVE               # 78
    assert g.kill_latched is True
    g2 = pl.LiveVetoGate(g.promotion, g.policy, pl.LiveVetoJournal(os.path.join(d, "v2.csv")), env_enabled=True,
                         status="ACTIVE", kill_file=kill, baseline=man())
    assert g2.evaluate("BTC", _r(time.time(), ticker="K3"), row(3))["decision"] == pl.BLOCK                 # 79


# ═══════════════════ 80-82 latency ═══════════════════
def test_latency():
    g, _, _ = live_gate_for()
    res = g.evaluate("BTC", _r(time.time(), ticker="L0"), prow(base_p_up=93.0, fav="UP"))
    assert res["latency_ms"] < pl.MAX_GATE_LATENCY_MS and res["decision"] in (pl.ALLOW, pl.BLOCK)           # 80
    def slow(row):
        time.sleep((pl.MAX_GATE_LATENCY_MS + 15) / 1000.0)
        return ps.FrozenPolicy(t10.make_policy(), allow_synthetic=True).score(row)
    gs, _, _ = live_gate_for(scorer=slow)
    r2 = gs.evaluate("BTC", _r(time.time(), ticker="L1"), prow(base_p_up=93.0, fav="UP"))
    assert r2["decision"] == pl.FAIL_OPEN and r2["reason"] == pl.FO_TIMEOUT and r2["fail_open"] is True     # 81
    out = _poller(FakeGate(pl.FAIL_OPEN, pl.FO_TIMEOUT, True))
    assert len(out["on_call"]) == 4                                                                          # 82


# ═══════════════════ 83-88 journal ═══════════════════
def test_journal():
    g, d, _ = live_gate_for(threshold=-0.001, gamma=-3.0)
    for i in range(3):
        g.evaluate("BTC", _r(time.time(), ticker=f"J{i}"),
                   prow(base_p_up=93.0, fav="UP", causal_premium_bps=5.0 if i else 0.0, binary_ticker=f"J{i}"))
    text = open(g.journal.path).read()
    rows = list(csv.DictReader(open(g.journal.path)))
    assert text.count("live_veto_schema_version") == 1 and len(rows) == 3                                   # 83
    assert next(csv.reader(open(g.journal.path))) == pl.JOURNAL_COLUMNS
    for r in rows:
        assert r["promotion_id"] == "TESTPROMO" and r["policy_id"] == g.policy.id                            # 85
        assert r["promotion_hash"] == g.promotion["promotion_hash"]
        assert r["legacy_p_up"] and r["legacy_conf"] and r["legacy_net_edge"]                                # 86
        assert (r["live_decision"] == pl.BLOCK) == (r["fail_open"] == "False" and r["live_reason"].startswith("BLOCK"))  # 87
        if r["live_decision"] != pl.FAIL_OPEN:
            assert r["conflict_score"] and r["selected_alpha"] and r["step4_decision"]
            # a Step 4 block never runs the Step 5 overlay, so it has no integrated values
            assert bool(r["integrated_p_up"]) == (r["live_reason"] != pl.B_STEP4)
    # 84: a journal whose header differs is renamed aside (never mixed, never deleted);
    # its keys still load, so restart de-duplication survives a schema change.
    shutil.copy(g.journal.path, g.journal.path + ".schema-1.bak")
    with open(g.journal.path, "w") as f:
        f.write("old,header\n1,2\n")
    old_bytes = open(g.journal.path, "rb").read()
    j2 = pl.LiveVetoJournal(g.journal.path)
    rotated = [n for n in os.listdir(os.path.dirname(g.journal.path))
               if n.startswith(os.path.basename(g.journal.path) + ".schema-")]
    assert len(rotated) == 2 and not os.path.exists(g.journal.path)
    assert old_bytes in [open(os.path.join(os.path.dirname(g.journal.path), n), "rb").read() for n in rotated]
    assert j2.seen("TESTPROMO", "J1") is not None and j2.seen("TESTPROMO", "J0") is not None                 # 88


# ═══════════════════ 89-93 health ═══════════════════
def test_health():
    out = _poller(None)
    assert out["health"]["configured"] is False and out["health"]["active"] is False                         # 89
    g, _, _ = live_gate_for(threshold=-0.001, gamma=-3.0)
    h = g.health()
    assert h["active"] is True and h["status"] == "ACTIVE" and h["mode"] == "LIVE_VETO_ONLY"                 # 90
    for i in range(6):
        row = prow(base_p_up=93.0, fav="UP", causal_premium_bps=5.0 if i < 2 else 0.0, binary_ticker=f"H{i}",
                   analysis_ready=i != 5)
        g.evaluate("BTC", _r(time.time(), ticker=f"H{i}"), row)
    h = g.health()
    assert h["blocked"] == 2 and h["allowed"] == 3 and h["fail_open"] == 1 and h["legacy_signals_seen"] == 6  # 91
    assert abs(h["rolling_block_fraction"] - 2 / 6) < 1e-9
    g.breaker_latched, g.breaker_reason = True, "CIRCUIT_BREAKER_BLOCK_RATE"
    assert g.health()["circuit_breaker_latched"] is True and g.health()["active"] is False                    # 92
    g2, _, _ = live_gate_for()
    open(g2.kill_file, "w").write("x")
    g2.evaluate("BTC", _r(time.time(), ticker="Z"), prow(base_p_up=93.0, fav="UP"))
    assert g2.health()["kill_switch_latched"] is True                                                         # 93
    json.dumps(g.health()); json.dumps(g2.health())


# ═══════════════════ 94-99 static safety, deployment status ═══════════════════
def _ast(path):
    tree = ast.parse(open(path).read())
    doc = {id(n.body[0].value) for n in ast.walk(tree)
           if isinstance(n, (ast.Module, ast.FunctionDef, ast.ClassDef)) and n.body and isinstance(n.body[0], ast.Expr)}
    mods = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
           {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | \
            {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | \
            {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    lits = {n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)
            and id(n) not in doc}
    return mods, names, lits


def test_static_safety():
    for f in ("perp_live.py", "promote_perp_integration.py", "check_perp_deployment.py", "strategy_fingerprint.py"):
        mods, names, lits = _ast(os.path.join(HERE, f))
        assert not mods & {"requests", "urllib", "urllib3", "httpx", "aiohttp", "socket", "http"}, (f, mods)   # 94, 96
        for bad in ("place_order", "cancel_order", "close_position", "transfer", "leverage", "create_order",
                    "post", "put", "patch", "delete"):
            assert bad not in names, (f, bad)                                                                  # 95
        for s in lits:
            assert not any(x in s.lower() for x in ("/orders", "/portfolio", "api.kalshi", "coinbase",
                                                    "begin rsa private key")), (f, s[:40])
        assert not any(x in open(os.path.join(HERE, f)).read().lower()
                       for x in ("begin private key", "sk_live", "-----begin"))                                 # 97
    # 98: no env var can bypass validation
    src = open(os.path.join(HERE, "perp_live.py")).read()
    envs = {n for n in _ast(os.path.join(HERE, "perp_live.py"))[2] if n.isupper() and "PERP" in n}
    assert not any("FORCE" in e or "BYPASS" in e for e in envs), envs
    f = chain()
    p = json.load(open(f["promo"]))
    for flag in ("PERP_LIVE_VETO_FORCE", "PERP_LIVE_VETO_ENABLED", "PERP_SKIP_VALIDATION"):
        os.environ[flag] = "1"
    try:
        bad = dict(p, conflict_threshold=-0.99)                        # tampered content, hash now stale
        d = tempfile.mkdtemp()
        bp = os.path.join(d, "p.json"); json.dump(bad, open(bp, "w"))
        g = pl.LiveVetoGate.from_files(bp, f["reg"], BASELINE, DASH, os.path.join(d, "v.csv"),
                                       env_enabled=True, allow_synthetic=True)
        assert g.active is False and g.status == "REFUSED"
        syn = pl.LiveVetoGate.from_files(f["promo"], f["reg"], BASELINE, DASH, os.path.join(d, "v2.csv"),
                                         env_enabled=True, allow_synthetic=False)
        assert syn.active is False
    finally:
        for flag in ("PERP_LIVE_VETO_FORCE", "PERP_LIVE_VETO_ENABLED", "PERP_SKIP_VALIDATION"):
            os.environ.pop(flag, None)


def test_real_zero_state():
    d = tempfile.mkdtemp()
    for tool, args in (("promote_perp_integration.py", ["--dashboard", DASH, "--baseline", BASELINE]),
                       ("check_perp_deployment.py", ["--dashboard", DASH, "--baseline", BASELINE])):
        p = subprocess.run([sys.executable, os.path.join(HERE, tool)] + args, cwd=d, capture_output=True, text=True)
        assert p.returncode == 0, (tool, p.stderr)
        assert "Promotions created: 0" in p.stdout or "FINAL STATUS: INACTIVE" in p.stdout                     # 99
    s = cd.status(os.path.join(d, "none.json"), os.path.join(d, "pol.json"), BASELINE, DASH,
                  os.path.join(d, "cand.json"), env_enabled=False)
    assert s["final_status"] == "INACTIVE" and s["validated_candidates"] == 0 and s["strategy_fingerprint_valid"]
    f = chain()
    s2 = cd.status(f["promo"], f["reg"], BASELINE, DASH, f["candidate"], env_enabled=False)
    assert s2["final_status"] == "REFUSED" and s2["promotion_synthetic"] is True                               # synthetic never active
    g = _poller(None)
    assert g["health"]["active"] is False and len(g["on_call"]) == 4                                            # legacy unchanged


def test_legacy_regression_env_unset():
    os.environ.pop("PERP_LIVE_VETO_ENABLED", None)
    base = _poller(None)
    import importlib
    assert k.PERP_LIVE_VETO_ENABLED is False
    with t7.patched(k, _gate=None):
        out = _poller(None)
    for c in k.COINS:
        for f in t7.STRATEGY_FIELDS + ("edge", "verdict"):
            assert out["coins"][c][f] == base["coins"][c][f]                                                    # 100
    assert out["on_call"] == base["on_call"] and out["log_call"] == base["log_call"]
    for tk, cp in out["pending"].items():
        assert cp == base["pending"][tk] or all(cp[x] == base["pending"][tk][x] for x in
                                                ("contracts", "stop", "entry", "entry_mode", "side", "maker_filled"))
    # settlement, fees and paper records are untouched by Step 6
    t10.test_legacy_rows_and_golden_settlement()
    t10.test_trades_csv_migration()


def test_previous_stages():
    # Master mode (run_all_tests.py): every stage is already run exactly once by the runner.
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    # Standalone: re-verify earlier stages, each EXACTLY once (children run in master mode).
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 12):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-500:], p.stderr[-500:])


if __name__ == "__main__":
    try:
        run("1+2+7 baseline manifest and fingerprint", test_baseline_manifest)
        run("3-6 fingerprint ignores comments/UI, catches strategy changes", test_fingerprint_sensitivity)
        run("6.1 fingerprint ignores empty version-specific AST fields (py3.13 regression)",
            test_fingerprint_ignores_empty_version_specific_ast_fields)
        run("6.1 semantic mutations caught; reformat/relocation/new helpers ignored", test_fingerprint_semantic_and_nonsemantic)
        run("6.1 deployment check: no false drift, true drift + v1 formats refused", test_deployment_check_drift)
        run("8  no candidate -> 0 promotions", test_no_candidate)
        run("9-16+20 promotion refusals (status/synthetic/confirm/hashes/drift/expiry)", test_promotion_refusals)
        run("13+17-19 promotion success, deterministic id, hash, expiry", test_promotion_success_and_identity)
        run("21-26 dual activation lock", test_activation_matrix)
        run("27-31 preview is read-only and networkless", test_preview_is_read_only)
        run("32-35 preview causality, parity, stale, irregular", test_preview_parity_and_causality)
        run("36-47 two-layer live veto math", test_live_math)
        run("48-55 gate can only remove calls", test_gate_cannot_upgrade)
        run("56-60 call path: ALLOW/FAIL_OPEN identical, BLOCK suppresses", test_call_path)
        run("61-64 first-signal semantics and restart dedupe", test_first_signal_and_restart)
        run("65-70 fail-open paths", test_fail_open)
        run("71-75 circuit breakers", test_circuit_breakers)
        run("76-79 kill switch latching", test_kill_switch)
        run("80-82 latency budget", test_latency)
        run("83-88 audit journal", test_journal)
        run("89-93 health state", test_health)
        run("94-98 static security, no bypass", test_static_safety)
        run("99 current real zero-promotion state", test_real_zero_state)
        run("100 legacy regression with env unset", test_legacy_regression_env_unset)
        if os.environ.get("STAGE12_SKIP_PRIOR") == "1":
            print("SKIP  101 all Stage 1-11 suites (STAGE12_SKIP_PRIOR=1; mutation runs only)")
        else:
            run("101 all Stage 1-11 suites", test_previous_stages)
    finally:
        if FIX.get("dir"):
            shutil.rmtree(FIX["dir"], ignore_errors=True)
    print("\nAll Stage 12 tests passed.")
