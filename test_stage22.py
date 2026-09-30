#!/usr/bin/env python3
"""Stage 22 — STEP 6 REAL-DATA EVALUATION, FEATURE-FAMILY ABLATION AND MODEL COMPARISON (research only).

Run:  py test_stage22.py                 (or py run_all_tests.py for stages 1-22)
      py test_stage22.py --only splits,fold   (a subset; used by scripts/mutation_test_step6.py)

Offline and deterministic. Covers the frozen baseline (every earlier fingerprint, the existing perp veto, LIVE refused),
the frozen feature universe, the real-session quality validator, the empirical Coinbase sequence validator, the
settlement-label gate, the legacy probability, the research matrix + immutable cache + dataset fingerprint, purged
chronological splits, market-normalized weights, structural pruning, missing data, the models, train-only fitting,
metrics / buckets, market-clustered bootstrap, Benjamini-Hochberg, calibration gates, executable economics with
versioned fees, sample / complexity gates, leakage guards, the research ledger, experiment fingerprints, the synthetic
self-test (planted signal found, noise rejected, SYNTHETIC_ONLY), INSUFFICIENT_DATA without real data, the CLIs, the
separate Step-6 fingerprint and production isolation.
"""
import ast
import hashlib
import json
import math
import os
import random
import shutil
import subprocess
import sys
import tempfile
from collections import namedtuple

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from feature_eval import RESULT_CLASSES, FEATURE_CLASSES                                          # noqa: E402
from feature_eval import ablation as AB                                                          # noqa: E402
from feature_eval import bootstrap as BS                                                         # noqa: E402
from feature_eval import calibration as CAL                                                      # noqa: E402
from feature_eval import coinbase_seq as CS                                                      # noqa: E402
from feature_eval import dataset as DS                                                           # noqa: E402
from feature_eval import economics as EC                                                         # noqa: E402
from feature_eval import experiment as EXP                                                       # noqa: E402
from feature_eval import fingerprint as S6FP                                                     # noqa: E402
from feature_eval import gates as GT                                                             # noqa: E402
from feature_eval import labels as LB                                                            # noqa: E402
from feature_eval import leakage as LK                                                           # noqa: E402
from feature_eval import ledger as LG                                                            # noqa: E402
from feature_eval import metrics as MT                                                           # noqa: E402
from feature_eval import models as MD                                                            # noqa: E402
from feature_eval import pruning as PR                                                           # noqa: E402
from feature_eval import quality as QU                                                           # noqa: E402
from feature_eval import report as RP                                                            # noqa: E402
from feature_eval import splits as SP                                                            # noqa: E402
from feature_eval import synthetic as SY                                                         # noqa: E402
from feature_eval import universe as UV                                                          # noqa: E402

EARLIER_FINGERPRINTS = {
    "legacy_strategy": "8d94f241e8fc8edadc76058e1f12f430b6e4f499f4c0a30fba1cb5cf07dad82a",
    "extended_strategy": "784141876a1b7f4c9f605036ef1358a6a3ea51ef32fced1f80f74045a6adef8c",
    "settlement": "6fed6efd324417353fa53a8a981cbdcbeb9d121103bbe7925c284874f737c1bf",   # Step 6.4 (live rule-text index ids); OLD b205709349492b71 (6.3), 4884524a (6.2), ba4e50c3 (6.1), 3eba791c (Steps 2-6)
    "market_data": "faf21c0af4bd3a9e828197081134faec3b7300bc870a982a143f2b2ce587911f",   # Step 6.4 (real-feed corrections); OLD ca98d418bf40075a (6.3), 609905956c9e6120 (6.2), 969cec83e8b9912e (Steps 3-6.1)
    "perp_data": "8a4743efcc1dddfb797aed9b3db3ec13819e8cef58ad352c7990325e50cad8f3",   # Step 6.4 (terminal REST availability); OLD 90543ddff68e9dec (Steps 4-6.3)
    "microstructure": "6971961992c95099c180c59af4b4840b6b8cd1d61842ef30d60417024d51dd32",   # Step 6.4 (per-book ordering, UNAVAILABLE books); OLD 695d8e7741287c0b (Steps 5.1-6.3)
}
EXISTING_PERP_VETO_FINGERPRINT = "499c1e16da5d9cc763babe7dea31db7104ebc3f1460a17241af83240c8de523b"
PRODUCTION_UNTOUCHED = {
    "kalshi_dashboard.py": "aa66c7ae1d6cf8b913b746b05bfd6cf64197454d3b7fd00a2ba174749150c595",
    "kalshi_backtest.py": "19f4bc281701db5234c29cc922b3806b21ac333883df174dcd936385fe9f117f",
    "kalshi_bot.py": "8278bdc4d2adaa117e2fdcce101924181d4bc64d3987b584a69741eeb474cd5d",
    "run_local.py": "0749c2a83b3c219ed63dc5d994253883983f9b6b4ebc81a2b72f46b067e53044",
    "config/strategy_baseline.json": "8f052394a830be88f4bd4c506b5d8069222e3c9b1c06c0bc63e3817463185d04",
}
STEP6_CLIS = ("run_step6_research.py", "research_status.py", "validate_research_session.py")
C = 1_790_001_000_000
_CACHE = {}
Raw = namedtuple("Raw", "source stream ingest_seq text")


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


def tmpdir():
    return tempfile.mkdtemp(prefix="stage22-")


def sha(rel):
    return hashlib.sha256(open(os.path.join(HERE, rel), "rb").read()).hexdigest()


def _imports(path):
    tree = ast.parse(open(path, encoding="utf-8").read())
    mods = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    return mods | {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}


def _py_files(d):
    out = []
    for root, dirs, files in os.walk(d):
        dirs[:] = [x for x in dirs if x != "__pycache__"]
        out += [os.path.join(root, f) for f in files if f.endswith(".py")]
    return sorted(out)


def session():
    """One small SYNTHETIC Step-3 session written through the real collector (validator / dataset tests)."""
    if "s" not in _CACHE:
        from market_data.synthetic import run_session
        root = tmpdir()
        run_session(root, ("BTC",), C - 700_000, 760, 5)
        d = [os.path.join(root, x) for x in os.listdir(root)][0]
        _CACHE["s"] = d
    return _CACHE["s"]


def synth(n_slots=400, cps=(300, 60), seed=7):
    k = ("m", n_slots, cps, seed)
    if k not in _CACHE:
        _CACHE[k] = SY.make_matrix(n_slots=n_slots, checkpoints_s=cps, seed=seed)
    return _CACHE[k]


def lenient():
    return AB.AblationConfig(bootstrap_reps=200, permutation_reps=500,
                             complexity=GT.ComplexityGates(min_events_per_parameter=5, min_train_markets_per_parameter=8))


def pipeline_result():
    if "pipe" not in _CACHE:
        from feature_eval.pipeline import run as prun
        m, recs = synth()
        _CACHE["pipe"] = prun(m, recs, cfg=lenient())
    return _CACHE["pipe"]


def rows_simple(n_markets=60, cps=(300, 60), start=1_700_000_000_000, seed=3):
    rng = random.Random(seed)
    rows = []
    for i in range(n_markets):
        close = start + (i // 2 + 1) * 900_000
        y = rng.randint(0, 1)
        for cp in cps:
            rows.append({"market_ticker": f"M{i:04d}", "asset": "BTC" if i % 2 else "ETH", "checkpoint_s": cp,
                         "checkpoint_ts_ms": close - cp * 1000, "close_ts_ms": close, "y": y,
                         "legacy_p_up": 0.5, "label_source": "OFFICIAL_RESULT"})
    return rows


# ═══════════════════ 1-3 frozen baseline, veto, isolation ═══════════════════
def test_frozen_baseline():
    for rel, h in PRODUCTION_UNTOUCHED.items():
        assert sha(rel) == h, f"PRODUCTION FILE CHANGED: {rel}"
    from kalshi_core import baseline
    ok, problems = baseline.verify()
    assert ok, problems
    p = subprocess.run([sys.executable, "-m", "regression.generate", "--check"], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "MATCH (47 cases)" in p.stdout, p.stdout + p.stderr
    import strategy_fingerprint as sf
    fp, _c, _v, _f = sf.current_fingerprint(os.path.join(HERE, "kalshi_dashboard.py"))
    assert fp == EARLIER_FINGERPRINTS["legacy_strategy"]
    assert baseline.build_manifest()["fingerprints"]["extended_strategy_fingerprint"] == EARLIER_FINGERPRINTS["extended_strategy"]
    from market_data import fingerprint as mdfp
    from microstructure import fingerprint as mfp
    from perp_data import fingerprint as pfp
    from settlement import fingerprint as sfp
    for name, mod, key in (("settlement", sfp, "settlement_fingerprint"), ("market_data", mdfp, "market_data_fingerprint"),
                           ("perp_data", pfp, "perp_data_fingerprint"), ("microstructure", mfp, "microstructure_fingerprint")):
        ok, problems = mod.verify()
        assert ok, (name, problems)
        stored = json.load(open(os.path.join(HERE, "config", f"{name}_baseline.json")))
        assert stored[key] == EARLIER_FINGERPRINTS[name], name


def test_existing_perp_veto_untouched():
    from perp_data import fingerprint as pfp
    assert pfp.perp_veto_fingerprint() == EXISTING_PERP_VETO_FINGERPRINT
    for rel in pfp.perp_veto_hashes():
        if rel.endswith(".py"):
            bad = [m for m in _imports(os.path.join(HERE, rel)) if m == "feature_eval" or m.startswith("feature_eval.")]
            assert not bad, f"the existing perp veto imports Step-6 code: {rel}"
    p = subprocess.run([sys.executable, "check_perp_deployment.py"], cwd=HERE, capture_output=True, text=True,
                       env={k: v for k, v in os.environ.items() if k != "PERP_LIVE_VETO_ENABLED"})
    assert "FINAL STATUS: INACTIVE" in p.stdout, p.stdout[-500:]
    assert json.load(open(S6FP.BASELINE_PATH))["existing_perp_veto_fingerprint"] == EXISTING_PERP_VETO_FINGERPRINT


def test_production_isolation():
    prod = ["kalshi_dashboard.py", "kalshi_backtest.py", "kalshi_bot.py", "run_local.py", "strategy_fingerprint.py",
            "collect_market_data.py", "collect_research_data.py"] + \
        [os.path.join("kalshi_core", x) for x in os.listdir(os.path.join(HERE, "kalshi_core")) if x.endswith(".py")]
    layers = [os.path.relpath(p, HERE) for d in ("settlement", "market_data", "perp_data", "microstructure")
              for p in _py_files(os.path.join(HERE, d))]
    for f in prod + layers:
        bad = [m for m in _imports(os.path.join(HERE, f)) if m == "feature_eval" or m.startswith("feature_eval.")]
        assert not bad, f"Step-6 code imported by {f}: {bad}"
    forbidden = {"discord", "kalshi_bot", "kalshi_api_learn", "kalshi_core.execution", "perp_live", "perp_shadow",
                 "perp_telemetry", "perp_probability", "kalshi_backtest", "run_local", "requests", "http", "socket",
                 "urllib.request"}
    for f in _py_files(os.path.join(HERE, "feature_eval")) + [os.path.join(HERE, c) for c in STEP6_CLIS] + \
            [os.path.join(HERE, "scripts", s) for s in ("mutation_test_step6.py", "bench_step6.py")]:
        mods = _imports(f)
        bad = {m for m in mods if m in forbidden or m.split(".")[0] in {"discord", "requests", "http", "socket"}}
        assert not bad, (f, bad)
        if "kalshi_dashboard" in mods:
            assert f.endswith(os.path.join("feature_eval", "legacy.py")), f"only the legacy wrapper may read the predictor: {f}"
        src = open(f, encoding="utf-8").read().lower()
        for s in ("/portfolio/orders", "create_order", "place_order", ".post(", ".put(", "api_secret", "private_key",
                  "promote("):
            assert s not in src, (f, s)
    assert "APPROVED_FOR_PRODUCTION" not in RESULT_CLASSES
    code = ("import sys; sys.path.insert(0, %r); import run_local, kalshi_core.signal, kalshi_core.adapter, perp_live;"
            "print(any(m == 'feature_eval' or m.startswith('feature_eval.') for m in sys.modules))") % HERE
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=HERE)
    assert p.returncode == 0 and p.stdout.strip() == "False", p.stdout + p.stderr
    from kalshi_core import execution as ex
    try:
        ex.get_execution_engine("LIVE"); raise AssertionError("LIVE engine returned")
    except ex.LiveExecutionUnavailable:
        pass


# ═══════════════════ 4 universe ═══════════════════
def test_feature_universe():
    ok, problems = UV.verify_frozen()
    assert ok, problems
    fr = UV.load_frozen()
    recs = fr["records"]
    assert len(recs) == 2047 and len({r["name"] for r in recs}) == 2047
    c = UV.counts(recs)
    assert c["by_layer"] == {"STEP2_SETTLEMENT": 11, "STEP3_SPOT": 441, "STEP4_PERP": 980, "STEP5_MICRO": 615}
    assert c["by_role"] == {"ALIAS": 4, "DATA_QUALITY": 39, "EXECUTION_STATE": 16, "MODEL_CANDIDATE": 1988}
    for r in recs:
        for k in ("name", "family", "source", "assets", "window", "units", "status_rules", "min_warmup_ms", "causal_rule",
                  "feature_version", "source_fingerprint", "role", "layer"):
            assert k in r, (r["name"], k)
    by = {r["name"]: r for r in recs}
    assert by["kalshi.yes_ask"]["role"] == "EXECUTION_STATE" and by["kalshi.no_mid"]["role"] == "ALIAS"
    assert by["kalshi.no_mid"]["alias_of"] == "kalshi.yes_mid"
    fams = set(c["by_family"])
    assert {"SPOT_PRICE_MOMENTUM", "SPOT_VOLATILITY", "FUNDING", "LIQUIDATION", "CVD", "ORDER_FLOW", "SWEEP", "TOXICITY",
            "REPLENISHMENT", "KALSHI_BOOK"} <= fams                          # finer families preserved
    assert c["by_tag"]["COINBASE_L2_SEQUENCE"] > 0 and c["by_tag"]["SESSION_NORMALIZED"] > 0
    assert UV.max_lookback_ms() == 1_500_000
    # S12: a changed record under the same version is refused (even with self-consistent hashes)
    t = tmpdir()
    bad = json.loads(json.dumps(fr))
    bad["records"][0]["window"] = "999s"
    bad["records_sha256"] = UV._sha(bad["records"])
    bad["fingerprint"] = UV._sha({"version": bad["version"], "records_sha256": bad["records_sha256"]})
    p = os.path.join(t, "u.json")
    json.dump(bad, open(p, "w"))
    ok2, pr2 = UV.verify_frozen(p)
    assert not ok2 and any("WITHOUT A VERSION CHANGE" in x for x in pr2), pr2
    hand = json.loads(json.dumps(fr))
    hand["records"][1]["units"] = "edited"
    json.dump(hand, open(p, "w"))
    ok3, pr3 = UV.verify_frozen(p)
    assert not ok3 and any("edited by hand" in x for x in pr3), pr3


# ═══════════════════ 5 quality ═══════════════════
def test_quality_validator():
    d = session()
    q = QU.validate_session(d)
    assert q["synthetic"] and q["real_data_verdict"] == "SYNTHETIC_ONLY" and not q["usable_for_research"]
    assert q["verdict"] in ("PASS", "DEGRADED") and q["raw_store_sha256"] and q["raw_checksums"]
    assert any("session only" in p for p in q["problems"])                  # a 12.7-minute session is DEGRADED
    assert q["checks"]["label_leakage"]["ok"] and "labels" in q["checks"] and "settlement_provenance" in q["checks"]
    for k, v in q["sources"].items():
        assert v["verdict"] in QU.VERDICTS and "future_timestamps" in v and "duplicates" in v and "availability" in v
    # segment hashes compared with an earlier report: a changed segment REJECTS
    prev = json.loads(json.dumps(q))
    first = sorted(prev["raw_checksums"])[0]
    prev["raw_checksums"][first] = "0" * 64
    q2 = QU.validate_session(d, previous=prev)
    assert q2["verdict"] == "REJECT" and q2["checks"]["segment_hashes_vs_previous"]["changed"] == [first]
    t = tmpdir()
    os.makedirs(os.path.join(t, "empty"))
    assert QU.validate_session(os.path.join(t, "empty"))["verdict"] == "REJECT"
    assert QU.worst("PASS", "DEGRADED") == "DEGRADED" and QU.worst("DEGRADED", "REJECT") == "REJECT"
    assert QU.structural_leakage_check()["ok"]


# ═══════════════════ 6 coinbase ═══════════════════
def _cb_stream(mode, n=6000, conns=2, products=("BTC-USD", "ETH-USD")):
    out, seqno = [], 0
    for c in range(conns):
        g = 0
        per = {p: 0 for p in products}
        for i in range(n // conns):
            if i % 50 == 0:
                ch, prods = "heartbeats", []
            else:
                ch, prods = "l2_data", [products[i % len(products)]]
            if mode == "global":
                s = g
                g += 1
            elif ch == "heartbeats":
                s = i                                         # heartbeats in their own (irregular) space
            else:
                s = per[prods[0]]
                per[prods[0]] += 1
            ev = [{"type": "snapshot" if i < 3 else "update", "product_id": p} for p in prods]
            out.append(Raw("coinbase_l2", f"ws#{c}", seqno, json.dumps({"channel": ch, "sequence_num": s, "events": ev})))
            seqno += 1
    return out


def test_coinbase_sequence_validator():
    r = CS.analyse_raw(_cb_stream("global"))
    assert r["status"] == "CONSISTENT_WITH_CURRENT_POLICY", r["reason"]
    assert r["answers"]["advances_globally_per_connection"] and r["answers"]["heartbeats_share_the_sequence_space"]
    r2 = CS.analyse_raw(_cb_stream("product"))
    assert r2["status"] == "CONTRADICTS_CURRENT_POLICY" and "required_correction" in r2, r2["reason"]
    assert r2["answers"]["advances_per_product"] and not r2["answers"]["advances_globally_per_connection"]
    small = CS.analyse_raw(_cb_stream("global", n=200))
    assert small["status"] == "UNVERIFIED_REAL_FEED" and "insufficient real evidence" in small["reason"]
    assert CS.analyse_raw(_cb_stream("global"), synthetic=True)["status"] == "SYNTHETIC_ONLY"
    assert CS.analyse_raw([])["status"] == "UNVERIFIED_REAL_FEED"
    for st in CS.STATUSES:
        assert CS.coinbase_features_allowed(st) == (st == "CONSISTENT_WITH_CURRENT_POLICY")
    # fail closed: Coinbase-tagged features never reach a model unless the policy is verified
    m, recs = synth()
    t = SP.market_table(AB.usable_rows(m.rows))
    parts = SP.partition(t)
    st = AB.Study(m, recs, parts, SP.walk_forward(t, parts), lenient(), coinbase_allowed=False)
    assert "micro.coinbase.syn_noise" in st.coinbase_gated
    assert all("micro.coinbase.syn_noise" not in v for v in st.fam_names.values())
    st2 = AB.Study(m, recs, parts, SP.walk_forward(t, parts), lenient(), coinbase_allowed=True)
    assert any("micro.coinbase.syn_noise" in v for v in st2.fam_names.values())


# ═══════════════════ 7 labels ═══════════════════
def test_settlement_label_gate():
    assert LB.market_label({"official_result": "yes", "reconstructed_outcome": "yes"}, "SETTLEMENT_UNVERIFIED") == (1, "OFFICIAL_RESULT")
    assert LB.market_label({"official_result": "yes", "reconstructed_outcome": "no"}, "VERIFIED") == (None, "LABEL_CONFLICT")
    assert LB.market_label({"reconstructed_outcome": "no", "settlement_rule_status": "RULE_VERIFIED_FOR_MARKET"},
                           "VERIFIED") == (0, "RECONSTRUCTED_VERIFIED")
    assert LB.market_label({"reconstructed_outcome": "no"}, "VERIFIED") == (0, "RULE_UNVERIFIED_FOR_MARKET")   # 6.2
    assert LB.market_label({"reconstructed_outcome": "no"}, "SETTLEMENT_UNVERIFIED") == (0, "SETTLEMENT_UNVERIFIED")
    assert LB.market_label({}, "VERIFIED") == (None, "UNLABELED")
    assert LB.is_gold("OFFICIAL_RESULT") and LB.is_gold("RECONSTRUCTED_VERIFIED")
    for s in ("SETTLEMENT_UNVERIFIED", "LABEL_CONFLICT", "UNLABELED", None):
        assert not LB.is_gold(s), s                                   # S14: unverified labels are never gold
    g = LB.convention_gate([], {}, [], synthetic=False)
    assert g["status"] == "SETTLEMENT_UNVERIFIED" and "only 0 real markets" in g["reason"]
    assert LB.convention_gate([], {}, [], synthetic=True)["status"] == "SYNTHETIC_ONLY"
    rows = rows_simple(10)
    for r in rows:
        r["label_source"] = "SETTLEMENT_UNVERIFIED"
    assert AB.usable_rows(rows) == []
    assert GT.sample_gate(rows)["counts"]["gold_markets"] == 0


# ═══════════════════ 8 legacy ═══════════════════
def test_legacy_probability():
    import types as _t
    import kalshi_dashboard as k
    from feature_eval.legacy import CoinbaseTape, LegacyEvaluator
    before = (k.current_market, k.spot, k.candles)
    tape = CoinbaseTape()
    rng = random.Random(1)
    px = 100.0
    t0 = C - 90 * 60_000
    for i in range(90 * 12):
        px *= math.exp(rng.gauss(0, 0.0004))
        ts = t0 + i * 5000
        tape.add(_t.SimpleNamespace(receive_ts_ms=ts, event_ts_ms=ts, payload={"price": px}))
    t = C - 300_000 + 2500
    with LegacyEvaluator() as le:
        p, info = le.p_up("BTC", t, "KXBTC15M-T", 100.0, C, {"yes_bid": 40, "yes_ask": 42}, tape)
    assert (k.current_market, k.spot, k.candles) == before              # the production module is restored
    assert p is not None and 0 < p < 1, info
    # independent re-derivation from the captured trades (not from the production code)
    closes = tape.candles(t)["close"]
    recent = closes[:-1][-(k.VOL_LOOKBACK_MIN + 1):]
    rets = [math.log(b_ / a_) for a_, b_ in zip(recent, recent[1:])]
    mu = sum(rets) / len(rets)
    sd = math.sqrt(sum((x - mu) ** 2 for x in rets) / (len(rets) - 1)) * 1.15
    rem = (C - t) / 60_000
    ref = 0.5 * (1 + math.erf(math.log(tape.spot(t) / 100.0) / (sd * math.sqrt(rem)) / math.sqrt(2)))
    assert abs(p - ref) < 1e-6, (p, ref)                                # evaluate() rounds p_up (percent, 4 dp)
    with LegacyEvaluator() as le:
        short = CoinbaseTape()
        short.add(_t.SimpleNamespace(receive_ts_ms=t - 1000, event_ts_ms=t - 1000, payload={"price": 100.0}))
        p2, info2 = le.p_up("BTC", t, "KXBTC15M-T", 100.0, C, None, short)
    assert p2 is None and info2["legacy_status"] == "stale"              # never guessed


# ═══════════════════ 9 dataset / cache / fingerprint ═══════════════════
def test_dataset_cache_fingerprint():
    d = session()
    q = QU.validate_session(d)
    m = DS.build_matrix([d], ("BTC",), quality_reports={d: q}, synthetic_mode=True, include_perp=False, include_micro=False)
    assert m.synthetic_only and m.rows and m.meta["settlement_gate"]["status"] == "SYNTHETIC_ONLY"
    assert all(r["synthetic"] and len(r["x"]) == len(m.columns) == len(r["st"]) for r in m.rows)
    assert {r["checkpoint_s"] for r in m.rows} <= set(DS.CHECKPOINT_GRID_S)
    assert not any(c.startswith("micro_label.") for c in m.columns)
    assert all(not LB.is_gold(r["label_source"]) for r in m.rows)      # synthetic reconstruction is never gold
    assert all(r["execution"] is None or r["execution"]["yes_ask_ladder"] is not None for r in m.rows)
    # counterfactual labels: flipping every settlement outcome must not change a single feature value (S6)
    import settlement.checkpoints as SC
    orig = SC.labels_for

    def flipped(*a, **kw):
        r = dict(orig(*a, **kw))
        for key in ("official_result", "reconstructed_outcome"):
            if r.get(key) in ("yes", "no"):
                r[key] = "no" if r[key] == "yes" else "yes"
        return r
    SC.labels_for = flipped
    try:
        mf = DS.build_matrix([d], ("BTC",), quality_reports={d: q}, synthetic_mode=True, include_perp=False,
                             include_micro=False)
    finally:
        SC.labels_for = orig
    assert [r["y"] for r in mf.rows] != [r["y"] for r in m.rows] and any(r["y"] is not None for r in m.rows)
    assert [(r["x"], r["st"], r["legacy_p_up"]) for r in mf.rows] == [(r["x"], r["st"], r["legacy_p_up"]) for r in m.rows], \
        "a settlement outcome changed a feature value"
    real = DS.build_matrix([d], ("BTC",), quality_reports={d: q}, synthetic_mode=False, include_perp=False, include_micro=False)
    assert not real.rows and real.meta["sessions_skipped"][0]["reason"] == "SYNTHETIC_SESSION_IN_REAL_MODE"
    # cache: immutable, verified against raw checksums
    t = tmpdir()
    cd = DS.save_cache(m, t)
    m2 = DS.load_cache(cd, [d])
    assert m2.fingerprint == m.fingerprint and len(m2.rows) == len(m.rows)
    before = os.path.getmtime(os.path.join(cd, "matrix.jsonl.gz"))
    DS.save_cache(m, t)
    assert os.path.getmtime(os.path.join(cd, "matrix.jsonl.gz")) == before
    cp = os.path.join(t, "copy")
    shutil.copytree(d, cp)
    seg = sorted(QU.raw_checksums(cp))[0]                               # e.g. step3/segment-000001.jsonl.gz
    store, name = seg.split("/", 1)
    with open(os.path.join(QU.store_dirs(cp)[store], name), "ab") as h:
        h.write(b"\x00")                                                 # the raw file changed after caching
    meta = json.load(open(os.path.join(cd, "meta.json")))
    meta["session_meta"][0]["session_id"] = "copy"
    json.dump(meta, open(os.path.join(cd, "meta.json"), "w"))
    assert QU.raw_checksums(cp) != QU.raw_checksums(d), "the raw-file edit did not change a checksum"
    if True:
        try:
            DS.load_cache(cd, [cp]); raise AssertionError("a changed raw session did not invalidate the cache")
        except DS.CacheInvalid:
            pass


def test_dataset_fingerprint_sessions():
    a = {"session_id": "S1", "raw_store_sha256": "a" * 64}
    b = {"session_id": "S2", "raw_store_sha256": "b" * 64}
    f1, _ = DS.dataset_fingerprint([a], "u", {"s": 1}, DS.CHECKPOINT_GRID_S, ("BTC",), [])
    f2, _ = DS.dataset_fingerprint([a, b], "u", {"s": 1}, DS.CHECKPOINT_GRID_S, ("BTC",), [])
    f3, _ = DS.dataset_fingerprint([a, dict(b, raw_store_sha256="c" * 64)], "u", {"s": 1}, DS.CHECKPOINT_GRID_S, ("BTC",), [])
    f4, _ = DS.dataset_fingerprint([a, b], "u2", {"s": 1}, DS.CHECKPOINT_GRID_S, ("BTC",), [])
    f5, _ = DS.dataset_fingerprint([a, b], "u", {"s": 2}, DS.CHECKPOINT_GRID_S, ("BTC",), [])
    f6, _ = DS.dataset_fingerprint([a, b], "u", {"s": 1}, (600, 300), ("BTC",), [])
    f7, _ = DS.dataset_fingerprint([a, b], "u", {"s": 1}, DS.CHECKPOINT_GRID_S, ("BTC", "ETH"), [])
    assert len({f1, f2, f3, f4, f5, f6, f7}) == 7                      # S10: every session / version input counts
    f2b, _ = DS.dataset_fingerprint([b, a], "u", {"s": 1}, DS.CHECKPOINT_GRID_S, ("BTC",), [])
    assert f2b == f2                                                      # order-independent


# ═══════════════════ 10-11 splits, weights ═══════════════════
def test_splits_purge_walk_forward():
    m, _ = synth()
    t = SP.market_table(AB.usable_rows(m.rows))
    cfg = SP.SplitConfig()
    parts = SP.partition(t, cfg)
    close = {e["market"]: e["close_ts_ms"] for e in t}
    tr, dv, ho = parts["TRAIN"], parts["DEVELOPMENT"], parts["FINAL_HOLDOUT"]
    assert tr and dv and ho and max(close[x] for x in tr) < min(close[x] for x in dv) <= max(close[x] for x in dv) < min(close[x] for x in ho)
    groups = {}
    for e in t:
        groups.setdefault(e["close_ts_ms"], set()).add(e["market"])
    for g in groups.values():                                           # one 15-minute slot never straddles a boundary
        assert sum(1 for p in (set(tr), set(dv), set(ho), set(parts["purged"])) if g & p) == 1 or g <= set(parts["purged"]) | set(tr) | set(dv) | set(ho)
        assert len({("T" if x in tr else "D" if x in dv else "H" if x in ho else "P") for x in g}) == 1
    first_dev = min(e["first_checkpoint_ts_ms"] for e in t if e["market"] in set(dv))
    assert all(close[x] <= first_dev - cfg.max_causal_lookback_ms for x in tr)          # purged
    assert parts["purged"] and parts["boundaries_close_ts_ms"]["TRAIN"] and cfg.to_dict()["purge_ms"] == 1_500_000
    folds = SP.walk_forward(t, parts, cfg)
    assert len(folds) == cfg.walk_forward_folds
    SP.assert_chronological(folds, t)
    hold = set(ho)
    for f in folds:
        assert not (set(f["train"]) | set(f["test"])) & hold             # the holdout never enters a fold
        assert set(f["test"]) <= set(dv)
        first = min(e["first_checkpoint_ts_ms"] for e in t if e["market"] in set(f["test"]))
        assert all(close[x] <= first - cfg.purge_ms for x in f["train"])
    shuffled = [dict(folds[0], train=folds[-1]["test"], test=folds[0]["test"])]
    try:
        SP.assert_chronological(shuffled, t); raise AssertionError("a shuffled fold passed")
    except AssertionError as e:
        assert "closes at / after" in str(e)
    fit, cal, _pg = SP.calibration_split(folds[-1]["train"], t, cfg)
    assert fit and cal and max(close[x] for x in fit) < min(close[x] for x in cal)
    p1 = SP.partition(t, cfg)
    assert p1 == parts                                                  # deterministic, no shuffling


def test_market_normalized_weights():
    mk = ["A"] * 10 + ["B"] * 1 + ["C"] * 4
    w = MD.market_weights(mk)
    tot = {}
    for m, x in zip(mk, w):
        tot[m] = tot.get(m, 0) + x
    assert all(abs(v - 1) < 1e-12 for v in tot.values())


# ═══════════════════ 12 pruning ═══════════════════
def test_structural_pruning():
    names = ["a", "a_dup", "a_x2", "const", "near", "never", "b", "exec"]
    ci = {n: i for i, n in enumerate(names)}
    role = {n: "MODEL_CANDIDATE" for n in names}
    role["exec"] = "EXECUTION_STATE"
    rng = random.Random(5)
    rows = []
    for i in range(300):
        a = rng.gauss(0, 1)
        b = rng.gauss(0, 1)
        x = [a, a, 2 * a + 1, 3.0, 1.0 if i else 2.0, None, b, 55.0]
        rows.append({"x": x, "st": ["READY"] * 5 + ["NOT_READY"] + ["READY"] * 2, "y": rng.randint(0, 1)})
    r = PR.structural_prune(rows, names, ci, role)
    rm = r["removed"]
    assert rm["const"] == "CONSTANT" and rm["near"] == "NEAR_CONSTANT" and rm["never"] == "NEVER_AVAILABLE"
    assert rm["exec"] == "NOT_MODEL_CANDIDATE" and rm["a_dup"].startswith("EXACT_DUPLICATE")
    assert rm["a_x2"].startswith("HIGHLY_CORRELATED")                  # algebraically identical
    assert r["kept"] == ["a", "b"] and r["counts"]["kept"] == 2
    for row in rows:
        row["y"] = 1 - row["y"]
    assert PR.structural_prune(rows, names, ci, role)["fingerprint"] == r["fingerprint"]   # label independent
    rep = PR.redundancy_report(rows, ["a", "a_x2", "b"], ci)
    assert any({p["a"], p["b"]} == {"a", "a_x2"} for p in rep["pairs"])


# ═══════════════════ 13 missing data ═══════════════════
def test_missing_never_zero():
    X = [[10.0 + i % 3, None if i % 4 == 0 else 5.0 + i % 2] for i in range(40)]
    w = [1.0] * 40
    pre = MD.Preprocessor("indicator").fit(X, w)
    z = pre.transform_row([None, None])
    raw_zero_std = (0.0 - pre.mean[0]) / pre.std[0]
    assert z[0] == 0.0 and abs(raw_zero_std) > 3                         # the TRAIN mean (standardized 0), not a raw 0
    assert z[1] == 0.0 and z[-1] == 1.0 and pre.n_out == 3               # indicator for the column with missing values
    assert pre.transform_row([10.0, 5.0])[-1] == 0.0
    cc = MD.Preprocessor("complete_case").fit(X, w)
    assert not cc.keep_row([1.0, None]) and cc.keep_row([1.0, 2.0]) and cc.n_out == 2
    try:
        MD.Preprocessor("zero"); raise AssertionError("zero fill accepted")
    except ValueError:
        pass
    m, recs = synth()
    res = pipeline_result()
    md = res["family_ablation"]["missing_data"]
    assert set(md["strategies"]) == {"indicator", "complete_case", "native"}
    assert "sample_loss_per_family" in md


# ═══════════════════ 14-15 models, train-only fitting ═══════════════════
def test_models():
    rng = random.Random(11)
    n = 3000
    Z = [[rng.gauss(0, 1), rng.gauss(0, 1)] for _ in range(n)]
    y = [1 if rng.random() < MT.sigmoid(0.3 + 1.5 * z[0] - 0.7 * z[1]) else 0 for z in Z]
    b = MD.fit_logistic(Z, y, [1.0] * n)
    assert abs(b[1] - 1.5) < 0.2 and abs(b[2] + 0.7) < 0.15 and abs(b[0] - 0.3) < 0.15, b
    leg = [MT.sigmoid(1.5 * z[0]) for z in Z]
    X = [[z[1], None if i % 5 == 0 else z[1] * 0 + rng.gauss(0, 1)] for i, z in enumerate(Z)]
    a2 = MD.LegacyRecalibrated().fit(None, leg, y, [1.0] * n)
    assert a2.n_params == 2
    meta = [{"market_ticker": f"M{i // 2:05d}", "asset": "BTC", "close_ts_ms": 1_700_000_000_000 + (i // 2) * 900_000,
             "checkpoint_ts_ms": 1_700_000_000_000 + (i // 2) * 900_000 - 60_000} for i in range(n)]
    rid = MD.RidgeLogisticModel().fit(X, leg, y, [1.0] * n, meta=meta)
    assert rid.l2 in MD.RIDGE_GRID and set(rid.inner_scores) == set(MD.RIDGE_GRID) and rid.inner_info["status"] == "OK"
    nometa = MD.RidgeLogisticModel().fit(X, leg, y, [1.0] * n)
    assert nometa.l2 == max(MD.RIDGE_GRID) and nometa.inner_info["status"] == "NO_MARKET_METADATA"   # fail closed
    bst = MD.BoostedStumps(rounds=20).fit(X, leg, y, [1.0] * n)
    pb = bst.predict(X[:5], leg[:5])
    assert all(0 < p < 1 for p in pb) and bst.n_params == len(bst.trees) <= 20
    assert MD.LegacyModel().predict(None, [0.0, 1.0, 0.3]) == [MT.clip(0.0), MT.clip(1.0), 0.3]
    # the legacy logit is not shrunk by ridge: with pure-noise features ridge == recalibrated legacy (approximately)
    Xn = [[rng.gauss(0, 1)] for _ in range(n)]
    r2 = MD.LogisticModel(l2=100.0).fit(Xn, leg, y, [1.0] * n)
    assert abs(r2.beta[1] - a2.beta[1]) < 0.05, (r2.beta, a2.beta)


def _fold_setup():
    m, recs = synth()
    t = SP.market_table(AB.usable_rows(m.rows))
    parts = SP.partition(t)
    folds = SP.walk_forward(t, parts)
    st = AB.Study(m, recs, parts, folds, lenient())
    return m, recs, st, parts, folds


def test_train_only_fitting():
    m, recs, st, parts, folds = _fold_setup()
    tr, te = st.fold_rows(folds[-1])
    fam = ["SPOT_PRICE_MOMENTUM", "SPOT_VOLATILITY"]
    p, a2, info = AB.fit_fold(tr, te, fam, st.fam_names, st.col_index, st.role_of, "B_logistic", st.cfg, st.holdout)
    mod = info["_model"]
    idx = [st.col_index[n] for n in info["selected_features"]]
    w = MD.market_weights([r["market_ticker"] for r in tr])
    for k, j in enumerate(idx):                                         # S2: the scaler saw TRAINING rows only
        vals = [(AB.feature_value(r, j), wi) for r, wi in zip(tr, w) if AB.feature_value(r, j) is not None]
        mu = sum(v * wi for v, wi in vals) / sum(wi for _, wi in vals)
        assert abs(mod.pre.mean[k] - mu) < 1e-9, (info["selected_features"][k], mod.pre.mean[k], mu)
    assert set(info["selection_markets"]) == {r["market_ticker"] for r in tr}
    assert "mom.syn.z" in info["selected_features"] and len(p) == len(te) and len(a2) == len(te)
    # S3: a feature that is predictive ONLY in the test block must not be selected
    j = st.col_index["vol.syn.noise"]
    rng = random.Random(3)
    tr2 = [dict(r, x=list(r["x"])) for r in tr]
    te2 = [dict(r, x=list(r["x"])) for r in te]
    for r in tr2:
        r["x"][j] = rng.gauss(0, 1)
    for r in te2:
        r["x"][j] = (r["y"] - r["legacy_p_up"]) * 50 + rng.gauss(0, 0.01)
    cfg2 = AB.AblationConfig(max_selected_features=2, complexity=lenient().complexity)
    _p, _b, info2 = AB.fit_fold(tr2, te2, ["SPOT_VOLATILITY"], st.fam_names, st.col_index, st.role_of, "B_logistic",
                                cfg2, st.holdout)
    _sel, train_only = AB.select_features(tr2, st.fam_names["SPOT_VOLATILITY"], st.col_index, 2)
    assert info2["selection_scores"] == train_only, (info2["selection_scores"], train_only)
    assert abs(info2["selection_scores"]["vol.syn.noise"]) < 0.1        # its test-block relation is invisible
    # S4: a FINAL_HOLDOUT market can never reach a fold
    hold_rows = [r for r in m.rows if r["market_ticker"] in set(parts["FINAL_HOLDOUT"])][:4]
    try:
        AB.fit_fold(tr, te + hold_rows, fam, st.fam_names, st.col_index, st.role_of, "B_logistic", st.cfg, st.holdout)
        raise AssertionError("holdout rows accepted")
    except AB.HoldoutLeak:
        pass
    # ridge lambda is chosen on the latest share of TRAINING rows only
    _p3, _b3, info3 = AB.fit_fold(tr, te, fam, st.fam_names, st.col_index, st.role_of, "C_ridge_logistic", st.cfg, st.holdout)
    assert str(info3["ridge_lambda"]) in info3["ridge_inner_scores"]


# ═══════════════════ 16-18 metrics, bootstrap, BH ═══════════════════
def test_metrics_and_buckets():
    p = [0.9, 0.2, 0.7, 0.4]
    y = [1, 0, 0, 1]
    assert abs(MT.brier(p, y) - ((0.01 + 0.04 + 0.49 + 0.36) / 4)) < 1e-12
    ll = -(math.log(0.9) + math.log(0.8) + math.log(0.3) + math.log(0.4)) / 4
    assert abs(MT.log_loss(p, y) - ll) < 1e-12
    assert MT.roc_auc(p, y) == 0.75
    rng = random.Random(2)
    pp = [rng.random() for _ in range(20000)]
    yy = [1 if rng.random() < q else 0 for q in pp]
    a, b = MT.calibration_intercept_slope(pp, yy)
    assert abs(a) < 0.1 and abs(b - 1) < 0.1 and MT.ece(pp, yy) < 0.02
    s = MT.summary(pp[:500], yy[:500])
    assert "never used to select" in s["classification"]["note"]
    mk = [f"M{i // 2}" for i in range(len(pp))]
    bk = MT.confidence_buckets(pp, yy, markets=mk)
    assert [b["bucket"] for b in bk][0] == "0.50-0.60" and bk[-1]["bucket"] == "0.99-1.00" and len(bk) == 11
    for b in bk:
        if b["status"] == "OK":
            assert " / " in b["wins"] and "%" not in b["wins"]
            w_, n_ = b["wins"].split(" / ")
            assert int(n_) == b["n_rows"]
    few = MT.confidence_buckets([0.995] * 5, [1] * 5, markets=["a"] * 5)
    assert few[-1]["status"] == "INSUFFICIENT_SAMPLE" and "wins" not in few[-1]  # never "100%" from 5 / 5


def test_cluster_bootstrap():
    mk = []
    vals = []
    rng = random.Random(4)
    for m in range(40):
        v = rng.gauss(0, 1)
        for _ in range(25):
            mk.append(f"M{m}")
            vals.append(v)                                               # rows of a market are perfectly correlated
    assert BS.resampling_units(mk) == sorted(set(mk))
    r = BS.cluster_bootstrap(mk, lambda ix: sum(vals[i] for i in ix) / len(ix), reps=400, seed=1)
    assert r["unit"] == "market" and r["clusters"] == 40
    width = r["ci_high"] - r["ci_low"]
    assert width > 0.4, width                                            # a row bootstrap would give ~0.1
    d = {f"M{i}": 0.1 + rng.gauss(0, 0.05) for i in range(50)}
    assert BS.sign_flip_pvalue(d, reps=2000) < 0.01
    d0 = {f"M{i}": (-1) ** i * (1 + (i // 2) / 10) for i in range(50)}    # mean exactly 0
    assert BS.sign_flip_pvalue(d0, reps=2000) > 0.5
    assert BS.cluster_bootstrap(mk, lambda ix: 1.0, reps=10, seed=9) == BS.cluster_bootstrap(mk, lambda ix: 1.0, reps=10, seed=9)


def test_benjamini_hochberg():
    from analyze_perp_predictive import bh_qvalues
    q = bh_qvalues([0.01, 0.04, 0.03, None, 0.5])
    assert q[3] is None and abs(q[0] - 0.04) < 1e-12 and abs(q[1] - 0.04 * 4 / 3 * 1.0) < 1e-9
    src = open(os.path.join(HERE, "feature_eval", "ablation.py"), encoding="utf-8").read()
    assert "from analyze_perp_predictive import bh_qvalues" in src      # reuses the repo's BH, no second copy
    res = pipeline_result()["family_ablation"]
    for stage in ("stage1_layer_hypotheses", "stage2_leave_one_family_out", "stage3_family_hypotheses"):
        for r in res[stage]:
            c = r["comparison_vs_A2"]
            if c.get("status") == "OK":
                assert r["q_value_bh"] is not None and r["q_value_bh"] >= c["p_value_sign_flip"] - 1e-12
                for k in ("n_rows", "n_markets", "delta_log_loss_ci", "log_loss_improvement", "p_value_sign_flip"):
                    assert k in c


# ═══════════════════ 19 calibration ═══════════════════
def test_calibration_gates():
    g = CAL.CalibrationGates()
    assert g.min_isotonic_markets >= 5 * g.min_platt_markets
    assert CAL.gate("platt", 39)[0] is False and CAL.gate("platt", 40)[0] is True
    assert CAL.gate("isotonic", 399)[0] is False and CAL.gate("isotonic", 400)[0] is True
    c, meta = CAL.fit_calibrator("isotonic", [0.1, 0.9], [0, 1], [1, 1], 10)
    assert c is None and meta["status"] == "DISABLED_BY_GATE"
    rng = random.Random(8)
    p = [rng.random() for _ in range(4000)]
    y = [1 if rng.random() < min(1, max(0, 0.2 + 0.6 * q)) else 0 for q in p]
    iso, _ = CAL.fit_calibrator("isotonic", p, y, [1.0] * 4000, 1000)
    out = iso.apply(sorted(p))
    assert all(b >= a - 1e-12 for a, b in zip(out, out[1:]))             # monotone
    pl, _ = CAL.fit_calibrator("platt", p, y, [1.0] * 4000, 1000)
    q = pl.apply([0.01, 0.99])
    assert 0.05 < q[0] < 0.3 and 0.7 < q[1] < 0.95 and pl.beta[1] < 1       # over-confident inputs are shrunk
    res = pipeline_result()["calibration"]
    for f in res["folds"]:
        if "methods" in f:
            assert f["calibration_markets"] > 0 and f["fit_markets"] > 0
    assert "strictly later" in res["rule"]
    assert res["methods"]["best:isotonic"]["status"] == "DISABLED_OR_NOT_EVALUATED"   # below the isotonic gate


# ═══════════════════ 20 economics ═══════════════════
def test_executable_economics():
    fm = EC.FeeModel()
    OLD, NEW = EC.JULY_2026_START_MS - 1, EC.JULY_2026_START_MS + 1
    assert fm.status("KXBTC15M-X", NEW) == "FEE_UNVERIFIED"            # no captured multiplier / override state
    assert fm.fee_dollars(1, 0.5, ts_ms=OLD) == 0.02                     # pre-7.7.26: ceil(0.0175) to a cent
    assert fm.fee_dollars(100, 0.5, ts_ms=OLD) == 1.75 and fm.fee_dollars(10, 0.9, ts_ms=OLD) == 0.07
    assert fm.fee_dollars(10, 0.5, maker=True, ts_ms=OLD) == 0.05
    assert fm.fee_dollars(1, 0.5, ts_ms=None) is None                    # no trade time -> no schedule
    assert EC.vwap([[40, 5], [42, 10]], 10) == ((5 * 40 + 5 * 42) / 10, 10)
    assert EC.vwap([[40, 5], [42, 3]], 10) == (None, 8)                  # never extrapolated
    e = EC.side_economics(0.60, [[40, 5], [42, 10]], 10, fm, "KXBTC", ts_ms=NEW)
    assert e["status"] == "EXECUTABLE" and abs(e["price"] - 0.41) < 1e-12 and abs(e["raw_edge"] - 0.19) < 1e-12
    exp = EC.fee_for_fills([(EC.dec("0.40"), EC.dec(5)), (EC.dec("0.42"), EC.dec(5))], EC.KALSHI_GENERAL_2026_07_07, 1)
    assert abs(e["fee_per_contract"] - float(exp["fee"]) / 10) < 1e-15 and e["fee_schedule"] == "kalshi_general_2026_07_07"
    assert abs(e["net_executable_edge"] - (0.60 - 0.41 - e["fee_per_contract"])) < 1e-12
    assert EC.side_economics(0.6, [[40, 5]], 10, fm, "K", ts_ms=NEW)["status"] == "NOT_EXECUTABLE"
    ev = EC.evaluate([{"market": "M", "p_yes": 0.7, "y": 1, "checkpoint_s": 60, "ticker": "M", "ts_ms": NEW,
                       "execution": {"yes_ask_ladder": [[50, 100]], "no_ask_ladder": [[52, 100]]}}])
    assert ev["status"] == "FEE_UNVERIFIED" and ev["maker"]["status"] == "NOT_EVALUATED"
    assert ev["by_size"]["1"]["labels"] == "SIMULATED" and ev["by_size"]["50"]["executable"] == 1
    from dataclasses import replace as _rp
    fm2 = EC.FeeModel(schedules=(_rp(EC.KALSHI_GENERAL_2026_07_07, taker_coefficient="0.05"),))
    assert fm2.fingerprint() != fm.fingerprint()
    # a special schedule applies only to its series and interval; the general one never becomes verified globally
    t0, t1 = EC.JULY_2026_START_MS, EC.JULY_2026_START_MS + 10 ** 9
    spec = EC.FeeSchedule("crypto15m_special", "v1", "test", "u", t0, t1, series_scope=("KXBTC15M",),
                          taker_coefficient="0.05", maker_coefficient="0", rounding="ROUND_UP_FEE_PLUS_COST_TO_CENTICENT",
                          verified=True)
    gen = _rp(EC.KALSHI_GENERAL_2026_07_07, excluded_series=("KXBTC15M",))
    fmv = EC.FeeModel(schedules=(gen, spec))
    ctx = {"fee_metadata": {"fee_type": "general", "fee_multiplier": 1, "fee_type_override": None,
                            "fee_multiplier_override": None}}
    assert fmv.status("KXBTC15M-26SEP291015", t0 + 1, ctx) == "FEE_VERIFIED"
    assert fmv.fee_dollars(10, 0.5, "KXBTC15M-A", ts_ms=t0 + 1, fee_context=ctx) == 0.125
    assert fmv.status("KXBTC15M-26SEP291015", t1, ctx) == "FEE_UNKNOWN"        # outside the effective range
    assert fmv.status("KXBTC15M-26SEP291015", None, ctx) == "FEE_UNKNOWN"      # unknown trade time
    assert fmv.status("KXETH15M-26SEP291015", t0 + 1, ctx) == "FEE_VERIFIED"   # the general July-2026 schedule
    evv = EC.evaluate([{"market": "M", "p_yes": 0.7, "y": 1, "checkpoint_s": 60, "ticker": "KXBTC15M-A", "ts_ms": t0 + 5,
                        "fee_context": ctx, "execution": {"yes_ask_ladder": [[50, 100]], "no_ask_ladder": [[52, 100]]}}],
                      fmv)
    assert evv["status"] == "FEE_VERIFIED" and evv["by_size"]["1"]["fee_status_counts"] == {"FEE_VERIFIED": 1}
    res = pipeline_result()["economic_metrics"]
    assert res["primary"].startswith("TAKER") and res["best"]["status"] == "FEE_UNVERIFIED"
    assert res["best"]["by_size"]["50"]["not_executable"] > 0            # thin synthetic books are NOT_EXECUTABLE


# ═══════════════════ 21 gates ═══════════════════
def test_sample_and_complexity_gates():
    rows = rows_simple(30)
    g = GT.sample_gate(rows, synthetic=False, checkpoints_s=(300, 60))
    assert g["status"] == "INSUFFICIENT_DATA"
    names = {f["requirement"] for f in g["failed_requirements"]}
    assert {"settled_independent_markets", "markets_asset_SOL", "markets_checkpoint_300s", "positive_outcomes",
            "negative_outcomes", "markets_direction_DOWN"} <= names
    assert GT.sample_gate(rows, synthetic=True)["status"] == "SYNTHETIC_ONLY"
    assert GT.sample_gate(rows)["counts"]["gold_markets"] == 30          # markets, not rows
    cg = GT.complexity_gate(500, 60, 440, 1000, 30, 12)
    assert cg["status"] == "INSUFFICIENT_DATA_FOR_COMPLEXITY" and cg["candidate_feature_count"] == 1000
    assert cg["post_pruning_feature_count"] == 30 and cg["effective_complexity_parameters"] == 12
    assert GT.complexity_gate(5000, 2000, 3000, 1000, 30, 12)["status"] == "OK"
    assert GT.effective_params("B_logistic", 5, 2) == 9 and GT.effective_params("D_boosted_stumps", 5, rounds=40) == 40
    # with the predeclared defaults a small matrix never reaches a model
    m, recs = SY.make_matrix(n_slots=30, checkpoints_s=(300, 60))
    m.meta["synthetic_only"] = False                                      # pretend-real to exercise the real-data gate
    from feature_eval.pipeline import run as prun
    out = prun(m, recs)
    assert out["family_ablation"]["result_class"] == "INSUFFICIENT_DATA"
    assert out["family_ablation"]["reason"] and "stage1_layer_hypotheses" not in out["family_ablation"]


# ═══════════════════ 22 leakage ═══════════════════
def test_leakage_guards():
    from settlement.checkpoints import LABEL_FIELDS
    cols = ["vol.cf.rv.5m", "micro.binance.ofi_l1"]
    assert LK.name_scan(cols)["ok"]
    for bad in list(LABEL_FIELDS)[:3] + ["official_result", "micro_label.cb.fwd_mid_bps.1000ms", "final_settlement_value"]:
        r = LK.name_scan(cols + [bad])
        assert not r["ok"] and bad in r["forbidden_columns"], bad          # S6 / S7 by name
    assert LK.name_scan([r["name"] for r in UV.load_frozen()["records"]])["ok"]
    rng = random.Random(6)
    rows = []
    for i in range(200):
        y = rng.randint(0, 1)
        rows.append({"market_ticker": f"M{i}", "y": y, "x": [rng.gauss(0, 1), y * 3 + rng.gauss(0, 0.01)],
                     "st": ["READY", "READY"], "checkpoint_ts_ms": 1, "close_ts_ms": 2, "label_source": "OFFICIAL_RESULT"})
    v = LK.value_scan(rows, cols, {"vol.cf.rv.5m": 0, "micro.binance.ofi_l1": 1})
    assert not v["ok"] and v["leakage_suspects"][0]["feature"] == "micro.binance.ofi_l1"   # S7 disguised post-event label
    try:
        LK.guard(cols, rows, cols, {"vol.cf.rv.5m": 0, "micro.binance.ofi_l1": 1}, rows)
        raise AssertionError("guard passed a leaked feature")
    except LK.LeakageError:
        pass
    late = [dict(rows[0], checkpoint_ts_ms=5, close_ts_ms=5)]
    assert not LK.availability_scan(late)["ok"]
    assert pipeline_result()["data"]["leakage_guard"]["ok"]


# ═══════════════════ 23-24 ledger, fingerprints ═══════════════════
def test_research_ledger():
    t = tmpdir()
    L = LG.Ledger(os.path.join(t, "l.jsonl"), clock=lambda: 1)
    L.append("DEVELOPMENT_EVALUATION", dataset_fingerprint="d1")
    L.open_holdout("d1", "e1", "test")
    try:
        L.open_holdout("d1", "e2", "retuned"); raise AssertionError("holdout reused")
    except LG.HoldoutBurned:
        pass                                                              # S11
    L.open_holdout("d2", "e3", "a new, later holdout")
    s = L.summary()
    assert s["chain_verified"] and s["by_kind"]["FINAL_HOLDOUT_ACCESS"] == 2 and len(s["holdout_accesses"]) == 2
    lines = open(L.path).read().splitlines()
    e = json.loads(lines[1])
    e["reason"] = "edited"
    lines[1] = json.dumps(e, sort_keys=True)
    open(L.path, "w").write("\n".join(lines) + "\n")
    try:
        L.verify(); raise AssertionError("tampering not detected")
    except LG.LedgerCorrupt:
        pass


def test_experiment_fingerprint():
    spec = {"model": "C", "families": ["A"], "missing_strategy": "indicator", "calibration": {"m": "none"},
            "split_config": SP.SplitConfig().to_dict(), "fee_model_fingerprint": EC.FeeModel().fingerprint(),
            "seed": EXP.SEED, "bootstrap": {"reps": 1000}}
    f0, _ = EXP.experiment_fingerprint("d", spec)
    from dataclasses import replace as _rp
    for k, v in (("model", "D"), ("families", ["B"]), ("missing_strategy", "complete_case"), ("seed", 1),
                 ("fee_model_fingerprint", EC.FeeModel(schedules=(_rp(EC.KALSHI_GENERAL_2026_07_07, taker_coefficient="0.05"),)).fingerprint()),
                 ("split_config", SP.SplitConfig(label_horizon_ms=1000).to_dict())):
        assert EXP.experiment_fingerprint("d", dict(spec, **{k: v}))[0] != f0, k
    assert EXP.experiment_fingerprint("d2", spec)[0] != f0
    try:
        EXP.experiment_fingerprint("d", {k: v for k, v in spec.items() if k != "seed"}); raise AssertionError("no seed")
    except ValueError:
        pass
    bad = dict(spec, split_config={"train_fraction": 0.6})
    try:
        EXP.experiment_fingerprint("d", bad); raise AssertionError("purge missing accepted")
    except ValueError:
        pass
    env = EXP.environment()
    assert env["seed"] == EXP.SEED and env["python"]


# ═══════════════════ 25 synthetic pipeline ═══════════════════
def test_synthetic_pipeline():
    res = pipeline_result()
    fa = res["family_ablation"]
    assert fa["result_class"] == "SYNTHETIC_ONLY" and fa["synthetic_only"] and res["data"]["synthetic_only"]
    s1 = {(r["hypothesis"], r["model"]): r for r in fa["stage1_layer_hypotheses"]}
    assert len(s1) == 27
    assert s1[("H02", "C_ridge_logistic")]["result_class"] == "PROMISING_RESEARCH_ONLY"   # planted signal (spot layer)
    for h in ("H01", "H03", "H04", "H08"):                                   # planted noise layers
        for mname in ("B_logistic", "C_ridge_logistic", "D_boosted_stumps"):
            assert s1[(h, mname)]["result_class"] != "PROMISING_RESEARCH_ONLY", (h, mname)
    s3 = {r["hypothesis"]: r for r in fa["stage3_family_hypotheses"]}
    assert s3["H02-SPOT_PRICE_MOMENTUM"]["result_class"] == "PROMISING_RESEARCH_ONLY"
    assert s3["H02-SPOT_VOLATILITY"]["result_class"] == "NO_INCREMENTAL_VALUE"
    fc = fa["family_classification"]
    assert fc["SPOT_PRICE_MOMENTUM"]["class"] == "KEEP_CANDIDATE" and fc["PERP_FLOW"]["class"] == "NO_INCREMENTAL_VALUE"
    assert all(v["class"] in FEATURE_CLASSES for v in fc.values())
    assert set(fa["feature_classification"]["counts"]) <= set(FEATURE_CLASSES)
    assert fa["feature_classification"]["by_feature"]["mom.syn.z_dup"] == "REDUNDANT"
    assert all(r["result_class"] in RESULT_CLASSES + ("UNAVAILABLE",) for s in ("stage1_layer_hypotheses",
               "stage2_leave_one_family_out", "stage3_family_hypotheses") for r in fa[s])
    assert fa["best_development_configuration"]["selected_by"].startswith("lowest DEVELOPMENT")
    lofo = {r["left_out_family"]: r for r in fa["stage2_leave_one_family_out"]}
    assert lofo["SPOT_PRICE_MOMENTUM"]["comparison_vs_A2"]["log_loss_improvement"] > 0.01
    assert fa["complexity_reports"] and all("independent_training_markets" in c for c in fa["complexity_reports"])
    assert fa["structural_pruning_train_partition"]["fingerprint"]
    assert fa["asset_specific"]["per_asset"] and fa["slices_best_configuration"]["by_checkpoint"]
    assert "per_session_generalization" in fa["session_normalized"]
    st = s1[("H02", "C_ridge_logistic")]["comparison_vs_A2"]["stability"]
    assert set(st["by"]) >= {"fold", "asset", "direction", "checkpoint", "volatility_slice"}
    assert res["data"]["regime"]["variable"] == "vol.cf.rv.5m"
    js = json.dumps(res, default=str)
    assert "APPROVED_FOR_PRODUCTION\"" not in js.replace('"never": "APPROVED_FOR_PRODUCTION"', "")
    # deterministic, including a process pool
    from feature_eval.pipeline import run as prun
    m, recs = SY.make_matrix(n_slots=120, checkpoints_s=(300, 60))
    a = prun(m, recs, cfg=lenient(), workers=1, stages=("ablation",))
    b = prun(m, recs, cfg=lenient(), workers=2, stages=("ablation",))
    def strip(d):
        return json.dumps({k: v for k, v in d["family_ablation"].items() if k != "environment"}, sort_keys=True, default=str)
    assert strip(a) == strip(b)
    assert a["calibration"]["result_class"] == "NOT_RUN"


def test_outputs_real_vs_synthetic():
    t = tmpdir()
    res = pipeline_result()
    try:
        RP.write_real_outputs(t, {"x": 1}, res); raise AssertionError("synthetic written to real outputs")
    except RP.SyntheticInRealOutput:
        pass                                                              # S8
    try:
        RP.write_real_outputs(t, {"synthetic_only": True}); raise AssertionError("synthetic data quality accepted")
    except RP.SyntheticInRealOutput:
        pass
    p = RP.write_selftest(t, res)
    d = json.load(open(p))
    assert d["synthetic_only"] and os.path.basename(p) == RP.SELFTEST_FILE and not os.path.exists(
        os.path.join(t, RP.OUTPUT_FILES["family_ablation"]))
    w = RP.write_real_outputs(t, {"real_sessions": 0}, None, {"entries": 0}, "no real research sessions",
                              [{"requirement": "settled_independent_markets", "have": 0, "need": 400}])
    assert set(w) == set(RP.OUTPUT_FILES)
    for k, fn in RP.OUTPUT_FILES.items():
        d = json.load(open(os.path.join(t, fn)))
        assert d["synthetic_only"] is False and d["never"] == "APPROVED_FOR_PRODUCTION"
        if k in ("family_ablation", "calibration", "confidence_buckets", "economic_metrics"):
            assert d["result_class"] == "INSUFFICIENT_DATA" and d["failed_requirements"]


# ═══════════════════ 26 CLIs ═══════════════════
def test_clis():
    t = tmpdir()
    empty = os.path.join(t, "sessions")
    os.makedirs(empty)
    out = os.path.join(t, "out")
    p = subprocess.run([sys.executable, "run_step6_research.py", "--dry-run", "--sessions", empty, "--out", out],
                       cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and '"real_sessions": 0' in p.stdout and not os.path.exists(out), p.stdout[-400:] + p.stderr
    p = subprocess.run([sys.executable, "run_step6_research.py", "--sessions", empty, "--out", out, "--assets",
                        "BTC,ETH,SOL,XRP"], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "INSUFFICIENT_DATA" in p.stdout, p.stdout + p.stderr
    for fn in RP.OUTPUT_FILES.values():
        assert json.load(open(os.path.join(out, fn)))["synthetic_only"] is False
    fa = json.load(open(os.path.join(out, RP.OUTPUT_FILES["family_ablation"])))
    assert fa["result_class"] == "INSUFFICIENT_DATA" and any(
        f["requirement"] == "settled_independent_markets" for f in fa["failed_requirements"])
    p = subprocess.run([sys.executable, "run_step6_research.py", "--report", "--out", out], cwd=HERE, capture_output=True,
                       text=True)
    assert p.returncode == 0 and "step6_family_ablation.json: result_class=INSUFFICIENT_DATA" in p.stdout
    p = subprocess.run([sys.executable, "run_step6_research.py", "--assets", "BTC,DOGE", "--dry-run"], cwd=HERE,
                       capture_output=True, text=True)
    assert p.returncode == 2
    # a synthetic session is never treated as real
    root = os.path.dirname(session())
    p = subprocess.run([sys.executable, "run_step6_research.py", "--sessions", root, "--out", os.path.join(t, "o2")],
                       cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "INSUFFICIENT_DATA: no real research sessions" in p.stdout, p.stdout + p.stderr
    dq = json.load(open(os.path.join(t, "o2", RP.OUTPUT_FILES["data_quality"])))
    assert dq["synthetic_sessions_ignored"] == [os.path.basename(session())]
    p = subprocess.run([sys.executable, "validate_research_session.py", session(), "--json", os.path.join(t, "q.json")],
                       cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "real-data verdict SYNTHETIC_ONLY" in p.stdout and "coinbase sequence semantics" in p.stdout
    p = subprocess.run([sys.executable, "research_status.py", "--sessions", root, "--out", os.path.join(t, "o3")],
                       cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "real sessions: 0" in p.stdout and "STATUS: INSUFFICIENT_DATA" in p.stdout
    assert "not estimated" in p.stdout and "synthetic sessions ignored: 1" in p.stdout


# ═══════════════════ Step 6.1 corrections ═══════════════════
LATER = 1_791_000_000_000                             # a close after the rule observation date (2026-09-29)


def _flat_obs(index_id, value, close, source="cfb_ws", asset="BTC"):
    from settlement.types import SettlementObservation
    return [SettlementObservation(asset, index_id, source, value, t, receive_ts_ms=t + 100)
            for t in range(close - 61_000, close + 1, 1000)]


def test_contract_rules():
    from dataclasses import replace as _rp
    from settlement import rules as RL
    from settlement.policy import SettlementWindowPolicy
    from settlement.reconstruction import reconstruct
    from settlement.types import Flag, SettlementMarket
    for series, dp in (("KXBTC15M", 2), ("KXETH15M", 2), ("KXXRP15M", 4)):
        r = RL.rule_for(series, LATER)
        assert r.known and r.comparison_operator == RL.GTE and r.settlement_decimal_places == dp, series
        assert r.rounding_method == "NEAREST" and r.tie_behavior == "UNSPECIFIED" and r.rule_url and r.observed_date
    sol = RL.rule_for("KXSOL15M", LATER)
    assert sol is not None and not sol.known and sol.comparison_operator is None and sol.settlement_decimal_places is None
    btc, eth, xrp = (RL.rule_for(x, LATER) for x in ("KXBTC15M", "KXETH15M", "KXXRP15M"))
    for rule, k, below, above, onto_yes, onto_no in ((btc, 100000.0, 99999.5, 100000.5, 99999.996, 99999.994),
                                                    (eth, 3500.25, 3500.0, 3500.5, 3500.246, 3500.244),
                                                    (xrp, 0.5124, 0.5120, 0.5130, 0.51236, 0.51234)):
        oc = RL.official_outcome
        assert oc([below], k, rule)["outcome"] == "no" and oc([above], k, rule)["outcome"] == "yes"
        eq = oc([k], k, rule)
        assert eq["outcome"] == "yes" and eq["at_strike"] and eq["status"] == "OK"   # equality under AT_LEAST -> YES
        up = oc([onto_yes], k, rule)
        assert up["settlement_value"] == k and up["outcome"] == "yes"              # rounds ONTO the strike -> YES
        assert oc([onto_no], k, rule)["outcome"] == "no"                            # rounds below -> NO
    # exact .5 ties: the documentation does not define the tie rule -> never invented
    tie = RL.official_outcome([99999.995], 100000.0, btc)
    assert tie["status"] == "ROUNDING_TIE_UNRESOLVED" and tie["outcome"] is None and tie["tie_candidates"] == (99999.99, 100000.0)
    inv = RL.official_outcome([99999.995], 99000.0, btc)
    assert inv["status"] == "ROUNDING_TIE_OUTCOME_INVARIANT" and inv["outcome"] == "yes" and inv["settlement_value"] is None
    assert RL.official_outcome([0.51245], 0.5125, xrp)["status"] == "ROUNDING_TIE_UNRESOLVED"
    # unknown semantics fail closed
    assert RL.official_outcome([1.0], 1.0, None)["status"] == "RULE_UNKNOWN"
    assert RL.official_outcome([150.0], 150.0, sol)["status"] == "RULE_UNVERIFIED"
    assert RL.rule_for("KXDOGE15M", LATER) is None
    for bad in ({"comparison_operator": "ABOVE_ISH"}, {"rounding_method": "BANKERS"}, {"tie_behavior": "COIN_FLIP"}):
        try:
            _rp(btc, **bad); raise AssertionError(f"accepted {bad}")
        except ValueError:
            pass
    try:
        RL.compare(1.0, 1.0, "WEIRD"); raise AssertionError("unknown comparator accepted")
    except ValueError:
        pass
    try:
        SettlementWindowPolicy("x", 1, round_decimals=2); raise AssertionError("window-policy rounding accepted")
    except ValueError:
        pass
    # through the engine: the unrounded mean is kept, the official-precision value decides
    m = SettlementMarket("KXBTC15M-TEST61", "BTC", LATER, "BRTI", strike=100000.0, series="KXBTC15M")
    r = reconstruct(m, _flat_obs("BRTI", 99999.996, LATER))
    assert abs(r.final_value - 99999.996) < 1e-9 and r.settlement_value == 100000.0 and r.reconstructed_outcome == "yes"
    assert Flag.AT_STRIKE.value in r.state.flags and r.settlement["rule_fingerprint"] == btc.fingerprint()
    rs = reconstruct(SettlementMarket("KXSOL15M-T", "SOL", LATER, "SOLUSD_RTI", strike=150.0, series="KXSOL15M"),
                     _flat_obs("SOLUSD_RTI", 151.0, LATER, asset="SOL"))
    assert rs.final_value is not None and rs.reconstructed_outcome is None and Flag.RULE_UNVERIFIED.value in rs.state.flags
    ru = reconstruct(SettlementMarket("KXFOO15M-T", "BTC", LATER, "BRTI", strike=1.0, series="KXFOO15M"),
                     _flat_obs("BRTI", 2.0, LATER))
    assert ru.reconstructed_outcome is None and Flag.RULE_UNKNOWN.value in ru.state.flags
    # versioned binding: markets stay tied to the version covering their close
    v1 = _rp(btc, effective_to_ms=LATER)
    v2 = _rp(btc, rule_id="kxbtc15m_rules_v2", version=2, comparison_operator=RL.GT, effective_from_ms=LATER)
    assert RL.rule_for("KXBTC15M", LATER - 1, (v1, v2)) is v1 and RL.rule_for("KXBTC15M", LATER, (v1, v2)) is v2
    try:
        RL.rule_for("KXBTC15M", LATER, (btc, v2)); raise AssertionError("overlapping versions accepted")
    except ValueError:
        pass
    assert v1.fingerprint() != btc.fingerprint() and RL.rule_set_fingerprint((v1, v2)) != RL.rule_set_fingerprint()
    from settlement import fingerprint as sfp
    assert sfp.build()["contract_rules"]["rule_set_fingerprint"] == RL.rule_set_fingerprint()
    from settlement.checkpoints import labels_for
    lab = labels_for(m, _flat_obs("BRTI", 99999.996, LATER))
    assert lab["final_settlement_value"] == 100000.0 and abs(lab["final_unrounded_mean"] - 99999.996) < 1e-9
    assert lab["settlement_rule_id"] == "kxbtc15m_rules_v1" and lab["settlement_rule_fingerprint"] == btc.fingerprint()


def test_precision_aware_verification():
    from settlement.resolution import summarize_rows, value_checks, verify_market, index_observations
    from settlement.policy import reconstruction_policy, window_policy
    from settlement.types import OfficialResolution, SettlementMarket
    ok = value_checks(0.51239, 0.5124, None, 0.5124, 4)
    assert ok["expiration_value_exact_after_rounding"] is True and ok["expiration_value_within_half_unit"] is True
    far = value_checks(0.5174, 0.5174, None, 0.5124, 4)          # 0.005 off: inside a universal 0.01 tolerance ...
    assert far["expiration_value_exact_after_rounding"] is False and far["expiration_value_within_half_unit"] is False
    assert abs(far["expiration_value_raw_abs_diff"] - 0.005) < 1e-12 and far["half_unit"] == 0.00005
    btc = value_checks(100000.004, 100000.0, None, 100000.0, 2)
    assert btc["expiration_value_exact_after_rounding"] and btc["expiration_value_within_half_unit"]
    unk = value_checks(151.0, None, None, 151.0, None)
    assert unk["expiration_value_exact_after_rounding"] is None and unk["expiration_value_raw_abs_diff"] == 0.0
    tie = value_checks(99999.995, None, (99999.99, 100000.0), 100000.0, 2)
    assert tie["expiration_value_exact_after_rounding"] is True
    m = SettlementMarket("KXXRP15M-T61", "XRP", LATER, "XRPUSD_RTI", strike=0.5100, series="KXXRP15M")
    by = index_observations(_flat_obs("XRPUSD_RTI", 0.5174, LATER, asset="XRP"))
    row = verify_market(m, OfficialResolution(m.ticker, "yes", 0.5124, "t"), by, window_policy(), reconstruction_policy())
    assert row["agreement"] is True and row["expiration_value_exact_after_rounding"] is False
    assert row["official_precision_dp"] == 4 and row["reconstructed_value"] == 0.5174
    s = summarize_rows([row])
    assert s["expiration_value"]["exact_after_rounding"] == 0 and s["expiration_value"]["by_asset"]["XRP"]["official_precision_dp"] == 4
    import settlement.resolution as SR
    assert not hasattr(SR, "VALUE_TOLERANCE")                        # no universal tolerance exists any more


def _gate_with(policies, verdict, **cfg):
    import settlement.resolution as SR
    orig = SR.verify_all
    SR.verify_all = lambda *a, **k: {"policies": policies, "convention_verdict": verdict}
    try:
        return LB.convention_gate([], {}, [], synthetic=cfg.pop("synthetic", False), config=LB.LabelGateConfig(**cfg))
    finally:
        SR.verify_all = orig


def _pol(compared, exact, agree):
    return {"expiration_value": {"compared": compared, "exact_after_rounding": exact, "within_half_unit": exact,
                                 "max_raw_abs_diff": 0.0}, "compared": compared, "agree": agree, "disagree": compared - agree}


def test_convention_ambiguity():
    P, ALT = LB.LabelGateConfig().label_window_policy, "cf_rti_60s_end_incl_asof_v1"
    g = _gate_with({P: _pol(60, 60, 60), ALT: _pol(60, 5, 40)}, {"status": "EVALUATED", "best_policies": [P]})
    assert g["status"] == "VERIFIED" and g["reason_code"] == "UNIQUE_CONVENTION" and g["convention"] == P
    g = _gate_with({P: _pol(60, 60, 60), ALT: _pol(60, 60, 60)}, {"status": "EVALUATED_TIED", "best_policies": [P, ALT]})
    assert g["status"] == "SETTLEMENT_UNVERIFIED" and g["reason_code"] == "AMBIGUOUS_CONVENTION" and g["convention"] is None
    g = _gate_with({P: _pol(60, 60, 60), ALT: _pol(60, 59, 60)}, {"status": "EVALUATED", "best_policies": [P]})
    assert g["status"] == "SETTLEMENT_UNVERIFIED" and g["reason_code"] == "AMBIGUOUS_CONVENTION"   # both pass the gate
    g = _gate_with({P: _pol(60, 60, 60), ALT: _pol(60, 1, 60)}, {"status": "EVALUATED_TIED", "best_policies": [P, ALT]})
    assert g["status"] == "SETTLEMENT_UNVERIFIED" and g["reason_code"] == "AMBIGUOUS_CONVENTION"   # verifier tie respected
    g = _gate_with({P: _pol(60, 10, 50), ALT: _pol(60, 60, 60)}, {"status": "EVALUATED", "best_policies": [ALT]})
    assert g["status"] == "SETTLEMENT_UNVERIFIED" and g["reason_code"] == "REQUIRES_VERSIONED_POLICY_MIGRATION"
    assert g["convention"] is None
    g = _gate_with({P: _pol(60, 10, 50), ALT: _pol(60, 11, 50)}, {"status": "EVALUATED", "best_policies": [ALT]})
    assert g["status"] == "SETTLEMENT_UNVERIFIED" and g["reason_code"] == "NO_CONVENTION_PASSES"
    g = _gate_with({P: _pol(10, 10, 10)}, {"status": "INSUFFICIENT_DATA"})
    assert g["status"] == "SETTLEMENT_UNVERIFIED" and g["reason_code"] == "INSUFFICIENT_MARKETS"
    g = _gate_with({P: _pol(60, 60, 60)}, {"status": "EVALUATED", "best_policies": [P]}, synthetic=True)
    assert g["status"] == "SYNTHETIC_ONLY"
    # the realistic case: an exact 1-s feed makes ASOF / EXACT / BUCKET indistinguishable -> never VERIFIED
    from settlement.cf_live import parse_kalshi_cfb_message
    from settlement.kalshi_markets import parse_market
    from settlement.synthetic import demo_dataset
    d = demo_dataset(40)
    ms, rs = [], {}
    for mj in d["markets"]:
        m, r, _ = parse_market(mj)
        ms.append(m)
        rs[m.ticker] = r
    obs = [parse_kalshi_cfb_message(ln["message"], ln["receive_ts_ms"], ln["seq"])[0] for ln in d["live_lines"]]
    g = LB.convention_gate(ms, rs, obs, config=LB.LabelGateConfig(min_markets_compared=30))
    assert g["status"] == "SETTLEMENT_UNVERIFIED" and g["reason_code"] == "AMBIGUOUS_CONVENTION"
    assert len(g["competing_conventions_passing"]) == 3 and P in g["competing_conventions_passing"]
    # reconstructed labels stay non-gold; official results stay gold
    assert LB.market_label({"reconstructed_outcome": "yes"}, g["status"]) == (1, "SETTLEMENT_UNVERIFIED")
    assert LB.market_label({"official_result": "no", "reconstructed_outcome": "no"}, g["status"]) == (0, "OFFICIAL_RESULT")


def _cf_events(source, obs_source, value, close, seq0, asset="BTC", index_id="BRTI"):
    from market_data.types import EventType, MarketEvent
    from settlement.types import SettlementObservation
    out = []
    for i, t in enumerate(range(close - 61_000, close + 1, 1000)):
        o = SettlementObservation(asset, index_id, obs_source, value, t, receive_ts_ms=t + 100)
        out.append(MarketEvent(source, asset, EventType.INDEX_VALUE, index_id, t, t + 100, seq0 + i,
                               {"index_id": index_id, "value": value, "amend_ts_ms": None, "repeat_of_previous": None,
                                "observation": o.to_dict()}))
    return out


def _resolution(ticker, result, ev, close, source="kalshi", asset="BTC"):
    from market_data.types import EventType, MarketEvent
    return MarketEvent(source, asset, EventType.RESOLUTION, ticker, close + 60_000, close + 60_000, 999_999,
                       {"ticker": ticker, "result": result, "expiration_value": ev})


def test_rejected_source_labels():
    from settlement.checkpoints import labels_for
    from settlement.types import SettlementMarket
    close, tk = LATER, "KXBTC15M-T61REJ"
    m = SettlementMarket(tk, "BTC", close, "BRTI", strike=100000.0, series="KXBTC15M")
    good = _cf_events("cf_via_kalshi", "cfb_ws_via_kalshi", 100010.0, close, 0)
    wrong = _cf_events("cf_direct", "cfb_ws", 99000.0, close, 10_000)             # source B: REJECT + wrong values
    res = _resolution(tk, "yes", 100010.0, close)
    rej = {"cf_direct:BTC"}

    def lab(events, rejected):
        obs, rs, rep = DS.settlement_inputs(events, rejected)
        flat = [o for v in obs.values() for o in v]
        return labels_for(m, flat, rs.get(tk)), obs, rs, rep, flat
    with_b, obs1, rs1, rep1, flat1 = lab(good + wrong + [res], rej)
    without_b, obs2, rs2, _rep2, flat2 = lab(good + [res], rej)
    assert with_b == without_b and obs1 == obs2 and rs1 == rs2                       # B is invisible to labels
    assert rep1["index_values_rejected_source"] == len(wrong) and with_b["reconstructed_outcome"] == "yes"
    assert with_b["final_settlement_value"] == 100010.0
    import settlement.resolution as SR
    v1 = SR.verify_all([m], rs1, flat1)
    v2 = SR.verify_all([m], rs2, flat2)
    assert v1["policies"] == v2["policies"]                                          # convention verifier identical
    leaked, *_ = lab(good + wrong + [res], set())                                   # (what a leak would do)
    assert leaked != with_b
    # every settlement source rejected -> no reconstructed value / outcome; nothing can become a gold reconstruction
    none_lab, obs3, rs3, _r, _f = lab(good + wrong, {"cf_direct:BTC", "cf_via_kalshi:BTC"})
    assert obs3 == {} and none_lab["final_settlement_value"] is None and none_lab["reconstructed_outcome"] is None
    assert LB.market_label(none_lab, "VERIFIED") == (None, "UNLABELED")
    # a rejected Kalshi resolution source contributes no official label
    k_lab, _o, rs4, rep4, _f = lab(good + [res], {"kalshi:BTC"})
    assert rs4 == {} and rep4["resolutions_rejected_source"] == 1 and k_lab["official_result"] is None
    assert LB.market_label(k_lab, "SETTLEMENT_UNVERIFIED") == (1, "SETTLEMENT_UNVERIFIED")
    # an untrusted resolution path is never an official label
    _o, rs5, rep5 = DS.settlement_inputs(good + [_resolution(tk, "yes", 100010.0, close, source="coinbase")], set())
    assert rs5 == {} and rep5["resolutions_untrusted_path"] == 1
    # a DEGRADED session keeps its surviving sources: only the rejected pair is dropped
    assert rep1["index_values_used"] == len(good)


def _awkward_meta(n_slots=5, cps=(300, 120, 60), assets=("BTC",), start=1_700_000_000_000):
    rows = []
    for sl in range(n_slots):
        close = start + (sl + 1) * 900_000
        for a in assets:
            for cp in cps:
                rows.append({"market_ticker": f"KX{a}15M-{sl:04d}", "asset": a, "close_ts_ms": close,
                             "checkpoint_ts_ms": close - cp * 1000})
    return rows


def test_ridge_inner_split():
    # the OLD row-based 75/25 cut splits a market (5 markets x 3 checkpoints = 15 rows; cut at row 11)
    meta = _awkward_meta()
    order = sorted(range(len(meta)), key=lambda i: (meta[i]["close_ts_ms"], meta[i]["market_ticker"]))
    cut = int(len(order) * 0.75)
    old_a = {meta[i]["market_ticker"] for i in order[:cut]}
    old_b = {meta[i]["market_ticker"] for i in order[cut:]}
    assert old_a & old_b, "the awkward dataset must defeat the old row cut"
    cfg0 = SP.SplitConfig(max_causal_lookback_ms=0, label_horizon_ms=0)
    tr, va, pg, info = SP.inner_split(SP.market_table(meta), 0.25, cfg0)
    assert not set(tr) & set(va) and info["status"] == "OK" and pg == []
    # same-close BTC / ETH / SOL / XRP markets stay together
    meta4 = _awkward_meta(n_slots=9, assets=("BTC", "ETH", "SOL", "XRP"))
    tab = SP.market_table(meta4)
    tr, va, pg, info = SP.inner_split(tab, 0.25, cfg0)
    side = {m: "T" for m in tr} | {m: "V" for m in va} | {m: "P" for m in pg}
    for c in {e["close_ts_ms"] for e in tab}:
        assert len({side[e["market"]] for e in tab if e["close_ts_ms"] == c}) == 1
    close = {e["market"]: e["close_ts_ms"] for e in tab}
    assert max(close[m] for m in tr) < min(close[m] for m in va)
    # the causal purge removes the boundary neighbours (lookback 25 min + label horizon)
    cfgp = SP.SplitConfig(max_causal_lookback_ms=1_500_000, label_horizon_ms=0)
    trp, vap, pgp, infop = SP.inner_split(tab, 0.25, cfgp)
    first_val_cp = min(e["first_checkpoint_ts_ms"] for e in tab if e["market"] in set(vap))
    assert pgp and all(close[m] > first_val_cp - 1_500_000 for m in pgp)
    assert all(close[m] <= first_val_cp - 1_500_000 for m in trp) and infop["purge_ms"] == 1_500_000
    # the ridge model uses exactly that split (market level), fits candidate scalers on inner-training rows only
    rng = random.Random(9)
    meta_r = _awkward_meta(n_slots=60, assets=("BTC", "ETH"))
    X = [[rng.gauss(0, 1)] for _ in meta_r]
    y = [rng.randint(0, 1) for _ in meta_r]
    leg = [0.5] * len(meta_r)
    rm = MD.RidgeLogisticModel(inner_split_cfg=cfgp).fit(X, leg, y, [1.0] * len(meta_r), meta=meta_r)
    ii = rm.inner_info
    assert ii["used_train_markets"] == sorted(ii["train_markets"]) and ii["used_validation_markets"] == sorted(ii["validation_markets"])
    assert not set(ii["used_train_markets"]) & set(ii["used_validation_markets"]) and ii["purged_markets"]
    assert ii["scaler_rows"] == sum(1 for r in meta_r if r["market_ticker"] in set(ii["train_markets"]))
    # inside a real fold: outer DEVELOPMENT test blocks and the FINAL_HOLDOUT never enter the inner training
    m, recs, st, parts, folds = _fold_setup()
    trf, tef = st.fold_rows(folds[-1])
    _p, _b, info = AB.fit_fold(trf, tef, ["SPOT_PRICE_MOMENTUM"], st.fam_names, st.col_index, st.role_of,
                               "C_ridge_logistic", st.cfg, st.holdout)
    ri = info["ridge_inner"]
    assert ri["status"] == "OK" and set(ri["used_train_markets"]) <= set(folds[-1]["train"])
    assert not set(ri["used_train_markets"]) & (set(folds[-1]["test"]) | st.holdout)
    assert not set(ri["used_validation_markets"]) & (set(folds[-1]["test"]) | st.holdout)


# ═══════════════════ Step 6.2 corrections ═══════════════════
def _btc_rule_obj(ticker, close_ms, strike, dp_words="2", comparator="is at least", index="Bitcoin Real-Time Index (BRTI)",
                  extra=None, asset_series="KXBTC15M"):
    import datetime as _dt
    obj = {"ticker": ticker, "close_time": _dt.datetime.fromtimestamp(close_ms / 1000, _dt.timezone.utc).isoformat()
           .replace("+00:00", "Z"), "floor_strike": float(strike), "status": "settled", "event_ticker": ticker,
           "rules_primary": f"If the simple average of the sixty seconds of the CF Benchmarks {index} before the close "
                            f"{comparator} {strike}, then the market resolves to Yes.",
           "rules_secondary": f"The 60-second average of the RTI prices is the official value, rounded to the nearest "
                              f"{dp_words} decimal places."}
    obj.update(extra or {})
    return obj


def _market_from(obj, capture_ts):
    from settlement.kalshi_markets import parse_market
    m, _r, issues = parse_market(obj, capture_ts_ms=capture_ts)
    assert m is not None, issues
    return m


def test_historical_rule_provenance():
    from dataclasses import replace as _rp
    from settlement import market_rules as MR
    from settlement.checkpoints import labels_for
    from settlement.reconstruction import reconstruct
    from settlement.rules import OBSERVED_2026_09_29_MS as T0
    from settlement.types import Flag, SettlementMarket
    before = T0 - 86_400_000                                    # closed one day BEFORE the rule was observed
    m = SettlementMarket("KXBTC15M-T62OLD", "BTC", before, "BRTI", strike=100000.0, series="KXBTC15M")
    obs = _flat_obs("BRTI", 100000.5, before)
    r = reconstruct(m, obs)
    assert r.final_value is not None and r.reconstructed_outcome == "yes"             # diagnostic value / outcome
    assert r.settlement["rule_status"] == MR.RULE_HISTORICALLY_UNVERIFIED and not r.settlement["rule_gold_eligible"]
    assert Flag.RULE_OBSERVED_AFTER_CLOSE.value in r.state.flags
    lab = labels_for(m, obs)
    assert LB.market_label(lab, "VERIFIED") == (1, "RULE_UNVERIFIED_FOR_MARKET")        # never gold
    assert not LB.is_gold("RULE_UNVERIFIED_FOR_MARKET")
    # the historical market's OWN trusted rules_primary makes it verifiable
    snapped = _market_from(_btc_rule_obj(m.ticker, before, "100000.00"), before - 900_000)
    assert snapped.rule_snapshot["rules_primary"].startswith("If the simple average")
    lab2 = labels_for(snapped, obs)
    assert lab2["settlement_rule_status"] == MR.RULE_VERIFIED_FOR_MARKET and lab2["reconstructed_outcome"] == "yes"
    assert LB.market_label(lab2, "VERIFIED") == (1, "RECONSTRUCTED_VERIFIED")
    assert lab2["settlement_rule_text_sha256"] == snapped.rule_snapshot["rule_text_sha256"]
    # a market closing after the observation: the current rule is valid when every other gate passes
    after = _rp(m, ticker="KXBTC15M-T62NEW", close_ts_ms=T0 + 900_000)
    lab3 = labels_for(after, _flat_obs("BRTI", 100000.5, T0 + 900_000))
    assert lab3["settlement_rule_status"] == MR.RULE_CURRENT_OBSERVED
    assert LB.market_label(lab3, "VERIFIED") == (1, "RECONSTRUCTED_VERIFIED")
    assert LB.market_label(lab3, "SETTLEMENT_UNVERIFIED") == (1, "SETTLEMENT_UNVERIFIED")
    # official Kalshi results stay authoritative regardless
    assert LB.market_label(dict(lab, official_result="yes"), "SETTLEMENT_UNVERIFIED") == (1, "OFFICIAL_RESULT")


def test_market_rule_capture_and_parser():
    from settlement import market_rules as MR
    from settlement.rules import GT, GTE
    from settlement.reconstruction import reconstruct
    close = LATER
    obj = _btc_rule_obj("KXBTC15M-T62CAP", close, "100000.00",
                        extra={"series_ticker": "KXBTC15M", "updated_time": "2026-10-01T00:00:00Z",
                               "fee_multiplier_override": None})
    m = _market_from(obj, close - 800_000)
    sn = m.rule_snapshot
    assert sn["rules_primary"] == obj["rules_primary"] and sn["rules_secondary"] == obj["rules_secondary"]   # verbatim
    assert sn["rule_text_sha256"] == MR.rule_text_sha256(obj["rules_primary"], obj["rules_secondary"])
    assert sn["ticker"] == m.ticker and sn["series_ticker"] == "KXBTC15M" and sn["event_ticker"] == m.ticker
    assert sn["capture_ts_ms"] == close - 800_000 and sn["market_updated_ts"] == "2026-10-01T00:00:00Z"
    assert sn["source"] == "kalshi_market_api" and sn["schema_fingerprint"] and sn["fee_metadata_state"] == "CAPTURED"
    rule, info = MR.resolve_market_rule(m)
    assert info["status"] == MR.RULE_VERIFIED_FOR_MARKET and info["parsed"]["status"] == "PARSED"
    assert rule.comparison_operator == GTE and rule.settlement_decimal_places == 2 and info["parsed"]["target_value"] == "100000.00"
    # the deterministic parser: only the documented wording
    P = MR.parse_rule_text
    ok_txt = "the 60-second average of the Bitcoin Real-Time Index is {c} 5, rounded to the nearest {d} decimal places"
    assert P(ok_txt.format(c="above", d="2"), "", "BTC")["comparison_operator"] == GT
    assert P(ok_txt.format(c="at least", d="two"), "", "BTC")["settlement_decimal_places"] == 2
    for bad in (ok_txt.format(c="at least 5 and above", d="2"), ok_txt.format(c="greater than", d="2"),
                ok_txt.format(c="at least", d="some"), ok_txt.replace("Bitcoin Real-Time Index", "Ether Real-Time Index"),
                ok_txt.replace("60-second average", "closing print").format(c="at least", d="2"), ""):
        assert P(bad.format(c="at least", d="2") if "{" in bad else bad, "", "BTC")["status"] == "UNRECOGNIZED", bad
    src = open(os.path.join(HERE, "settlement", "market_rules.py"), encoding="utf-8").read()
    assert not {m_ for m_ in _imports(os.path.join(HERE, "settlement", "market_rules.py"))} - {
        "hashlib", "json", "re", "dataclasses", "settlement.rules", "settlement.assets"}, "parser must stay deterministic"
    assert "anthropic" not in src.lower() and "openai" not in src.lower()
    # per-market text contradicting the static rule -> RULE_CONFLICT, never silently overridden
    c4 = _market_from(_btc_rule_obj("KXBTC15M-T62C4", close, "100000.00", dp_words="4"), close - 800_000)
    r4 = reconstruct(c4, _flat_obs("BRTI", 100000.5, close))
    assert r4.settlement["rule_status"] == MR.RULE_CONFLICT and r4.reconstructed_outcome is None
    tgt = _market_from(_btc_rule_obj("KXBTC15M-T62TG", close, "99999.00"), close - 800_000)
    assert MR.resolve_market_rule(__import__("dataclasses").replace(tgt, strike=100000.0))[1]["status"] == MR.RULE_CONFLICT
    unrec = _market_from(_btc_rule_obj("KXBTC15M-T62UN", close, "100000.00", comparator="closes higher than"),
                         close - 800_000)
    ru = reconstruct(unrec, _flat_obs("BRTI", 100000.5, close))
    assert ru.settlement["rule_status"] == MR.RULE_TEXT_UNRECOGNIZED and ru.reconstructed_outcome is None   # no fallback
    two = MR.with_snapshots(m, [sn, dict(sn, rules_primary=sn["rules_primary"] + " ",
                                         rule_text_sha256=MR.rule_text_sha256(sn["rules_primary"] + " ",
                                                                              sn["rules_secondary"]))])
    assert MR.resolve_market_rule(two)[1]["status"] == MR.RULE_CONFLICT
    untrusted = __import__("dataclasses").replace(m, rule_snapshot=dict(sn, source="scraped_web_page"))
    assert MR.resolve_market_rule(untrusted)[1]["status"] == MR.RULE_SNAPSHOT_UNTRUSTED
    # SOL: never inferred; a SOL market's OWN rule text may verify that market
    from settlement.types import SettlementMarket
    sol = SettlementMarket("KXSOL15M-T62", "SOL", close, "SOLUSD_RTI", strike=150.0, series="KXSOL15M")
    assert MR.resolve_market_rule(sol)[1]["status"] == MR.RULE_UNVERIFIED
    sol_obj = _btc_rule_obj("KXSOL15M-T62", close, "150.0000", dp_words="4", index="Solana Real-Time Index (SOLUSD_RTI)")
    solm = _market_from(sol_obj, close - 800_000)
    sr, si = MR.resolve_market_rule(solm)
    assert si["status"] == MR.RULE_VERIFIED_FOR_MARKET and sr.settlement_decimal_places == 4 and sr.comparison_operator == GTE
    # the Step-3 collector retains the snapshot and the event's fee metadata (synthetic session)
    from market_data.replay import load_sessions
    from market_data.types import EventType
    s3 = load_sessions([session()], include_raw=True)
    states = [e for e in s3.events if e.event_type == EventType.MARKET_STATE]
    assert states and all(e.payload["contract"]["rules_primary"] for e in states)
    later_states = [e for e in states if e.payload["contract"]["fee_metadata_state"] == "CAPTURED"]
    assert later_states and "fee_multiplier_override" in later_states[-1].payload["contract"]["fee_metadata"]
    assert any(r_.stream == "event" for r_ in s3.raw)                            # the event's raw text is stored


def test_rule_fingerprinting():
    a = {"session_id": "S1", "raw_store_sha256": "a" * 64,
         "rule_provenance": {"KXBTC15M-X": {"rule_text_sha256": "1" * 64, "rule_status": "RULE_VERIFIED_FOR_MARKET",
                                            "rule_fingerprint": "f" * 64}}}
    f1, body = DS.dataset_fingerprint([a], "u", {"s": 1}, DS.CHECKPOINT_GRID_S, ("BTC",), [])
    b = dict(a, rule_provenance={"KXBTC15M-X": dict(a["rule_provenance"]["KXBTC15M-X"], rule_text_sha256="2" * 64)})
    f2, _ = DS.dataset_fingerprint([b], "u", {"s": 1}, DS.CHECKPOINT_GRID_S, ("BTC",), [])
    c = dict(a, rule_provenance={"KXBTC15M-X": dict(a["rule_provenance"]["KXBTC15M-X"], rule_status="RULE_CONFLICT")})
    f3, _ = DS.dataset_fingerprint([c], "u", {"s": 1}, DS.CHECKPOINT_GRID_S, ("BTC",), [])
    assert len({f1, f2, f3}) == 3 and body["market_rules"][0][1] == "1" * 64
    d = session()
    q = QU.validate_session(d)
    m = DS.build_matrix([d], ("BTC",), quality_reports={d: q}, synthetic_mode=True, include_perp=False, include_micro=False)
    rp = m.meta["session_meta"][0]["rule_provenance"]
    assert rp and all(v["rule_text_sha256"] for v in rp.values())
    assert m.meta["fingerprint_body"]["market_rules"] and all(r_["fee_context"] is None or "fee_metadata" in r_["fee_context"]
                                                              for r_ in m.rows)
    from settlement.market_rules import RULE_VERIFIED_FOR_MARKET
    assert all(v["rule_status"] == RULE_VERIFIED_FOR_MARKET for v in rp.values())    # BTC synthetic text parses


def test_fee_schedules_centicent():
    from decimal import Decimal as D
    new = EC.KALSHI_GENERAL_2026_07_07
    assert new.rounding == "ROUND_UP_FEE_PLUS_COST_TO_CENTICENT" and new.effective_from_ms == EC.JULY_2026_START_MS
    cases = ((1, "0.50", "0.0175", "0.5000"), (10, "0.50", "0.1750", "5.0000"), (100, "0.50", "1.7500", "50.0000"),
             (1, "0.01", "0.0007", "0.0100"), (3, "0.37", "0.0490", "1.1100"), (7, "0.63", "0.1143", "4.4100"))
    for c, p, fee, cost in cases:
        r = EC.fee_for_fills([(D(p), D(c))], new, 1)
        raw = D("0.07") * c * D(p) * (1 - D(p))
        assert r["raw_fee"] == raw and r["fee"] == D(fee) and r["position_cost"] == D(cost), (c, p, r)
        assert r["total"] == r["fee"] + r["position_cost"] and r["total"] % D("0.0001") == 0
        assert r["total"] >= raw + r["position_cost"] and r["total"] - (raw + r["position_cost"]) < D("0.0001")
        assert all(isinstance(r[k], D) for k in ("raw_fee", "fee", "position_cost", "total"))    # exact decimals
    old = EC.KALSHI_GENERAL_PRE_2026_07_07
    assert EC.fee_for_fills([(D("0.50"), D(1))], old, 1)["fee"] == D("0.02")                     # cent rounding kept
    assert EC.fee_for_fills([(D("0.37"), D(3))], old, 1)["fee"] == D("0.05")
    assert EC.fee_for_fills([(D("0.50"), D("2.5"))], new, 1)["fee"] == D("0.0438")               # fractional qty
    assert EC.fee_for_fills([(D("0.50"), D(1))], new, 1, maker=True)["fee"] == D("0.0044")        # maker 0.0175
    fm = EC.FeeModel()
    NEW, OLD = EC.JULY_2026_START_MS + 60_000, EC.JULY_2026_START_MS - 60_000
    known = {"fee_metadata": {"fee_type": "general", "fee_multiplier": 1, "fee_type_override": None,
                              "fee_multiplier_override": None}}
    r = fm.fee([(D("0.50"), D(1))], "KXBTC15M-A", NEW, known)
    assert r["status"] == "FEE_VERIFIED" and r["fee"] == D("0.0175") and r["schedule_id"] == "kalshi_general_2026_07_07"
    # a Kalshi override multiplier takes precedence over the default / series multiplier
    ov = {"fee_metadata": dict(known["fee_metadata"], fee_multiplier_override=2)}
    r2 = fm.fee([(D("0.50"), D(1))], "KXBTC15M-A", NEW, ov)
    assert r2["fee"] == D("0.0350") and r2["override_state"] == "OVERRIDE" and r2["multiplier"] == 2
    # historical trades keep the schedule that governed them (no retroactive centicent rounding)
    r3 = fm.fee([(D("0.37"), D(3))], "KXBTC15M-A", OLD, known)
    assert r3["schedule_id"] == "kalshi_general_pre_2026_07_07" and r3["fee"] == D("0.05") and r3["status"] == "FEE_UNVERIFIED"
    # FEE_VERIFIED needs every condition
    assert fm.fee([(D("0.5"), D(1))], "KXBTC15M-A", NEW, None)["status"] == "FEE_UNVERIFIED"           # nothing captured
    nomult = {"fee_metadata": {"fee_type_override": None, "fee_multiplier_override": None}}
    assert fm.fee([(D("0.5"), D(1))], "KXBTC15M-A", NEW, nomult)["status"] == "FEE_UNVERIFIED"         # multiplier unknown
    special = {"fee_metadata": dict(known["fee_metadata"], fee_type_override="special_crypto")}
    assert fm.fee([(D("0.5"), D(1))], "KXBTC15M-A", NEW, special)["status"] == "FEE_UNVERIFIED"
    assert fm.fee([(D("0.5"), D(1))], "KXBTC15M-A", None, known)["status"] == "FEE_UNKNOWN"
    assert "KXBTC15M" not in repr(new.series_scope) and new.series_scope == ("*",)                     # not hardcoded
    # per-row captured fee context reaches the economics (synthetic session: override fields captured, no multiplier)
    ctx_rows = [r_ for r_ in DS.build_matrix([session()], ("BTC",), synthetic_mode=True, include_perp=False,
                                            include_micro=False).rows if r_["fee_context"]]
    assert ctx_rows and ctx_rows[-1]["fee_context"]["fee_metadata"].get("fee_multiplier_override", "absent") is None


def test_cf_exact_decimal():
    from settlement.cf_live import parse_kalshi_cfb_message
    from settlement.reconstruction import reconstruct
    from settlement.synthetic import kalshi_message
    from settlement.types import SettlementMarket, SettlementObservation
    raw = "99999.994999999999999"                                  # 20 significant digits
    o, _a, _i = parse_kalshi_cfb_message(kalshi_message({"type": "value", "id": "BRTI", "value": raw, "time": LATER}))
    assert o.value_text == raw and o.value == 99999.995            # the float round-trip crosses the .5 boundary
    num = {"type": "cfbenchmarks_value", "sid": 1,
           "msg": {"data": '{"type":"value","id":"BRTI","value":99999.994999999999999,"time":%d}' % LATER}}
    o2, _a, _i = parse_kalshi_cfb_message(num)
    assert o2.value_text == raw                                    # a JSON NUMBER keeps its text too
    m = SettlementMarket("KXBTC15M-T62EX", "BTC", LATER, "BRTI", strike=100000.0, series="KXBTC15M")
    exact_obs = [SettlementObservation("BTC", "BRTI", "cfb_ws", float(raw), t, receive_ts_ms=t + 100, value_text=raw)
                 for t in range(LATER - 61_000, LATER + 1, 1000)]
    r = reconstruct(m, exact_obs)
    assert r.settlement_value == 99999.99 and r.reconstructed_outcome == "no" and r.settlement["status"] == "OK"
    lossy = [SettlementObservation("BTC", "BRTI", "cfb_ws", float(raw), t, receive_ts_ms=t + 100)
             for t in range(LATER - 61_000, LATER + 1, 1000)]
    rl = reconstruct(m, lossy)
    assert rl.settlement["status"] == "ROUNDING_TIE_UNRESOLVED"     # what a float round-trip would have done
    # typical CF values (<= 15 significant digits) round-trip exactly through a float (documented evidence)
    for t in ("109234.56", "3500.25", "0.51234567", "151.2345", "2.3456789012345"):
        assert repr(float(t)) == t or __import__("decimal").Decimal(repr(float(t))) == __import__("decimal").Decimal(t)


# ═══════════════════ 29k-29m Step 6.3 final pre-collection hardening ═══════════════════
RAW_TIE = "99999.994999999999999"            # 20 significant digits: the binary float is 99999.995, a 2-dp .5 tie


def _cf_market():
    from settlement.types import SettlementMarket
    return SettlementMarket("KXBTC15M-T63EX", "BTC", LATER, "BRTI", strike=100000.0, series="KXBTC15M")


def _cf_ticks():
    return range(LATER - 61_000, LATER + 1, 1000)


def _assert_exact_settlement(obs):
    from settlement.reconstruction import reconstruct
    assert obs and all(o.value_text == RAW_TIE for o in obs), {o.value_text for o in obs}
    r = reconstruct(_cf_market(), obs)
    assert r.settlement_value == 99999.99 and r.reconstructed_outcome == "no" and r.settlement["status"] == "OK", \
        (r.settlement_value, r.reconstructed_outcome, r.settlement)


def _import_script():
    import importlib
    sd = os.path.join(HERE, "scripts")
    if sd not in sys.path:
        sys.path.insert(0, sd)
    return importlib.import_module("settlement_import")


def test_cf_exact_all_paths():
    """Step 6.3: the exact CF decimal survives EVERY ingestion path (direct websocket, Kalshi wrapper, offline
    websocket import, REST / history import); a value that already went through a binary float fails closed."""
    from market_data.clock import FakeClock
    from market_data.collector import Collector
    from market_data.replay import load_sessions
    from market_data.sources.base import Ctx, Sequencer
    from market_data.sources.cf import CfDirectAdapter, CfViaKalshiAdapter
    from market_data.types import EventType
    from settlement.cache import load as load_store
    from settlement.cf_history import parse_cfb_historical, parse_cfb_historical_text
    from settlement.cf_live import parse_cfb_frame, parse_kalshi_cfb_message
    from settlement.schemas import (CF_DOC_VALUE, CFB_REST_ELEMENT, CFB_REST_HISTORICAL, CFB_WS_VALUE,
                                    LEGACY_NUMERIC_SCHEMA_SUFFIX)
    from settlement.types import SettlementObservation
    assert float(RAW_TIE) == 99999.995 and repr(float(RAW_TIE)) != RAW_TIE          # the adversarial boundary
    # documented provider contract: value is a decimal STRING; JSON numbers are legacy / non-standard only
    assert CF_DOC_VALUE == ("str",)
    assert CFB_WS_VALUE.version == CFB_REST_HISTORICAL.version == CFB_REST_ELEMENT.version == 2
    assert "STRING" in CFB_WS_VALUE.notes and "legacy" in CFB_WS_VALUE.notes and "legacy" in CFB_REST_HISTORICAL.notes

    def frame_text(t, numeric):
        v = RAW_TIE if numeric else f'"{RAW_TIE}"'
        return '{"type":"value","id":"BRTI","value":%s,"time":%d}' % (v, t)

    # (1) DIRECT CF websocket raw text -> Collector -> adapter -> MarketEvent (stored, replayed) -> settlement store
    #     -> SettlementObservation -> reconstruction. Documented STRING and legacy NUMBER both keep the exact text.
    for numeric in (False, True):
        d, store = tmpdir(), os.path.join(tmpdir(), "settle.jsonl")
        clock = FakeClock(LATER - 62_000)
        ad = CfDirectAdapter(["BTC"])
        col = Collector(d, ["BTC"], {"cf_direct": {"adapter": ad, "enabled": True, "critical": True}}, clock,
                        settlement_store_path=store, fsync=False,
                        live_features=False)
        for t in _cf_ticks():
            clock.advance(t + 100 - clock.wall_ms())
            col.on_message(ad, frame_text(t, numeric), clock.wall_ms(), clock.mono_ns())
        col.close()
        assert col.counts["failures"] == 0, col.counts
        evs = [e for e in load_sessions([col.dir]).events if e.event_type == EventType.INDEX_VALUE]
        assert len(evs) == 62 and all(e.payload["observation"]["value_text"] == RAW_TIE for e in evs)
        replayed = [SettlementObservation.from_dict(e.payload["observation"]) for e in evs]
        assert all(o.schema_id.endswith(LEGACY_NUMERIC_SCHEMA_SUFFIX) == numeric for o in replayed)
        _assert_exact_settlement(replayed)
        stored = [o for o in load_store(store).observations if o.index_id == "BRTI"]
        assert len(stored) == 62
        _assert_exact_settlement(stored)
    # an already-decoded object (binary float) is NOT accepted lossily: it fails closed, no event, no observation
    lossy = json.loads(frame_text(LATER, True))
    assert isinstance(lossy["value"], float)
    r = CfDirectAdapter(["BTC"]).parse(lossy, Ctx("s", Sequencer(), LATER + 100))
    assert not r.events and r.failures and "VALUE_PRECISION_LOST" in r.failures[0].reason
    o, iss = parse_cfb_frame(lossy)
    assert o is None and iss[-1].kind == "VALUE_PRECISION_LOST"

    # (2) Kalshi CF wrapper (live adapter): a numeric upstream token keeps its text, and is labelled legacy
    ka, seq, wrapped = CfViaKalshiAdapter(["BTC"]), Sequencer(), []
    for t in _cf_ticks():
        msg = json.dumps({"type": "cfbenchmarks_value", "sid": 1, "seq": t // 1000,
                          "msg": {"data": frame_text(t, True)}})
        r = ka.parse(msg, Ctx("s", seq, t + 100))
        assert not r.failures and len(r.events) == 1
        wrapped.append(SettlementObservation.from_dict(r.events[0].payload["observation"]))
    assert all(o.schema_id.endswith(LEGACY_NUMERIC_SCHEMA_SUFFIX) for o in wrapped)
    _assert_exact_settlement([SettlementObservation(**dict(o.to_dict(), source="cfb_ws")) for o in wrapped])
    o, _a, _i = parse_kalshi_cfb_message(json.dumps({"type": "cfbenchmarks_value",
                                                     "msg": {"data": frame_text(LATER, False)}}))
    assert o.value_text == RAW_TIE and not o.schema_id.endswith(LEGACY_NUMERIC_SCHEMA_SUFFIX)

    # (3) OFFLINE websocket import (cf-ws-jsonl, capture envelope with an OBJECT message; kalshi-ws-jsonl)
    si = _import_script()
    t_dir = tmpdir()
    cf_path, k_path = os.path.join(t_dir, "cf.jsonl"), os.path.join(t_dir, "k.jsonl")
    with open(cf_path, "w") as f:
        for i, t in enumerate(_cf_ticks()):
            f.write('{"receive_ts_ms": %d, "seq": %d, "message": %s}\n' % (t + 100, i, frame_text(t, True)))
    with open(k_path, "w") as f:
        for i, t in enumerate(_cf_ticks()):
            f.write(json.dumps({"receive_ts_ms": t + 100, "seq": i, "message": {
                "type": "cfbenchmarks_value", "sid": 1, "msg": {"data": frame_text(t, True)}}}) + "\n")
    recs, iss = si.parse_file("cf-ws-jsonl", cf_path)
    assert not [i for i in iss if i.kind != "SCHEMA_EXTRA_FIELDS"], iss
    _assert_exact_settlement([o for k, o in recs if k == "observation"])
    recs, iss = si.parse_file("kalshi-ws-jsonl", k_path)
    kobs = [o for k, o in recs if k == "observation"]
    assert len(kobs) == 62 and all(o.value_text == RAW_TIE for o in kobs)
    _assert_exact_settlement([SettlementObservation(**dict(o.to_dict(), source="cfb_ws")) for o in kobs])

    # (4) CF REST / history raw JSON -> importer / parser -> SettlementObservation -> reconstruction
    body = ('{"serverTime":"2026-09-29T00:00:00.000Z","payload":[%s]}'
            % ",".join('{"value":%s,"time":%d}' % (RAW_TIE if i % 2 else f'"{RAW_TIE}"', t)
                       for i, t in enumerate(_cf_ticks())))
    hist, iss = parse_cfb_historical_text(body, "BRTI")
    assert not iss and len(hist) == 62 and all(h.value_text == RAW_TIE for h in hist)
    assert {h.schema_id for h in hist} == {"cfb_rest_historical_values@2",
                                           "cfb_rest_historical_values@2" + LEGACY_NUMERIC_SCHEMA_SUFFIX}
    rest_path = os.path.join(t_dir, "brti.json")
    open(rest_path, "w").write(body)
    recs, iss = si.parse_file("cf-rest-json", rest_path, "BRTI")
    robs = [o for k, o in recs if k == "observation"]
    assert not iss and [o.value_text for o in robs] == [RAW_TIE] * 62
    # history carries no receive time; give it the live receipt so the same reconstruction applies
    _assert_exact_settlement([SettlementObservation(**dict(o.to_dict(), source="cfb_ws", receive_ts_ms=o.event_ts_ms + 100))
                              for o in robs])
    # an already-decoded REST payload: numeric elements are rejected one by one (never lossy), strings survive
    hist, iss = parse_cfb_historical(json.loads(body), "BRTI")
    assert len(hist) == 31 and all(h.value_text == RAW_TIE for h in hist)
    assert [i.kind for i in iss] == ["VALUE_PRECISION_LOST"] * 31


def test_kalshi_event_retry():
    """Step 6.3: an event ticker is marked fetched only after its event object was parsed and retained; transport
    failures and malformed bodies stay retryable (capped exponential backoff, no hammering)."""
    from market_data.clock import FakeClock
    from market_data.collector import Collector
    from market_data.kalshi_poller import KalshiPoller
    from market_data.replay import load_sessions
    from market_data.sources.kalshi import KalshiAdapter
    from market_data.synthetic import FakeKalshi, World
    from market_data.types import EventType

    class Flaky(FakeKalshi):
        def __init__(self, world, clock, script):
            super().__init__(world, clock)
            self.script, self.event_calls = list(script), 0

        def get_json(self, url, params=None):
            if "/events/" in url:
                self.event_calls += 1
                self.requests.append(("GET", url, dict(params or {})))
                step = self.script.pop(0) if self.script else "ok"
                if step == "transport":
                    raise ConnectionError("HTTP 503 (transient)")
                if step == "malformed":
                    return {"unexpected": "shape"}
                if step == "other":
                    return {"event": {"event_ticker": "KXBTC15M-OTHER", "fee_multiplier_override": None}}
            return super().get_json(url, params)

    def run_scenario(script, polls=40):
        w = World(["BTC"], C - 900_000, C + 900_000, seed=2)
        clock = FakeClock(C - 600_000)
        col = Collector(tmpdir(), ["BTC"], {}, clock, fsync=False, live_features=False)
        fk = Flaky(w, clock, script)
        ad = KalshiAdapter(["BTC"])
        p = KalshiPoller(ad, fk, col, clock, trades_every=1, event_retry_s=2, event_retry_max_s=8)
        et, first = None, None
        for i in range(polls):
            p.poll()
            if first is None:
                et = next(iter(ad.event_of.values()))
                first = (et in p.events_fetched, fk.event_calls, dict(p.event_attempts))
            clock.advance(1000)
        col.close()
        return p, ad, fk, et, first, col

    # attempt 1 -> transport failure, attempt 2 -> valid event: metadata eventually captured
    p, ad, fk, et, first, col = run_scenario(["transport"])
    assert first[0] is False and first[1] == 1 and first[2][et][0] == 1, first      # retry eligible after failure
    assert et in p.events_fetched and isinstance(ad.events.get(et), dict) and fk.event_calls == 2
    assert not p.event_attempts
    states = [e for e in load_sessions([col.dir]).events if e.event_type == EventType.MARKET_STATE]
    fee_meta = [((e.payload.get("contract") or {}).get("fee_metadata") or {}) for e in states]
    assert fee_meta and "fee_multiplier_override" in fee_meta[-1], fee_meta[-1]       # later snapshots carry it
    assert col.manifest.disconnects.get("kalshi_event") == 1
    # malformed body and a DIFFERENT event object: both remain retryable, then succeed
    for bad in (["malformed"], ["other"], ["malformed", "transport", "malformed"]):
        p, ad, fk, et, first, _col = run_scenario(bad)
        assert first[0] is False and et in first[2], (bad, first)
        assert et in p.events_fetched and fk.event_calls == len(bad) + 1, (bad, fk.event_calls)
    # backoff: persistent failure is retried with capped exponential spacing, never every poll
    p, ad, fk, et, first, _col = run_scenario(["transport"] * 100, polls=60)
    assert et not in p.events_fetched and p.event_attempts[et][0] == fk.event_calls
    assert 5 <= fk.event_calls <= 12, fk.event_calls                    # 60 s: 2, 4, 8, 8, ... s, not 60 attempts
    # success path: exactly one GET per event, never re-fetched
    p, ad, fk, et, first, _col = run_scenario([])
    assert first[0] is True and fk.event_calls == 1


def test_maker_taker_multipliers():
    """Step 6.3: separate default taker / maker multipliers; maker never reuses the taker default; overrides apply
    only where their semantics are documented; fee provenance hashes named accurately."""
    D = __import__("decimal").Decimal
    new = EC.KALSHI_GENERAL_2026_07_07
    old = EC.KALSHI_GENERAL_PRE_2026_07_07
    assert new.default_taker_multiplier == "1" and new.default_maker_multiplier == "0"
    assert not hasattr(new, "default_multiplier") and not hasattr(new, "source_sha256")
    fm = EC.FeeModel()
    NEW = EC.JULY_2026_START_MS + 3_600_000
    OLD = EC.JULY_2026_START_MS - 3_600_000
    meta = {"fee_type": "general", "fee_multiplier": "1", "fee_type_override": None, "fee_multiplier_override": None}
    ctx = {"fee_metadata": meta}
    fills = [(D("0.50"), D(10))]
    # taker: unchanged Step-6.2 arithmetic (default 1, captured multiplier, override precedence)
    t = fm.fee(fills, "KXBTC15M-A", NEW, ctx)
    assert t["fee"] == D("0.1750") and t["multiplier"] == D(1) and t["status"] == "FEE_VERIFIED"
    assert fm.fee(fills, "KXBTC15M-A", NEW, {"fee_metadata": dict(meta, fee_multiplier_override="2")})["fee"] == D("0.3500")
    assert fm.fee(fills, "KXBTC15M-A", NEW, None)["multiplier"] == D(1)                     # taker default 1
    # maker: default 0 under the July-2026 schedule, never the taker default 1
    mk = fm.fee(fills, "KXBTC15M-A", NEW, ctx, maker=True)
    assert mk["multiplier"] == D(0) and mk["fee"] == D(0) and mk["multiplier_known"] is False
    assert mk["status"] == "FEE_UNVERIFIED" and any("maker" in r for r in mk["reasons"])
    assert fm.fee(fills, "KXBTC15M-A", NEW, None, maker=True)["multiplier"] == D(0)
    # the generic (taker) fee_multiplier / override are NOT inferred to apply to maker fees
    for m2 in (dict(meta, fee_multiplier="3"), dict(meta, fee_multiplier_override="2")):
        assert fm.fee(fills, "KXBTC15M-A", NEW, {"fee_metadata": m2}, maker=True)["multiplier"] == D(0)
    # an explicit maker-scoped multiplier in the fee context overrides the maker default
    assert EC.MAKER_MULTIPLIER_FIELDS == ("maker_fee_multiplier",)
    ex = fm.fee(fills, "KXBTC15M-A", NEW, {"fee_metadata": dict(meta, maker_fee_multiplier="1")}, maker=True)
    assert ex["multiplier"] == D(1) and ex["fee"] == D("0.0438") and ex["multiplier_known"] is True
    assert ex["status"] == "FEE_VERIFIED"
    for bad in (True, "x", "-1"):
        assert fm.fee(fills, "KXBTC15M-A", NEW, {"fee_metadata": dict(meta, maker_fee_multiplier=bad)},
                      maker=True)["multiplier"] == D(0)
    # the capture layer records no maker-scoped field (nothing undocumented is inferred at capture time)
    from settlement.market_rules import FEE_FIELDS
    assert not any("maker" in f for f in FEE_FIELDS)
    # the pre-July schedule keeps its preserved (unverified) historical arithmetic, and is never used after July 7
    assert old.default_maker_multiplier == "1" and fm.fee_dollars(10, 0.5, maker=True, ts_ms=OLD) == 0.05
    assert fm.resolve("KXBTC15M-A", NEW) is new
    # maker economics stay NOT_EVALUATED for research
    assert EC.evaluate([])["maker"]["status"] == "NOT_EVALUATED"
    # provenance naming: a hash of the locally encoded definition text is not the official document hash
    import hashlib as _h
    assert new.schedule_definition_sha256 == _h.sha256(new.source.encode()).hexdigest()
    assert new.source_document_sha256 == "" and old.source_document_sha256 == ""
    assert t["schedule_definition_sha256"] == new.schedule_definition_sha256 and t["source_document_sha256"] == ""
    assert "schedule_source_sha256" not in t and fm.version == "fee_model_v4"


# ═══════════════════ 29n-29u Step 6.4 first real-feed corrections ═══════════════════
REAL_FX = os.path.join(HERE, "real_capture_fixtures", "first_real_session_20260930T004307Z-51664fe9.json")


def real_fx():
    if "realfx" not in _CACHE:
        with open(REAL_FX, encoding="utf-8") as f:
            _CACHE["realfx"] = json.load(f)
    return _CACHE["realfx"]


def _fx_mev(d):
    from microstructure.types import MicroEvent, MicroEventType
    return MicroEvent(source=d["source"], asset=d["asset"], event_type=MicroEventType(d["event_type"]), symbol=d["symbol"],
                      event_ts_ms=d["event_ts_ms"], receive_ts_ms=d["receive_ts_ms"], ingest_seq=d["ingest_seq"],
                      payload=d["payload"], receive_mono_ns=d["receive_mono_ns"], channel=d.get("channel") or "ws")


def _md_ctx(rx=1_790_729_000_000):
    from market_data.sources.base import Ctx, Sequencer
    return Ctx("s", Sequencer(), rx)


def test_real_fixture_provenance():
    fx = real_fx()
    assert fx["provenance"] == "derived from first real session 20260930T004307Z-51664fe9"
    assert os.path.getsize(REAL_FX) < 200_000                                  # minimal excerpt, not the 28 MB session
    txt = open(REAL_FX, encoding="utf-8").read()
    for bad in ("C:\\", "Users", "ezhou", "KALSHI-ACCESS", "api_key", "signature", "conn_id", "connection_id", "Bearer"):
        assert bad not in txt, bad
    for k in ("cross_source_inversions", "clock_processing_order", "kalshi_orderbook_fp", "kalshi_zero_quote_market",
              "kalshi_market_rules", "http_access_denied", "kalshi_trade_pages"):
        assert k in fx, k
    # the gates are NOT loosened to make the short real session pass (Coinbase evidence, session length, quality)
    from feature_eval.coinbase_seq import SequenceEvidenceConfig
    sc = SequenceEvidenceConfig()
    assert (sc.min_envelopes, sc.consistent_share, sc.min_connections, sc.min_heartbeats) == (5000, 0.995, 2, 30)
    qc = QU.QualityConfig()
    assert (qc.min_session_minutes, qc.book_ready_degraded, qc.book_ready_reject, qc.duplicate_share_degraded) == (15.0, 0.9, 0.5, 0.05)
    assert (qc.coverage_reject, qc.coverage_degraded, qc.regress_share_reject, qc.reconnects_per_hour_degraded) == (0.5, 0.9, 0.01, 6.0)
    # the replay analysis of the real session (read-only) is recorded with the current code's results
    rp = json.load(open(os.path.join(HERE, "analysis_output", "step6_4_first_real_session_replay.json")))
    assert rp["read_only"] is True and rp["book_ordering"]["ordering_errors_with_current_code"] == 0
    assert rp["book_ordering"]["recorded_disconnects_from_the_global_ordering_check"] == 69
    assert not rp["kalshi_rest"]["failures_with_current_code"] and sum(rp["kalshi_rest"]["recorded_failures"].values()) == 146
    assert all(not v["anomalies"] for v in rp["clock_monitor"].values())
    assert {v["status"] for v in rp["market_rules"].values()} == {"RULE_VERIFIED_FOR_MARKET"} and len(rp["market_rules"]) == 4
    assert [v["normalized"] for _k, v in sorted(rp["kalshi_trades"].items())] == [1319, 625, 1119, 1166]


def test_book_ordering_per_book():
    """Step 6.4 ISSUE 1: receive-time order is enforced per book / per chain, never globally across venues."""
    import itertools
    from microstructure.reconstruction import BookOrderError, BookReconstructor, BookStatus as BS
    from microstructure.types import MicroEvent, MicroEventType as MT
    from microstructure.venues import VENUES as MV
    n = itertools.count(1)

    def ev(src, sym, r, et, **p):
        base = ({"update_id": None, "depth": None} if et == MT.BOOK_SNAPSHOT else
                {"update_id": None, "prev_update_id": None, "first_update_id": None, "checksum": None})
        payload = dict(base, book=sym, price_unit=MV[src].price_unit, qty_unit=MV[src].qty_unit)
        payload.update(p)
        return MicroEvent(source=src, asset="BTC", event_type=et, symbol=sym, event_ts_ms=None, receive_ts_ms=r,
                          ingest_seq=next(n), payload=payload)

    def snap(src, sym, r, **kw):
        return ev(src, sym, r, MT.BOOK_SNAPSHOT, bids=[[100.0, 1.0]], asks=[[101.0, 1.0]], **kw)

    def dl(src, sym, r, px=99.0, **kw):
        return ev(src, sym, r, MT.BOOK_DELTA, changes=[["bid", px, 2.0, "abs"]], **kw)

    # independent books: Coinbase received 1002 processed BEFORE Kraken received 1000 -> BOTH accepted
    rc = BookReconstructor(warmup_ms=0)
    rc.apply(snap("coinbase_l2", "BTC-USD", 990)); rc.apply(snap("kraken_book", "BTC/USD", 990))
    u1 = rc.apply(dl("coinbase_l2", "BTC-USD", 1002))
    u2 = rc.apply(dl("kraken_book", "BTC/USD", 1000))
    assert (u1.kind, u2.kind) == ("delta", "delta") and not u1.resnapshot_needed and not u2.resnapshot_needed
    assert rc.status(("coinbase_l2", "BTC-USD"), 1003) == BS.READY and rc.status(("kraken_book", "BTC/USD"), 1003) == BS.READY
    # the SAME book: 1002 then 1000 -> fail closed
    rc = BookReconstructor(warmup_ms=0)
    rc.apply(snap("coinbase_l2", "BTC-USD", 990)); rc.apply(dl("coinbase_l2", "BTC-USD", 1002))
    try:
        rc.apply(dl("coinbase_l2", "BTC-USD", 1000, px=98.0)); raise AssertionError("same-book regression accepted")
    except BookOrderError as e:
        assert isinstance(e, ValueError) and "same book" in str(e)
    # the same sequence CHAIN (one connection, two books): a regression fails closed too
    rc = BookReconstructor(warmup_ms=0)
    rc.apply(snap("kalshi_ws", "MKT-A", 990, chain="k:c1", update_id=1))
    rc.apply(snap("kalshi_ws", "MKT-B", 990, chain="k:c1", update_id=2))
    rc.apply(dl("kalshi_ws", "MKT-A", 1002, chain="k:c1", prev_update_id=2, update_id=3))
    try:
        rc.apply(dl("kalshi_ws", "MKT-B", 1000, chain="k:c1", prev_update_id=3, update_id=4))
        raise AssertionError("same-chain regression accepted")
    except BookOrderError as e:
        assert "same sequence chain" in str(e)
    # real venue sequence gaps are still caught
    rc = BookReconstructor(warmup_ms=0)
    rc.apply(snap("kalshi_ws", "MKT-A", 990, chain="k:c2", update_id=5))
    g = rc.apply(dl("kalshi_ws", "MKT-A", 991, chain="k:c2", prev_update_id=6, update_id=8))      # seq 7 skipped
    assert g.kind == "gap" and g.status == BS.NEEDS_RESNAPSHOT and g.resnapshot_needed
    rc.apply(snap("okx_swap_book", "BTC-USDT-SWAP", 990, update_id=10))
    g = rc.apply(dl("okx_swap_book", "BTC-USDT-SWAP", 991, prev_update_id=11, update_id=12))
    assert g.kind == "gap" and rc.status(("okx_swap_book", "BTC-USDT-SWAP"), 992) == BS.NEEDS_RESNAPSHOT
    rc.apply(snap("bybit_linear_book", "BTCUSDT", 990, update_id=20))
    g = rc.apply(dl("bybit_linear_book", "BTCUSDT", 991, update_id=19))
    assert g.status == BS.INVALID and "NON_MONOTONIC" in g.reason
    rc.apply(ev("binance_usdm_book", "BTCUSDT", 990, MT.BOOK_DELTA, changes=[], first_update_id=99, update_id=101, prev_update_id=98))
    rc.apply(ev("binance_usdm_book", "BTCUSDT", 991, MT.BOOK_SNAPSHOT, bids=[[100.0, 1.0]], asks=[[101.0, 1.0]], update_id=100))
    assert rc.status(("binance_usdm_book", "BTCUSDT"), 5000) == BS.READY
    g = rc.apply(ev("binance_usdm_book", "BTCUSDT", 992, MT.BOOK_DELTA, changes=[], first_update_id=105, update_id=106,
                    prev_update_id=104))
    assert g.kind == "gap" and "pu 104" in g.reason
    # the REAL capture: consecutive deltas of different venues, the second received 1-5 ms earlier
    fx = real_fx()["cross_source_inversions"]
    assert fx["session_totals"]["same_source_regressions"] == 0 and fx["session_totals"]["same_book_regressions"] == 0
    assert len(fx["pairs"]) >= 3 and len({(p["processed_first"]["source"], p["processed_second"]["source"]) for p in fx["pairs"]}) >= 3
    for pr in fx["pairs"]:
        a, b = _fx_mev(pr["processed_first"]), _fx_mev(pr["processed_second"])
        assert a.source != b.source and -5 <= b.receive_ts_ms - a.receive_ts_ms < 0
        rc = BookReconstructor()
        ua, ub = rc.apply(a), rc.apply(b)                    # processed in the captured order: no exception
        for u in (ua, ub):
            assert u is None or (not u.resnapshot_needed and u.status not in (BS.INVALID, BS.NEEDS_RESNAPSHOT)), u
    # through the live MicroCollector: no ResnapshotRequired, no book gap, no reset
    from types import SimpleNamespace
    from market_data.clock import FakeClock
    from microstructure.collector import MicroCollector
    col = MicroCollector(tmpdir(), ["BTC"], {}, FakeClock(1_790_728_990_000), fsync=False)
    for pr in fx["pairs"]:
        for d in (pr["processed_first"], pr["processed_second"]):
            ad = SimpleNamespace(source=d["source"], rest_snapshots=False, assets=["BTC"])
            _e, _f, g_, resnap = col._handle(ad, SimpleNamespace(events=[_fx_mev(d)], failures=[], control=[]),
                                             d["receive_ts_ms"])
            assert resnap is None and g_ == 0
    assert col.counts["gaps"] == 0 and col.counts["resets"] == 0
    col.close()
    # offline research ordering is unchanged: replay applies the merged stream sorted by receive time
    import inspect
    from microstructure.replay import rebuild_books
    assert "sorted(events, key=lambda e: (e.receive_ts_ms, e.ingest_seq))" in inspect.getsource(rebuild_books)


def test_clock_monitor_reordering():
    """Step 6.4 ISSUE 2: an older (wall, mono) pair processed after a newer one is scheduling, not a clock anomaly."""
    from market_data.clock import ClockMonitor
    M = 1_000_000
    cm = ClockMonitor()
    assert cm.check(1000, 100 * M) is None and cm.check(1003, 103 * M) is None
    assert cm.check(1001, 101 * M) is None and cm.reordered == 1 and not cm.anomalies     # cross-thread older pair
    assert cm._last == (1003, 103 * M)                                    # the latest chronological pair is kept
    assert cm.check(1004, 103 * M + 500_000) is None                      # sub-ms capture skew is not an anomaly
    a = cm.check(500, 114 * M)                                            # monotonic +10 ms, wall steps back 504 ms
    assert a is not None and a.kind == "WALL_BACKWARDS" and a.mono_delta_ms > 0
    cm2 = ClockMonitor(threshold_ms=1000)
    cm2.check(1000, 100 * M)
    j = cm2.check(7000, 110 * M)                                          # +6 s wall with +10 ms monotonic
    assert j is not None and j.kind == "WALL_JUMP"
    fx = real_fx()["clock_processing_order"]
    assert fx["session_totals"]["with_monotonic_also_negative"] == 99
    for pair in fx["pairs"]:
        cm3 = ClockMonitor()
        for _src, w, m in pair:
            assert cm3.check(w, m) is None, pair
        assert cm3.reordered == 1 and not cm3.anomalies
    from market_data.collector import Collector
    from microstructure.collector import MicroCollector
    from perp_data.collector import PerpCollector
    import inspect
    for cls in (Collector, MicroCollector, PerpCollector):
        assert "ClockMonitor()" in inspect.getsource(cls.__init__)


def test_kalshi_orderbook_fp():
    """Step 6.4 ISSUE 3: the CURRENT Kalshi REST orderbook schema (orderbook_fp) parses exactly, with zero failures."""
    from market_data.sources.kalshi import KalshiAdapter
    fx = real_fx()["kalshi_orderbook_fp"]
    k = KalshiAdapter(["BTC"])
    body = json.loads(fx["responses"][0]["text"])
    assert set(body) == {"orderbook_fp"} and set(body["orderbook_fp"]) == {"yes_dollars", "no_dollars"}
    r = k.parse_orderbook("BTC", fx["ticker"], body, _md_ctx(fx["responses"][0]["receive_ts_ms"]))
    assert not r.failures and len(r.events) == 1
    b = r.events[0].payload
    ob = body["orderbook_fp"]
    D = __import__("decimal").Decimal
    best_yes = max(ob["yes_dollars"], key=lambda x: D(x[0]))
    best_no = max(ob["no_dollars"], key=lambda x: D(x[0]))
    assert b["bids"][0] == [float(D(best_yes[0]) * 100), float(D(best_yes[1]))]          # YES bid in cents
    assert b["asks"][0] == [float((1 - D(best_no[0])) * 100), float(D(best_no[1]))]      # YES ask = 1 - NO bid
    assert b["bids"] == sorted(b["bids"], key=lambda x: -x[0]) and b["asks"] == sorted(b["asks"], key=lambda x: x[0])
    assert any(q == 2169.15 for _p, q in b["asks"])                                        # fractional count kept
    assert len(b["bids"]) == len(ob["yes_dollars"]) and len(b["asks"]) == len(ob["no_dollars"])
    assert "LEGACY_SCHEMA" not in r.events[0].flags and "DERIVED_ASK" in r.events[0].flags
    e = json.loads(fx["responses"][1]["text"])                                            # empty YES side
    r1 = k.parse_orderbook("BTC", fx["ticker"], e, _md_ctx())
    assert not r1.failures and r1.events[0].payload["bids"] == [] and r1.events[0].payload["asks"]
    leg = k.parse_orderbook("BTC", "T", {"orderbook": {"yes": [[45, 100]], "no": [[53, 7]]}}, _md_ctx())
    assert not leg.failures and "LEGACY_SCHEMA" in leg.events[0].flags                     # historical captures only
    for bad in ({"orderbook_fp": {"weird": []}}, {"orderbook_fp": []}, {"book": {}}, {"orderbook_fp": {"yes_dollars": [["0.5"]]}},
                {"orderbook_fp": {"yes_dollars": [["0.0000", "1.00"]]}}, {"orderbook_fp": {"no_dollars": [["0.5", "-1"]]}},
                {"orderbook_fp": {"yes_dollars": "x"}}):
        rb = k.parse_orderbook("BTC", "T", bad, _md_ctx())
        assert rb.failures and not rb.events, bad
    # the synthetic Kalshi fake serves the current schema too
    from market_data.replay import load_sessions
    from market_data.types import EventType
    books = [x for x in load_sessions([session()]).events if x.event_type == EventType.BOOK and x.source == "kalshi"]
    assert books and all("LEGACY_SCHEMA" not in x.flags for x in books)


def test_kalshi_zero_quote():
    """Step 6.4 ISSUE 4: 0.0000 with size 0 is an UNAVAILABLE side - never a zero price, never a parse failure."""
    from market_data.sources.kalshi import KalshiAdapter, quote_cents
    fx = real_fx()["kalshi_zero_quote_market"]
    k = KalshiAdapter(["BTC"])
    now = fx["receive_ts_ms"]

    def state(m):
        res, _m = k.parse_markets("BTC", {"markets": [m]}, _md_ctx(now), now_ms=now)
        return res

    res = state(fx["market"])
    assert fx["market"]["yes_bid_dollars"] == "0.0000" and fx["market"]["yes_bid_size_fp"] == "0.00"
    assert not res.failures and len(res.events) == 1
    p, fl = res.events[0].payload, res.events[0].flags
    assert p["yes_bid"] is None and p["no_ask"] is None and "NO_QUOTE" in fl       # 1.0000 mirror of the missing bid
    assert abs(p["yes_ask"] - 0.1) < 1e-9 and abs(p["no_bid"] - 99.9) < 1e-9
    base = {k_: v for k_, v in fx["market"].items() if not any(x in k_ for x in ("_bid", "_ask"))}
    ok = state(dict(base, yes_bid_dollars="0.4000", yes_bid_size_fp="10.00", yes_ask_dollars="0.4200",
                    no_bid_dollars="0.5800", no_ask_dollars="0.6000")).events[0]
    assert (ok.payload["yes_bid"], ok.payload["yes_ask"], ok.payload["no_bid"], ok.payload["no_ask"]) == (40.0, 42.0, 58.0, 60.0)
    assert "NO_QUOTE" not in ok.flags and "DERIVED_ASK" not in ok.flags
    miss = state(dict(base, yes_bid_dollars="0.4000", no_bid_dollars="0.5800")).events[0]         # asks missing
    assert miss.payload["yes_ask"] == 42.0 and miss.payload["no_ask"] == 60.0 and "DERIVED_ASK" in miss.flags
    none = state(dict(base)).events[0]                                                              # nothing quoted
    assert all(none.payload[x] is None for x in ("yes_bid", "yes_ask", "no_bid", "no_ask"))
    one = state(dict(base, yes_bid_dollars="0.0000", yes_bid_size_fp="0.00", yes_ask_dollars="0.0010",
                     no_bid_dollars="0.9990")).events[0]                                            # one-sided
    assert one.payload["yes_bid"] is None and one.payload["no_ask"] is None      # never derived from an unavailable bid
    assert abs(one.payload["yes_ask"] - 0.1) < 1e-9 and abs(one.payload["no_bid"] - 99.9) < 1e-9
    bad = state(dict(base, yes_bid_dollars="0.0000", yes_bid_size_fp="5.00"))                       # contradictory
    assert bad.failures and not bad.events
    assert quote_cents({"a": "0.0000", "s": "0.00"}, "a", "s", "x") == (None, "NO_QUOTE")
    assert quote_cents({"a": "0.0000"}, "a", "s", "x") == (None, "NO_QUOTE")
    assert quote_cents({}, "a", "s", "x") == (None, "ABSENT")
    assert quote_cents({"a": "0.5000"}, "a", "s", "x") == (50.0, "PRESENT")


def test_live_rule_identifiers():
    """Step 6.4 ISSUE 5: the live BTC / ETH / SOL / XRP rule texts (BRTI, ETHUSDRTI, SOLUSDRTI, XRPUSDRTI) all parse."""
    from dataclasses import replace
    from settlement.kalshi_markets import parse_market
    from settlement.market_rules import (GOLD_RULE_STATUSES, PARSER_VERSION, RULE_VERIFIED_FOR_MARKET, parse_rule_text,
                                         resolve_market_rule)
    mk = real_fx()["kalshi_market_rules"]["markets"]
    want = {"BTC": ("BRTI", 2), "ETH": ("ETHUSDRTI", 2), "SOL": ("SOLUSDRTI", 4), "XRP": ("XRPUSDRTI", 4)}
    assert sorted(x["market"]["ticker"][2:5] for x in mk) == sorted(want)
    assert PARSER_VERSION == "crypto15m_rule_parser_v2"
    sol = None
    for x in mk:
        o, asset = x["market"], x["market"]["ticker"][2:5]
        ident, dp = want[asset]
        assert f"CF Benchmarks' {ident} " in o["rules_primary"]
        r = parse_rule_text(o["rules_primary"], o["rules_secondary"], asset)
        assert r["status"] == "PARSED" and r["comparison_operator"] == "GREATER_THAN_OR_EQUAL", (asset, r)
        assert r["settlement_decimal_places"] == dp and r["index_confirmed"] and r["averaging_60s_confirmed"]
        m, _res, _iss = parse_market(o, source="kalshi_market_api", capture_ts_ms=x["receive_ts_ms"])
        _rule, info = resolve_market_rule(m)
        assert info["status"] == RULE_VERIFIED_FOR_MARKET and info["basis"] == "MARKET_RULE_TEXT" and info["gold_eligible"]
        if asset == "SOL":
            sol = m
        # exact aliases only: no fuzzy / partial match
        p2 = o["rules_primary"].replace(f" {ident} ", f" {ident}X ")
        assert parse_rule_text(p2, o["rules_secondary"], asset)["status"] == "UNRECOGNIZED"
        other = {"BTC": "ETH", "ETH": "SOL", "SOL": "XRP", "XRP": "BTC"}[asset]
        assert parse_rule_text(o["rules_primary"], o["rules_secondary"], other)["status"] == "UNRECOGNIZED"
    # SOL: THIS captured contract is verified by its own text; nothing is claimed for other SOL markets
    _r, info = resolve_market_rule(replace(sol, rule_snapshot=None))
    assert info["status"] not in GOLD_RULE_STATUSES


def test_terminal_rest_availability():
    """Step 6.4 ISSUE 6: typed HTTP errors; 403 / 451 are terminal; the Binance full book becomes UNAVAILABLE with no
    retry storm and no reconnect loop; transient errors are still retried."""
    from market_data.clock import FakeClock
    from market_data.feed import FeedState, SourceUnavailable
    from market_data.runner import WsFeedRunner
    from market_data.transport.http import HttpError, HttpGetter, classify, safe_endpoint
    from microstructure.collector import MicroCollector
    from microstructure.poller import SnapshotPoller
    from microstructure.reconstruction import BookStatus as BS
    from microstructure.replay import load_micro_sessions, rebuild_books
    from microstructure.sources.binance_depth import BinanceDepthAdapter
    import requests
    for st, want in ((429, ("RATE_LIMITED", True, False)), (500, ("SERVER_ERROR", True, False)), (503, ("SERVER_ERROR", True, False)),
                     (None, ("NETWORK", True, False)), (403, ("ACCESS_DENIED", False, True)), (451, ("ACCESS_DENIED", False, True)),
                     (404, ("CLIENT_ERROR", False, False)), (400, ("CLIENT_ERROR", False, False))):
        assert classify(st) == want, st
    fx = real_fx()["http_access_denied"]
    assert {x["endpoint"] for x in fx["binance_451"]} >= {"fapi.binance.com/fapi/v1/depth", "fapi.binance.com/fapi/v1/openInterest",
                                                         "fapi.binance.com/fapi/v1/fundingRate", "fapi.binance.com/fapi/v1/fundingInfo"}
    assert [x["endpoint"] for x in fx["bybit_403"]] == ["api.bybit.com/v5/market/funding/history"]
    for x in fx["binance_451"] + fx["bybit_403"]:
        e = HttpError(x["status"], f"https://{x['endpoint']}?symbol=BTCUSDT&limit=3")
        assert e.terminal and not e.retryable and e.kind == "ACCESS_DENIED" and e.endpoint == x["endpoint"]
        assert isinstance(e, ConnectionError) and "symbol" not in str(e)
    assert safe_endpoint("https://user:pw@h.example:8443/p/q?x=1") == "h.example:8443/p/q"

    class _Resp:
        def __init__(self, code, body=None):
            self.status_code, self.reason, self._b = code, "x", body

        def json(self):
            return self._b

    class _Sess:
        def __init__(self, out):
            self.out, self.headers = out, {}

        def get(self, url, params=None, timeout=None):
            if isinstance(self.out, Exception):
                raise self.out
            return self.out
    for out, kind in ((_Resp(451), "ACCESS_DENIED"), (_Resp(403), "ACCESS_DENIED"), (_Resp(429), "RATE_LIMITED"),
                      (_Resp(502), "SERVER_ERROR"), (requests.Timeout("t"), "NETWORK"), (_Resp(404), "CLIENT_ERROR")):
        try:
            HttpGetter(session=_Sess(out)).get_json("https://fapi.binance.com/fapi/v1/depth", {"symbol": "BTCUSDT"})
            raise AssertionError(kind)
        except HttpError as e:
            assert e.kind == kind
    assert HttpGetter(session=_Sess(_Resp(200, {"ok": 1}))).get_json("https://x.example/a") == {"ok": 1}

    # ---- Binance full book: every required snapshot answers HTTP 451 ----
    class Denied:
        def __init__(self, status=451, only=None):
            self.calls, self.status, self.only = [], status, only

        def get_json(self, url, params=None):
            self.calls.append((params or {}).get("symbol"))
            if self.only is None or (params or {}).get("symbol") in self.only:
                raise HttpError(self.status, url)
            raise HttpError(503, url)
    ad = BinanceDepthAdapter(["BTC", "ETH"])
    clock = FakeClock(1_790_728_990_000)
    root = tmpdir()
    col = MicroCollector(root, ["BTC", "ETH"], {"binance_usdm_book": ad}, clock, fsync=False)
    col.on_connect(ad, clock.wall_ms())
    g = Denied()
    sp = SnapshotPoller(ad, g, col, clock, min_interval_s=0)
    assert sp.poll() == (0, 0, 0)                                          # terminal only: nothing to back off from
    assert len(g.calls) == 2 and not col.snapshot_needed
    assert set(col.unavailable_books) == {("binance_usdm_book", "BTC"), ("binance_usdm_book", "ETH")}
    for _ in range(30):
        clock.advance(1000)
        sp.poll()
    assert len(g.calls) == 2                                               # no retry storm
    assert col.manifest.unavailable["binance_usdm_book:BTC"]["status"] == 451
    assert col.recon.status(("binance_usdm_book", "BTCUSDT"), clock.wall_ms()) == BS.UNAVAILABLE
    delta = json.dumps({"stream": "btcusdt@depth@100ms", "data": {"e": "depthUpdate", "E": clock.wall_ms(), "T": clock.wall_ms(),
                        "s": "BTCUSDT", "U": 1, "u": 2, "pu": 0, "b": [["100.0", "1.0"]], "a": []}})
    try:
        col.on_message(ad, delta, clock.wall_ms(), clock.mono_ns()); raise AssertionError("delta accepted")
    except SourceUnavailable:
        pass

    class FakeWS:
        def __init__(self):
            self.n = 0

        def send_text(self, t):
            pass

        def settimeout(self, s):
            pass

        def recv_text(self):
            self.n += 1
            return delta if self.n < 50 else None

        def close(self):
            pass
    conns = []
    wr = WsFeedRunner(ad, col, clock, connect_fn=lambda url, headers=None: conns.append(1) or FakeWS(), max_attempts=5)
    wr.run()
    assert len(conns) == 1 and wr.attempts == 0 and wr.health.state == FeedState.DISCONNECTED   # stopped, never reconnected
    assert any("disabled" in n for n in col.manifest.notes)
    col.close()
    books = rebuild_books(load_micro_sessions([os.path.dirname(col.dir)]).events)
    assert books.tracks[("binance_usdm_book", "BTCUSDT")].base == BS.UNAVAILABLE             # replay agrees
    # partial: BTC denied (terminal), ETH transiently failing -> ETH retried with backoff, the websocket stays up
    col2 = MicroCollector(tmpdir(), ["BTC", "ETH"], {"binance_usdm_book": ad}, clock, fsync=False)
    col2.on_connect(ad, clock.wall_ms())
    g2 = Denied(only={"BTCUSDT"})
    sp2 = SnapshotPoller(ad, g2, col2, clock, min_interval_s=0)
    try:
        sp2.poll(); raise AssertionError("transient failure swallowed")
    except ConnectionError as e:
        assert "snapshot requests failed" in str(e)
    clock.advance(1000)
    try:
        sp2.poll()
    except ConnectionError:
        pass
    assert g2.calls.count("BTCUSDT") == 1 and g2.calls.count("ETHUSDT") == 2
    assert col2.on_message(ad, delta, clock.wall_ms(), clock.mono_ns()) is not None             # ETH book still served
    col2.close()


def test_optional_rest_enrichment():
    """Step 6.4 ISSUE 6: a denied optional REST enrichment stream is UNAVAILABLE; the venue's websocket stays active."""
    from market_data.clock import FakeClock
    from market_data.transport.http import HttpError
    from perp_data.collector import PerpCollector
    from perp_data.poller import PerpPoller
    from perp_data.sources.binance import BinanceUsdmAdapter
    from perp_data.sources.bybit import BybitLinearAdapter
    clock = FakeClock(1_790_728_990_000)

    class G:
        def __init__(self, status):
            self.status, self.calls = status, []

        def get_json(self, url, params=None):
            self.calls.append(url)
            raise HttpError(self.status, url)
    for ad, status, ws_msg in (
            (BybitLinearAdapter(["BTC", "ETH"]), 403,
             json.dumps({"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": 1, "data": [
                 {"T": 1_790_728_990_001, "s": "BTCUSDT", "S": "Buy", "v": "0.1", "p": "100", "i": "a", "BT": False}]})),
            (BinanceUsdmAdapter(["BTC", "ETH"]), 451,
             json.dumps({"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "E": 1_790_728_990_002, "s": "BTCUSDT",
                                                                "a": 7, "p": "100.0", "q": "0.5", "f": 1, "l": 1,
                                                                "T": 1_790_728_990_001, "m": False}}))):
        pc = PerpCollector(tmpdir(), ["BTC", "ETH"], {ad.source: ad}, clock, fsync=False)
        g = G(status)
        pp = PerpPoller(ad, g, pc, clock)
        assert pp.poll() == (0, 0, 0)                              # terminal only: no "all streams failed" backoff
        n0 = len(g.calls)
        assert n0 == len(ad.rest_urls()) and set(pp.unavailable) == set(ad.rest_urls())
        for _ in range(12):
            clock.advance(61_000)
            pp.poll()
        assert len(g.calls) == n0                                  # never re-requested this session
        assert set(pc.manifest.unavailable) == {f"{ad.source}:{s}" for s in ad.rest_urls()}
        assert f"{ad.source}:*" not in pc.manifest.unavailable
        assert not any(k.startswith(ad.source) for k in pc.manifest.disconnects)   # no reconnect accounting
        ev, fail, _g = pc.on_message(ad, ws_msg, clock.wall_ms(), clock.mono_ns())
        assert ev >= 1 and fail == 0, (ad.source, ev, fail)       # the websocket source stays active
        pc.close()
    # transient failures still back off (all attempted streams failed) and are retried
    y = BybitLinearAdapter(["BTC"])
    pc = PerpCollector(tmpdir(), ["BTC"], {"bybit_linear": y}, clock, fsync=False)
    pp = PerpPoller(y, G(503), pc, clock)
    try:
        pp.poll(); raise AssertionError("transient failure swallowed")
    except ConnectionError:
        pass
    assert not pp.unavailable and pc.manifest.disconnects
    pc.close()


def test_kalshi_trade_dedup():
    """Step 6.4 ISSUE 7: incremental Kalshi trade polling; each trade_id is normalized once; raw pages retained."""
    from market_data.clock import FakeClock
    from market_data.collector import Collector
    from market_data.kalshi_poller import KalshiPoller
    from market_data.replay import load_sessions
    from market_data.sources.kalshi import KalshiAdapter
    from market_data.types import EventType, IngestMode
    rules = {x["market"]["ticker"][2:5]: x for x in real_fx()["kalshi_market_rules"]["markets"]}

    def run(asset, pages, clock0, polls=None, **kw):
        mkt = rules[asset]["market"]

        class Fake:
            def __init__(self):
                self.trade_req, self.i = [], 0

            def get_json(self, url, params=None):
                if url.endswith("/markets"):
                    return {"markets": [mkt], "cursor": ""}
                if url.endswith("/orderbook"):
                    return {"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}
                if "/events/" in url:
                    return {"event": {"event_ticker": mkt["event_ticker"]}}
                if url.endswith("/markets/trades"):
                    self.trade_req.append(dict(params))
                    out = pages[min(self.i, len(pages) - 1)]
                    self.i += 1
                    return out
                raise ConnectionError("404")
        clock = FakeClock(clock0)
        col = Collector(tmpdir(), [asset], {}, clock, fsync=False, live_features=False)
        fk = Fake()
        p = KalshiPoller(KalshiAdapter([asset]), fk, col, clock, trades_every=1, **kw)
        for _ in range(polls or len(pages)):
            p.poll()
            clock.advance(1000)
        col.close()
        s = load_sessions([col.dir], include_raw=True)
        tr = [e for e in s.events if e.event_type == EventType.TRADE]
        return tr, [r for r in s.raw if r.stream == "trades"], fk, col, p, s

    mk = rules["BTC"]["market"]
    t0 = 1_790_729_000_000

    def trade(i, ts_s=None):
        ts = ts_s if ts_s is not None else t0 // 1000 - 200 + i
        return {"trade_id": f"id{i:03d}", "ticker": mk["ticker"], "created_time": f"{__import__('datetime').datetime.fromtimestamp(ts, __import__('datetime').timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')}.000000Z",
                "yes_price_dollars": "0.5000", "count_fp": "1.00", "taker_side": "yes", "no_price_dollars": "0.5000"}
    A = {"trades": [trade(i) for i in range(100, 0, -1)], "cursor": ""}
    B = {"trades": [trade(i) for i in range(150, 79, -1)], "cursor": ""}
    tr, raw, fk, col, _p, _s = run("BTC", [A, B], t0)
    ids = [e.payload["trade_id"] for e in tr]
    assert len(ids) == 150 and sorted(ids) == [f"id{i:03d}" for i in range(1, 151)]           # 1-150 exactly once each
    assert len(raw) == 2 and col.counts["duplicates"] == 0                                     # both raw responses kept
    assert fk.trade_req[0] == {"ticker": mk["ticker"], "limit": 100}                           # first poll: BACKFILLED
    assert fk.trade_req[1]["limit"] == 1000 and fk.trade_req[1]["min_ts"] == (t0 // 1000 - 200 + 100) - 1
    assert all(e.mode == IngestMode.BACKFILLED for e in tr if int(e.payload["trade_id"][2:]) <= 100)
    assert all(e.mode == IngestMode.LIVE for e in tr if int(e.payload["trade_id"][2:]) > 100)
    # trades sharing the boundary timestamp are never missed
    T = t0 // 1000 - 50
    P1 = {"trades": [trade(i, T) for i in (3, 2, 1)], "cursor": ""}
    P2 = {"trades": [trade(i, T) for i in (5, 4, 3, 2, 1)], "cursor": ""}
    tr, _raw, fk, _c, _p, _s = run("BTC", [P1, P2], t0)
    assert sorted(e.payload["trade_id"] for e in tr) == [f"id{i:03d}" for i in range(1, 6)] and fk.trade_req[1]["min_ts"] == T - 1
    # cursor pagination exhausts the interval; a walk longer than trade_max_pages is recorded as a gap
    first = {"trades": [trade(i) for i in (2, 1)], "cursor": ""}
    pg1 = {"trades": [trade(i) for i in range(12, 7, -1)], "cursor": "c1"}
    pg2 = {"trades": [trade(i) for i in range(7, 2, -1)], "cursor": ""}
    tr, raw, fk, col, _p, _s = run("BTC", [first, pg1, pg2], t0, polls=2, trade_page_limit=5)
    assert sorted(e.payload["trade_id"] for e in tr) == [f"id{i:03d}" for i in range(1, 13)] and len(raw) == 3
    assert fk.trade_req[2].get("cursor") == "c1" and fk.trade_req[2]["min_ts"] == fk.trade_req[1]["min_ts"]
    endless = {"trades": [trade(i) for i in range(12, 7, -1)], "cursor": "more"}
    _tr, _raw, _fk, _col, _p, s = run("BTC", [first, endless], t0, polls=2, trade_page_limit=5, trade_max_pages=3)
    assert any(g.kind == "TRADE_PAGES_TRUNCATED" for g in s.gaps)
    # the REAL capture: two consecutive overlapping 100-trade polls
    fx = real_fx()["kalshi_trade_pages"]
    pages = [json.loads(x["text"]) for x in fx["pages"]]
    asset = fx["ticker"][2:5]
    tr, raw, fk, col, p, _s = run(asset, pages, fx["pages"][0]["receive_ts_ms"])
    ids = [e.payload["trade_id"] for e in tr]
    assert len(ids) == len(set(ids)) == fx["union_ids"] == 150 and fx["overlap_ids"] == 50
    assert len(raw) == 2 and col.counts["duplicates"] == 0 and p.adapter.trade_overlap_skipped == 50
    # a market no longer polled drops its dedup state
    p.adapter.forget_trades(fx["ticker"])
    assert fx["ticker"] not in p.adapter.trade_seen


# ═══════════════════ 27-29 fingerprint, docs, previous ═══════════════════
def test_step6_fingerprint():
    ok, problems = S6FP.verify()
    assert ok, problems
    stored = json.load(open(S6FP.BASELINE_PATH))
    assert stored["step6_fingerprint"] == S6FP.build()["step6_fingerprint"]
    assert stored["step6_fingerprint"] not in EARLIER_FINGERPRINTS.values()
    assert stored["feature_universe_fingerprint"] == UV.load_frozen()["fingerprint"]
    t = tmpdir()
    pkg = os.path.join(t, "feature_eval")
    shutil.copytree(os.path.join(HERE, "feature_eval"), pkg, ignore=shutil.ignore_patterns("__pycache__"))
    assert S6FP.verify(pkg_dir=pkg)[0]
    with open(os.path.join(pkg, "splits.py"), "a", encoding="utf-8") as f:
        f.write("\nMUTATED = 1\n")
    ok3, pr3 = S6FP.verify(pkg_dir=pkg)
    assert not ok3 and any("splits.py" in x for x in pr3)
    p = subprocess.run([sys.executable, "-m", "feature_eval.fingerprint", "--write"], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 2


def test_docs_and_outputs():
    doc = open(os.path.join(HERE, "docs", "STEP6_FEATURE_EVALUATION.md"), encoding="utf-8").read().lower()
    for s in ("insufficient_data", "final_holdout", "purge", "market-normalized", "benjamini", "fee_unverified",
              "not_executable", "synthetic_only", "coinbase", "settlement_unverified", "complexity", "structural pruning",
              "session_normalized", "no automatic promotion", "research ledger"):
        assert s in doc, s
    for f in ("ARCHITECTURE.md", "ROADMAP.md", "BASELINE.md"):
        assert "STEP6_FEATURE_EVALUATION" in open(os.path.join(HERE, "docs", f), encoding="utf-8").read(), f
    for f in ("README.md", "SETUP.txt"):
        assert "run_step6_research" in open(os.path.join(HERE, f), encoding="utf-8").read(), f
    perf = json.load(open(os.path.join(HERE, "analysis_output", "step6_performance.json")))
    assert perf["synthetic"] is True and "SYNTHETIC" in perf["note"]
    mut = json.load(open(os.path.join(HERE, "analysis_output", "step6_mutation_results.json")))
    assert mut["all_caught_and_controls_pass"] is True
    assert {m["id"] for m in mut["mutations"]} >= {f"S{i}" for i in range(1, 21)}
    for s in ("step 6.1", "ambiguous_convention", "greater_than_or_equal", "requires_versioned_policy_migration",
              "fee_unknown", "inner_split", "settlement_inputs", "old", "new", "why"):
        assert s in doc, s
    sdoc = open(os.path.join(HERE, "docs", "SETTLEMENT_ENGINE.md"), encoding="utf-8").read()
    assert "settlement/rules.py" in sdoc and "ba4e50c3" in sdoc and "3eba791c" in sdoc
    assert "market_rules.py" in sdoc and "4884524a" in sdoc and "rule_historically_unverified" in sdoc.lower()
    for s_ in ("step 6.2", "rule_historically_unverified", "centicent", "rules_primary", "fee_multiplier_override",
               "fee_verified", "s27", "609905956c9e6120"):
        assert s_ in doc, s_
    assert {m["id"] for m in mut["mutations"]} >= {f"S{i}" for i in range(1, 28)}
    for s_ in ("step 6.3", "loads_exact", "value_precision_lost", "legacy", "events_fetched", "default_maker_multiplier",
               "schedule_definition_sha256", "source_document_sha256", "s28", "s29", "s30", "b205709349492b71",
               "ca98d418bf40075a"):
        assert s_ in doc, s_
    assert "9c. exact cf decimals" in sdoc.lower() and "b2057093" in sdoc
    assert {m["id"] for m in mut["mutations"]} >= {f"S{i}" for i in range(1, 31)}
    for s_ in ("step 6.4", "orderbook_fp", "no_quote", "ethusdrti", "solusdrti", "xrpusdrti", "unavailable",
               "sourceunavailable", "bookordererror", "min_ts", "s31", "s39", "6fed6efd32441735", "faf21c0af4bd3a9e",
               "20260930t004307z-51664fe9", "replay_real_feed_analysis"):
        assert s_ in doc, s_
    assert "9d. live rule-text index identifiers" in sdoc.lower() and "6fed6efd" in sdoc
    assert {m["id"] for m in mut["mutations"]} >= {f"S{i}" for i in range(1, 40)}


def test_previous_stages():
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 22):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-600:], p.stderr[-600:])


TESTS = [
    ("baselines", "1 frozen baseline: 47 fixtures, legacy + extended, settlement / market-data / perp-data / microstructure fingerprints, production files", test_frozen_baseline),
    ("veto", "2 EXISTING perp veto chain byte-identical, INACTIVE, never imports feature_eval", test_existing_perp_veto_untouched),
    ("isolation", "3 production isolation: nothing production imports feature_eval; read only; no promotion class; LIVE refused", test_production_isolation),
    ("universe", "4 frozen feature universe (2047): fields, roles, aliases, finer families, tags; changed-without-version refused", test_feature_universe),
    ("quality", "5 real-session quality validator: verdicts, synthetic flag, segment hashes, empty session REJECT", test_quality_validator),
    ("coinbase", "6 Coinbase sequence validator: consistent / contradicts / unverified / synthetic; features fail closed", test_coinbase_sequence_validator),
    ("labels", "7 settlement-label gate: official / conflict / verified / unverified; unverified never gold", test_settlement_label_gate),
    ("legacy", "8 legacy probability via the unmodified evaluate() == independent formula; globals restored", test_legacy_probability),
    ("dataset", "9 research matrix from a session, real mode skips synthetic, immutable cache, raw change invalidates", test_dataset_cache_fingerprint),
    ("dsfp", "10 dataset fingerprint covers every session, raw checksum, universe, labels, grid, assets", test_dataset_fingerprint_sessions),
    ("splits", "11 purged chronological TRAIN / DEV / HOLDOUT, close-time groups, walk-forward, no holdout in folds", test_splits_purge_walk_forward),
    ("weights", "12 market-normalized weights (each market sums to 1)", test_market_normalized_weights),
    ("pruning", "13 structural pruning: constant / near-constant / duplicate / algebraic / unavailable / role; label independent", test_structural_pruning),
    ("missing", "14 missing data never zero: train mean + indicator, complete case, native", test_missing_never_zero),
    ("models", "15 models: logistic recovers coefficients, ridge grid on train, stumps, legacy logit unshrunk", test_models),
    ("fold", "16 train-only scaler / selection / ridge; future-only feature not selected; holdout leak refused", test_train_only_fitting),
    ("metrics", "17 metrics + fixed confidence buckets ('wins / n', insufficient samples)", test_metrics_and_buckets),
    ("bootstrap", "18 market-clustered bootstrap + sign-flip p-values", test_cluster_bootstrap),
    ("bh", "19 Benjamini-Hochberg via the repo machinery; q >= p; n / CI / effect reported", test_benjamini_hochberg),
    ("calibration", "20 calibration: Platt / isotonic gates, monotone PAV, strictly later calibration data", test_calibration_gates),
    ("economics", "21 executable economics: depth VWAP, NOT_EXECUTABLE, versioned taker fees, FEE_UNVERIFIED, maker not evaluated", test_executable_economics),
    ("gates", "22 sample gates name failed requirements; complexity gate; small real data -> INSUFFICIENT_DATA", test_sample_and_complexity_gates),
    ("leakage", "23 leakage guards: label / post-event names, disguised leaked values, availability", test_leakage_guards),
    ("ledger", "24 research ledger: hash chain, tamper detection, holdout single use", test_research_ledger),
    ("expfp", "25 experiment fingerprint covers model, families, missing strategy, purge, fees, seed", test_experiment_fingerprint),
    ("pipeline", "26 synthetic self-test: planted family found, noise rejected, SYNTHETIC_ONLY, deterministic with workers", test_synthetic_pipeline),
    ("outputs", "27 outputs: synthetic refused in real files; INSUFFICIENT_DATA files name requirements", test_outputs_real_vs_synthetic),
    ("cli", "28 CLIs: dry run writes nothing, no-data run, report, validator, status, synthetic never real", test_clis),
    ("rules", "29a Step 6.1 contract rules: AT_LEAST equality -> YES, official precision, ties, unknown rules fail closed, versions", test_contract_rules),
    ("precision", "29b Step 6.1 precision-aware expiration-value verification (no universal 0.01 tolerance)", test_precision_aware_verification),
    ("convention", "29c Step 6.1 ambiguous / tied window conventions never VERIFIED; migration required", test_convention_ambiguity),
    ("rejected", "29d Step 6.1 rejected settlement / resolution sources never reach labels", test_rejected_source_labels),
    ("ridge", "29e Step 6.1 ridge lambda: market-level, close-group, purged inner split", test_ridge_inner_split),
    ("histrule", "29f Step 6.2 a rule observed later never makes a pre-observation reconstruction gold", test_historical_rule_provenance),
    ("rulecapture", "29g Step 6.2 per-market rule text retained; deterministic parser; conflicts / SOL / untrusted fail closed", test_market_rule_capture_and_parser),
    ("rulefp", "29h Step 6.2 dataset fingerprint covers the per-market rule-text hashes", test_rule_fingerprinting),
    ("fees62", "29i Step 6.2 July-2026 schedule: exact centicent fee + cost, overrides, historical schedules", test_fee_schedules_centicent),
    ("cfexact", "29j Step 6.2 CF exact decimal text survives to the settlement rounding", test_cf_exact_decimal),
    ("cfpaths", "29k Step 6.3 CF exact decimal on every ingestion path (direct WS, Kalshi wrapper, offline import, REST); lossy floats fail closed", test_cf_exact_all_paths),
    ("eventretry", "29l Step 6.3 Kalshi event metadata marked fetched only after a parsed + retained response; retries back off", test_kalshi_event_retry),
    ("makerfee", "29m Step 6.3 separate taker (1) / maker (0) default multipliers; no inferred maker override; fee provenance names", test_maker_taker_multipliers),
    ("realfx", "29n Step 6.4 real-capture fixtures: provenance, minimal, sanitized", test_real_fixture_provenance),
    ("bookorder", "29o Step 6.4 book ordering per book / chain (cross-venue scheduling accepted; same-book regression and sequence gaps fail closed)", test_book_ordering_per_book),
    ("clockorder", "29p Step 6.4 ClockMonitor: out-of-order (wall, mono) pairs are scheduling, not WALL_BACKWARDS", test_clock_monitor_reordering),
    ("kalshiob", "29q Step 6.4 Kalshi orderbook_fp current schema parses exactly (real capture, zero failures)", test_kalshi_orderbook_fp),
    ("zeroquote", "29r Step 6.4 Kalshi 0.0000 / size-0 quote is an unavailable side, never a parse failure", test_kalshi_zero_quote),
    ("liverules", "29s Step 6.4 live BRTI / ETHUSDRTI / SOLUSDRTI / XRPUSDRTI rule texts parse; exact aliases only", test_live_rule_identifiers),
    ("restterminal", "29t Step 6.4 typed HTTP errors; 403 / 451 terminal; Binance book UNAVAILABLE, no retry storm", test_terminal_rest_availability),
    ("restoptional", "29u Step 6.4 denied optional REST enrichment never disables a healthy websocket", test_optional_rest_enrichment),
    ("tradededup", "29v Step 6.4 incremental Kalshi trade polling: each trade_id once, raw pages retained", test_kalshi_trade_dedup),
    ("fingerprint", "29 separate Step-6 fingerprint; detects module changes; refuses silent rewrites", test_step6_fingerprint),
    ("docs", "30 docs + benchmark / mutation outputs", test_docs_and_outputs),
    ("previous", "31 all previous stage suites", test_previous_stages),
]


if __name__ == "__main__":
    only = None
    if "--only" in sys.argv:
        only = set(sys.argv[sys.argv.index("--only") + 1].split(","))
    for key, name, fn in TESTS:
        if only is None or key in only:
            run(name, fn)
    if only is None:
        print("\nAll Stage 22 tests passed.")
    else:
        print(f"\nSelected Stage 22 tests passed: {','.join(sorted(only))}")
