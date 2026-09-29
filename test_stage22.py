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
    "settlement": "4884524a795f0e2cc63bc561074d00e2a2351bba4f0f1ef3cf7092803db17aad",    # Step 6.2; OLD ba4e50c3... (6.1), 3eba791c... (Steps 2-6)
    "market_data": "609905956c9e6120c1021f3f72017db87ee5eeaeedac236bf822d630473e3f5f",   # Step 6.2 metadata extension; OLD 969cec83e8b9912e1fb91d02e8663acc943424dce7d33456b8850b0fb58297fd
    "perp_data": "90543ddff68e9dece7443fd9a7d876070f14a06d008a013e8980cb6aa292758c",
    "microstructure": "695d8e7741287c0bbac40ae9835148e4a0fbcd0e517669784aae647a8dcb0799",
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
