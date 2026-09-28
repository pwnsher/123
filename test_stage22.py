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
    "settlement": "3eba791cfe8163cc17406ecdbd0c0e04abfe178afb7b6695565a3484318ff2be",
    "market_data": "969cec83e8b9912e1fb91d02e8663acc943424dce7d33456b8850b0fb58297fd",
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
    assert LB.market_label({"reconstructed_outcome": "no"}, "VERIFIED") == (0, "RECONSTRUCTED_VERIFIED")
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
    rid = MD.RidgeLogisticModel().fit(X, leg, y, [1.0] * n, order=list(range(n)))
    assert rid.l2 in MD.RIDGE_GRID and set(rid.inner_scores) == set(MD.RIDGE_GRID)
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
    assert not fm.verified and fm.taker_rate == 0.07
    assert fm.fee_dollars(1, 0.5) == 0.02                                # ceil(0.07*0.25*100)/100 = ceil(1.75)/100
    assert fm.fee_dollars(100, 0.5) == 1.75 and fm.fee_dollars(10, 0.9) == 0.07
    assert fm.fee_dollars(10, 0.5, maker=True) == 0.05
    assert EC.vwap([[40, 5], [42, 10]], 10) == ((5 * 40 + 5 * 42) / 10, 10)
    assert EC.vwap([[40, 5], [42, 3]], 10) == (None, 8)                  # never extrapolated
    e = EC.side_economics(0.60, [[40, 5], [42, 10]], 10, fm, "KXBTC")
    assert e["status"] == "EXECUTABLE" and abs(e["price"] - 0.41) < 1e-12
    assert abs(e["raw_edge"] - 0.19) < 1e-12
    assert abs(e["fee_per_contract"] - fm.fee_dollars(10, 0.41) / 10) < 1e-12
    assert abs(e["net_executable_edge"] - (0.60 - 0.41 - e["fee_per_contract"])) < 1e-12
    assert EC.side_economics(0.6, [[40, 5]], 10, fm, "K")["status"] == "NOT_EXECUTABLE"
    ev = EC.evaluate([{"market": "M", "p_yes": 0.7, "y": 1, "checkpoint_s": 60, "ticker": "M",
                       "execution": {"yes_ask_ladder": [[50, 100]], "no_ask_ladder": [[52, 100]]}}])
    assert ev["status"] == "FEE_UNVERIFIED" and ev["maker"]["status"] == "NOT_EVALUATED"
    assert ev["by_size"]["1"]["labels"] == "SIMULATED" and ev["by_size"]["50"]["executable"] == 1
    fm2 = EC.FeeModel(taker_rate=0.05)
    assert fm2.fingerprint() != fm.fingerprint()
    ex = EC.FeeModel(market_exceptions={"KXSPECIAL": {"taker_rate": 0.0, "maker_rate": 0.0}})
    assert ex.fee_dollars(10, 0.5, "KXSPECIAL-1") == 0.0 and ex.fee_dollars(10, 0.5, "KXBTC") == 0.18
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
    for k, v in (("model", "D"), ("families", ["B"]), ("missing_strategy", "complete_case"), ("seed", 1),
                 ("fee_model_fingerprint", EC.FeeModel(taker_rate=0.05).fingerprint()),
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
    assert {m["id"] for m in mut["mutations"]} >= {f"S{i}" for i in range(1, 15)}


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
