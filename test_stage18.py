#!/usr/bin/env python3
"""Stage 18 — STEP 2 SETTLEMENT ENGINE (research / observation only).

Run:  py test_stage18.py        (or py run_all_tests.py for stages 1-18)

All data here is synthetic and deterministic; no network is used. Covers window boundaries,
duplicates, out-of-order, missing, malformed, stale, late and proxy data, schema mismatch,
deterministic reconstruction, live == batch equivalence, causal checkpoints, label-leakage
(including a canary that proves the leakage check catches a leaky builder), resolution and
overlap tools, the cache, offline replay, timezone / DST handling, the separate settlement
fingerprint, and that the Step-1 strategy + execution boundary are untouched.
"""
import ast
import hashlib
import inspect
import json
import os
import random
import subprocess
import sys
import tempfile
from dataclasses import replace

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "scripts"))

from settlement import cache, checkpoints as ck, fingerprint as sfp                          # noqa: E402
from settlement.accumulator import CausalityError, SettlementAccumulator                     # noqa: E402
from settlement.assets import check_ticker_close, ticker_close_candidates_ms                 # noqa: E402
from settlement.cf_history import parse_cfb_historical                                       # noqa: E402
from settlement.cf_live import parse_cfb_frame, parse_kalshi_cfb_message                     # noqa: E402
from settlement.engine import arrival_order                                                  # noqa: E402
from settlement.kalshi_markets import parse_market                                           # noqa: E402
from settlement.overlap import compare_market, compare_observations, compare_published       # noqa: E402
from settlement.policy import (RECONSTRUCTION_POLICIES, WINDOW_POLICIES, ReconstructionPolicy,  # noqa: E402
                               SettlementWindowPolicy, available_ts, reconstruction_policy, window_policy)
from settlement.reconstruction import reconstruct                                            # noqa: E402
from settlement.resolution import verify_all, verify_market, index_observations              # noqa: E402
from settlement.schemas import parse_epoch_ms, parse_iso_utc_ms, parse_value, structure_fingerprint  # noqa: E402
from settlement.synthetic import cf_frame, demo_dataset, kalshi_message, market_json          # noqa: E402
from settlement.types import (Flag, OfficialResolution, ParseIssue, Phase, Quality, SampleKind,  # noqa: E402
                              SettlementMarket, SettlementObservation)

C = 1_790_000_100_000                                  # a 15-minute boundary (UTC)
M = SettlementMarket("KXBTC15M-TEST", "BTC", C, "BRTI", strike=100000.0, strike_source="floor_strike",
                     open_ts_ms=C - 900_000)
TP = reconstruction_policy("test_synthetic_v1")        # strict, but trusts the synthetic source
W = window_policy()
STEP1_ARTIFACTS = {   # Step-1 baseline artifacts: Step 2 must leave them byte-identical
    "config/strategy_baseline.json": "8f052394a830be88f4bd4c506b5d8069222e3c9b1c06c0bc63e3817463185d04",
    "step5_baseline_manifest.json": "6a2994ff8bdc2d36b41608515c77a13d71fd371030487b5e62b2b064cd096db8",
    "regression/strategy_cases.json": "e942ba3716e42a55fbf8e6086283eb0c8f124564b1cb44a3301b6c12c47abf9c",
    "kalshi_dashboard.py": "aa66c7ae1d6cf8b913b746b05bfd6cf64197454d3b7fd00a2ba174749150c595",
}


def run(name, fn):
    try:
        fn(); print(f"PASS  {name}")
    except Exception as e:
        print(f"FAIL  {name}: {type(e).__name__}: {e}"); raise


def val(t):
    return round(100000.0 + (t - C) / 1000.0 * 0.5 + (t // 1000 % 7) * 0.01, 2)


def ob(t, v=None, rcv="auto", src="synthetic", amend=None, seq=None, idx="BRTI"):
    return SettlementObservation("BTC", idx, src, val(t) if v is None else v, t,
                                 receive_ts_ms=(t + 100 if rcv == "auto" else rcv), amend_ts_ms=amend, seq=seq)


def stream(lo=C - 120_000, hi=C + 5_000, step=1000, **kw):
    return [ob(t, **kw) for t in range(lo, hi + 1, step)]


def full_mean(grid):
    import math
    return math.fsum(val(g) for g in grid) / len(grid)


# ═══════════════════ 1-3 Step-1 guard, import boundaries ═══════════════════
def test_step1_untouched():
    for rel, h in STEP1_ARTIFACTS.items():
        assert hashlib.sha256(open(os.path.join(HERE, rel), "rb").read()).hexdigest() == h, rel
    from kalshi_core import baseline
    ok, problems = baseline.verify()
    assert ok, problems
    p = subprocess.run([sys.executable, "-m", "regression.generate", "--check"], cwd=HERE, capture_output=True, text=True)
    assert p.returncode == 0 and "MATCH (47 cases)" in p.stdout, p.stdout + p.stderr
    import strategy_fingerprint as sf
    assert sf.current_fingerprint(os.path.join(HERE, "kalshi_dashboard.py"))[0] == \
        "8d94f241e8fc8edadc76058e1f12f430b6e4f499f4c0a30fba1cb5cf07dad82a"


def _imports(path):
    tree = ast.parse(open(path, encoding="utf-8").read())
    mods = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    return mods | {n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}


def test_import_boundaries_and_no_execution():
    pkg = os.path.join(HERE, "settlement")
    forbidden = {"kalshi_dashboard", "kalshi_bot", "discord", "kalshi_api_learn", "kalshi_core.execution", "requests",
                 "urllib", "urllib3", "http", "http.client", "socket", "httpx", "aiohttp", "websocket", "websockets"}
    files = [os.path.join(pkg, f) for f in os.listdir(pkg) if f.endswith(".py")] + \
            [os.path.join(HERE, "scripts", f) for f in os.listdir(os.path.join(HERE, "scripts")) if f.endswith(".py")]
    for f in files:
        bad = {m for m in _imports(f) if m in forbidden or m.split(".")[0] in {"requests", "discord", "socket", "urllib"}}
        assert not bad, (f, bad)
        src = open(f, encoding="utf-8").read().lower()
        for s in ("/portfolio/orders", "create_order", "place_order", "coinbase.com", "api.exchange.coinbase"):
            assert s not in src, (f, s)
    # production / strategy / core code never imports the settlement research layer
    for f in ("kalshi_dashboard.py", "kalshi_backtest.py", "perp_live.py", "perp_shadow.py", "perp_telemetry.py",
              "perp_probability.py", "kalshi_bot.py", "run_local.py", "strategy_fingerprint.py") + \
            tuple(os.path.join("kalshi_core", x) for x in os.listdir(os.path.join(HERE, "kalshi_core")) if x.endswith(".py")):
        assert not any(m == "settlement" or m.startswith("settlement.") for m in _imports(os.path.join(HERE, f))), f
    from kalshi_core import execution as ex
    try:
        ex.get_execution_engine("LIVE"); raise AssertionError("LIVE engine returned")
    except ex.LiveExecutionUnavailable:
        pass


# ═══════════════════ 4-6 window convention ═══════════════════
def test_window_boundaries():
    g = W.grid(C)
    assert len(g) == 60 and g[0] == C - 60_000 and g[-1] == C - 1000                   # start IN, close OUT
    e = window_policy("cf_rti_60s_end_incl_asof_v1").grid(C)
    assert len(e) == 60 and e[0] == C - 59_000 and e[-1] == C                          # start OUT, close IN
    for p in WINDOW_POLICIES.values():
        assert p.expected_samples() == 60 and p.verified is False
    from settlement.types import Membership as Mb
    assert W.membership(C - 60_000, C) == Mb.IN_WINDOW and W.membership(C - 60_001, C) == Mb.BEFORE_WINDOW
    assert W.membership(C, C) == Mb.AFTER_WINDOW and W.membership(C - 1, C) == Mb.IN_WINDOW
    ew = window_policy("cf_rti_60s_end_incl_asof_v1")
    assert ew.membership(C, C) == Mb.IN_WINDOW and ew.membership(C - 60_000, C) == Mb.BEFORE_WINDOW
    for bad in (dict(sampling="MEDIAN"), dict(window_seconds=0), dict(sample_interval_ms=7000), dict(max_sample_age_ms=-1)):
        try:
            SettlementWindowPolicy("x", 1, **bad); raise AssertionError(bad)
        except ValueError:
            pass
    try:
        ReconstructionPolicy("x", 1, allow_partial_mean=True); raise AssertionError("research flag without research_only")
    except ValueError:
        pass
    # grids are pure epoch-ms arithmetic: independent of the process timezone
    code = ("import sys; sys.path.insert(0, %r)\nfrom settlement.policy import window_policy\n"
            "print(window_policy().grid(%d)[0], window_policy().grid(%d)[-1])" % (HERE, C, C))
    outs = {subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                           env=dict(os.environ, TZ=tz)).stdout for tz in ("UTC", "America/New_York", "Asia/Kolkata")}
    assert outs == {f"{C - 60_000} {C - 1000}\n"}, outs


def test_boundary_observations():
    base = stream()
    r = reconstruct(M, base, W, TP)
    assert r.quality == Quality.HEALTHY and abs(r.final_value - full_mean(W.grid(C))) < 1e-9
    assert r.samples[0].kind == SampleKind.OBSERVED_EXACT and r.samples[0].grid_ts_ms == C - 60_000
    # a value AT the close instant is outside the default window ...
    at_close = [o if o.event_ts_ms != C else replace(o, value=1.0) for o in base]
    assert reconstruct(M, at_close, W, TP).final_value == r.final_value
    # ... but inside the end-inclusive candidate
    ew = window_policy("cf_rti_60s_end_incl_asof_v1")
    assert reconstruct(M, at_close, ew, TP).final_value != reconstruct(M, base, ew, TP).final_value
    # the first instant may use the value from <= max_sample_age before it (ASOF), no further
    no_start = [o for o in base if o.event_ts_ms != C - 60_000]
    r2 = reconstruct(M, no_start, W, TP)
    assert r2.samples[0].kind == SampleKind.ASOF and r2.samples[0].source_event_ts_ms == C - 61_000 and r2.final_value is not None
    older = [o for o in base if o.event_ts_ms not in (C - 60_000, C - 61_000)]
    r3 = reconstruct(M, older, W, TP)
    assert r3.samples[0].kind == SampleKind.MISSING and r3.final_value is None
    assert r3.quality == Quality.INSUFFICIENT_COVERAGE
    # EXACT convention refuses as-of values; BUCKET_LAST uses the last value inside [k, k+1s)
    ex_ = window_policy("cf_rti_60s_start_incl_exact_v1")
    assert reconstruct(M, no_start, ex_, TP).final_value is None
    fine = [ob(t) for t in range(C - 61_000, C + 1, 200)]
    bk = reconstruct(M, fine, window_policy("cf_rti_60s_start_incl_bucket_v1"), TP)
    assert bk.samples[0].source_event_ts_ms == C - 60_000 + 800 and bk.quality == Quality.HEALTHY
    # tie at the strike -> no outcome, flagged
    flat = [ob(t, 100000.0) for t in range(C - 61_000, C + 1, 1000)]
    rt = reconstruct(M, flat, W, TP)
    assert rt.final_value == 100000.0 and rt.reconstructed_outcome is None and Flag.AT_STRIKE.value in rt.state.flags


# ═══════════════════ 7-11 data defects ═══════════════════
def test_duplicates_conflicts_amendments():
    base = stream()
    ref = reconstruct(M, base, W, TP)
    dup = base + [replace(o, receive_ts_ms=o.receive_ts_ms + 5) for o in base[50:80]] + \
        [replace(o, source="cfb_ws") for o in base[60:70]]
    rp = replace(TP, trusted_sources=TP.trusted_sources | {"cfb_ws"})
    rd = reconstruct(M, dup, W, rp)
    assert rd.final_value == ref.final_value and rd.provenance["counts"]["duplicates"] == 31      # inside the lookback only
    assert Flag.DUPLICATE_DROPPED.value in rd.state.flags and rd.quality == Quality.HEALTHY
    t = C - 30_000
    conf = base + [ob(t, val(t) + 1.0, rcv=t + 300)]
    rc = reconstruct(M, conf, W, TP)
    assert rc.quality == Quality.CONFLICT and rc.final_value is None and Flag.CONFLICTING_VALUES.value in rc.state.flags
    amended = base + [ob(t, val(t) + 1.0, rcv=t + 2000, amend=t + 1500)]
    ra = reconstruct(M, amended, W, TP)
    assert ra.quality == Quality.HEALTHY and Flag.AMENDMENT_APPLIED.value in ra.state.flags
    assert abs(ra.final_value - (ref.final_value + 1.0 / 60)) < 1e-9
    # causal: before the amendment ARRIVES, the original value stands
    before = reconstruct(M, amended, W, TP, as_of_ms=t + 1000)
    assert before.samples[30].value == val(t) and before.quality == Quality.HEALTHY
    fail = replace(TP, conflict_rule="FAIL")
    assert reconstruct(M, amended, W, fail).quality == Quality.CONFLICT


def test_out_of_order_and_determinism():
    base = stream()
    ref = reconstruct(M, base, W, TP)
    shuffled = list(base)
    random.Random(4).shuffle(shuffled)
    rs = reconstruct(M, shuffled, W, TP)
    assert rs.to_dict(True) == ref.to_dict(True) and rs.provenance == ref.provenance      # input order irrelevant
    jitter = [replace(o, receive_ts_ms=o.event_ts_ms + (1500 if i % 3 == 0 else 100)) for i, o in enumerate(base)]
    rj = reconstruct(M, jitter, W, TP)                                                    # within reorder tolerance
    assert rj.final_value == ref.final_value and rj.quality == Quality.HEALTHY
    late = [replace(o, receive_ts_ms=o.event_ts_ms + 4000) if o.event_ts_ms == C - 40_000 else o for o in base]
    rl = reconstruct(M, late, W, TP)                                                      # 4 s behind: out of order
    assert rl.quality == Quality.OUT_OF_ORDER and rl.final_value == ref.final_value
    assert Flag.OUT_OF_ORDER_ARRIVAL.value in rl.state.flags
    for _ in range(3):
        assert reconstruct(M, base, W, TP).provenance["input_digest"] == ref.provenance["input_digest"]


def test_missing_partial_interpolation():
    gap = [o for o in stream() if not (C - 25_000 <= o.event_ts_ms < C - 22_000)]
    strict = reconstruct(M, gap, W, TP)
    assert strict.final_value is None and strict.quality == Quality.INSUFFICIENT_COVERAGE
    assert strict.state.samples_missing == 2 and strict.state.max_gap_s == 2.0 and Flag.GAP.value in strict.state.flags
    rp = replace(reconstruction_policy("research_partial_v1"), trusted_sources=TP.trusted_sources)
    part = reconstruct(M, gap, W, rp)
    usable = [s.value for s in part.samples if s.value is not None]
    assert part.quality == Quality.PARTIAL and abs(part.final_value - sum(usable) / len(usable)) < 1e-9
    assert Flag.PARTIAL_MEAN.value in part.state.flags
    ri = replace(reconstruction_policy("research_interpolate_v1"), trusted_sources=TP.trusted_sources)
    ip = reconstruct(M, gap, W, ri)
    kinds = [s.kind for s in ip.samples]
    assert kinds.count(SampleKind.INTERPOLATED) == 2 and ip.state.samples_filled == 58
    assert ip.quality == Quality.PARTIAL and Flag.INTERPOLATED_SAMPLES.value in ip.state.flags
    big_gap = [o for o in stream() if not (C - 40_000 <= o.event_ts_ms < C - 20_000)]
    assert reconstruct(M, big_gap, W, rp).final_value is None                              # below 90 %: refused
    assert reconstruct(M, [], W, TP).quality == Quality.MISSING
    assert reconstruct(M, stream(), W, reconstruction_policy()).quality == Quality.MISSING  # synthetic never trusted


def test_malformed_and_schema():
    good = cf_frame("BRTI", C - 5000, 100000.5)
    o, iss = parse_cfb_frame(good, receive_ts_ms=C - 4900, seq=1)
    assert o is not None and o.value == 100000.5 and o.event_ts_ms == C - 5000 and o.receive_ts_ms == C - 4900 and not iss
    for bad, kind in ((dict(good, value="abc"), "MALFORMED_VALUE"), (dict(good, value="nan"), "MALFORMED_VALUE"),
                      (dict(good, value="-5"), "MALFORMED_VALUE"), (dict(good, value="0"), "MALFORMED_VALUE"),
                      (dict(good, value=True), "SCHEMA_MISMATCH"), (dict(good, time=(C - 5000) // 1000), "INVALID_TIMESTAMP"),
                      (dict(good, time=1.5e12 + 0.5), "SCHEMA_MISMATCH"), ({k: v for k, v in good.items() if k != "time"}, "SCHEMA_MISMATCH"),
                      (dict(good, type="heartbeat"), "SCHEMA_MISMATCH"), ("not a dict", "SCHEMA_MISMATCH")):
        o, iss = parse_cfb_frame(bad) if isinstance(bad, dict) else parse_cfb_frame({})
        assert o is None and iss and iss[-1].kind == kind, (bad, [i.kind for i in iss])
    o, iss = parse_cfb_frame(dict(good, newField=1))
    assert o is not None and iss[0].kind == "SCHEMA_EXTRA_FIELDS"
    assert structure_fingerprint(good) != structure_fingerprint(dict(good, newField=1))
    assert structure_fingerprint(good) == structure_fingerprint(dict(good, value="1.00", time=C))   # values not in fp
    m = kalshi_message(good, seq=3, last60=100000.25, avg60=100000.1)
    o, avgs, iss = parse_kalshi_cfb_message(json.dumps(m), receive_ts_ms=C - 4800)
    assert o.source == "cfb_ws_via_kalshi" and {a.kind for a in avgs} == {"kalshi_avg_60s", "kalshi_last_60s_windowed_average_15min"}
    assert parse_kalshi_cfb_message({"type": "x", "msg": {}})[2][0].kind == "SCHEMA_MISMATCH"
    assert parse_kalshi_cfb_message("{oops")[2][0].kind == "CORRUPT_RECORD"
    assert parse_kalshi_cfb_message({"type": "x", "msg": {"data": "not json"}})[2][0].kind == "SCHEMA_MISMATCH"
    hist, iss = parse_cfb_historical({"payload": [{"value": "1.5", "time": C}, {"value": "x", "time": C + 1000},
                                                  {"time": C + 2000}]}, "BRTI")
    assert len(hist) == 1 and hist[0].receive_ts_ms is None and [i.kind for i in iss] == ["MALFORMED_VALUE", "SCHEMA_MISMATCH"]
    assert parse_cfb_historical({"data": []}, "BRTI")[1][0].kind == "SCHEMA_MISMATCH"
    for v, ok in (("1.25", True), (2, True), (float("inf"), False), (None, False), ("", False)):
        assert (parse_value(v)[0] is not None) == ok, v
    assert parse_epoch_ms(True)[0] is None and parse_epoch_ms(C)[0] == C
    # a trusted-source schema mismatch inside the window degrades reconstruction (fail closed)
    issue = ParseIssue("cfb_ws_via_kalshi", "SCHEMA_MISMATCH", "missing time", index_id="BRTI", receive_ts_ms=C - 30_000)
    rs = reconstruct(M, stream(), W, TP, issues=[issue])
    assert rs.quality == Quality.SCHEMA_MISMATCH and rs.final_value is None
    early = reconstruct(M, stream(), W, TP, as_of_ms=C - 40_000, issues=[issue])
    assert early.quality != Quality.SCHEMA_MISMATCH                                          # not yet known at T-40 s
    other = replace(issue, index_id="ETHUSD_RTI")
    assert reconstruct(M, stream(), W, TP, issues=[other]).quality == Quality.HEALTHY


def test_stale_late_proxy():
    obs = stream(hi=C - 45_000)
    st = reconstruct(M, obs, W, TP, as_of_ms=C - 30_000).state                              # last obs 15 s old
    assert st.quality in (Quality.STALE, Quality.INSUFFICIENT_COVERAGE) and Flag.STALE_LAST_OBSERVATION.value in st.flags
    st2 = reconstruct(M, stream(lo=C - 400_000, hi=C - 300_000), W, TP, as_of_ms=C - 300_000).state
    assert st2.phase == Phase.PRE_WINDOW and st2.quality == Quality.HEALTHY and Flag.WINDOW_NOT_STARTED.value in st2.flags
    assert st2.samples_elapsed == 0 and st2.current_index == val(C - 301_000)     # the T value arrives 100 ms later
    assert reconstruct(M, [], W, TP, as_of_ms=C - 300_000).state.quality == Quality.MISSING
    stale_pre = reconstruct(M, stream(lo=C - 400_000, hi=C - 390_000), W, TP, as_of_ms=C - 300_000).state
    assert stale_pre.quality == Quality.STALE
    base = stream()
    late = [replace(o, receive_ts_ms=C + 20_000) if C - 10_000 <= o.event_ts_ms < C else o for o in base]
    at_close = reconstruct(M, late, W, TP, as_of_ms=C)
    final = reconstruct(M, late, W, TP)
    assert at_close.final_value is None and at_close.state.samples_missing == 9    # not visible at close (C-10 s is ASOF C-11 s)
    assert final.final_value is not None and Flag.LATE_OBSERVATION.value in final.state.flags     # label may use them
    excl = reconstruct(M, late, W, replace(TP, include_late_in_final=False))
    assert excl.final_value is None and excl.provenance["counts"]["late_excluded"] == 10
    proxy = [replace(o, source="kalshi_perp_reference_price") for o in base]
    rp = reconstruct(M, proxy, W, TP)
    assert rp.quality == Quality.PROXY_SOURCE and rp.final_value is None                          # never a fallback
    mixed = reconstruct(M, base + proxy, W, TP)
    assert mixed.final_value == reconstruct(M, base, W, TP).final_value and Flag.PROXY_SOURCE.value in mixed.state.flags
    wrong_index = reconstruct(M, [replace(o, index_id="ETHUSD_RTI") for o in base], W, TP)
    assert wrong_index.quality == Quality.MISSING


# ═══════════════════ 12-13 live accumulator ═══════════════════
def _messy_stream():
    base = stream(lo=C - 200_000, hi=C + 10_000)
    rng = random.Random(9)
    out = []
    for o in base:
        if C - 33_000 <= o.event_ts_ms < C - 31_000:
            continue                                                                    # gap
        out.append(replace(o, receive_ts_ms=o.event_ts_ms + rng.choice((50, 120, 900, 1700))))
    t = C - 20_000
    out.append(ob(t, rcv=t + 400))                                                      # exact duplicate
    out.append(ob(t - 5000, val(t - 5000) + 2.0, rcv=t + 3000, amend=t + 2500))         # late amendment
    out.append(ob(C - 15_000, val(C - 15_000) + 3.0, rcv=C - 14_000))                   # unresolved conflict
    out.append(ob(C - 50_000, rcv=C - 44_000))                                          # out-of-order (6 s)
    return out


def test_accumulator_equals_batch_and_is_causal():
    obs = _messy_stream()
    ordered = arrival_order(obs, TP)
    acc = SettlementAccumulator(M, W, TP)
    i = 0
    for T in range(C - 200_000, C + 12_000, 500):
        while i < len(ordered) and available_ts(ordered[i], TP) <= T:
            acc.ingest(ordered[i]); i += 1
        a = acc.state(T)
        b = reconstruct(M, obs, W, TP, as_of_ms=T).state
        assert a == b, (T, a.quality, b.quality, a.samples_filled, b.samples_filled)
    fin = acc.result(C + 12_000)
    assert fin.state.quality == reconstruct(M, obs, W, TP, as_of_ms=C + 12_000).state.quality
    acc2 = SettlementAccumulator(M, W, TP)
    acc2.ingest(ob(C - 10_000, rcv=C - 9_000))
    try:
        acc2.state(C - 9_500); raise AssertionError("non-causal state accepted")
    except CausalityError:
        pass
    try:
        acc2.ingest(ob(C - 20_000, rcv=C - 19_000)); raise AssertionError("out-of-arrival-order ingest accepted")
    except CausalityError:
        pass


def test_accumulator_is_incremental():
    acc = SettlementAccumulator(M, W, TP)
    calls = {"n": 0}
    orig = acc.book.sample

    def counting(g):
        calls["n"] += 1
        return orig(g)
    acc.book.sample = counting
    for o in arrival_order(stream(lo=C - 3_600_000, hi=C + 5_000), TP):          # an hour of history
        acc.ingest(o)
        acc.state(available_ts(o, TP))
    assert calls["n"] <= 60 + 2 * acc.sample_revisions + 2, calls                   # each instant sampled ~once
    s = acc.state(C + 5_100)
    assert s.samples_filled == 60 and s.quality == Quality.HEALTHY


# ═══════════════════ 14-17 checkpoints + LABEL LEAKAGE ═══════════════════
RES = OfficialResolution("KXBTC15M-TEST", "yes", 100000.0, "test")


def test_checkpoint_dataset_shape():
    recs = ck.build_checkpoints(M, stream(lo=C - 700_000), RES, W, TP)
    assert [r.key["checkpoint_seconds_remaining"] for r in recs] == [600, 480, 360, 300, 240, 180, 120, 90, 60, 30, 0]
    assert ck.assert_separated(recs) and not set(ck.FEATURE_FIELDS) & set(ck.LABEL_FIELDS)
    assert all(r.labels == recs[0].labels for r in recs)
    assert recs[-1].features["samples_elapsed"] == 60 and recs[0].features["phase"] == "PRE_WINDOW"
    assert recs[-3].features["samples_elapsed"] == 1 and recs[-2].features["samples_elapsed"] == 31   # instant == T counts
    odd = ck.build_checkpoints(M, stream(lo=C - 700_000), RES, W, TP, checkpoints_s=(45.5, 1, 7200))
    assert odd[0].features["samples_elapsed"] == 15 and odd[2].features["phase"] == "PRE_WINDOW"
    assert "resolution" not in inspect.signature(ck.features_at).parameters
    for r in recs:
        row = r.to_row()
        assert all(not k.startswith("f_") or k[2:] in ck.FEATURE_FIELDS for k in row)
    d = tempfile.mkdtemp()
    for name in ("x.csv", "x.jsonl"):
        assert ck.write_dataset(recs, os.path.join(d, name)) == 11
    assert [n for n in os.listdir(d) if n.startswith(".tmp")] == []


def leak_check(builder, market, observations, resolution, checkpoints_s=(600, 300, 90, 30, 0)):
    """Raises AssertionError if any checkpoint's features change when ONLY future information changes."""
    rng = random.Random(1)
    for s in checkpoints_s:
        T = market.close_ts_ms - s * 1000
        base = builder(market, observations, resolution, T)
        vis = lambda o: available_ts(o, TP) <= T and o.event_ts_ms <= T                     # noqa: E731
        future_scaled = [o if vis(o) else replace(o, value=o.value * 1.013) for o in observations]
        future_dropped = [o for o in observations if vis(o) or rng.random() < 0.5]
        future_added = observations + [ob(t, 1.0, rcv=max(t, T) + 10) for t in range(T - 30_000, T + 60_000, 7000)]
        post_close = observations + [ob(t, 5.0) for t in range(market.close_ts_ms, market.close_ts_ms + 30_000, 1000)]
        other_res = OfficialResolution(resolution.ticker, "no" if resolution.result == "yes" else "yes", 1.0, "x")
        for variant, res in ((future_scaled, resolution), (future_dropped, resolution), (future_added, resolution),
                             (post_close, resolution), (observations, other_res), (observations, None)):
            got = builder(market, variant, res, T)
            assert got == base, f"LEAK at T-{s}s"


def honest_builder(market, observations, resolution, T):
    return ck.features_at(market, observations, T, W, TP)


def direct_reconstruct_builder(market, observations, resolution, T):
    """No pre-filtering: guards reconstruct()'s OWN causality (features_at filters too, which could mask a leak)."""
    return reconstruct(market, observations, W, TP, as_of_ms=T).state.to_dict()


def leaky_builder(market, observations, resolution, T):
    f = dict(ck.features_at(market, observations, T, W, TP))
    f["accumulated_mean"] = reconstruct(market, observations, W, TP).final_value            # the final value leaks
    return f


def leaky_label_builder(market, observations, resolution, T):
    f = dict(ck.features_at(market, observations, T, W, TP))
    f["quality"] = (resolution.result if resolution else None)                              # the outcome leaks
    return f


def leaky_as_of_builder(market, observations, resolution, T):
    return reconstruct(market, observations, W, TP, as_of_ms=T + 60_000).state.to_dict()    # post-checkpoint data


def test_no_label_leakage():
    obs = _messy_stream() + stream(lo=C - 700_000, hi=C - 200_001)
    clean = stream(lo=C - 700_000)                        # has a final value, so a leaked final is visible
    assert reconstruct(M, clean, W, TP).final_value is not None
    for builder in (honest_builder, direct_reconstruct_builder):
        leak_check(builder, M, obs, RES)
        leak_check(builder, M, clean, RES)
    # the leakage check itself must catch leaks (otherwise it proves nothing)
    for bad in (leaky_builder, leaky_label_builder, leaky_as_of_builder):
        try:
            leak_check(bad, M, clean, RES)
        except AssertionError as e:
            assert "LEAK" in str(e)
        else:
            raise AssertionError(f"{bad.__name__} was not detected")
    # the full dataset path: every checkpoint of build_checkpoints is invariant to future changes
    ref = ck.build_checkpoints(M, obs, RES, W, TP)
    flipped = ck.build_checkpoints(M, [o if o.event_ts_ms < C - 60_000 else replace(o, value=o.value + 250.0) for o in obs],
                                   OfficialResolution(RES.ticker, "no", 1.0, "x"), W, TP)
    for a, b in zip(ref, flipped):
        if a.key["checkpoint_seconds_remaining"] >= 60:
            assert a.features == b.features, a.key
    assert ref[0].labels != flipped[0].labels


# ═══════════════════ 18-19 resolution + overlap ═══════════════════
def test_resolution_verification():
    base = stream()
    by_idx = index_observations(base)
    ref = reconstruct(M, base, W, TP)
    final, outcome = ref.final_value, ref.reconstructed_outcome
    assert outcome == "no"                                         # the synthetic path ends below the strike
    ok = verify_market(M, OfficialResolution(M.ticker, outcome, final, "t"), by_idx, W, TP)
    assert ok["category"] == "AGREE" and ok["agreement"] is True and ok["expiration_value_abs_diff"] == 0.0
    bad = verify_market(M, OfficialResolution(M.ticker, "yes", final + 5, "t"), by_idx, W, TP)
    assert bad["category"] == "DISAGREE" and bad["agreement"] is False and abs(bad["expiration_value_abs_diff"] - 5) < 1e-9
    assert verify_market(M, None, by_idx, W, TP)["category"] == "NO_OFFICIAL_RESULT"
    gap = index_observations([o for o in base if o.event_ts_ms != C - 30_000 and o.event_ts_ms != C - 31_000])
    miss = verify_market(M, RES, gap, W, TP)
    assert miss["category"] == "NO_RECONSTRUCTION" and miss["quality"] == "INSUFFICIENT_COVERAGE"
    at = replace(M, strike=final)
    assert verify_market(at, RES, by_idx, W, TP)["category"] == "AT_STRIKE"
    rep = verify_all([M], {M.ticker: RES}, base, None, TP)
    assert rep["convention_verdict"]["status"] == "INSUFFICIENT_DATA" and set(rep["policies"]) == set(WINDOW_POLICIES)
    # with enough markets the verifier identifies the generating convention (end-inclusive is rejected)
    d = demo_dataset(32)
    ms, rs = [], {}
    for mj in d["markets"]:
        m, r, _ = parse_market(mj)
        ms.append(m); rs[m.ticker] = r
    obs = []
    for line in d["live_lines"]:
        o, _a, _i = parse_kalshi_cfb_message(line["message"], line["receive_ts_ms"], line["seq"])
        obs.append(o)
    rep2 = verify_all(ms, rs, obs, None, reconstruction_policy())
    v = rep2["convention_verdict"]
    assert v["status"] in ("EVALUATED", "EVALUATED_TIED") and "cf_rti_60s_start_incl_asof_v1" in v["best_policies"]
    assert "cf_rti_60s_end_incl_asof_v1" not in v["best_policies"]
    s = rep2["policies"]["cf_rti_60s_start_incl_asof_v1"]
    assert s["disagree"] == 0 and s["insufficient_coverage"] == 1 and s["agree"] == 31     # the planted gap market
    sc = [r["strike_check"] for r in rep2["rows"] if r["window_policy"] == "cf_rti_60s_start_incl_asof_v1"][1]
    assert sc["abs_diff_vs_strike"] is not None and sc["abs_diff_vs_strike"] < 0.006           # strike = prev window


def test_overlap_tool():
    live = stream()
    hist = [replace(o, source="cfb_rest_history", receive_ts_ms=None) for o in live]
    same = compare_observations(live, hist)
    assert same["common"] == len(live) and same["exact_matches"] == len(live) and same["mismatches"] == 0
    altered = [replace(o, value=o.value + 0.4) if o.event_ts_ms == C - 20_000 else o for o in hist]
    lv = [o for o in live if o.event_ts_ms not in (C - 12_000, C - 11_000)]
    diff = compare_observations(lv, altered + [replace(hist[0], event_ts_ms=C - 500_000)])
    assert diff["mismatches"] == 1 and abs(diff["max_abs_error"] - 0.4) < 1e-9 and diff["missing_in_live"] == 3
    assert diff["worst_mismatches"][0]["event_ts_ms"] == C - 20_000 and diff["extra_in_live"] == 0
    rp = replace(TP, trusted_sources=TP.trusted_sources | {"cfb_rest_history"})
    mk = compare_market(M, lv, altered, W, TP, rp)
    assert mk["boundary_disagreements"] >= 1 and mk["instant_disagreements"] >= 2
    assert mk["live_quality"] == "INSUFFICIENT_COVERAGE" and mk["history_quality"] == "HEALTHY"
    assert len(mk["checkpoints"]) == 11
    from settlement.cf_live import PublishedAverage
    pub = [PublishedAverage("BRTI", "kalshi_last_60s_windowed_average_15min", 100010.0, C, C + 50, "cfb_ws_via_kalshi")]
    pr = compare_published([M], pub, live, W, TP)
    assert pr["compared"] == 1 and abs(pr["rows"][0]["abs_diff"] - abs(100010.0 - reconstruct(M, live, W, TP).final_value)) < 1e-9


# ═══════════════════ 20-21 cache + offline replay ═══════════════════
def test_cache():
    d = tempfile.mkdtemp()
    p = os.path.join(d, "s.jsonl")
    s = cache.SettlementStore(p)
    obs = stream()[:5]
    s.append([("observation", o) for o in obs], session={"tool": "t"})
    s.append([("market", M), ("resolution", RES), ("observation", obs[0])])                  # duplicate obs
    with open(p, "a", encoding="utf-8") as f:
        f.write('{"store":"kalshi_settlement_store","v":1,"kind":"observation","data":{"asset":"BTC"')  # truncated
    lines = open(p, encoding="utf-8").read().splitlines()
    assert sum(1 for x in lines if '"kind":"header"' in x) == 1
    tampered = json.loads(lines[2]); tampered["data"]["value"] = 1.0
    with open(os.path.join(d, "t.jsonl"), "w", encoding="utf-8") as f:
        f.write(json.dumps(tampered) + "\n" + '{"store":"kalshi_settlement_store","v":1,"kind":"weird","data":{},"sha":"x"}\n')
    st = cache.load([os.path.join(d, "t.jsonl"), p])
    assert len(st.observations) == 5 and st.markets[M.ticker] == M and st.resolutions[M.ticker] == RES
    assert sorted(r for _f, _n, r in st.corrupt) == sorted(["checksum mismatch", "unknown kind 'weird'",
                                                           next(r for _f, _n, r in st.corrupt if r.startswith("invalid JSON"))])
    s.append([("observation", stream()[6])])                                                 # appending after a truncated line
    st2 = cache.load(p)
    assert len(st2.observations) == 6 and len(st2.corrupt) == 1
    assert [o.sort_key() for o in st2.observations] == sorted(o.sort_key() for o in st2.observations)
    try:
        cache.SettlementStore(os.path.join(d, "x.jsonl")).append([("session", {"api_key": "x"})]); raise AssertionError
    except cache.StoreError:
        pass
    hdr = json.loads(open(p, encoding="utf-8").readline())
    assert hdr["data"]["created_utc"].endswith("Z")
    cache.write_json_atomic(os.path.join(d, "o.json"), {"a": 1})
    assert json.load(open(os.path.join(d, "o.json"))) == {"a": 1} and not [n for n in os.listdir(d) if n.startswith(".tmp")]
    m2 = replace(M, strike=1.0)
    cache.SettlementStore(p).append([("market", m2)])
    assert cache.load(p).conflicts and cache.load(p).markets[M.ticker] == M                     # first kept, conflict reported


OFFLINE_SCRIPT = r'''
import json, os, socket, sys
def boom(*a, **k): raise RuntimeError("NETWORK USED")
socket.socket.connect = boom; socket.socket.connect_ex = boom; socket.create_connection = boom
sys.path.insert(0, os.path.join(REPO, "scripts")); sys.path.insert(0, REPO)
import verify_settlement_resolution as V, build_settlement_checkpoints as B, compare_settlement_overlap as O
from settlement.cache import load
st = load([STORE])
rep = V.run([STORE], ["cf_rti_60s_start_incl_asof_v1"], None, os.path.join(OUT, "r.json"), os.path.join(OUT, "r.csv"))
n = B.run([STORE], os.path.join(OUT, "c.csv"), (300, 60, 0))
live = [o for o in st.observations if o.source == "cfb_ws_via_kalshi"]
hist = [o for o in st.observations if o.source == "cfb_rest_history"]
ov = O.run(live, hist, list(st.markets.values()), st.published)
print(json.dumps({"summary": rep["policies"]["cf_rti_60s_start_incl_asof_v1"], "rows": n,
                  "overlap": ov["points"], "published": ov["published_vs_live"]["compared"],
                  "store": st.summary()}))
'''


def test_offline_replay_end_to_end():
    import settlement_import as imp
    d = tempfile.mkdtemp()
    data = demo_dataset(6, gap_market=2, history_mismatch_market=4)
    live_f, hist_f, mk_f = (os.path.join(d, n) for n in ("live.jsonl", "hist.json", "markets.json"))
    with open(live_f, "w") as f:
        for line in data["live_lines"]:
            f.write(json.dumps(line) + "\n")
    json.dump(data["history"], open(hist_f, "w"))
    json.dump({"markets": data["markets"]}, open(mk_f, "w"))
    store = os.path.join(d, "store.jsonl")
    for kind, path, idx in (("kalshi-ws-jsonl", live_f, None), ("cf-rest-json", hist_f, "BRTI"), ("kalshi-markets-json", mk_f, None)):
        assert imp.main(["--kind", kind, "--input", path, "--store", store] + (["--index-id", idx] if idx else [])) == 0
    assert imp.main(["--kind", "kalshi-ws-jsonl", "--input", live_f, "--store", os.path.join(d, "none.jsonl"), "--inspect"]) == 0
    assert not os.path.exists(os.path.join(d, "none.jsonl"))
    code = f"REPO = {HERE!r}\nSTORE = {store!r}\nOUT = {d!r}\n" + OFFLINE_SCRIPT
    p = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=d)
    assert p.returncode == 0, p.stderr[-1500:]
    r = json.loads(p.stdout.strip().splitlines()[-1])
    s = r["summary"]
    # live+history combined: history fills the live disconnect; the altered history value makes one CONFLICT
    assert s["markets"] == 6 and s["agree"] == 5 and s["disagree"] == 0, s
    assert s["categories"]["NO_RECONSTRUCTION"] == 1 and s["quality_counts"] == {"CONFLICT": 1, "HEALTHY": 5}
    assert s["expiration_value"]["within_tolerance"] == 5
    assert r["rows"] == 18 and r["overlap"]["mismatches"] == 1 and r["overlap"]["missing_in_live"] == 5
    assert r["published"] >= 5 and r["store"]["corrupt_records"] == 0
    # replay twice -> identical output (determinism from the store)
    p2 = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=d)
    assert p2.stdout == p.stdout


# ═══════════════════ 22 timezone / DST ═══════════════════
def test_timezone_and_dst():
    assert parse_iso_utc_ms("2026-09-21T14:15:00Z")[0] == parse_iso_utc_ms("2026-09-21T10:15:00-04:00")[0] == \
        parse_iso_utc_ms("2026-09-21T14:15:00+00:00")[0]
    assert parse_iso_utc_ms("2026-09-21T14:15:00")[0] is None                               # naive: refused
    m, r, iss = parse_market({"ticker": "KXBTC15M-26SEP211015", "close_time": "2026-09-21T14:15:00", "floor_strike": 1})
    assert m is None and iss[0].kind == "INVALID_TIMESTAMP"
    c = ticker_close_candidates_ms("KXBTC15M-26SEP211015")
    if c is None:
        print("SKIP  22a tz database unavailable: ticker/DST cross-checks not run")
        return
    assert c == [parse_iso_utc_ms("2026-09-21T14:15:00Z")[0]]                              # EDT (UTC-4)
    assert ticker_close_candidates_ms("KXBTC15M-26DEC211015") == [parse_iso_utc_ms("2026-12-21T15:15:00Z")[0]]  # EST
    assert ticker_close_candidates_ms("KXBTC15M-26MAR080215") == []                        # spring-forward gap
    amb = ticker_close_candidates_ms("KXBTC15M-26NOV010115")                                # fall-back hour: ambiguous
    assert amb == [parse_iso_utc_ms("2026-11-01T05:15:00Z")[0], parse_iso_utc_ms("2026-11-01T06:15:00Z")[0]]
    assert check_ticker_close("KXBTC15M-26NOV010115", amb[1]) is True
    assert check_ticker_close("KXBTC15M-26SEP211015", c[0] + 900_000) is False and check_ticker_close("KXBTC15M-X", 0) is None
    m, r, iss = parse_market(market_json("BTC", c[0] + 900_000, 1.0))
    m2, _r, iss2 = parse_market(dict(market_json("BTC", c[0], 1.0), ticker="KXBTC15M-26SEP211015"))
    assert not iss2 and m2.close_ts_ms == c[0]
    bad = dict(market_json("BTC", c[0], 1.0), ticker="KXBTC15M-26SEP211030")
    assert [i.kind for i in parse_market(bad)[2]] == ["TICKER_CLOSE_MISMATCH"]
    # windows straddling a DST change are pure UTC arithmetic: always 60 instants, 1 s apart
    for iso in ("2026-11-01T06:15:00Z", "2026-03-08T07:00:00Z"):
        g = W.grid(parse_iso_utc_ms(iso)[0])
        assert len(g) == 60 and all(b - a == 1000 for a, b in zip(g, g[1:]))


# ═══════════════════ 23-25 fingerprint, markets, report ═══════════════════
def test_settlement_fingerprint():
    ok, problems = sfp.verify()
    assert ok, problems
    d = tempfile.mkdtemp()
    import shutil
    shutil.copytree(os.path.join(HERE, "settlement"), os.path.join(d, "settlement"))
    p = os.path.join(d, "settlement", "policy.py")
    s = open(p).read()
    open(p, "w").write(s.replace("stale_after_ms: int = 5000", "stale_after_ms: int = 6000"))
    ok2, pr2 = sfp.verify(pkg_dir=os.path.join(d, "settlement"))
    assert not ok2 and "modules/policy.py changed" in pr2
    open(p, "w").write(s.replace('"""\nSettlement-window convention', '"""\nReworded docstring. Settlement-window convention', 1)
                       + "\n# a trailing comment\n")
    assert sfp.verify(pkg_dir=os.path.join(d, "settlement"))[0]                              # cosmetic: unchanged
    q = subprocess.run([sys.executable, "-m", "settlement.fingerprint", "--write"], cwd=HERE, capture_output=True, text=True)
    assert q.returncode == 2 and "Refusing" in q.stderr
    b = json.load(open(os.path.join(HERE, "config", "settlement_baseline.json")))
    assert set(b["policies"]["window"]) == set(WINDOW_POLICIES) and set(b["policies"]["reconstruction"]) == set(RECONSTRUCTION_POLICIES)


def test_market_parsing():
    mj = market_json("ETH", C, 3500.25, "no", 3499.5)
    m, r, iss = parse_market({"market": mj})
    assert m.asset == "ETH" and m.index_id == "ETHUSD_RTI" and m.strike == 3500.25 and m.strike_source == "floor_strike"
    assert r.result == "no" and r.expiration_value == 3499.5 and m.close_ts_ms == C
    assert not hasattr(m, "result") and not hasattr(m, "expiration_value")                  # labels live elsewhere
    m, r, iss = parse_market(dict(mj, result="", expiration_value=None))
    assert r is None
    m, r, iss = parse_market(dict(mj, ticker="KXDOGE15M-26SEP211015"))
    assert m is None and iss[-1].kind == "UNSUPPORTED_SERIES"
    m, r, iss = parse_market({k: v for k, v in mj.items() if k != "close_time"})
    assert m is None and iss[0].kind == "SCHEMA_MISMATCH"
    m, r, iss = parse_market(dict(mj, floor_strike=None, cap_strike=2.0))
    assert m.strike == 2.0 and m.strike_source == "cap_strike"


def test_validation_report():
    d = tempfile.mkdtemp()
    data_dir, out = os.path.join(d, "data"), os.path.join(d, "out")
    os.makedirs(data_dir)
    cache.SettlementStore(os.path.join(data_dir, "syn.jsonl")).append(
        [("observation", o) for o in stream()[:3]], session={"tool": "t", "synthetic": True})
    p = subprocess.run([sys.executable, os.path.join(HERE, "scripts", "settlement_validation_report.py"),
                        "--data-dir", data_dir, "--out-dir", out, "--no-synthetic-demo"], capture_output=True, text=True)
    assert p.returncode == 0, p.stderr[-800:]
    rep = json.load(open(os.path.join(out, "settlement_validation.json")))
    assert rep["real_data_status"] == "INSUFFICIENT_DATA" and rep["real_data"]["excluded_synthetic_files"] == ["syn.jsonl"]
    assert rep["real_data"]["inventory"]["store"]["observations"] == 0 and rep["synthetic_demonstration"] is None
    md = open(os.path.join(out, "SETTLEMENT_VALIDATION.md")).read()
    assert "INSUFFICIENT_DATA" in md and "SYNTHETIC demonstration" not in md
    committed = json.load(open(os.path.join(HERE, "analysis_output", "settlement_validation.json")))
    assert committed["real_data_status"] == "INSUFFICIENT_DATA"
    assert committed["synthetic_demonstration"]["is_real_data"] is False


def test_previous_stages():
    # Master mode (run_all_tests.py): every stage is already run exactly once by the runner.
    if os.environ.get("KALSHI_MASTER_TEST_RUN") == "1":
        print("  (master run: earlier stages are run once each by run_all_tests.py)")
        return
    # Standalone: re-verify earlier stages, each EXACTLY once (children run in master mode).
    env = dict(os.environ, KALSHI_MASTER_TEST_RUN="1")
    for i in range(1, 18):
        p = subprocess.run([sys.executable, f"test_stage{i}.py"], cwd=HERE, capture_output=True, text=True, env=env)
        assert p.returncode == 0 and f"All Stage {i} tests passed." in p.stdout, (i, p.stdout[-600:], p.stderr[-600:])


if __name__ == "__main__":
    run("1 Step-1 baseline untouched: artifacts, fingerprints, 47 strategy fixtures", test_step1_untouched)
    run("2 import boundaries: settlement is isolated; production never imports it; LIVE refused",
        test_import_boundaries_and_no_execution)
    run("3 window convention: grid, begin/close inclusivity, membership, policy validation, TZ-independent",
        test_window_boundaries)
    run("4 boundary observations: start/close instants, as-of age, EXACT/BUCKET, tie at strike", test_boundary_observations)
    run("5 duplicates, conflicts, amendments (causal)", test_duplicates_conflicts_amendments)
    run("6 out-of-order arrivals + deterministic reconstruction", test_out_of_order_and_determinism)
    run("7 missing data fails closed; research partial/interpolation distinguishable", test_missing_partial_interpolation)
    run("8 malformed values + schema capture / mismatch", test_malformed_and_schema)
    run("9 stale, late and proxy data (no fallback)", test_stale_late_proxy)
    run("10 live accumulator == batch reconstruction at every checkpoint; causality guard",
        test_accumulator_equals_batch_and_is_causal)
    run("11 live accumulator is incremental (no full recomputation)", test_accumulator_is_incremental)
    run("12 checkpoint dataset: default/arbitrary checkpoints, features vs labels", test_checkpoint_dataset_shape)
    run("13 LABEL LEAKAGE: future data / outcome cannot reach features; leaky builders are caught", test_no_label_leakage)
    run("14 resolution verification + convention verdict", test_resolution_verification)
    run("15 live-vs-history overlap + published averages", test_overlap_tool)
    run("16 cache: append-safe, checksums, corruption, secrets refused, deterministic", test_cache)
    run("17 offline replay end-to-end with the network disabled", test_offline_replay_end_to_end)
    run("18 timezone correctness + DST independence", test_timezone_and_dst)
    run("19 separate settlement fingerprint", test_settlement_fingerprint)
    run("20 Kalshi market parsing (features vs labels)", test_market_parsing)
    run("21 validation report: real data only, synthetic excluded/labelled", test_validation_report)
    run("22 all previous stage suites", test_previous_stages)
    print("\nAll Stage 18 tests passed.")
