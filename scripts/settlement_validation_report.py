#!/usr/bin/env python3
"""
Settlement validation report -> analysis_output/settlement_validation.json + SETTLEMENT_VALIDATION.md

    py scripts/settlement_validation_report.py                 # real data in settlement_data/ + synthetic demo
    py scripts/settlement_validation_report.py --no-synthetic-demo

REAL DATA section: only store files whose capture sessions are all non-synthetic. Resolution agreement
(combined / live-only / history-only, every window convention), live-vs-history overlap and Kalshi
published averages. With fewer than 30 comparable markets it states INSUFFICIENT_DATA.
SYNTHETIC DEMONSTRATION section (optional): an in-memory fake world run through the same code path in a
temporary directory. It proves the tooling works end to end; it says nothing about Kalshi.
"""
import argparse
import json
import os
import sys
import tempfile

import _settlement_cli as cli
from settlement import ENGINE_VERSION, RECONSTRUCTION_VERSION
from settlement.cache import SettlementStore, load, write_json_atomic
from settlement.policy import reconstruction_policy
from settlement.resolution import MIN_MARKETS_FOR_CONVENTION_VERDICT, verify_all
import compare_settlement_overlap as overlap_cli
import settlement_import as importer

STATIC_FINDINGS = [
    {"id": "WINDOW_CONVENTION_UNVERIFIED",
     "finding": "Public descriptions say the settlement is the mean of the CF RTI sampled once per second over the 60 s "
                "before close, but not which 60 instants (close-60..close-1 vs close-59..close) nor how a sample is "
                "taken. The default convention is an assumption; verify_settlement_resolution evaluates every "
                "candidate against Kalshi's expiration_value."},
    {"id": "BACKTEST_PROXY_MISALIGNED",
     "finding": "kalshi_backtest.backtest_coin and kalshi_dashboard.backtest_coin key Coinbase 1-min candles by bucket "
                "START: the 'strike' is the close at open+1 min and the 'settlement' is the close at close+1 min, not a "
                "60-s CF RTI average before close. Unchanged in Step 2 (strategy-pinned); documented only."},
    {"id": "PRODUCTION_MODEL_SPOT_NOT_INDEX",
     "finding": "kalshi_dashboard.evaluate compares Coinbase spot with the strike over the minutes to close; it does "
                "not model the CF RTI, the CF-vs-Coinbase basis, or the variance reduction of 60-s averaging. "
                "Unchanged (research target for Step 3+)."},
    {"id": "LABELER_DROPS_EXPIRATION_VALUE",
     "finding": "label_binary_outcomes keeps market.result only; expiration_value (the settled index value) is not "
                "stored. The new kalshi_markets parser keeps both."},
    {"id": "PERP_REFERENCE_IS_A_PROXY",
     "finding": "The telemetry index_price is Kalshi's perp reference_price, 'CF index scaled per contract', sampled "
                "every ~4 s. It is loaded only as PROXY_SOURCE and never used for settlement."},
]


def _synthetic_store_file(path):
    st = load(path)
    return any(s.get("synthetic") for s in st.sessions) or any(o.source == "synthetic" for o in st.observations)


def inventory(st):
    by_source, by_index = {}, {}
    for o in st.observations:
        by_source[o.source] = by_source.get(o.source, 0) + 1
        by_index[o.index_id] = by_index.get(o.index_id, 0) + 1
    ts = [o.event_ts_ms for o in st.observations]
    ik = {}
    for i in st.issues:
        ik[i.kind] = ik.get(i.kind, 0) + 1
    return {"store": st.summary(), "observations_by_source": by_source, "observations_by_index": by_index,
            "event_ts_min_ms": min(ts) if ts else None, "event_ts_max_ms": max(ts) if ts else None,
            "resolutions_with_result": sum(1 for r in st.resolutions.values() if r.result),
            "resolutions_with_expiration_value": sum(1 for r in st.resolutions.values() if r.expiration_value is not None),
            "issue_kinds": ik, "corrupt_examples": [list(c) for c in st.corrupt[:10]]}


def analyse(st, rpol_id=None):
    rpol = reconstruction_policy(rpol_id)
    live, hist = cli.split_live_history(st.observations)
    markets = list(st.markets.values())
    res = {}
    for name, obs in (("combined", st.observations), ("live_only", live), ("history_only", hist)):
        r = verify_all(markets, st.resolutions, obs, None, rpol, st.issues)
        res[name] = {"policies": r["policies"], "convention_verdict": r["convention_verdict"],
                     "disagreement_rows": [x for x in r["rows"] if x["category"] == "DISAGREE"][:50],
                     "failure_rows": [{"ticker": x["ticker"], "window_policy": x["window_policy"], "quality": x["quality"],
                                       "category": x["category"], "coverage": x["coverage"]}
                                      for x in r["rows"] if x["category"] in ("NO_RECONSTRUCTION",)][:100]}
    ov = overlap_cli.run(live, hist, markets, st.published)
    compared = max((s["compared"] for s in res["combined"]["policies"].values()), default=0)
    status = "INSUFFICIENT_DATA" if compared < MIN_MARKETS_FOR_CONVENTION_VERDICT else "EVALUATED"
    return {"status": status, "markets_compared_max_over_policies": compared,
            "minimum_for_statement": MIN_MARKETS_FOR_CONVENTION_VERDICT, "resolution": res, "overlap": ov,
            "reconstruction_policy": rpol.policy_id}


def synthetic_demo(n_markets=40):
    from settlement.synthetic import demo_dataset
    d = demo_dataset(n_markets)
    with tempfile.TemporaryDirectory() as tmp:
        live_f = os.path.join(tmp, "live.jsonl")
        with open(live_f, "w", encoding="utf-8") as f:
            for line in d["live_lines"]:
                f.write(json.dumps(line) + "\n")
        hist_f = os.path.join(tmp, "hist.json")
        with open(hist_f, "w", encoding="utf-8") as f:
            json.dump(d["history"], f)
        mk_f = os.path.join(tmp, "markets.json")
        with open(mk_f, "w", encoding="utf-8") as f:
            json.dump({"markets": d["markets"]}, f)
        store = os.path.join(tmp, "demo_store.jsonl")
        for kind, path, idx in (("kalshi-ws-jsonl", live_f, None), ("cf-rest-json", hist_f, d["index_id"]),
                                ("kalshi-markets-json", mk_f, None)):
            recs, issues = importer.parse_file(kind, path, idx)
            SettlementStore(store).append(recs + [("issue", i) for i in issues],
                                          session={"tool": "settlement_validation_report(demo)", "kind": kind,
                                                   "synthetic": True})
        st = load(store)
        out = analyse(st)
    out.update({"is_real_data": False, "generated_by": "settlement.synthetic.demo_dataset", "markets": n_markets,
                "planted_defects": {"live_disconnect_market": d["gap_market"],
                                    "history_value_altered_market": d["history_mismatch_market"],
                                    "history_value_altered_ts_ms": d["history_mismatch_ts"]},
                "caveat": "Official values were generated with the default convention BY CONSTRUCTION; agreement "
                          "demonstrates the tooling only and is NOT evidence about Kalshi's settlement."})
    return out


def render_md(rep):
    L = ["# Settlement validation report", "",
         f"Engine `{rep['engine_version']}` / `{rep['reconstruction_version']}`. Generated by "
         "`py scripts/settlement_validation_report.py`.", "", "## Real data", ""]
    r = rep["real_data"]
    inv = r["inventory"]
    L += [f"* Store files used: {len(r['store_files'])}; excluded as synthetic: {len(r['excluded_synthetic_files'])}",
          f"* Observations: {inv['store']['observations']} {inv['observations_by_source']}",
          f"* Markets: {inv['store']['markets']}; with official result: {inv['resolutions_with_result']}; "
          f"with expiration_value: {inv['resolutions_with_expiration_value']}",
          f"* Corrupt records: {inv['store']['corrupt_records']}; parse issues: {inv['issue_kinds']}", ""]
    if r.get("analysis") is None or r["analysis"]["status"] == "INSUFFICIENT_DATA":
        n = r["analysis"]["markets_compared_max_over_policies"] if r.get("analysis") else 0
        L += [f"**Status: INSUFFICIENT_DATA.** {n} market(s) could be compared with an official Kalshi outcome "
              f"(at least {MIN_MARKETS_FOR_CONVENTION_VERDICT} are needed for any statement). No agreement rate, "
              "convention verdict or overlap statistic is claimed from real data.", ""]
    if r.get("analysis"):
        for name, block in r["analysis"]["resolution"].items():
            L.append(f"### Resolution ({name})")
            L.append("| window policy | markets | adequate | compared | agree | disagree | EV within tol |")
            L.append("|---|---|---|---|---|---|---|")
            for pid, s in block["policies"].items():
                L.append(f"| {pid} | {s['markets']} | {s['with_adequate_data']} | {s['compared']} | {s['agree']} | "
                         f"{s['disagree']} | {s['expiration_value']['within_tolerance']}/{s['expiration_value']['compared']} |")
            L += ["", f"Convention verdict: `{block['convention_verdict']}`", ""]
        L += [f"Overlap: `{r['analysis']['overlap']['summary']}`", ""]
    L += ["## Boundary findings (from the code)", ""] + [f"* **{f['id']}** — {f['finding']}" for f in rep["boundary_findings"]]
    s = rep.get("synthetic_demonstration")
    if s:
        L += ["", "## SYNTHETIC demonstration (not real data)", "", f"> {s['caveat']}", "",
              f"Planted defects: `{s['planted_defects']}`", ""]
        for pid, v in s["resolution"]["combined"]["policies"].items():
            L.append(f"* combined / {pid}: compared {v['compared']}, agree {v['agree']}, disagree {v['disagree']}, "
                     f"EV within tol {v['expiration_value']['within_tolerance']}/{v['expiration_value']['compared']}")
        lo = s["resolution"]["live_only"]["policies"]
        L.append(f"* live only / default: {lo[next(iter(lo))]['quality_counts']}")
        pts = s["overlap"]["points"]
        L += [f"* overlap points: common {pts['common']}, exact {pts['exact_matches']}, mismatches {pts['mismatches']}, "
              f"missing in live {pts['missing_in_live']}, max abs error {pts['max_abs_error']}",
              f"* convention verdict (synthetic): `{s['resolution']['combined']['convention_verdict']}`"]
    return "\n".join(L) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=cli.DEFAULT_DATA_DIR)
    ap.add_argument("--out-dir", default=os.path.join(cli.REPO, "analysis_output"))
    ap.add_argument("--no-synthetic-demo", action="store_true")
    a = ap.parse_args(argv)
    paths = cli.store_paths(None, a.data_dir)
    synthetic_files = [p for p in paths if _synthetic_store_file(p)]
    real_files = [p for p in paths if p not in synthetic_files]
    st = load(real_files) if real_files else None
    real = {"store_files": [os.path.basename(p) for p in real_files],
            "excluded_synthetic_files": [os.path.basename(p) for p in synthetic_files],
            "inventory": inventory(st) if st else inventory(load([])),
            "analysis": analyse(st) if st and st.markets else None}
    rep = {"report": "settlement_validation", "engine_version": ENGINE_VERSION,
           "reconstruction_version": RECONSTRUCTION_VERSION, "real_data": real,
           "real_data_status": real["analysis"]["status"] if real["analysis"] else "INSUFFICIENT_DATA",
           "boundary_findings": STATIC_FINDINGS,
           "synthetic_demonstration": None if a.no_synthetic_demo else synthetic_demo()}
    os.makedirs(a.out_dir, exist_ok=True)
    write_json_atomic(os.path.join(a.out_dir, "settlement_validation.json"), rep)
    with open(os.path.join(a.out_dir, "SETTLEMENT_VALIDATION.md"), "w", encoding="utf-8", newline="\n") as f:
        f.write(render_md(rep))
    print(f"real data status: {rep['real_data_status']} (store files: {len(real_files)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
