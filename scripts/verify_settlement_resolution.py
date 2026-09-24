#!/usr/bin/env python3
"""
Compare reconstructed settlements with Kalshi's official results (offline, from the store).

    py scripts/verify_settlement_resolution.py
    py scripts/verify_settlement_resolution.py --store settlement_data/a.jsonl --window-policy cf_rti_60s_start_incl_asof_v1
    outputs: analysis_output/settlement_resolution.json and .csv (one row per market x window policy)
"""
import argparse
import csv
import os
import sys

import _settlement_cli as cli
from settlement.cache import load, write_json_atomic
from settlement.policy import WINDOW_POLICIES, reconstruction_policy
from settlement.resolution import verify_all


def run(stores, window_policies=None, recon_policy=None, out_json=None, out_csv=None):
    st = load(stores)
    rpol = reconstruction_policy(recon_policy)
    rep = verify_all(list(st.markets.values()), st.resolutions, st.observations, window_policies, rpol, st.issues)
    rep["store"] = st.summary()
    rep["corrupt_records"] = [list(c) for c in st.corrupt[:50]]
    if out_json:
        write_json_atomic(out_json, rep)
    if out_csv and rep["rows"]:
        os.makedirs(os.path.dirname(os.path.abspath(out_csv)), exist_ok=True)
        cols = [k for k in rep["rows"][0] if k not in ("flags", "sources", "strike_check")] + ["flags", "sources"]
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            for r in rep["rows"]:
                w.writerow(dict(r, flags=";".join(r["flags"]), sources=";".join(r["sources"])))
    return rep


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", action="append")
    ap.add_argument("--window-policy", action="append", choices=sorted(WINDOW_POLICIES))
    ap.add_argument("--reconstruction-policy", default=None)
    ap.add_argument("--out-json", default=os.path.join(cli.REPO, "analysis_output", "settlement_resolution.json"))
    ap.add_argument("--out-csv", default=os.path.join(cli.REPO, "analysis_output", "settlement_resolution.csv"))
    a = ap.parse_args(argv)
    paths = cli.store_paths(a.store)
    if not paths:
        print("No settlement store found (settlement_data/*.jsonl). Import captured data first; see docs/SETTLEMENT_ENGINE.md.")
        return 1
    rep = run(paths, a.window_policy, a.reconstruction_policy, a.out_json, a.out_csv)
    for pid, s in rep["policies"].items():
        print(f"{pid}: markets={s['markets']} adequate={s['with_adequate_data']} compared={s['compared']} "
              f"agree={s['agree']} disagree={s['disagree']}")
    print("convention verdict:", rep["convention_verdict"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
