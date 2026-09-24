#!/usr/bin/env python3
"""
Causal checkpoint dataset (features f_*, labels y_*) from the offline store.

    py scripts/build_settlement_checkpoints.py --out analysis_output/settlement_checkpoints.csv
    py scripts/build_settlement_checkpoints.py --checkpoints 600,300,60,0 --out ckpt.jsonl
Research data only: nothing here feeds the production watcher.
"""
import argparse
import os
import sys

import _settlement_cli as cli
from settlement.cache import load
from settlement.checkpoints import DEFAULT_CHECKPOINTS_S, build_checkpoints, write_dataset
from settlement.policy import reconstruction_policy, window_policy


def run(stores, out, checkpoints_s=DEFAULT_CHECKPOINTS_S, wpol_id=None, rpol_id=None):
    st = load(stores)
    wpol, rpol = window_policy(wpol_id), reconstruction_policy(rpol_id)
    by_index = {}
    for o in st.observations:
        by_index.setdefault(o.index_id, []).append(o)
    records = []
    for m in sorted(st.markets.values(), key=lambda m: (m.close_ts_ms, m.ticker)):
        lo = m.close_ts_ms - max(checkpoints_s) * 1000 - 600_000
        obs = [o for o in by_index.get(m.index_id, []) if lo <= o.event_ts_ms <= m.close_ts_ms]
        records += build_checkpoints(m, obs, st.resolutions.get(m.ticker), wpol, rpol, checkpoints_s, st.issues)
    n = write_dataset(records, out) if records else 0
    return n


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", action="append")
    ap.add_argument("--out", default=os.path.join(cli.REPO, "analysis_output", "settlement_checkpoints.csv"))
    ap.add_argument("--checkpoints", default=",".join(str(s) for s in DEFAULT_CHECKPOINTS_S))
    ap.add_argument("--window-policy", default=None)
    ap.add_argument("--reconstruction-policy", default=None)
    a = ap.parse_args(argv)
    paths = cli.store_paths(a.store)
    if not paths:
        print("No settlement store found; nothing to build.")
        return 1
    cps = tuple(float(x) if "." in x else int(x) for x in a.checkpoints.split(",") if x.strip())
    n = run(paths, a.out, cps, a.window_policy, a.reconstruction_policy)
    print(f"wrote {n} checkpoint rows to {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
