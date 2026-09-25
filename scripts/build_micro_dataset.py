#!/usr/bin/env python3
"""
Build the JOINT research dataset (Step 2 labels + Step 3 + Step 4 + Step 5 features) from captured sessions.
RESEARCH ONLY - offline, no network, nothing is fed to the production watcher or the existing perp veto.

    py scripts/build_micro_dataset.py market_data_sessions/<session> --assets BTC --out analysis_output/joint_btc
    py scripts/build_micro_dataset.py <s1> <s2> --assets BTC,ETH --checkpoints 600,300,120,60,30,10 --format jsonl
    py scripts/build_micro_dataset.py <session> --assets BTC --no-step3 --no-perp       # micro features only
    py scripts/build_micro_dataset.py <session> --assets BTC --grid-ms 250 --out analysis_output/grid_btc
    py scripts/build_micro_dataset.py <session> --assets BTC --trade-labels binance_usdm_book --out analysis_output/impact

Outputs (separate files): features.csv|jsonl, labels.jsonl (Step-2 settlement labels), micro_labels.jsonl (POST-EVENT
forward mid moves - research labels, never features), provenance.json. --grid-ms writes a sub-second grid (sources
slower than the step are excluded, not forward-filled); --trade-labels writes per-trade price-impact labels.
"""
import argparse
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from market_data.replay import load_sessions                                   # noqa: E402
from microstructure.dataset import build_joint_dataset, write_joint_dataset    # noqa: E402
from microstructure.grid import STEPS_MS, subsecond_rows                       # noqa: E402
from microstructure.labels import trade_impact_labels                          # noqa: E402
from microstructure.replay import load_micro_sessions                           # noqa: E402
from microstructure.venues import BOOK_VENUES                                  # noqa: E402
from perp_data.replay import load_perp_sessions                                # noqa: E402
from settlement.checkpoints import DEFAULT_CHECKPOINTS_S                       # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser(description="Joint Step 2-5 research dataset (offline).")
    ap.add_argument("sessions", nargs="+")
    ap.add_argument("--assets", default="BTC")
    ap.add_argument("--out", default=os.path.join(REPO, "analysis_output", "joint_dataset"))
    ap.add_argument("--checkpoints", default=",".join(str(x) for x in DEFAULT_CHECKPOINTS_S))
    ap.add_argument("--format", choices=("csv", "jsonl"), default="csv")
    ap.add_argument("--no-step3", action="store_true")
    ap.add_argument("--no-perp", action="store_true")
    ap.add_argument("--grid-ms", type=int, choices=STEPS_MS, default=None)
    ap.add_argument("--trade-labels", choices=BOOK_VENUES, default=None)
    a = ap.parse_args(argv)
    assets = [x.strip().upper() for x in a.assets.split(",") if x.strip()]
    s3 = load_sessions(a.sessions)
    p4 = None if a.no_perp else load_perp_sessions(a.sessions)
    m5 = load_micro_sessions(a.sessions)
    os.makedirs(a.out, exist_ok=True)
    if a.grid_ms or a.trade_labels:
        evs = list(s3.events) + (list(p4.events) if p4 else []) + list(m5.events)
        for asset in assets:
            if a.grid_ms:
                ts = [e.receive_ts_ms for e in m5.events]
                g = subsecond_rows(asset, evs, min(ts), max(ts), a.grid_ms)
                path = os.path.join(a.out, f"grid_{asset}_{a.grid_ms}ms.jsonl")
                with open(path, "w", encoding="utf-8") as f:
                    f.write(json.dumps({k: v for k, v in g.items() if k != "rows"}, sort_keys=True) + "\n")
                    for r in g["rows"]:
                        f.write(json.dumps(r, sort_keys=True, default=str) + "\n")
                print(f"{asset}: {len(g['rows'])} grid rows at {a.grid_ms} ms -> {path}; excluded {g['excluded_sources']}")
            if a.trade_labels:
                labs = trade_impact_labels(evs, asset, a.trade_labels)
                path = os.path.join(a.out, f"trade_impact_{asset}_{a.trade_labels}.jsonl")
                with open(path, "w", encoding="utf-8") as f:
                    for r in labs:
                        f.write(json.dumps(r, sort_keys=True) + "\n")
                print(f"{asset}: {len(labs)} POST-EVENT trade impact labels -> {path}")
        return 0
    cps = tuple(float(x) for x in a.checkpoints.split(",") if x.strip())
    ds = build_joint_dataset(s3, p4, m5, assets, cps, include_step3=not a.no_step3, include_perp=not a.no_perp)
    path = write_joint_dataset(ds, a.out, a.format)
    print(f"{len(ds.rows)} rows ({ds.skipped} skipped), {len(ds.labels)} markets labelled, {len(ds.micro_labels)} post-event "
          f"label rows -> {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
