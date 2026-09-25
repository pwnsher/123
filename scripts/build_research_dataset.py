#!/usr/bin/env python3
"""
Build the JOINT Step-3 + Step-4 causal research dataset from captured sessions (offline, no network).

    py scripts/build_research_dataset.py market_data_sessions/<session> [more] --assets BTC,ETH \
        --out analysis_output/research_btc [--format csv|jsonl] [--perp-venues binance_usdm,bybit_linear]

Writes features.<fmt> (Step-3 + perp values and <feature>__status masks; missing = empty, never 0), labels.jsonl
(separate) and provenance.json (versions, sessions, venues, feature definitions + FAMILIES for later family-level
ablation, fingerprints incl. the untouched existing perp-veto chain). Nothing here feeds the production veto.
"""
import argparse
import os
import sys
from collections import Counter

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from market_data.replay import load_sessions  # noqa: E402
from perp_data.dataset import build_research_dataset, write_research_dataset  # noqa: E402
from perp_data.features.definitions import PerpFeatureConfig  # noqa: E402
from perp_data.replay import load_perp_sessions  # noqa: E402
from perp_data.venues import PERP_VENUES  # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser(description="Build the joint causal research dataset (offline).")
    ap.add_argument("sessions", nargs="+")
    ap.add_argument("--assets", default="BTC,ETH,SOL,XRP")
    ap.add_argument("--out", required=True)
    ap.add_argument("--format", choices=("csv", "jsonl"), default="csv")
    ap.add_argument("--perp-venues", default=",".join(PERP_VENUES))
    ap.add_argument("--partial", action="store_true", help="research mode: PARTIAL windows allowed")
    a = ap.parse_args(argv)
    s3 = load_sessions(a.sessions)
    p4 = load_perp_sessions(a.sessions)
    ds = build_research_dataset(s3, p4, [x.strip().upper() for x in a.assets.split(",") if x.strip()],
                                p4_config=PerpFeatureConfig(partial_windows=a.partial),
                                venues=tuple(v.strip() for v in a.perp_venues.split(",") if v.strip()))
    path = write_research_dataset(ds, a.out, a.format)
    st = Counter(v for r in ds.rows for v in r["perp_status"].values())
    print(f"rows {len(ds.rows)}  markets labelled {len(ds.labels)}  skipped {dict(ds.skipped)}")
    print("perp status counts: " + ", ".join(f"{k}={v}" for k, v in sorted(st.items())))
    if any(m and m.get("synthetic") for m in p4.manifests):
        print("NOTE: input includes SYNTHETIC sessions - not market data.")
    print(f"wrote {path} (+ labels.jsonl, provenance.json)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
