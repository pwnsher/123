#!/usr/bin/env python3
"""
Build the causal RESEARCH feature dataset from captured sessions (offline, no network).

    py scripts/build_market_features.py market_data_sessions/<session> [more sessions] \
        --assets BTC,ETH --out analysis_output/features_<name> [--format csv|jsonl] [--partial]

Writes features.<fmt> (values + <feature>__status masks; missing values are EMPTY, never 0),
labels.jsonl (separate: final settlement reconstruction + official Kalshi result) and provenance.json
(feature-set version, sessions, sources, definitions, windows, strategy / settlement / market-data
fingerprints). --partial enables the named research mode with PARTIAL windows (default is strict).
"""
import argparse
import os
import sys
from collections import Counter

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from market_data.features.dataset import build_dataset, write_dataset  # noqa: E402
from market_data.features.definitions import FeatureConfig  # noqa: E402
from market_data.manifest import load_manifest  # noqa: E402
from market_data.replay import load_sessions  # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser(description="Build the causal research feature dataset (offline).")
    ap.add_argument("sessions", nargs="+")
    ap.add_argument("--assets", default="BTC,ETH,SOL,XRP")
    ap.add_argument("--out", required=True)
    ap.add_argument("--format", choices=("csv", "jsonl"), default="csv")
    ap.add_argument("--partial", action="store_true", help="research mode: PARTIAL windows allowed")
    ap.add_argument("--sources", default="cf,coinbase,kraken,kalshi")
    a = ap.parse_args(argv)
    manifests = []
    for s in a.sessions:
        try:
            manifests.append(load_manifest(s))
        except (OSError, ValueError):
            manifests.append({"session_id": os.path.basename(s.rstrip("/\\")), "fingerprints": None})
    loaded = load_sessions(a.sessions)
    cfg = FeatureConfig(partial_windows=a.partial)
    ds = build_dataset(loaded, [x.strip().upper() for x in a.assets.split(",") if x.strip()], config=cfg,
                       sources_enabled=tuple(x.strip() for x in a.sources.split(",") if x.strip()),
                       session_manifests=manifests)
    path = write_dataset(ds, a.out, a.format)
    st = Counter(v for r in ds.rows for v in r["status"].values())
    print(f"rows {len(ds.rows)}  markets labelled {len(ds.labels)}  skipped {dict(ds.skipped)}")
    print("status counts: " + ", ".join(f"{k}={v}" for k, v in sorted(st.items())))
    if any(m.get("status") and "SYNTHETIC" in str(m.get("notes")) for m in manifests):
        print("NOTE: input includes SYNTHETIC sessions - not market data.")
    print(f"wrote {path} (+ labels.jsonl, provenance.json)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
