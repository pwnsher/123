#!/usr/bin/env python3
"""
Retention for captured research sessions (local files only).

    py scripts/prune_sessions.py market_data_sessions --keep-days 14            # list what WOULD be deleted (default)
    py scripts/prune_sessions.py market_data_sessions --keep-days 14 --delete   # delete whole session directories

Only complete sessions (a manifest with a start time and a status other than RUNNING) older than --keep-days are
considered; a session is always removed as a whole (Step-3 store, perp/ and micro/ together), never partially.
"""
import argparse
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from microstructure.storage import prune_sessions  # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser(description="Session retention (dry run unless --delete).")
    ap.add_argument("root")
    ap.add_argument("--keep-days", type=float, required=True)
    ap.add_argument("--delete", action="store_true")
    a = ap.parse_args(argv)
    if a.keep_days < 0:
        ap.error("--keep-days must be >= 0")
    out = prune_sessions(a.root, a.keep_days, dry_run=not a.delete)
    for d, why in out:
        print(("deleted " if a.delete else "would delete ") + f"{d}: {why}")
    print(f"{len(out)} session(s) {'deleted' if a.delete else 'eligible (dry run; pass --delete)'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
