"""Shared helpers for the settlement scripts (path setup, store discovery, source split)."""
import glob
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO not in sys.path:
    sys.path.insert(0, REPO)

DEFAULT_DATA_DIR = os.path.join(REPO, "settlement_data")
DEFAULT_STORE = os.path.join(DEFAULT_DATA_DIR, "settlement_store.jsonl")
LIVE_SOURCES = {"cfb_ws", "cfb_ws_via_kalshi"}
HISTORY_SOURCES = {"cfb_rest_history", "cfb_rest_via_kalshi"}


def store_paths(stores, data_dir=DEFAULT_DATA_DIR):
    if stores:
        return list(stores)
    return sorted(glob.glob(os.path.join(data_dir, "*.jsonl")))


def split_live_history(observations):
    live = [o for o in observations if o.source in LIVE_SOURCES]
    hist = [o for o in observations if o.source in HISTORY_SOURCES]
    return live, hist
