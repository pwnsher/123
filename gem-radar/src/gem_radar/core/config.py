"""Configuration: defaults, overridden by a JSON file and selected env vars.

Credentials are never stored here: providers read them from environment
variables by name (see config.example.json / .env.example).
"""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from typing import Any

DEFAULTS: dict[str, Any] = {
    "verdict_bands": {"gem_min": 80, "watch_min": 40},
    "critical_cap": 20,
    # Below this share of verified scoring inputs (and with no critical flag) the
    # verdict is UNRATED: missing data is never read as a negative.
    "min_completeness_for_verdict": 0.4,
    "thresholds": {
        "dev_critical_pct": 15.0,
        "sell_tax_critical_pct": 50.0,
        "buy_tax_critical_pct": 50.0,
        "unlocked_lp_max_locked_pct": 50.0,     # LP locked/burned below this is "removable"
        "unlocked_lp_top10_critical_pct": 50.0,  # ...and top10 above this -> CRITICAL
        "top10_high_pct": 60.0,
        "wash_volume_liquidity_ratio": 15.0,
        "blacklist_is_critical": False,
    },
    # Seconds a reading stays CACHED (not STALE), per freshness class.
    "freshness_seconds": {
        "market": 300,
        "liquidity": 900,
        "holders": 3600,
        "contract": 86400,
        "age": 7 * 86400,
        "social": 86400,
    },
    # /gem reuses a provider response this recent; /gem-recheck never does.
    "cache_ttl_seconds": 120,
    "conflict_policy": "conservative",  # or "exclude"
    "deep_scan": {
        "score_low": 40,
        "score_high": 80,
        "confidence_below": 60,
        "on_conflict": True,
        "on_suspicious_activity": True,
    },
    "http": {"timeout_seconds": 10, "retries": 2, "backoff_seconds": 1.0},
    "rate_limit_min_interval_seconds": {
        "dexscreener": 0.25,
        "goplus": 1.0,
        "honeypot_is": 0.5,
        "rugcheck": 0.5,
        "solana_rpc": 0.2,
        "evm_rpc": 0.2,
    },
    "provider_reliability": {
        "dexscreener": 0.85,
        "goplus": 0.8,
        "honeypot_is": 0.85,
        "rugcheck": 0.8,
        "solana_rpc": 0.95,
        "evm_rpc": 0.95,
        "local": 0.5,
        "mock": 0.5,
    },
    "providers_disabled": [],
}

ENV_CONFIG_PATH = "GEM_RADAR_CONFIG"
ENV_HOME = "GEM_RADAR_HOME"
ENV_DATA_DIR = "GEM_RADAR_DATA_DIR"


def _merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _merge(out[k], v)
        else:
            out[k] = v
    return out


def home_dir() -> Path:
    """Where history, watchlist and cache live (outside the plugin folder)."""
    p = Path(os.environ.get(ENV_HOME) or Path.home() / ".gem-radar")
    p.mkdir(parents=True, exist_ok=True)
    return p


def data_dir() -> Path:
    """The local fallback folder: $GEM_RADAR_DATA_DIR, else ./radar-data."""
    return Path(os.environ.get(ENV_DATA_DIR) or Path.cwd() / "radar-data")


def load_config(path: str | os.PathLike | None = None) -> dict[str, Any]:
    cfg = copy.deepcopy(DEFAULTS)
    candidates = [path, os.environ.get(ENV_CONFIG_PATH), home_dir() / "config.json"]
    for c in candidates:
        if c and Path(c).is_file():
            with open(c, encoding="utf-8") as f:
                user = json.load(f)
            if not isinstance(user, dict):
                raise ValueError(f"config {c} must be a JSON object")
            cfg = _merge(cfg, user)
            break
    return cfg
