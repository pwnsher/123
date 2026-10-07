"""Reachability check for each provider host (one small request each)."""
from __future__ import annotations

from ..core.config import load_config
from ..core.errors import ProviderError
from . import http

TARGETS = {
    "dexscreener": "https://api.dexscreener.com/latest/dex/tokens/"
                   "So11111111111111111111111111111111111111112",
    "goplus": "https://api.gopluslabs.io/api/v1/supported_chains",
    "honeypot_is": "https://api.honeypot.is/v2/IsHoneypot?address="
                   "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2&chainID=1",
    "rugcheck": "https://api.rugcheck.xyz/v1/stats/new_tokens",
}


def probe() -> dict:
    cfg = load_config()
    out = {}
    for name, url in TARGETS.items():
        try:
            http.request_json(name, url, timeout=float(cfg["http"]["timeout_seconds"]), retries=0)
            out[name] = "REACHABLE"
        except ProviderError as e:
            out[name] = e.status.value
    return out
