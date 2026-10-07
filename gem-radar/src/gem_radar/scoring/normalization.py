"""Normalization of raw provider values. Each provider declares its units
explicitly (fraction vs percent, ms vs s); nothing is guessed from magnitude.
Invalid values become None (=> no reading), never a substitute value.
"""
from __future__ import annotations

import math
from typing import Any, Optional


def num(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    if isinstance(x, (int, float)):
        v = float(x)
    elif isinstance(x, str):
        s = x.strip().replace(",", "")
        if not s:
            return None
        try:
            v = float(s)
        except ValueError:
            return None
    else:
        return None
    return v if math.isfinite(v) else None


def usd(x: Any) -> Optional[float]:
    v = num(x)
    return v if v is not None and v >= 0 else None


def count(x: Any) -> Optional[int]:
    v = num(x)
    if v is None or v < 0:
        return None
    return int(v)


def pct_from_fraction(x: Any) -> Optional[float]:
    """0.12 -> 12.0. Outside [0, 1] (beyond float noise) is invalid."""
    v = num(x)
    if v is None or v < 0 or v > 1.0000001:
        return None
    return round(min(v, 1.0) * 100.0, 6)


def pct_from_percent(x: Any) -> Optional[float]:
    v = num(x)
    if v is None or v < 0 or v > 100.0000001:
        return None
    return round(min(v, 100.0), 6)


def signed_pct(x: Any) -> Optional[float]:
    return num(x)


def flag01(x: Any) -> Optional[bool]:
    """GoPlus-style "1"/"0" flags. Anything else (absent, "", unknown) -> None."""
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)) and x in (0, 1):
        return bool(x)
    if isinstance(x, str) and x.strip() in ("0", "1"):
        return x.strip() == "1"
    return None


def ts_from_ms(x: Any) -> Optional[float]:
    v = num(x)
    return v / 1000.0 if v is not None and v > 0 else None


def ts_from_s(x: Any) -> Optional[float]:
    v = num(x)
    return v if v is not None and v > 0 else None


def evm_address(x: Any) -> Optional[str]:
    if isinstance(x, str) and x.startswith("0x") and len(x) == 42:
        return x.lower()
    return None


def chain_id(x: str) -> str:
    """Map provider chain identifiers to Gem Radar's names."""
    aliases = {"eth": "ethereum", "1": "ethereum", "bnb": "bsc", "56": "bsc", "8453": "base",
               "arb": "arbitrum", "42161": "arbitrum", "matic": "polygon", "137": "polygon",
               "op": "optimism", "10": "optimism", "avax": "avalanche", "43114": "avalanche",
               "sol": "solana"}
    s = str(x).strip().lower()
    return aliases.get(s, s)
