"""Freshness: classify a reading's age against its metric's window."""
from __future__ import annotations

from ..core.enums import Status
from ..core.types import METRICS


def window_seconds(metric: str, cfg: dict) -> float:
    cls = METRICS[metric].freshness if metric in METRICS else "market"
    return float(cfg["freshness_seconds"].get(cls, 300))


def classify(metric: str, fetched_at: float, now: float, cfg: dict, *, live: bool) -> Status:
    """LIVE only for a value fetched from a provider during this scan.
    Anything else (cache, local files) is CACHED inside its window, STALE outside.
    A capture timestamp in the future is treated as STALE (untrustworthy clock)."""
    if fetched_at > now + 300:
        return Status.STALE
    if live:
        return Status.LIVE
    return Status.CACHED if now - fetched_at <= window_seconds(metric, cfg) else Status.STALE
