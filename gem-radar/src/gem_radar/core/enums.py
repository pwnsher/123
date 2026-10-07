"""Enumerations shared across Gem Radar."""
from __future__ import annotations

from enum import Enum


class Status(str, Enum):
    """Data status of a reading or a metric."""

    LIVE = "LIVE"          # fetched from a provider during this scan
    CACHED = "CACHED"      # served from cache / local data, within its freshness window
    STALE = "STALE"        # older than its freshness window: shown, never scored
    UNKNOWN = "UNKNOWN"    # no usable reading
    CONFLICT = "CONFLICT"  # independent sources disagree materially


class Verdict(str, Enum):
    GEM = "GEM"
    WATCH = "WATCH"
    AVOID = "AVOID"
    UNRATED = "UNRATED"  # too little verified data to rate (and no critical flag)


class Severity(str, Enum):
    CRITICAL = "CRITICAL"  # caps the final score
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    INFO = "INFO"


class AnalysisMode(str, Enum):
    FAST = "FAST"
    DEEP = "DEEP"


class ProviderStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    CACHED = "CACHED"
    TIMEOUT = "TIMEOUT"
    RATE_LIMITED = "RATE_LIMITED"
    BLOCKED = "BLOCKED"          # network policy / proxy refused the host
    HTTP_ERROR = "HTTP_ERROR"
    PARSE_ERROR = "PARSE_ERROR"
    NOT_FOUND = "NOT_FOUND"      # provider answered but has no data for this token
    UNSUPPORTED = "UNSUPPORTED"  # provider does not cover this chain
    SKIPPED = "SKIPPED"          # not part of this scan stage
    ERROR = "ERROR"


class Category(str, Enum):
    LIQUIDITY = "liquidity"
    CONTRACT = "contract"
    HOLDERS = "holders"
    VOLUME = "volume"
    AGE_SOCIAL = "age_social"
    MARKET = "market"  # informational only (price, mcap): not scored
