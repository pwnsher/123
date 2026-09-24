"""
Lightweight feed-health representation (architecture preparation only).

DESCRIPTIVE ONLY: nothing in the strategy reads these values. The legacy strategy's own data
guards (evaluate()'s BLOCK_STALE_DATA, the perp telemetry's source_status / quality flags) are
unchanged and remain the only things that affect behaviour. No new thresholds are introduced
here: every status is derived from a status the existing code already produced.
"""
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional


class FeedStatus(str, Enum):
    HEALTHY = "HEALTHY"
    STALE = "STALE"
    DISCONNECTED = "DISCONNECTED"
    INVALID = "INVALID"
    WARMING_UP = "WARMING_UP"
    UNAVAILABLE = "UNAVAILABLE"
    DISABLED = "DISABLED"
    UNKNOWN = "UNKNOWN"


# Names of the feeds that exist today (no new feed is added).
FEED_KALSHI_MARKET = "kalshi_market"        # GET /markets?series_ticker=...  (strike, book, close time)
FEED_SPOT = "coinbase_spot"                 # GET /products/{p}/ticker + /candles (spot, 1-min closes)
FEED_KALSHI_PERP = "kalshi_perp"            # perp telemetry (observation only unless a veto is promoted)


@dataclass(frozen=True)
class FeedHealth:
    feed: str
    status: FeedStatus
    detail: Optional[str] = None
    observed_at_epoch_s: Optional[float] = None

    def to_dict(self):
        d = asdict(self)
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d):
        return cls(feed=d["feed"], status=FeedStatus(d["status"]), detail=d.get("detail"),
                   observed_at_epoch_s=d.get("observed_at_epoch_s"))


def health_from_evaluation(r):
    """Feed health implied by ONE legacy evaluate() result. Returns a tuple of FeedHealth.

    ok          -> market HEALTHY, spot HEALTHY
    no market   -> market UNAVAILABLE, spot UNKNOWN (never fetched)
    stale       -> market HEALTHY, spot STALE (legacy BLOCK_STALE_DATA: < 20 usable candles,
                   missing/invalid strike or spot, or non-positive volatility)
    net error   -> both UNKNOWN (a requests exception cannot be attributed to one feed)
    error: ...  -> both UNKNOWN with the error text as detail"""
    r = r if isinstance(r, dict) else {}
    st = r.get("status")
    ts = r.get("spot_observed_ts")
    if st == "ok":
        return (FeedHealth(FEED_KALSHI_MARKET, FeedStatus.HEALTHY),
                FeedHealth(FEED_SPOT, FeedStatus.HEALTHY, observed_at_epoch_s=ts))
    if st == "no market":
        return (FeedHealth(FEED_KALSHI_MARKET, FeedStatus.UNAVAILABLE, "no open market in series"),
                FeedHealth(FEED_SPOT, FeedStatus.UNKNOWN, "not fetched"))
    if st == "stale":
        return (FeedHealth(FEED_KALSHI_MARKET, FeedStatus.HEALTHY),
                FeedHealth(FEED_SPOT, FeedStatus.STALE, r.get("verdict"), observed_at_epoch_s=ts))
    if st == "net error":
        return (FeedHealth(FEED_KALSHI_MARKET, FeedStatus.UNKNOWN, "network error (feed not attributable)"),
                FeedHealth(FEED_SPOT, FeedStatus.UNKNOWN, "network error (feed not attributable)"))
    detail = str(st)[:200] if st else "no result"
    return (FeedHealth(FEED_KALSHI_MARKET, FeedStatus.UNKNOWN, detail),
            FeedHealth(FEED_SPOT, FeedStatus.UNKNOWN, detail))


# perp_telemetry.STATUS_* -> FeedStatus (a 1:1 renaming of the existing statuses)
_PERP_STATUS = {"fresh": FeedStatus.HEALTHY, "stale": FeedStatus.STALE, "unavailable": FeedStatus.UNAVAILABLE,
                "error": FeedStatus.DISCONNECTED, "disabled": FeedStatus.DISABLED}


def health_from_perp_row(row):
    """Perp feed health from a telemetry row's existing source_status/source_error."""
    row = row if isinstance(row, dict) else {}
    st = _PERP_STATUS.get(str(row.get("source_status") or ""), FeedStatus.UNKNOWN)
    return FeedHealth(FEED_KALSHI_PERP, st, row.get("source_error") or None)


def overall(healths):
    """Worst status among feeds, for display. HEALTHY only if every feed is HEALTHY."""
    order = [FeedStatus.HEALTHY, FeedStatus.DISABLED, FeedStatus.WARMING_UP, FeedStatus.UNKNOWN,
             FeedStatus.STALE, FeedStatus.INVALID, FeedStatus.UNAVAILABLE, FeedStatus.DISCONNECTED]
    worst = FeedStatus.HEALTHY
    for h in healths or ():
        if order.index(h.status) > order.index(worst):
            worst = h.status
    return worst
