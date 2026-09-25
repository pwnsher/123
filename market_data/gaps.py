"""
Gap detection. Gaps are RECORDED, never filled: raw data is not forward-filled anywhere.

    CadenceGapDetector   regular feeds (CF RTI ~1 value/s): consecutive EVENT times further apart than
                         expected_interval_ms * tolerance -> gap (not recoverable live; history may cover it)
    IdGapDetector        feeds with contiguous ids (Coinbase trade_id per product): id jump -> gap with the
                         exact missing count; recoverable when the venue has a REST backfill
    SequenceGapDetector  book/sequence feeds where every message increments the sequence
    disconnect_gap()     a transport outage from the feed runner (start = last message, end = reconnect)

Trades are irregular, so trade streams are NEVER checked by time cadence (a quiet minute is not a gap).
"""
from dataclasses import dataclass, asdict
from typing import Optional


@dataclass(frozen=True)
class Gap:
    source: str
    asset: str
    stream: str
    kind: str                         # CADENCE | TRADE_ID | SEQUENCE | DISCONNECT
    start_ts_ms: int
    end_ts_ms: int
    duration_ms: int
    missing_count: Optional[int]
    recoverable: bool
    detail: str = ""
    known_at_ms: Optional[int] = None     # receive time of the message that revealed the gap (causal use)
    ingest_seq: Optional[int] = None      # position in the arrival order (replay interleaves gaps with events)

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**d)

    def overlaps(self, lo_ms, hi_ms):
        return self.start_ts_ms < hi_ms and self.end_ts_ms > lo_ms


class CadenceGapDetector:
    def __init__(self, source, asset, stream, expected_interval_ms=1000, tolerance=2.5):
        self.source, self.asset, self.stream = source, asset, stream
        self.limit_ms = int(expected_interval_ms * tolerance)
        self.last_ts = None

    def observe(self, event_ts_ms):
        if event_ts_ms is None:
            return None
        g = None
        if self.last_ts is not None and event_ts_ms - self.last_ts > self.limit_ms:
            g = Gap(self.source, self.asset, self.stream, "CADENCE", self.last_ts, event_ts_ms,
                    event_ts_ms - self.last_ts, None, False, f"> {self.limit_ms} ms between values")
        if self.last_ts is None or event_ts_ms > self.last_ts:
            self.last_ts = event_ts_ms
        return g


class IdGapDetector:
    def __init__(self, source, asset, stream, recoverable=False):
        self.source, self.asset, self.stream, self.recoverable = source, asset, stream, recoverable
        self.last_id = None
        self.last_ts = None
        self.duplicates_or_old = 0

    def observe(self, ident, ts_ms):
        if ident is None:
            return None
        try:
            i = int(ident)
        except (TypeError, ValueError):
            return None
        g = None
        if self.last_id is not None:
            if i <= self.last_id:
                self.duplicates_or_old += 1
                return None
            if i > self.last_id + 1:
                g = Gap(self.source, self.asset, self.stream, "TRADE_ID", self.last_ts or ts_ms, ts_ms,
                        max(ts_ms - (self.last_ts or ts_ms), 0), i - self.last_id - 1, self.recoverable,
                        f"ids {self.last_id + 1}..{i - 1} missing")
        self.last_id, self.last_ts = i, ts_ms
        return g


class SequenceGapDetector(IdGapDetector):
    def observe(self, ident, ts_ms):
        g = super().observe(ident, ts_ms)
        return None if g is None else Gap(g.source, g.asset, g.stream, "SEQUENCE", g.start_ts_ms, g.end_ts_ms,
                                          g.duration_ms, g.missing_count, g.recoverable, g.detail)


def disconnect_gap(source, asset, stream, last_message_ts_ms, reconnect_ts_ms, recoverable):
    return Gap(source, asset, stream, "DISCONNECT", last_message_ts_ms, reconnect_ts_ms,
               max(reconnect_ts_ms - last_message_ts_ms, 0), None, recoverable, "transport outage")
