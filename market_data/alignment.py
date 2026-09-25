"""
Event-time alignment — THE causal rule of the research pipeline.

    An observation is usable at time T if and only if   receive_ts_ms <= T.

An event time before T does NOT make data available if it was received after T (a delayed trade,
an amended CF value, a late book). Everything downstream (feature windows, cross-source alignment,
settlement joins) goes through `available()` / `Timeline.visible()`.

Inside one stream, once data is available, values are ordered by `series_ts_ms` (the source event time
when the source provides one, else the receive time), so late-arriving events land in their true
place — but only from the moment they were received.
"""
import bisect


def available(event, t_ms):
    return event.receive_ts_ms <= t_ms


class Timeline:
    """Events of any mix of sources, in arrival order, with O(log n) visibility queries."""

    def __init__(self, events=()):
        self.events = sorted(events, key=lambda e: (e.receive_ts_ms, e.ingest_seq))
        self._rts = [e.receive_ts_ms for e in self.events]

    def visible(self, t_ms):
        return self.events[:bisect.bisect_right(self._rts, t_ms)]

    def __len__(self):
        return len(self.events)


def latest_available(events, t_ms, max_age_ms=None):
    """The event with the greatest series time among those AVAILABLE at t (ties: later arrival).
    None if nothing is available or the newest is older than max_age_ms (relative to t)."""
    best = None
    for e in events:
        if e.receive_ts_ms > t_ms:
            continue
        k = (e.series_ts_ms, e.receive_ts_ms, e.ingest_seq)
        if best is None or k > best[0]:
            best = (k, e)
    if best is None:
        return None
    e = best[1]
    if max_age_ms is not None and t_ms - e.series_ts_ms > max_age_ms:
        return None
    return e
