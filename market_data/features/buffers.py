"""Per-stream buffers ordered by series time, with O(log n) as-of and range queries."""
import bisect


class StreamBuffer:
    __slots__ = ("keys", "times", "vals", "first_series", "first_receive", "last_receive")

    def __init__(self):
        self.keys, self.times, self.vals = [], [], []
        self.first_series = self.first_receive = self.last_receive = None

    def add(self, series_ts, receive_ts, seq, val):
        k = (series_ts, receive_ts, seq)
        if not self.keys or k >= self.keys[-1]:
            i = len(self.keys)                          # the common case: in order -> O(1) append
        else:
            i = bisect.bisect_right(self.keys, k)       # a late (out-of-order) event lands in its series place
        self.keys.insert(i, k)
        self.times.insert(i, series_ts)
        self.vals.insert(i, val)
        if self.first_series is None or series_ts < self.first_series:
            self.first_series = series_ts
        if self.first_receive is None:
            self.first_receive = receive_ts
        self.last_receive = receive_ts

    def prune(self, before_series):
        i = bisect.bisect_left(self.times, before_series)
        if i:
            del self.keys[:i], self.times[:i], self.vals[:i]

    def asof(self, t, max_age_ms):
        """(series_ts, value) of the newest entry with series_ts <= t (latest arrival wins a tie),
        or None if none exists or it is older than max_age_ms."""
        i = bisect.bisect_right(self.times, t) - 1
        if i < 0 or t - self.times[i] > max_age_ms:
            return None
        return self.times[i], self.vals[i]

    def range(self, lo_exclusive, hi_inclusive):
        a = bisect.bisect_right(self.times, lo_exclusive)
        b = bisect.bisect_right(self.times, hi_inclusive)
        return self.vals[a:b]

    def __len__(self):
        return len(self.vals)
