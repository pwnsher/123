"""
Clocks. Two clocks, two jobs:

    wall_ms()   UTC epoch milliseconds (time.time). Used for receive timestamps and cross-source
                alignment, because event times from venues are UTC wall-clock times.
    mono_ns()   monotonic nanoseconds (time.monotonic_ns). Used for durations: transport and
                processing latency, stale-feed duration, reconnect duration, backoff. A system clock
                step (NTP, manual change, DST-unrelated) cannot corrupt these.

ClockMonitor compares the two between observations and records an anomaly when the wall clock moved
by more than `threshold_ms` differently from the monotonic clock (e.g. an NTP step or a manual change),
including any backwards wall-clock step. Anomalies go to the session manifest.
"""
import time
from dataclasses import dataclass, asdict


class SystemClock:
    def wall_ms(self):
        return int(time.time() * 1000)

    def mono_ns(self):
        return time.monotonic_ns()


class FakeClock:
    """Deterministic clock for tests/replay. advance() moves both clocks; step_wall() only the wall."""

    def __init__(self, wall_ms=1_790_000_000_000, mono_ns=1_000_000_000):
        self._wall = int(wall_ms)
        self._mono = int(mono_ns)

    def wall_ms(self):
        return self._wall

    def mono_ns(self):
        return self._mono

    def advance(self, ms):
        self._wall += int(ms)
        self._mono += int(ms) * 1_000_000

    def step_wall(self, ms):
        self._wall += int(ms)


@dataclass(frozen=True)
class ClockAnomaly:
    kind: str                 # WALL_BACKWARDS | WALL_JUMP
    wall_ms: int
    wall_delta_ms: int
    mono_delta_ms: float

    def to_dict(self):
        return asdict(self)


class ClockMonitor:
    def __init__(self, threshold_ms=1000):
        self.threshold_ms = threshold_ms
        self._last = None
        self.anomalies = []

    def check(self, wall_ms, mono_ns):
        a = None
        if self._last is not None:
            lw, lm = self._last
            dw = wall_ms - lw
            dm = (mono_ns - lm) / 1e6
            if dw < 0:
                a = ClockAnomaly("WALL_BACKWARDS", wall_ms, dw, dm)
            elif abs(dw - dm) > self.threshold_ms:
                a = ClockAnomaly("WALL_JUMP", wall_ms, dw, dm)
            if a is not None:
                self.anomalies.append(a)
        self._last = (wall_ms, mono_ns)
        return a


def mono_elapsed_ms(start_ns, end_ns):
    return (end_ns - start_ns) / 1e6
