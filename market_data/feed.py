"""
Feed health and reconnect state machine (one per source connection).

States
    DISCONNECTED   not connected (initial, stopped, or given up)
    CONNECTED      transport open, subscribing
    WARMING_UP     receiving, but for less than warmup_ms since (re)connect, or no data message yet
    HEALTHY        receiving data, no recent gap / parse problem
    DEGRADED       receiving data, but a gap or parse failure happened within degraded_window_ms
    STALE          connected but no message for stale_after_ms (monotonic clock)
    RECONNECTING   transport lost / connect failed; waiting out an exponential backoff

All durations use the MONOTONIC clock. One source's failure never blocks another: every feed runs
on its own thread and reports through its FeedHealth.
"""
import random
from dataclasses import dataclass, asdict, field
from enum import Enum
from typing import Optional


class FeedState(str, Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTED = "CONNECTED"
    WARMING_UP = "WARMING_UP"
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    RECONNECTING = "RECONNECTING"


class Backoff:
    """Exponential backoff with deterministic jitter: initial * factor^n, capped, +/- jitter fraction."""

    def __init__(self, initial_s=1.0, factor=2.0, max_s=60.0, jitter=0.1, seed=12345):
        self.initial_s, self.factor, self.max_s, self.jitter = initial_s, factor, max_s, jitter
        self.attempt = 0
        self._rng = random.Random(seed)

    def next_delay(self):
        base = min(self.initial_s * (self.factor ** self.attempt), self.max_s)
        self.attempt += 1
        return max(0.0, base * (1.0 + self.jitter * (2 * self._rng.random() - 1)))

    def reset(self):
        self.attempt = 0


@dataclass
class FeedHealth:
    name: str
    critical: bool = False
    state: FeedState = FeedState.DISCONNECTED
    connects: int = 0
    disconnects: int = 0
    reconnect_attempts: int = 0
    messages: int = 0
    data_events: int = 0
    parse_failures: int = 0
    gaps: int = 0
    last_error: Optional[str] = None
    connected_mono_ns: Optional[int] = None
    last_message_mono_ns: Optional[int] = None
    last_problem_mono_ns: Optional[int] = None
    disconnected_mono_ns: Optional[int] = None
    last_reconnect_duration_ms: Optional[float] = None
    latency_ms_last: Optional[float] = None          # receive wall - event wall (cross-clock, approximate)
    transitions: list = field(default_factory=list)

    def set(self, state, mono_ns, reason=""):
        if state != self.state:
            self.transitions.append((mono_ns, self.state.value, state.value, reason))
            del self.transitions[:-50]
            self.state = state

    def to_dict(self):
        d = asdict(self)
        d["state"] = self.state.value
        d["transitions"] = [list(t) for t in self.transitions[-10:]]
        return d


class FeedMonitor:
    """Pure state logic, driven by the runner (and by tests with a fake clock)."""

    def __init__(self, health, warmup_ms=5000, stale_after_ms=10_000, degraded_window_ms=30_000):
        self.h = health
        self.warmup_ms, self.stale_after_ms, self.degraded_window_ms = warmup_ms, stale_after_ms, degraded_window_ms

    def on_connect(self, mono_ns):
        h = self.h
        h.connects += 1
        if h.disconnected_mono_ns is not None:
            h.last_reconnect_duration_ms = (mono_ns - h.disconnected_mono_ns) / 1e6
        h.connected_mono_ns = mono_ns
        h.set(FeedState.CONNECTED, mono_ns, "transport open")

    def on_message(self, mono_ns, data_events=0, failures=0, gaps=0):
        h = self.h
        h.messages += 1
        h.data_events += data_events
        h.parse_failures += failures
        h.gaps += gaps
        if failures or gaps:
            h.last_problem_mono_ns = mono_ns
        if data_events or h.last_message_mono_ns is None:
            h.last_message_mono_ns = mono_ns
        self.evaluate(mono_ns)

    def on_disconnect(self, mono_ns, error):
        h = self.h
        h.disconnects += 1
        h.last_error = (error or "")[:200]
        h.disconnected_mono_ns = mono_ns
        h.set(FeedState.RECONNECTING, mono_ns, h.last_error)

    def on_retry(self, mono_ns):
        self.h.reconnect_attempts += 1

    def on_stop(self, mono_ns):
        self.h.set(FeedState.DISCONNECTED, mono_ns, "stopped")

    def evaluate(self, mono_ns):
        h = self.h
        if h.state in (FeedState.DISCONNECTED, FeedState.RECONNECTING):
            return h.state
        if h.connected_mono_ns is None:
            return h.state
        since_msg = (mono_ns - h.last_message_mono_ns) / 1e6 if h.last_message_mono_ns is not None else None
        since_conn = (mono_ns - h.connected_mono_ns) / 1e6
        if since_msg is not None and since_msg > self.stale_after_ms:
            h.set(FeedState.STALE, mono_ns, f"no message for {since_msg:.0f} ms")
            h.last_problem_mono_ns = mono_ns          # a stale spell counts as a recent problem (-> DEGRADED after)
        elif since_msg is None and since_conn > self.stale_after_ms:
            h.set(FeedState.STALE, mono_ns, "no message since connect")
            h.last_problem_mono_ns = mono_ns
        elif h.data_events == 0 or since_conn < self.warmup_ms:
            h.set(FeedState.WARMING_UP, mono_ns, "warming up")
        elif h.last_problem_mono_ns is not None and (mono_ns - h.last_problem_mono_ns) / 1e6 < self.degraded_window_ms:
            h.set(FeedState.DEGRADED, mono_ns, "recent gap / parse failure")
        else:
            h.set(FeedState.HEALTHY, mono_ns, "")
        return h.state
