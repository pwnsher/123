"""
Feed runners — one thread per source; a failing source never blocks the others.

WsFeedRunner    connect -> subscribe -> (after a reconnect: backfill) -> read loop.
                Transport errors -> RECONNECTING, exponential backoff (Backoff), retry. Missing credentials /
                dependencies -> DISCONNECTED with the reason (no retry storm). recv timeouts only trigger
                liveness evaluation (STALE after stale_after_ms without messages, monotonic clock).
PollRunner      the same state machine for REST polling (Kalshi): each cycle is one "message".

Receive times are taken the moment data arrives (wall clock for alignment, monotonic for durations).
Both runners accept injected connect/get/sleep/clock functions, so every path is testable offline.
"""
import threading

from market_data.feed import Backoff, FeedHealth, FeedMonitor, FeedState
from market_data.transport.kalshi_auth import AuthUnavailable
from market_data.transport.ws import WebSocket, WebSocketClosed


class WsFeedRunner(threading.Thread):
    def __init__(self, adapter, collector, clock, headers_fn=None, connect_fn=None, backoff=None, stop_event=None,
                 backfill_fn=None, recv_timeout_s=1.0, max_attempts=None, monitor=None):
        super().__init__(name=f"feed-{adapter.source}", daemon=True)
        self.adapter, self.collector, self.clock = adapter, collector, clock
        self.headers_fn = headers_fn
        self.connect_fn = connect_fn or (lambda url, headers=None: WebSocket.connect(url, headers=headers))
        self.backoff = backoff or Backoff()
        self.stop_event = stop_event or threading.Event()
        self.backfill_fn = backfill_fn
        self.recv_timeout_s = recv_timeout_s
        self.max_attempts = max_attempts
        self.health = FeedHealth(adapter.source, critical=adapter.critical)
        self.monitor = monitor or FeedMonitor(self.health)
        self.attempts = 0
        self.backoff_delays = []

    def run(self):
        ever_connected = False
        while not self.stop_event.is_set():
            ws = None
            try:
                headers = self.headers_fn() if self.headers_fn else None
                ws = self.connect_fn(self.adapter.url, headers=headers)
                mono = self.clock.mono_ns()
                self.monitor.on_connect(mono)
                self.collector.on_connect(self.adapter, self.clock.wall_ms(), reconnect=ever_connected)
                for msg in self.adapter.subscribe_messages():
                    ws.send_text(msg)
                if ever_connected and self.backfill_fn is not None:
                    self.backfill_fn()
                ever_connected = True
                if hasattr(ws, "settimeout"):
                    ws.settimeout(self.recv_timeout_s)
                while not self.stop_event.is_set():
                    try:
                        text = ws.recv_text()
                    except TimeoutError:
                        self.monitor.evaluate(self.clock.mono_ns())
                        continue
                    if text is None:
                        raise WebSocketClosed("closed by server")
                    wall, mono = self.clock.wall_ms(), self.clock.mono_ns()
                    n_ev, n_fail, n_gap = self.collector.on_message(self.adapter, text, wall, mono)
                    self.monitor.on_message(mono, n_ev, n_fail, n_gap)
                    if n_ev:
                        self.backoff.reset()
            except AuthUnavailable as e:
                self.monitor.on_disconnect(self.clock.mono_ns(), f"credentials unavailable: {e}")
                self.health.set(FeedState.DISCONNECTED, self.clock.mono_ns(), "credentials unavailable")
                self.collector.on_source_disabled(self.adapter, str(e))
                break
            except Exception as e:                                   # noqa: BLE001 - every transport error reconnects
                self.monitor.on_disconnect(self.clock.mono_ns(), f"{type(e).__name__}: {e}")
                self.collector.on_disconnect(self.adapter, self.clock.wall_ms(), str(e))
                self.attempts += 1
                if self.max_attempts is not None and self.attempts >= self.max_attempts:
                    self.health.set(FeedState.DISCONNECTED, self.clock.mono_ns(), "retry limit reached")
                    break
                delay = self.backoff.next_delay()
                self.backoff_delays.append(delay)
                self.monitor.on_retry(self.clock.mono_ns())
                self.stop_event.wait(delay)
            finally:
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:                                # noqa: BLE001
                        pass
        if self.health.state != FeedState.DISCONNECTED:
            self.monitor.on_stop(self.clock.mono_ns())


class PollRunner(threading.Thread):
    """Calls poll_fn() every interval_s. poll_fn returns (n_events, n_failures, n_gaps) or raises."""

    def __init__(self, name, poll_fn, collector, clock, interval_s=1.0, backoff=None, stop_event=None,
                 max_attempts=None, critical=True):
        super().__init__(name=f"poll-{name}", daemon=True)
        self.poll_fn, self.collector, self.clock = poll_fn, collector, clock
        self.interval_s = interval_s
        self.backoff = backoff or Backoff()
        self.stop_event = stop_event or threading.Event()
        self.max_attempts = max_attempts
        self.health = FeedHealth(name, critical=critical)
        self.monitor = FeedMonitor(self.health, warmup_ms=int(interval_s * 3000), stale_after_ms=int(interval_s * 10_000))
        self.attempts = 0
        self.backoff_delays = []
        self.connected = False

    def run(self):
        while not self.stop_event.is_set():
            try:
                if not self.connected:
                    self.monitor.on_connect(self.clock.mono_ns())
                    self.connected = True
                n_ev, n_fail, n_gap = self.poll_fn(reconnect=self.attempts > 0 and self.health.connects > 1)
                self.monitor.on_message(self.clock.mono_ns(), n_ev, n_fail, n_gap)
                self.backoff.reset()
                self.stop_event.wait(self.interval_s)
            except Exception as e:                                   # noqa: BLE001
                self.connected = False
                self.monitor.on_disconnect(self.clock.mono_ns(), f"{type(e).__name__}: {e}")
                self.collector.on_poll_error(self.health.name, self.clock.wall_ms(), str(e))
                self.attempts += 1
                if self.max_attempts is not None and self.attempts >= self.max_attempts:
                    self.health.set(FeedState.DISCONNECTED, self.clock.mono_ns(), "retry limit reached")
                    break
                delay = self.backoff.next_delay()
                self.backoff_delays.append(delay)
                self.monitor.on_retry(self.clock.mono_ns())
                self.stop_event.wait(delay)
        if self.health.state != FeedState.DISCONNECTED:
            self.monitor.on_stop(self.clock.mono_ns())
