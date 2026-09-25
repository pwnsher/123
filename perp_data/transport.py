"""
Transport glue for the perp venues, reusing Step 3's stdlib websocket client and GET-only HTTP client.

KeepaliveWebSocket wraps a connected websocket and sends the venue's APPLICATION-level ping (Bybit
{"op":"ping"} every 20 s, OKX "ping" every 25 s) before reads once the interval has elapsed. Step 3's feed
runner calls recv_text() at least once per recv timeout (1 s), so the ping is never late by more than that.
Binance pings the client itself; the websocket client already answers with pongs.
"""
from market_data.transport.ws import WebSocket


class KeepaliveWebSocket:
    def __init__(self, ws, text, interval_s, mono_ns):
        self.ws, self.text, self.interval_ns, self.mono_ns = ws, text, int(interval_s * 1e9), mono_ns
        self.last_ping_ns = mono_ns()
        self.pings = 0

    def _maybe_ping(self):
        now = self.mono_ns()
        if now - self.last_ping_ns >= self.interval_ns:
            self.ws.send_text(self.text)
            self.last_ping_ns = now
            self.pings += 1

    def send_text(self, text):
        self.ws.send_text(text)

    def recv_text(self):
        self._maybe_ping()
        return self.ws.recv_text()

    def settimeout(self, s):
        if hasattr(self.ws, "settimeout"):
            self.ws.settimeout(s)

    def close(self):
        self.ws.close()


def connect_fn_for(adapter, clock, base_connect=None):
    """A WsFeedRunner connect_fn that adds the adapter's keep-alive (if any)."""
    base = base_connect or (lambda url, headers=None: WebSocket.connect(url, headers=headers))

    def connect(url, headers=None):
        ws = base(url, headers=headers)
        if adapter.keepalive_text and adapter.keepalive_s:
            return KeepaliveWebSocket(ws, adapter.keepalive_text, adapter.keepalive_s, clock.mono_ns)
        return ws
    return connect
