"""
Transport glue for the book venues, reusing Step 3's stdlib websocket client.

MicroWebSocket wraps a connected websocket and, before each read, sends (a) the adapter's pending SUBSCRIPTION
messages (Kalshi: new markets / unsubscribe closed ones) and (b) the venue's application keep-alive (Bybit
{"op":"ping"}, OKX "ping"). Only subscription / ping texts are ever sent: there is no order command anywhere.
"""
from market_data.transport.ws import WebSocket


class MicroWebSocket:
    def __init__(self, ws, adapter, clock):
        self.ws, self.adapter, self.clock = ws, adapter, clock
        self.interval_ns = int(adapter.keepalive_s * 1e9) if adapter.keepalive_text and adapter.keepalive_s else None
        self.last_ping_ns = clock.mono_ns()
        self.pings = 0
        self.sent = []

    def _before_read(self):
        for msg in self.adapter.pending_messages(self.clock.wall_ms()):
            self.ws.send_text(msg)
            self.sent.append(msg)
            del self.sent[:-100]
        if self.interval_ns is not None:
            now = self.clock.mono_ns()
            if now - self.last_ping_ns >= self.interval_ns:
                self.ws.send_text(self.adapter.keepalive_text)
                self.last_ping_ns = now
                self.pings += 1

    def send_text(self, text):
        self.ws.send_text(text)

    def recv_text(self):
        self._before_read()
        return self.ws.recv_text()

    def settimeout(self, s):
        if hasattr(self.ws, "settimeout"):
            self.ws.settimeout(s)

    def close(self):
        self.ws.close()


def connect_fn_for(adapter, clock, base_connect=None):
    base = base_connect or (lambda url, headers=None: WebSocket.connect(url, headers=headers))

    def connect(url, headers=None):
        return MicroWebSocket(base(url, headers=headers), adapter, clock)
    return connect
