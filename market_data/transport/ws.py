"""
Minimal RFC 6455 websocket CLIENT on the standard library (no third-party dependency).

Supports: ws:// and wss:// (TLS with certificate verification), optional HTTP CONNECT proxy
(HTTPS_PROXY / https_proxy), extra handshake headers (auth), text/binary frames, fragmentation,
automatic pong replies, close handshake, client masking. Not supported (not needed): compression
extensions, subprotocols. recv_text() returns None on a clean close and raises WebSocketClosed on errors;
a socket timeout raises TimeoutError so the caller can run liveness checks without dropping the link.
"""
import base64
import hashlib
import os
import socket
import ssl
import struct
from urllib.parse import urlparse

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
OP_CONT, OP_TEXT, OP_BIN, OP_CLOSE, OP_PING, OP_PONG = 0x0, 0x1, 0x2, 0x8, 0x9, 0xA
MAX_MESSAGE_BYTES = 16 << 20


class WebSocketClosed(ConnectionError):
    pass


def accept_key(key):
    return base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()


def encode_frame(opcode, payload, mask=True, fin=True, mask_key=None):
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    b0 = (0x80 if fin else 0) | opcode
    n = len(payload)
    mbit = 0x80 if mask else 0
    if n < 126:
        header = struct.pack("!BB", b0, mbit | n)
    elif n < 65536:
        header = struct.pack("!BBH", b0, mbit | 126, n)
    else:
        header = struct.pack("!BBQ", b0, mbit | 127, n)
    if not mask:
        return header + payload
    key = mask_key if mask_key is not None else os.urandom(4)
    masked = bytes(b ^ key[i % 4] for i, b in enumerate(payload))
    return header + key + masked


class _Reader:
    """Receive buffer. Frames are consumed only when COMPLETE, so a socket timeout in the middle of a
    frame leaves the buffer intact and the next read resumes where it stopped (no desync)."""

    def __init__(self, sock, initial=b""):
        self.sock, self.buf = sock, bytearray(initial)

    def fill(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(max(65536, n - len(self.buf)))
            if not chunk:
                raise WebSocketClosed("connection closed by peer")
            self.buf += chunk

    def take(self, n):
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out


def read_frame(reader):
    reader.fill(2)
    b0, b1 = reader.buf[0], reader.buf[1]
    fin, opcode = bool(b0 & 0x80), b0 & 0x0F
    masked, n = bool(b1 & 0x80), b1 & 0x7F
    hl = 2
    if n == 126:
        reader.fill(4)
        n = struct.unpack("!H", bytes(reader.buf[2:4]))[0]
        hl = 4
    elif n == 127:
        reader.fill(10)
        n = struct.unpack("!Q", bytes(reader.buf[2:10]))[0]
        hl = 10
    if n > MAX_MESSAGE_BYTES:
        raise WebSocketClosed(f"frame too large ({n} bytes)")
    kl = 4 if masked else 0
    reader.fill(hl + kl + n)                      # may raise TimeoutError-like socket.timeout: nothing consumed
    frame = reader.take(hl + kl + n)
    key = frame[hl:hl + kl] if masked else None
    data = frame[hl + kl:]
    if key:
        data = bytes(b ^ key[i % 4] for i, b in enumerate(data))
    return fin, opcode, data


def _proxy_for(host):
    p = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    no = [x.strip() for x in (os.environ.get("NO_PROXY") or os.environ.get("no_proxy") or "").split(",") if x.strip()]
    if not p or any(host == n or host.endswith("." + n.lstrip(".")) for n in no) or host in ("127.0.0.1", "localhost"):
        return None
    return urlparse(p if "://" in p else "http://" + p)


class WebSocket:
    def __init__(self, sock, reader):
        self.sock, self.reader = sock, reader
        self.closed = False
        self._parts, self._opcode0 = [], None           # fragments survive a timeout between frames

    @classmethod
    def connect(cls, url, headers=None, timeout=10.0, ssl_context=None):
        u = urlparse(url)
        if u.scheme not in ("ws", "wss"):
            raise ValueError(f"not a websocket url: {url}")
        host, port = u.hostname, u.port or (443 if u.scheme == "wss" else 80)
        path = (u.path or "/") + (f"?{u.query}" if u.query else "")
        proxy = _proxy_for(host)
        if proxy is not None:
            raw = socket.create_connection((proxy.hostname, proxy.port or 80), timeout=timeout)
            req = f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n"
            if proxy.username:
                cred = base64.b64encode(f"{proxy.username}:{proxy.password or ''}".encode()).decode()
                req += f"Proxy-Authorization: Basic {cred}\r\n"
            raw.sendall((req + "\r\n").encode())
            resp = b""
            while b"\r\n\r\n" not in resp:
                chunk = raw.recv(4096)
                if not chunk:
                    raise WebSocketClosed("proxy closed the connection")
                resp += chunk
            status_line = resp.split(b"\r\n", 1)[0]
            if b" 200" not in status_line:
                raise WebSocketClosed(f"proxy refused CONNECT: {status_line[:80]!r}")
        else:
            raw = socket.create_connection((host, port), timeout=timeout)
        sock = raw
        if u.scheme == "wss":
            ctx = ssl_context or ssl.create_default_context()
            sock = ctx.wrap_socket(raw, server_hostname=host)
        key = base64.b64encode(os.urandom(16)).decode()
        lines = [f"GET {path} HTTP/1.1", f"Host: {host}" + (f":{port}" if u.port else ""), "Upgrade: websocket",
                 "Connection: Upgrade", f"Sec-WebSocket-Key: {key}", "Sec-WebSocket-Version: 13",
                 "User-Agent: kalshi-local-research-collector"]
        for k, v in (headers or {}).items():
            lines.append(f"{k}: {v}")
        sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode())
        resp = b""
        while b"\r\n\r\n" not in resp:
            chunk = sock.recv(4096)
            if not chunk:
                raise WebSocketClosed("closed during handshake")
            resp += chunk
            if len(resp) > 65536:
                raise WebSocketClosed("handshake response too large")
        head, rest = resp.split(b"\r\n\r\n", 1)
        status = head.split(b"\r\n", 1)[0]
        if b" 101" not in status:
            raise WebSocketClosed(f"handshake refused: {status[:80]!r}")
        hdrs = {}
        for line in head.split(b"\r\n")[1:]:
            if b":" in line:
                k, v = line.split(b":", 1)
                hdrs[k.strip().lower()] = v.strip()
        if hdrs.get(b"sec-websocket-accept", b"").decode() != accept_key(key):
            raise WebSocketClosed("bad Sec-WebSocket-Accept")
        return cls(sock, _Reader(sock, rest))

    def settimeout(self, s):
        self.sock.settimeout(s)

    def send_text(self, text):
        self.sock.sendall(encode_frame(OP_TEXT, text))

    def recv_text(self):
        while True:
            try:
                fin, op, data = read_frame(self.reader)
            except socket.timeout:
                raise TimeoutError("no complete frame within timeout")
            except (OSError, ssl.SSLError) as e:
                raise WebSocketClosed(str(e))
            if op == OP_PING:
                self.sock.sendall(encode_frame(OP_PONG, data))
                continue
            if op == OP_PONG:
                continue
            if op == OP_CLOSE:
                try:
                    self.sock.sendall(encode_frame(OP_CLOSE, data[:2]))
                except OSError:
                    pass
                self.closed = True
                return None
            if op in (OP_TEXT, OP_BIN):
                self._parts, self._opcode0 = [data], op
            elif op == OP_CONT:
                self._parts.append(data)
            if fin:
                payload, opcode0 = b"".join(self._parts), self._opcode0
                self._parts, self._opcode0 = [], None
                return payload.decode("utf-8") if opcode0 == OP_TEXT else payload.decode("utf-8", errors="replace")

    def close(self):
        if not self.closed:
            try:
                self.sock.sendall(encode_frame(OP_CLOSE, struct.pack("!H", 1000)))
            except OSError:
                pass
        self.closed = True
        try:
            self.sock.close()
        except OSError:
            pass
