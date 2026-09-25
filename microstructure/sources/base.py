"""
Micro adapter base: Step 3's SourceAdapter helpers (JSON load, recorded parse failures, guarded parsing) with an
event() that builds MicroEvents carrying the raw message they came from (raw_seq) and the websocket CONNECTION
number (conn). The collector numbers connections per source and records the number in the raw message's stream
("ws#<conn>"), so chain ids (one per connection / subscription) are reproduced exactly when the stored raw
messages are re-normalized.

Adapters never do I/O and never read a clock; stateful adapters (chain sequence tracking, known subscriptions)
rebuild their state by replaying the raw messages in arrival order.
"""
import math
from dataclasses import dataclass
from typing import Optional

from market_data.normalization import NormalizationError
from market_data.sources.base import Ctx, SourceAdapter, new_result  # noqa: F401  (re-export)
from market_data.types import IngestMode
from microstructure.types import MicroEvent
from microstructure.venues import VENUES


@dataclass
class MicroCtx(Ctx):
    raw_seq: Optional[int] = None
    channel: str = "ws"
    conn: int = 0


def num(v, name, positive=False):
    if isinstance(v, bool) or v is None or v == "":
        raise NormalizationError(f"{name} missing")
    try:
        x = float(v)
    except (TypeError, ValueError):
        raise NormalizationError(f"{name} not numeric: {str(v)[:30]!r}")
    if not math.isfinite(x) or (x <= 0 if positive else x < 0):
        raise NormalizationError(f"{name} out of range: {x}")
    return x


def conn_of(stream):
    """'ws#3' -> 3 (0 when absent)."""
    _, _, n = str(stream).partition("#")
    return int(n) if n.isdigit() else 0


class MicroAdapter(SourceAdapter):
    venue = "?"
    keepalive_text: Optional[str] = None
    keepalive_s: Optional[float] = None
    rest_snapshots = False                  # True: a GET snapshot is required to start / restart a book (Binance)

    def __init__(self, assets, depth=None):
        self.spec = VENUES[self.venue]
        self.assets = [a for a in assets if not self.spec.symbols or a in self.spec.symbols]
        self.depth = depth if depth is not None else self.spec.default_depth
        self.by_symbol = {self.spec.symbols[a]: a for a in self.assets} if self.spec.symbols else {}
        self.url = self.spec.url

    def event(self, ctx, **kw):
        kw.setdefault("receive_mono_ns", ctx.receive_mono_ns)
        kw.setdefault("raw_seq", getattr(ctx, "raw_seq", None))
        kw.setdefault("channel", getattr(ctx, "channel", "ws"))
        return MicroEvent(source=self.source, receive_ts_ms=ctx.receive_ts_ms, ingest_seq=ctx.seq(),
                          session_id=ctx.session_id, **kw)

    def units(self):
        return {"price_unit": self.spec.price_unit, "qty_unit": self.spec.qty_unit}

    def mode(self, backfill):
        return IngestMode.BACKFILLED if backfill else IngestMode.LIVE

    def pending_messages(self, wall_ms):
        """Messages to send on the open connection now (subscription changes); default none."""
        return []

    def on_new_connection(self, conn):
        """Called by the collector before the first message of connection `conn` is parsed (and in replay)."""

    def rest_urls(self):
        return {}

    def snapshot_url(self, asset):
        raise NotImplementedError
