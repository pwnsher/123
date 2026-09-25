"""
Adapter base: the shared parse context and helpers.

An adapter turns ONE received message (text) into ParseResult(events, failures, control). It never
does I/O and never reads a clock: the caller passes the receive time (UTC wall ms), the monotonic
receive time and an ingest-sequence allocator, so parsing is deterministic and replayable.
"""
import json
import threading
from dataclasses import dataclass
from typing import Callable, Optional

from market_data.normalization import NormalizationError
from market_data.types import MarketEvent, ParseFailure, ParseResult


class Sequencer:
    """Thread-safe global arrival counter (one per session)."""

    def __init__(self, start=0):
        self._n = start
        self._lock = threading.Lock()

    def __call__(self):
        with self._lock:
            self._n += 1
            return self._n

    @property
    def value(self):
        return self._n


@dataclass
class Ctx:
    session_id: str
    seq: Callable[[], int]
    receive_ts_ms: int
    receive_mono_ns: Optional[int] = None


class SourceAdapter:
    source = "?"
    stream = "?"
    transport = "ws"                   # ws | rest
    critical = False
    auth_required = False
    documentation = ""

    def subscribe_messages(self):
        return []

    def parse(self, text, ctx):
        raise NotImplementedError

    # ---------- helpers ----------
    def load(self, text, ctx, res):
        try:
            return json.loads(text) if isinstance(text, (str, bytes)) else text
        except ValueError as e:
            self.fail(res, ctx, f"invalid JSON: {e}", text)
            return None

    def fail(self, res, ctx, reason, text=""):
        excerpt = text if isinstance(text, str) else json.dumps(text, default=str)
        res.failures.append(ParseFailure(self.source, self.stream, str(reason)[:200], ctx.receive_ts_ms,
                                         ctx.seq(), excerpt[:300]))

    def event(self, ctx, **kw):
        kw.setdefault("receive_mono_ns", ctx.receive_mono_ns)
        return MarketEvent(source=self.source, receive_ts_ms=ctx.receive_ts_ms, ingest_seq=ctx.seq(),
                           session_id=ctx.session_id, **kw)

    def guarded(self, res, ctx, raw, fn):
        """Run fn(); a NormalizationError / bad shape becomes a recorded parse failure, never a guess."""
        try:
            fn()
        except (NormalizationError, KeyError, TypeError, ValueError) as e:
            self.fail(res, ctx, f"{type(e).__name__}: {e}", raw)


def new_result():
    return ParseResult()
