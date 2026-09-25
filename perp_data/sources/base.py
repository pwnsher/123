"""
Perp adapter base: Step 3's SourceAdapter helpers (JSON load, recorded parse failures, guarded parsing)
with an event() that builds PerpEvents and records the raw message they came from (raw_seq).

Stateful venues (snapshot + delta streams, contract values learned from INSTRUMENT metadata) keep their
state inside the adapter; the state is rebuilt deterministically by replaying the stored raw messages
in arrival order.
"""
from dataclasses import dataclass
from typing import Optional

from market_data.sources.base import Ctx, SourceAdapter, new_result  # noqa: F401  (re-export)
from market_data.types import IngestMode
from perp_data.types import PerpEvent, PerpFlag
from perp_data.venues import VENUES as _SPECS, spec


@dataclass
class PerpCtx(Ctx):
    raw_seq: Optional[int] = None
    channel: str = "ws"


class PerpAdapter(SourceAdapter):
    venue = "?"
    keepalive_text: Optional[str] = None     # application-level ping text (None = the venue pings us)
    keepalive_s: Optional[float] = None

    def __init__(self, assets, depth=10):
        self.spec = spec(self.venue) if self.venue in _SPECS else None
        self.assets = [a for a in assets if not self.spec or not self.spec.symbols or a in self.spec.symbols]
        self.depth = depth
        self.by_symbol = {self.spec.symbols[a]: a for a in self.assets} if self.spec and self.spec.symbols else {}
        cv = self.spec.contract_values if self.spec else {}
        verified = bool(self.spec and self.spec.contract_values_verified)
        self.contract_values = {a: (cv[a], verified) for a in self.assets if a in cv}

    # ---------- events ----------
    def event(self, ctx, **kw):
        kw.setdefault("receive_mono_ns", ctx.receive_mono_ns)
        kw.setdefault("raw_seq", getattr(ctx, "raw_seq", None))
        kw.setdefault("channel", getattr(ctx, "channel", "ws"))
        return PerpEvent(source=self.source, receive_ts_ms=ctx.receive_ts_ms, ingest_seq=ctx.seq(),
                         session_id=ctx.session_id, **kw)

    def rest_urls(self):
        """{stream_name: (url, params, interval_s)} of periodic read-only REST polls for this venue."""
        return {}

    # ---------- contract values ----------
    def to_coin(self, asset, qty_native):
        """(qty_coin, flags, quality) for a native quantity in this venue's size unit."""
        if qty_native is None:
            return None, (), "OK"
        if self.spec.size_unit == "coin":
            return qty_native, (), "OK"
        cv = self.contract_values.get(asset)
        if cv is None:
            return None, (PerpFlag.CONTRACT_SIZE_UNVERIFIED.value,), "UNVERIFIED_UNITS"
        value, verified = cv
        if not verified:
            return None, (PerpFlag.CONTRACT_SIZE_UNVERIFIED.value,), "UNVERIFIED_UNITS"
        return qty_native * value, (), "OK"

    def mode(self, backfill):
        return IngestMode.BACKFILLED if backfill else IngestMode.LIVE

