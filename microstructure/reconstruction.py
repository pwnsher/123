"""
BookReconstructor: the ONE place local books are rebuilt from MicroEvents - used identically by the live
collector (to detect gaps and request a resnapshot) and by replay / the feature engine (deterministic).

Events must arrive in availability order (receive_ts non-decreasing; ties by ingest_seq). apply(ev) returns a
BookUpdate describing what happened; the book's history of VALID INTERVALS is kept so features can require a
book that was continuously valid over a whole window.

Book status (status(key, t))
    NO_BOOK            nothing received yet
    AWAITING_SNAPSHOT  deltas seen (Binance: buffered) but no snapshot yet
    WARMING_UP         a snapshot was applied less than warmup_ms before t (level features not yet READY)
    READY              valid, warmed up and not stale
    STALE              valid, but no message from the book's source for > stale_after_ms
    INVALID            crossed / locked book, checksum mismatch, negative level, non-monotonic id, BOOK_RESET
    NEEDS_RESNAPSHOT   an unrecoverable sequence gap: the book is never continued, only replaced by a snapshot
An INVALID / NEEDS_RESNAPSHOT book stays so until a snapshot from a trustworthy chain arrives; the snapshot is
effective from ITS receive time - nothing before it is repaired (there is no retroactive repair).
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from microstructure.book import LocalBook, NegativeLevel, kraken_checksum
from microstructure.types import MicroEventType as MT
from microstructure.venues import VENUES


class BookStatus(str, Enum):
    NO_BOOK = "NO_BOOK"
    AWAITING_SNAPSHOT = "AWAITING_SNAPSHOT"
    WARMING_UP = "WARMING_UP"
    READY = "READY"
    STALE = "STALE"
    INVALID = "INVALID"
    NEEDS_RESNAPSHOT = "NEEDS_RESNAPSHOT"


VALID_BASE = (BookStatus.READY,)
BAD_BASE = (BookStatus.INVALID, BookStatus.NEEDS_RESNAPSHOT)


@dataclass
class BookUpdate:
    key: tuple
    receive_ts_ms: int
    ingest_seq: int
    kind: str                       # snapshot | delta | reset | gap | invalid | ignored | buffered | dropped | instrument
    status: BookStatus
    changes: list = field(default_factory=list)     # [(side, px, old, new, rank_before)]
    reason: str = ""
    l1_before: Optional[tuple] = None                # (bid, bid_qty, ask, ask_qty) or None
    l1_after: Optional[tuple] = None
    resnapshot_needed: bool = False


class BookTrack:
    def __init__(self, key, policy, depth_cap=None):
        self.key, self.policy, self.depth_cap = key, policy, depth_cap
        self.book = LocalBook()
        self.base = BookStatus.NO_BOOK
        self.update_id = None
        self.chain = None
        self.ready_from = None          # receive ts of the snapshot that started the current valid interval
        self.intervals = []             # [[from_ts, to_ts or None]] valid intervals (append-only)
        self.buffer = []                # Binance deltas awaiting / aligning with a snapshot
        self.aligning = False
        self.snapshot_id = None
        self.reason = ""
        self.flags = set()
        self.counts = {"snapshots": 0, "resnapshots": 0, "deltas": 0, "gaps": 0, "invalid": 0, "crossed": 0,
                       "checksum_ok": 0, "checksum_fail": 0, "buffered": 0, "dropped": 0, "ignored": 0}

    def l1(self):
        b, a = self.book.best_bid(), self.book.best_ask()
        return (b[0] if b else None, b[1] if b else None, a[0] if a else None, a[1] if a else None)


def _chain_break(p):
    prev, uid = p.get("prev_update_id"), p.get("update_id")
    return prev is not None and uid is not None and uid != prev + 1


class BookReconstructor:
    def __init__(self, warmup_ms=1000, buffer_max=5000, stale_after_ms=None):
        self.warmup_ms = warmup_ms
        self.buffer_max = buffer_max
        self.stale_after_ms = stale_after_ms or {}
        self.tracks = {}                # (source, symbol) -> BookTrack
        self.chain_members = {}         # chain -> set(keys)
        self.broken_chains = set()
        self.precision = {}             # (source, symbol) -> (price_precision, qty_precision)
        self.source_last = {}           # source -> last receive ts of any book event (liveness)
        self.max_receive = None

    # ---------------- helpers ----------------
    def track(self, key):
        t = self.tracks.get(key)
        if t is None:
            v = VENUES.get(key[0])
            t = self.tracks[key] = BookTrack(key, v.sequence_policy if v else "monotonic_only")
        return t

    def _invalidate(self, tr, ev, base, reason, kind="invalid"):
        if tr.base == BookStatus.READY and tr.intervals and tr.intervals[-1][1] is None:
            tr.intervals[-1][1] = ev.receive_ts_ms
        tr.base, tr.reason = base, reason
        tr.ready_from = None
        tr.aligning = False
        tr.buffer.clear()
        tr.counts["gaps" if base == BookStatus.NEEDS_RESNAPSHOT else "invalid"] += 1
        return BookUpdate(tr.key, ev.receive_ts_ms, ev.ingest_seq, kind, base, reason=reason, resnapshot_needed=True)

    def _became_valid(self, tr, ev):
        if tr.counts["snapshots"] > 0 and tr.intervals:
            tr.counts["resnapshots"] += 1
        tr.counts["snapshots"] += 1
        tr.base, tr.reason = BookStatus.READY, ""
        tr.ready_from = ev.receive_ts_ms
        tr.intervals.append([ev.receive_ts_ms, None])

    def _check_book(self, tr, ev, upd):
        """Post-apply integrity: crossed / locked book and (Kraken) checksum."""
        if tr.book.crossed():
            tr.counts["crossed"] += 1
            return self._invalidate(tr, ev, BookStatus.INVALID, "CROSSED_OR_LOCKED_BOOK")
        if tr.policy == "checksum" and ev.payload.get("checksum") is not None:
            prec = self.precision.get(tr.key)
            if prec is None:
                tr.flags.add("CHECKSUM_UNVERIFIED")
            else:
                if kraken_checksum(tr.book, *prec) != int(ev.payload["checksum"]):
                    tr.counts["checksum_fail"] += 1
                    return self._invalidate(tr, ev, BookStatus.INVALID, "CHECKSUM_MISMATCH")
                tr.counts["checksum_ok"] += 1
                tr.flags.discard("CHECKSUM_UNVERIFIED")
        return upd

    def _apply_changes(self, tr, changes):
        out = []
        b = tr.book
        for side, px, qty, mode in changes:
            r = b.rank(side, px)
            old, new = b.add_level(side, px, qty) if mode == "rel" else b.set_level(side, px, qty)
            if old != new:
                out.append((side, px, old, new, r))
        if tr.depth_cap:
            b.truncate(tr.depth_cap)
        return out

    # ---------------- main entry ----------------
    def apply(self, ev):
        if self.max_receive is not None and ev.receive_ts_ms < self.max_receive:
            raise ValueError("book events must be applied in availability order")
        self.max_receive = ev.receive_ts_ms
        et, p = ev.event_type, ev.payload
        if et == MT.TRADE:
            return None
        self.source_last[ev.source] = ev.receive_ts_ms
        if et == MT.INSTRUMENT:
            info = p.get("info") or {}
            if info.get("price_precision") is not None and info.get("qty_precision") is not None:
                self.precision[(ev.source, p["book"])] = (int(info["price_precision"]), int(info["qty_precision"]))
            return BookUpdate((ev.source, p["book"]), ev.receive_ts_ms, ev.ingest_seq, "instrument", BookStatus.NO_BOOK)
        if et == MT.BOOK_RESET:
            targets = [k for k in self.tracks if k[0] == ev.source and (p["book"] == "*" or k[1] == p["book"])]
            if p["book"] != "*" and not targets:
                targets = [(ev.source, p["book"])]
            ups = []
            for k in targets:
                tr = self.track(k)
                if tr.base not in BAD_BASE:
                    ups.append(self._invalidate(tr, ev, BookStatus.INVALID, f"RESET:{p['reason']}", "reset"))
            return ups[0] if len(ups) == 1 else (ups or None)
        key = (ev.source, p["book"])
        tr = self.track(key)
        chain = p.get("chain")
        if chain is not None:
            self.chain_members.setdefault(chain, set()).add(key)
            if chain in self.broken_chains:
                tr.counts["ignored"] += 1
                return BookUpdate(key, ev.receive_ts_ms, ev.ingest_seq, "ignored", tr.base, reason="BROKEN_CHAIN")
            if tr.policy == "chain_contiguous" and _chain_break(p):
                self.broken_chains.add(chain)
                ups = [self._invalidate(self.track(k), ev, BookStatus.NEEDS_RESNAPSHOT,
                                        f"SEQUENCE_GAP chain {chain}: {p.get('prev_update_id')} -> {p.get('update_id')}", "gap")
                       for k in sorted(self.chain_members[chain]) if self.track(k).base not in BAD_BASE or k == key]
                return next((u for u in ups if u.key == key), ups[0])
        if et == MT.BOOK_SNAPSHOT:
            return self._snapshot(tr, ev, chain)
        return self._delta(tr, ev, chain)

    def _snapshot(self, tr, ev, chain):
        p = ev.payload
        before = tr.l1()
        tr.chain = chain
        tr.depth_cap = p.get("depth") if tr.policy == "checksum" else None
        tr.book.load(p["bids"], p["asks"])
        tr.update_id = p.get("update_id")
        if tr.policy == "binance_diff":
            tr.snapshot_id = p.get("update_id")
            tr.aligning = True
            tr.base = BookStatus.AWAITING_SNAPSHOT
            if tr.intervals and tr.intervals[-1][1] is None:
                tr.intervals[-1][1] = ev.receive_ts_ms
            pending, tr.buffer = tr.buffer, []
            upd = BookUpdate(tr.key, ev.receive_ts_ms, ev.ingest_seq, "snapshot", tr.base, l1_before=before)
            for d in pending:
                r = self._binance_delta(tr, d, ev)
                if r is not None and r.kind == "gap":
                    return r
            upd.status, upd.l1_after = tr.base, tr.l1()
            return upd
        if tr.intervals and tr.intervals[-1][1] is None:
            tr.intervals[-1][1] = ev.receive_ts_ms
        tr.base = BookStatus.NO_BOOK
        self._became_valid(tr, ev)
        upd = BookUpdate(tr.key, ev.receive_ts_ms, ev.ingest_seq, "snapshot", tr.base, l1_before=before, l1_after=tr.l1())
        return self._check_book(tr, ev, upd)

    def _delta(self, tr, ev, chain):
        p = ev.payload
        if tr.policy == "binance_diff":
            if tr.base != BookStatus.READY and not tr.aligning:
                # no usable book: buffer until a (re)snapshot arrives (the documented procedure needs the deltas
                # received BEFORE the snapshot); an INVALID / NEEDS_RESNAPSHOT status is kept, not upgraded
                if tr.base == BookStatus.NO_BOOK:
                    tr.base = BookStatus.AWAITING_SNAPSHOT
                tr.buffer.append(ev)
                tr.counts["buffered"] += 1
                if len(tr.buffer) > self.buffer_max:
                    tr.buffer.pop(0)
                    tr.counts["dropped"] += 1
                return BookUpdate(tr.key, ev.receive_ts_ms, ev.ingest_seq, "buffered", tr.base)
            return self._binance_delta(tr, ev, ev)
        if tr.base in BAD_BASE or tr.base == BookStatus.NO_BOOK or (chain is not None and tr.chain != chain):
            tr.counts["ignored"] += 1
            if tr.base == BookStatus.NO_BOOK:
                tr.base = BookStatus.AWAITING_SNAPSHOT
            return BookUpdate(tr.key, ev.receive_ts_ms, ev.ingest_seq, "ignored", tr.base,
                              reason=tr.reason or "NO_SNAPSHOT")
        if tr.base == BookStatus.AWAITING_SNAPSHOT:
            tr.counts["ignored"] += 1
            return BookUpdate(tr.key, ev.receive_ts_ms, ev.ingest_seq, "ignored", tr.base, reason="NO_SNAPSHOT")
        uid, prev = p.get("update_id"), p.get("prev_update_id")
        if tr.policy == "prev_chain" and prev is not None and prev != tr.update_id:
            return self._invalidate(tr, ev, BookStatus.NEEDS_RESNAPSHOT, f"SEQUENCE_GAP prev {prev} != last {tr.update_id}", "gap")
        if tr.policy == "monotonic_only" and uid is not None and tr.update_id is not None and uid <= tr.update_id:
            return self._invalidate(tr, ev, BookStatus.INVALID, f"NON_MONOTONIC_UPDATE_ID {uid} <= {tr.update_id}")
        return self._apply(tr, ev, uid)

    def _apply(self, tr, ev, uid):
        before = tr.l1()
        try:
            ch = self._apply_changes(tr, ev.payload["changes"])
        except NegativeLevel as e:
            return self._invalidate(tr, ev, BookStatus.INVALID, f"NEGATIVE_LEVEL {e}")
        if uid is not None:
            tr.update_id = uid
        tr.counts["deltas"] += 1
        upd = BookUpdate(tr.key, ev.receive_ts_ms, ev.ingest_seq, "delta", tr.base, ch, l1_before=before, l1_after=tr.l1())
        return self._check_book(tr, ev, upd)

    def _binance_delta(self, tr, d, at_ev):
        """Binance documented alignment: drop u < lastUpdateId; the first applied delta needs U <= L <= u;
        afterwards pu must equal the previous u. `at_ev` supplies the time a buffered delta becomes effective."""
        p = d.payload
        U, u, pu = p.get("first_update_id"), p.get("update_id"), p.get("prev_update_id")
        eff = at_ev if at_ev is not d else d
        if tr.aligning:
            L = tr.snapshot_id
            if u < L:
                tr.counts["dropped"] += 1
                return BookUpdate(tr.key, eff.receive_ts_ms, eff.ingest_seq, "dropped", tr.base)
            if U > L:
                return self._invalidate(tr, eff, BookStatus.NEEDS_RESNAPSHOT,
                                        f"SNAPSHOT_TOO_OLD first delta U {U} > lastUpdateId {L}", "gap")
            tr.aligning = False
            upd = self._apply(tr, d if eff is d else _Eff(d, eff), u)
            if upd.kind == "delta":
                self._became_valid(tr, eff)
                upd.status = tr.base
            return upd
        if tr.base != BookStatus.READY:
            return BookUpdate(tr.key, eff.receive_ts_ms, eff.ingest_seq, "ignored", tr.base)
        if pu != tr.update_id:
            return self._invalidate(tr, eff, BookStatus.NEEDS_RESNAPSHOT, f"SEQUENCE_GAP pu {pu} != previous u {tr.update_id}", "gap")
        return self._apply(tr, d if eff is d else _Eff(d, eff), u)

    # ---------------- status ----------------
    def status(self, key, t):
        tr = self.tracks.get(key)
        if tr is None:
            return BookStatus.NO_BOOK
        if tr.base != BookStatus.READY:
            return tr.base
        stale = self.stale_after_ms.get(key[0]) or (VENUES[key[0]].stale_after_ms if key[0] in VENUES else 5000)
        if t - self.source_last.get(key[0], t) > stale:
            return BookStatus.STALE
        if t - tr.ready_from < self.warmup_ms:
            return BookStatus.WARMING_UP
        return BookStatus.READY

    def valid_since(self, key, t):
        """Start of the valid interval containing t (None if the book is not valid at t)."""
        tr = self.tracks.get(key)
        if tr is None:
            return None
        for lo, hi in reversed(tr.intervals):
            if lo <= t and (hi is None or hi > t):
                return lo
            if hi is not None and hi <= t:
                break
        return None

    def valid_throughout(self, key, lo, hi):
        s = self.valid_since(key, hi)
        return s is not None and s <= lo


class _Eff:
    """A buffered Binance delta made effective at the snapshot's receive time (it was received earlier, but the
    book it modifies exists only from the snapshot on)."""
    __slots__ = ("payload", "receive_ts_ms", "ingest_seq")

    def __init__(self, d, at):
        self.payload, self.receive_ts_ms, self.ingest_seq = d.payload, at.receive_ts_ms, at.ingest_seq
