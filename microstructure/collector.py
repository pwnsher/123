"""
MicroCollector: the sink Step 3's feed runners (WsFeedRunner / PollRunner) report BOOK messages to. Same contract as
the Step-3 / Step-4 collectors (on_message / on_rest / on_connect / on_disconnect / on_poll_error /
on_source_disabled / tick / close); a separate store: <root>/<session_id>/micro/.

Per received message, in arrival order:
    1. arrival counter (shared with the Step-3 / Step-4 collectors) + the RAW text (kind "raw"; stream "ws#<conn>")
    2. venue adapter -> MicroEvents (kind "event") / recorded parse failures (kind "failure")
    3. the LIVE BookReconstructor (the same code replay uses) applies every book event:
         Binance: a gap or a missing snapshot -> the book is queued for a GET snapshot (SnapshotPoller)
         other venues: an unrecoverable gap / invalid book -> ResnapshotRequired is raised AFTER everything has been
         written, so the runner reconnects and the venue sends fresh snapshots (the old book is never continued)
    4. book gaps / invalidations are also written as gap records (kind "gap") with the time they became known
On disconnect a BOOK_RESET event (reason DISCONNECT) is written for every book of the source, so replay sees the
same invalidation at the same receive time. Nothing is dropped silently: raw texts longer than MAX_RAW_CHARS are
counted in manifest.drops (their events are still parsed from the full text), and venue-side losses surface as
sequence gaps.
"""
import json
import os
import threading

from market_data.clock import ClockMonitor
from market_data.gaps import Gap, disconnect_gap
from market_data.manifest import new_session_id, utc_iso
from market_data.sources.base import Sequencer
from market_data.types import RawMessage
from microstructure.manifest import MicroManifest
from microstructure.reconstruction import BookReconstructor, BookStatus
from microstructure.sources.base import MicroCtx
from microstructure.storage import MicroStoreWriter, storage_estimate
from microstructure.types import MicroEvent, MicroEventType as MT
from microstructure.venues import VENUES

MAX_RAW_CHARS = 4 << 20


class ResnapshotRequired(ConnectionError):
    """Raised from on_message after a book became unrecoverable: the runner reconnects (fresh snapshots)."""


class MicroCollector:
    def __init__(self, root, assets, adapters, clock, session_id=None, seq=None, keep_raw=True, fsync=True,
                 flush_lines=500, manifest_every_s=5.0, synthetic=False, step3_session_dir=None, compression_level=6,
                 segment_max_bytes=64 << 20, segment_max_s=None, warmup_ms=1000):
        self.clock = clock
        self.assets = list(assets)
        self.adapters = dict(adapters)
        self.session_id = session_id or new_session_id(clock.wall_ms())
        self.dir = os.path.join(root, self.session_id, "micro")
        self.writer = MicroStoreWriter(self.dir, compression_level=compression_level, segment_max_bytes=segment_max_bytes,
                                       segment_max_s=segment_max_s, flush_lines=flush_lines, fsync=fsync,
                                       clock=lambda: clock.mono_ns() / 1e9)
        self.seq = seq or Sequencer()
        self.keep_raw = keep_raw
        self.lock = threading.RLock()
        self.clock_monitor = ClockMonitor()
        self.recon = BookReconstructor(warmup_ms=warmup_ms)
        sem = {n: {k: v for k, v in VENUES[n].__dict__.items() if k != "symbols"} for n in self.adapters if n in VENUES}
        self.manifest = MicroManifest(
            session_id=self.session_id, started_utc=utc_iso(clock.wall_ms()), assets=self.assets,
            book_sources={n: {"transport": a.transport, "url": getattr(a, "url", None), "depth": a.depth,
                              "symbols": dict(VENUES[n].symbols) if n in VENUES else {}, "documentation": a.documentation}
                          for n, a in self.adapters.items()},
            step3_session_dir=step3_session_dir, venue_semantics=sem, synthetic=synthetic,
            storage_settings={"compression_level": compression_level, "segment_max_bytes": segment_max_bytes,
                              "segment_max_s": segment_max_s, "keep_raw": keep_raw, "flush_lines": flush_lines})
        self.counts = {"raw": 0, "events": 0, "failures": 0, "gaps": 0, "duplicates": 0, "control": 0, "resets": 0}
        self.by_type = {}
        self.dedup = set()
        self.conn = {}
        self.last_ws_wall = {}
        self.open_since = {}
        self.snapshot_needed = {}                 # (source, asset) -> wall ms since when
        self.latency = {}                         # source -> [receive - event ms] (bounded)
        self.started_mono = clock.mono_ns()
        self._last_manifest_mono = None
        self.manifest_every_s = manifest_every_s
        self.manifest.write(self.dir)

    # ---------------- inputs ----------------
    def _ctx(self, src, wall, mono, raw_seq, channel="ws"):
        return MicroCtx(self.session_id, self.seq, wall, mono, raw_seq, channel, self.conn.get(src, 0))

    def _raw(self, source, stream, text, wall, mono):
        n = self.seq()
        if self.keep_raw:
            t = text if isinstance(text, str) else json.dumps(text, default=str)
            if len(t) > MAX_RAW_CHARS:
                self.manifest.drops[f"{source}:raw_truncated"] = self.manifest.drops.get(f"{source}:raw_truncated", 0) + 1
            self.writer.write("raw", RawMessage(source, stream, wall, n, t[:MAX_RAW_CHARS], mono, self.session_id).to_dict())
            self.counts["raw"] += 1
        return n

    def on_message(self, adapter, text, wall, mono):
        with self.lock:
            a = self.clock_monitor.check(wall, mono)
            if a is not None:
                self.manifest.clock_anomalies.append(a.to_dict())
            src = adapter.source
            n = self._raw(src, f"{adapter.stream}#{self.conn.get(src, 0)}", text, wall, mono)
            self.last_ws_wall[src] = wall
            res = adapter.parse(text, self._ctx(src, wall, mono, n))
            e, f, g, resnap = self._handle(adapter, res, wall)
        if resnap:
            raise ResnapshotRequired(f"{src}: {resnap}")
        return e, f, g

    def on_rest(self, adapter, stream, body, wall, mono):
        with self.lock:
            src = adapter.source
            n = self._raw(src, f"rest:{stream}", json.dumps(body, default=str), wall, mono)
            res = adapter.parse_rest(stream, body, self._ctx(src, wall, mono, n, "rest"))
            e, f, g, _resnap = self._handle(adapter, res, wall)
            return e, f, g

    def on_connect(self, adapter, wall, reconnect=False):
        with self.lock:
            src = adapter.source
            self.conn[src] = self.conn.get(src, 0) + 1
            adapter.on_new_connection(self.conn[src])
            self.writer.write("feed", {"source": src, "event": "connect", "reconnect": reconnect, "wall_ms": wall,
                                       "conn": self.conn[src], "ingest_seq": self.seq()})
            if reconnect:
                self.manifest.reconnects[src] = self.manifest.reconnects.get(src, 0) + 1
            if reconnect or src in self.open_since:
                last = self.open_since.pop(src, None)
                if last is None:
                    last = self.last_ws_wall.get(src, wall)
                for asset in adapter.assets:
                    g = disconnect_gap(src, asset, "ws", last, wall, False)
                    self._gap(Gap(**dict(g.to_dict(), known_at_ms=wall, ingest_seq=self.seq())))
            if getattr(adapter, "rest_snapshots", False):
                for asset in adapter.assets:
                    self.snapshot_needed.setdefault((src, asset), wall)

    def on_disconnect(self, adapter, wall, error):
        with self.lock:
            src = adapter.source
            self.manifest.disconnects[src] = self.manifest.disconnects.get(src, 0) + 1
            self.writer.write("feed", {"source": src, "event": "disconnect", "error": str(error)[:200], "wall_ms": wall,
                                       "ingest_seq": self.seq()})
            reason = "RESNAPSHOT" if "ResnapshotRequired" in str(type(error)) or str(error).startswith(src + ":") else "DISCONNECT"
            self._reset(adapter, "*", reason, wall)
            if src not in self.open_since:
                start = self.last_ws_wall.get(src, wall)
                self.open_since[src] = start
                for asset in adapter.assets:
                    self._gap(Gap(src, asset, "ws", "DISCONNECT_OPEN", start, start, 0, None, False,
                                  "outage in progress; closed by the DISCONNECT gap with the same start",
                                  known_at_ms=wall, ingest_seq=self.seq()))

    def on_poll_error(self, name, wall, error):
        with self.lock:
            self.manifest.disconnects[name] = self.manifest.disconnects.get(name, 0) + 1
            self.writer.write("feed", {"source": name, "event": "poll_error", "error": str(error)[:200], "wall_ms": wall,
                                       "ingest_seq": self.seq()})

    def on_source_disabled(self, adapter, reason):
        with self.lock:
            self.manifest.notes.append(f"{adapter.source} disabled: {reason}")
            self.writer.write("note", {"source": adapter.source, "disabled": str(reason)[:200]})

    def record_overload(self, adapter, wall, detail):
        """The caller could not keep up (e.g. a bounded queue overflowed): the loss is recorded and every book of the
        source is invalidated until a fresh snapshot - never silently continued."""
        with self.lock:
            k = f"{adapter.source}:overload"
            self.manifest.drops[k] = self.manifest.drops.get(k, 0) + 1
            self._reset(adapter, "*", "OVERLOAD", wall)
            self._gap(Gap(adapter.source, "*", "book", "OVERLOAD", wall, wall, 0, None, False, str(detail)[:200],
                          known_at_ms=wall, ingest_seq=self.seq()))

    # ---------------- internals ----------------
    def _reset(self, adapter, book, reason, wall):
        ev = MicroEvent(source=adapter.source, asset="*", event_type=MT.BOOK_RESET, symbol=book, event_ts_ms=None,
                        receive_ts_ms=wall, ingest_seq=self.seq(), payload={"book": book, "reason": reason},
                        receive_mono_ns=self.clock.mono_ns(), session_id=self.session_id, channel="collector")
        self.writer.write("event", ev.to_dict())
        self.counts["resets"] += 1
        self.recon.apply(ev)
        if getattr(adapter, "rest_snapshots", False):
            for asset in adapter.assets:
                self.snapshot_needed.setdefault((adapter.source, asset), wall)

    def _gap(self, g):
        self.counts["gaps"] += 1
        d = g.to_dict()
        self.manifest.gaps.append(d)
        del self.manifest.gaps[:-500]
        self.writer.write("gap", d)

    def _handle(self, adapter, res, wall):
        src = adapter.source
        n_gap = 0
        resnap = None
        for f in res.failures:
            self.writer.write("failure", f.to_dict())
            self.counts["failures"] += 1
            self.manifest.parse_failures[src] = self.manifest.parse_failures.get(src, 0) + 1
        for c in res.control:
            self.counts["control"] += 1
            self.writer.write("note", {"source": src, "control": c if isinstance(c, (dict, str)) else str(c)})
        for ev in res.events:
            key = ev.dedup_key()
            if key is not None:
                if key in self.dedup:
                    self.counts["duplicates"] += 1
                else:
                    self.dedup.add(key)
            self.writer.write("event", ev.to_dict())
            self.counts["events"] += 1
            k = f"{src}:{ev.event_type.value}"
            self.by_type[k] = self.by_type.get(k, 0) + 1
            if ev.event_ts_ms is not None:
                lat = self.latency.setdefault(src, [])
                lat.append(ev.receive_ts_ms - ev.event_ts_ms)
                del lat[:-2000]
            ups = self.recon.apply(ev)
            for u in (ups if isinstance(ups, list) else [ups] if ups is not None else []):
                if u.kind == "snapshot" and getattr(adapter, "rest_snapshots", False):
                    self.snapshot_needed.pop((src, ev.asset), None)
                if u.resnapshot_needed:
                    n_gap += 1
                    self._gap(Gap(src, ev.asset, f"book:{u.key[1]}", "BOOK_SEQUENCE" if u.kind == "gap" else "BOOK_INVALID",
                                  wall, wall, 0, None, getattr(adapter, "rest_snapshots", False), u.reason[:200],
                                  known_at_ms=wall, ingest_seq=self.seq()))
                    if getattr(adapter, "rest_snapshots", False):
                        self.snapshot_needed.setdefault((src, ev.asset), wall)
                        self.manifest.resnapshot_requests[src] = self.manifest.resnapshot_requests.get(src, 0) + 1
                    else:
                        resnap = u.reason
                        self.manifest.resnapshot_requests[src] = self.manifest.resnapshot_requests.get(src, 0) + 1
        return len(res.events), len(res.failures), n_gap, resnap

    # ---------------- periodic / end ----------------
    def book_status(self, now=None):
        now = now if now is not None else self.clock.wall_ms()
        return {f"{k[0]}:{k[1]}": {"status": self.recon.status(k, now).value, "reason": tr.reason,
                                    "levels": tr.book.levels(), "flags": sorted(tr.flags), **tr.counts}
                for k, tr in sorted(self.recon.tracks.items())}

    def tick(self, force=False):
        with self.lock:
            mono = self.clock.mono_ns()
            if not force and self._last_manifest_mono is not None and \
                    (mono - self._last_manifest_mono) / 1e9 < self.manifest_every_s:
                return
            self._last_manifest_mono = mono
            self.writer.flush()
            self._update_manifest()
            self.manifest.write(self.dir)

    def _update_manifest(self):
        self.manifest.counts = {"totals": dict(self.counts), "by_venue_event_type": dict(sorted(self.by_type.items()))}
        self.manifest.books = self.book_status()
        lat = {}
        for s, xs in self.latency.items():
            v = sorted(xs)
            if v:
                lat[s] = {"n": len(v), "median_ms": v[len(v) // 2], "p90_ms": v[int(len(v) * 0.9)], "min_ms": v[0],
                          "negative_share": sum(1 for x in v if x < 0) / len(v)}
        self.manifest.latency = lat
        secs = (self.clock.mono_ns() - self.started_mono) / 1e9
        self.manifest.store = {"records": self.writer.records, "bytes_compressed": self.writer.bytes_compressed,
                               "bytes_uncompressed": self.writer.bytes_uncompressed, "flushes": self.writer.flushes,
                               "rotations": self.writer.rotations, "last_ingest_seq": self.seq.value,
                               "estimate": storage_estimate(self.writer.bytes_compressed, self.writer.bytes_uncompressed,
                                                            secs, self.by_type) if secs > 0 else None}

    def close(self, status="COMPLETED", health=None):
        with self.lock:
            self.writer.flush()
            self._update_manifest()
            if health:
                self.manifest.notes.append({"final_health": health})
            self.manifest.ended_utc = utc_iso(self.clock.wall_ms())
            self.manifest.status = status
            self.manifest.write(self.dir)
            self.writer.close()


__all__ = ["MicroCollector", "ResnapshotRequired", "BookStatus"]
