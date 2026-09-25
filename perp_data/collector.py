"""
PerpCollector: the sink the Step-3 feed runners (WsFeedRunner / PollRunner) report perp messages to.
Same contract as market_data.collector.Collector (on_message / on_rest / on_connect / on_disconnect /
on_poll_error / on_source_disabled / tick / close), a separate store: <root>/<session_id>/perp/.

Per received message, in arrival order:
    1. arrival counter (shared with the Step-3 collector when collected together) + the RAW text (kind "raw")
    2. venue adapter -> PerpEvents (kind "event") / recorded parse failures (kind "failure")
    3. duplicates counted (reconnect / backfill / repeated history polls); stored, collapsed at replay
    4. SOURCE-APPROPRIATE gap detection (no universal "one per second" rule):
         TRADE_ID      Binance aggregate trade ids (sequential) - exact missing count, recoverable via REST fromId
         SEQUENCE      Binance partial-depth u / pu chain (each message is a full top-N snapshot: recoverable)
         CADENCE       Binance markPrice@1s stream (periodic 1 s: gap if > 3 s between event times)
         FUNDING       settled funding records farther apart than 1.5 x the funding interval (missing settlement)
         POLL_CADENCE  a periodic REST poll (e.g. OI) with no successful response for > max(2.5 x interval, interval + 5 s)
         DISCONNECT    transport outage (last message -> reconnect), per asset of the venue
       Trades, liquidations and Bybit/OKX ticker deltas are irregular: no cadence rule applies to them.
    5. manifest counters per venue and event type
"""
import json
import os
import threading

from market_data.clock import ClockMonitor
from market_data.gaps import CadenceGapDetector, Gap, IdGapDetector, disconnect_gap
from market_data.manifest import new_session_id, utc_iso
from market_data.sources.base import Sequencer
from market_data.storage import EventStoreWriter
from market_data.types import RawMessage
from perp_data.manifest import PerpManifest
from perp_data.sources.base import PerpCtx
from perp_data.types import PerpEventType as T
from perp_data.venues import VENUES

MAX_RAW_CHARS = 1 << 20


class PerpCollector:
    def __init__(self, root, assets, adapters, clock, session_id=None, seq=None, keep_raw=True, fsync=True,
                 flush_lines=500, manifest_every_s=5.0, synthetic=False, step3_session_dir=None):
        self.clock = clock
        self.assets = list(assets)
        self.adapters = dict(adapters)                      # name -> adapter
        self.session_id = session_id or new_session_id(clock.wall_ms())
        self.dir = os.path.join(root, self.session_id, "perp")
        self.writer = EventStoreWriter(self.dir, flush_lines=flush_lines, fsync=fsync)
        self.seq = seq or Sequencer()
        self.keep_raw = keep_raw
        self.lock = threading.RLock()
        self.clock_monitor = ClockMonitor()
        sem = {n: {k: v for k, v in VENUES[n].__dict__.items() if k not in ("symbols", "index_symbols")}
               for n in self.adapters if n in VENUES}
        self.manifest = PerpManifest(
            session_id=self.session_id, started_utc=utc_iso(clock.wall_ms()), assets=self.assets,
            derivatives_sources={n: {"transport": a.transport, "url": getattr(a, "url", None),
                                     "symbols": dict(VENUES[n].symbols) if n in VENUES else {},
                                     "documentation": a.documentation} for n, a in self.adapters.items()},
            step3_session_dir=step3_session_dir, venue_semantics=sem, synthetic=synthetic)
        self.counts = {"raw": 0, "events": 0, "failures": 0, "gaps": 0, "duplicates": 0, "control": 0}
        self.by_type = {}
        self.dedup = set()
        self.last_msg_wall = {}
        self.last_ws_wall = {}
        self.open_since = {}
        self.disconnected_at = {}
        self.trade_ids = {}                                 # (source, symbol) -> IdGapDetector
        self.book_u = {}                                    # (source, symbol) -> (last update id, series ts)
        self.cadence = {}
        self.funding_last = {}
        self.poll_last = {}
        self._last_manifest_mono = None
        self.manifest_every_s = manifest_every_s
        self.manifest.write(self.dir)

    # ---------------- inputs ----------------
    def _ctx(self, wall, mono, raw_seq, channel="ws"):
        return PerpCtx(self.session_id, self.seq, wall, mono, raw_seq, channel)

    def _raw(self, source, stream, text, wall, mono):
        n = self.seq()
        if self.keep_raw:
            t = text if isinstance(text, str) else json.dumps(text, default=str)
            self.writer.write("raw", RawMessage(source, stream, wall, n, t[:MAX_RAW_CHARS], mono, self.session_id).to_dict())
            self.counts["raw"] += 1
        return n

    def on_message(self, adapter, text, wall, mono):
        with self.lock:
            a = self.clock_monitor.check(wall, mono)
            if a is not None:
                self.manifest.clock_anomalies.append(a.to_dict())
            n = self._raw(adapter.source, adapter.stream, text, wall, mono)
            self.last_ws_wall[adapter.source] = wall
            return self._handle(adapter, adapter.parse(text, self._ctx(wall, mono, n)), wall)

    def on_rest(self, adapter, stream, body, wall, mono):
        """A REST response for `stream` (see adapter.rest_urls / backfill): raw first, then normalized."""
        with self.lock:
            n = self._raw(adapter.source, f"rest:{stream}", json.dumps(body, default=str), wall, mono)
            res = adapter.parse_rest(stream, body, self._ctx(wall, mono, n, "rest"))
            n_gap = self._poll_cadence(adapter, stream, wall)
            e, f, g = self._handle(adapter, res, wall)
            return e, f, g + n_gap

    def on_connect(self, adapter, wall, reconnect=False):
        with self.lock:
            src = adapter.source
            self.writer.write("feed", {"source": src, "event": "connect", "reconnect": reconnect, "wall_ms": wall,
                                       "ingest_seq": self.seq()})
            if reconnect:
                self.manifest.reconnects[src] = self.manifest.reconnects.get(src, 0) + 1
            if reconnect or src in self.open_since:          # a failed FIRST attempt also left an open outage
                last = self.open_since.pop(src, None)
                if last is None:
                    last = self.last_ws_wall.get(src, self.disconnected_at.get(src, wall))
                for asset in adapter.assets:
                    g = disconnect_gap(src, asset, "ws", last, wall, src in ("binance_usdm", "bybit_linear", "okx_swap"))
                    self._gap(Gap(**dict(g.to_dict(), known_at_ms=wall, ingest_seq=self.seq())))

    def on_disconnect(self, adapter, wall, error):
        with self.lock:
            src = adapter.source
            self.manifest.disconnects[src] = self.manifest.disconnects.get(src, 0) + 1
            self.disconnected_at[src] = wall
            self.writer.write("feed", {"source": src, "event": "disconnect", "error": str(error)[:200], "wall_ms": wall,
                                       "ingest_seq": self.seq()})
            if src not in self.open_since and adapter.transport == "ws":
                # an OPEN outage, known NOW (not only at reconnect): features over windows after `start` are MISSING
                # until the matching DISCONNECT gap (same start) closes it at reconnect
                start = self.last_ws_wall.get(src, wall)
                self.open_since[src] = start
                for asset in adapter.assets:
                    self._gap(Gap(src, asset, "ws", "DISCONNECT_OPEN", start, start, 0, None, True,
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

    # ---------------- internals ----------------
    def _gap(self, g):
        self.counts["gaps"] += 1
        d = g.to_dict()
        self.manifest.gaps.append(d)
        del self.manifest.gaps[:-500]
        self.writer.write("gap", d)

    def _record_gap(self, g, wall):
        self._gap(Gap(**dict(g.to_dict(), known_at_ms=wall, ingest_seq=self.seq())))
        return 1

    def _handle(self, adapter, res, wall):
        src = adapter.source
        n_gap = 0
        for f in res.failures:
            self.writer.write("failure", f.to_dict())
            self.counts["failures"] += 1
            self.manifest.parse_failures[src] = self.manifest.parse_failures.get(src, 0) + 1
        for c in res.control:
            self.counts["control"] += 1
            self.writer.write("note", {"source": src, "control": c if isinstance(c, (dict, str)) else str(c)})
        for ev in res.events:
            self.last_msg_wall[src] = wall
            key = ev.dedup_key()
            if key is not None:
                if key in self.dedup:
                    self.counts["duplicates"] += 1
                    self.manifest.duplicates[src] = self.manifest.duplicates.get(src, 0) + 1
                else:
                    self.dedup.add(key)
            self.writer.write("event", ev.to_dict())
            self.counts["events"] += 1
            k = f"{src}:{ev.event_type.value}"
            self.by_type[k] = self.by_type.get(k, 0) + 1
            n_gap += self._detect(ev, wall)
            if ev.event_type == T.INSTRUMENT:
                p = ev.payload
                if p.get("contract_value") is not None:
                    self.manifest.contract_values[f"{src}:{ev.symbol}"] = {"value": p["contract_value"], "verified": p["verified"]}
                if p.get("funding_interval_ms"):
                    self.manifest.funding_intervals[f"{src}:{ev.symbol}"] = {"ms": p["funding_interval_ms"],
                                                                            "source": p.get("interval_source")}
        return len(res.events), len(res.failures), n_gap

    def _detect(self, ev, wall):
        et, key = ev.event_type, (ev.source, ev.symbol)
        g = None
        if et == T.PERP_TRADE and ev.source == "binance_usdm":
            det = self.trade_ids.setdefault(key, IdGapDetector(ev.source, ev.asset, "PERP_TRADE", True))
            g = det.observe(ev.payload.get("trade_id"), ev.series_ts_ms)
        elif et == T.ORDERBOOK_TOP and ev.payload.get("prev_update_id") is not None:
            last = self.book_u.get(key)
            u, pu = ev.payload.get("update_id"), ev.payload.get("prev_update_id")
            if last is not None and pu != last[0]:
                g = Gap(ev.source, ev.asset, "ORDERBOOK_TOP", "SEQUENCE", last[1], ev.series_ts_ms,
                        max(ev.series_ts_ms - last[1], 0), None, True, f"pu {pu} != previous u {last[0]}")
            self.book_u[key] = (u, ev.series_ts_ms)
        elif et == T.PERP_MARK_PRICE and ev.source == "binance_usdm":
            det = self.cadence.setdefault(key, CadenceGapDetector(ev.source, ev.asset, "PERP_MARK_PRICE", 1000, 3.0))
            g = det.observe(ev.event_ts_ms)
        elif et == T.FUNDING_SETTLED and ev.payload.get("interval_ms"):
            t, iv = ev.payload["funding_ts_ms"], ev.payload["interval_ms"]
            last = self.funding_last.get(key)
            if last is not None and t > last and t - last > 1.5 * iv:
                g = Gap(ev.source, ev.asset, "FUNDING_SETTLED", "FUNDING", last, t, t - last,
                        int(round((t - last) / iv)) - 1, True, "settled funding record(s) missing")
            if last is None or t > last:
                self.funding_last[key] = t
        return self._record_gap(g, wall) if g is not None else 0

    def _poll_cadence(self, adapter, stream, wall):
        if stream.startswith("backfill"):
            return 0
        iv = (adapter.rest_urls().get(stream) or (None, None, None))[2]
        key = (adapter.source, stream)
        last = self.poll_last.get(key)
        self.poll_last[key] = wall
        if iv is None or last is None:
            return 0
        limit = max(2.5 * iv * 1000, iv * 1000 + 5000)
        if wall - last > limit:
            asset = stream.partition(":")[2] or "*"
            return self._record_gap(Gap(adapter.source, asset, f"rest:{stream}", "POLL_CADENCE", last, wall, wall - last,
                                        None, False, f"no successful poll for {(wall - last) / 1000:.1f} s (interval {iv} s)"),
                                    wall)
        return 0

    # ---------------- periodic / end ----------------
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
        self.manifest.store = {"records": self.writer.records, "bytes_compressed": self.writer.bytes_compressed,
                               "flushes": self.writer.flushes, "last_ingest_seq": self.seq.value}

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
