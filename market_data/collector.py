"""
Collector: the sink every feed runner reports to. RESEARCH / OBSERVATION ONLY.

For each received message (in arrival order):
    1. assign the global arrival counter (ingest_seq) and record the RAW text (kind "raw")
    2. normalize via the source adapter -> events (kind "event") / parse failures (kind "failure")
    3. duplicates counted (reconnect / backfill overlap; the raw record is kept, replay de-duplicates)
    4. gap detection: CF cadence, Coinbase trade ids (+ heartbeat last_trade_id), Kraken trade ids,
       transport outages (DISCONNECT) -> kind "gap" with known_at = receive time
    5. settlement evidence (point 32): CF observations, Kalshi-published averages, Kalshi market metadata
       and settled results are appended to a Step-2 settlement store, so verify_settlement_resolution /
       compare_settlement_overlap can run later. Nothing is declared verified.
    6. live feature engines (one per asset) for the research status page only
The session manifest is rewritten atomically every few seconds and at the end. Nothing here can reach
the production watcher or place an order.
"""
import os
import threading

from market_data import APP_VERSION
from market_data.alignment import available  # noqa: F401  (documented rule; engines enforce it)
from market_data.clock import ClockMonitor
from market_data.features.engine import CausalityError, FeatureEngine
from market_data.gaps import CadenceGapDetector, Gap, IdGapDetector, disconnect_gap
from market_data.manifest import SessionManifest, new_session_id, utc_iso
from market_data.sources.base import Ctx, Sequencer
from market_data.storage import EventStoreWriter
from market_data.types import EventType, RawMessage
from settlement.assets import ASSET_INDEX
from settlement.cache import SettlementStore
from settlement.cf_live import PublishedAverage
from settlement.types import OfficialResolution, SettlementMarket, SettlementObservation

MAX_RAW_CHARS = 1 << 20
RECOVERABLE = {"coinbase": True}


class Collector:
    def __init__(self, root, assets, sources, clock, session_id=None, settlement_store_path=None, keep_raw=True,
                 live_features=True, manifest_every_s=5.0, flush_lines=500, fsync=True, synthetic=False):
        self.clock = clock
        self.synthetic = synthetic
        self.assets = list(assets)
        self.sources = dict(sources)                      # name -> {"adapter", "enabled", "critical", "config"}
        self.session_id = session_id or new_session_id(clock.wall_ms())
        self.dir = os.path.join(root, self.session_id)
        self.writer = EventStoreWriter(self.dir, flush_lines=flush_lines, fsync=fsync)
        self.seq = Sequencer()
        self.keep_raw = keep_raw
        self.lock = threading.RLock()
        self.clock_monitor = ClockMonitor()
        self.manifest = SessionManifest(
            session_id=self.session_id, started_utc=utc_iso(clock.wall_ms()), assets=self.assets,
            sources={n: {k: v for k, v in s.items() if k != "adapter"} for n, s in self.sources.items()})
        self.cadence = {}
        self.idgaps = {}
        self._hb_pending = {}
        self.last_msg_wall = {}
        self.disconnected_at = {}
        self.dedup = set()
        self.counts = {"events": 0, "raw": 0, "failures": 0, "gaps": 0, "duplicates": 0, "control": 0}
        self.settlement_store = SettlementStore(settlement_store_path) if settlement_store_path else None
        self.manifest.settlement_store = settlement_store_path
        self._settle_buf = []
        self._settle_session_written = False
        self._markets_seen = set()
        self.live_features = live_features
        self.engines = {a: FeatureEngine(a) for a in self.assets} if live_features else {}
        self.current_market = {}
        self.last_values = {}
        self.manifest_every_s = manifest_every_s
        self._last_manifest_mono = None
        self.manifest.write(self.dir)

    # ---------------- inputs from runners ----------------
    def ctx(self, wall, mono):
        return Ctx(self.session_id, self.seq, wall, mono)

    def on_message(self, adapter, text, wall, mono):
        with self.lock:
            anomaly = self.clock_monitor.check(wall, mono)
            if anomaly is not None:
                self.manifest.clock_anomalies.append(anomaly.to_dict())
            self._raw(adapter.source, adapter.stream, text, wall, mono)
            res = adapter.parse(text, self.ctx(wall, mono))
            return self._handle(adapter.source, res, wall)

    def on_rest(self, source, stream, body_text, parse_fn, wall, mono):
        """A REST response: record raw text, then parse_fn(ctx) -> ParseResult."""
        with self.lock:
            self._raw(source, stream, body_text, wall, mono)
            return self._handle(source, parse_fn(self.ctx(wall, mono)), wall)

    def on_connect(self, adapter, wall, reconnect=False):
        with self.lock:
            src = adapter.source
            self.writer.write("feed", {"source": src, "event": "connect", "reconnect": reconnect, "wall_ms": wall,
                                       "ingest_seq": self.seq()})
            if reconnect:
                self.manifest.reconnects[src] = self.manifest.reconnects.get(src, 0) + 1
                last = self.last_msg_wall.get(src, self.disconnected_at.get(src, wall))
                for asset in getattr(adapter, "assets", self.assets):
                    g = disconnect_gap(src, asset, adapter.stream, last, wall, RECOVERABLE.get(src, False))
                    self._gap(Gap(**dict(g.to_dict(), known_at_ms=wall, ingest_seq=self.seq())))

    def on_disconnect(self, adapter, wall, error):
        with self.lock:
            src = adapter.source
            self.manifest.disconnects[src] = self.manifest.disconnects.get(src, 0) + 1
            self.disconnected_at[src] = wall
            self.writer.write("feed", {"source": src, "event": "disconnect", "error": str(error)[:200], "wall_ms": wall,
                                       "ingest_seq": self.seq()})

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
    def _raw(self, source, stream, text, wall, mono):
        n = self.seq()
        if self.keep_raw:
            t = text if isinstance(text, str) else str(text)
            self.writer.write("raw", RawMessage(source, stream, wall, n, t[:MAX_RAW_CHARS], mono, self.session_id).to_dict())
            self.counts["raw"] += 1

    def _gap(self, g):
        self.counts["gaps"] += 1
        self.manifest.gaps.append(g.to_dict())
        del self.manifest.gaps[:-500]
        self.writer.write("gap", g.to_dict())
        for eng in self.engines.values():
            eng.ingest_gap(g)

    def _handle(self, source, res, wall):
        n_gap = 0
        for f in res.failures:
            self.writer.write("failure", f.to_dict())
            self.counts["failures"] += 1
            self.manifest.parse_failures[source] = self.manifest.parse_failures.get(source, 0) + 1
        for c in res.control:
            self.counts["control"] += 1
            self.writer.write("note", {"source": source, "control": c})
        for ev in res.events:
            self.last_msg_wall[source] = wall
            key = ev.dedup_key()
            if key is not None:
                if key in self.dedup:
                    self.counts["duplicates"] += 1
                    self.manifest.duplicates[source] = self.manifest.duplicates.get(source, 0) + 1
                else:
                    self.dedup.add(key)
            self.writer.write("event", ev.to_dict())
            self.counts["events"] += 1
            n_gap += self._detect_gaps(ev, wall)
            self._settlement_capture(ev)
            self._live(ev)
        return len(res.events), len(res.failures), n_gap

    def _detect_gaps(self, ev, wall):
        g = None
        if ev.event_type == EventType.INDEX_VALUE:
            det = self.cadence.setdefault(ev.symbol, CadenceGapDetector(ev.source, ev.asset, ev.symbol, 1000, 2.5))
            g = det.observe(ev.event_ts_ms)
        elif ev.event_type == EventType.TRADE and ev.source in ("coinbase", "kraken"):
            # live AND backfilled trades advance the id detector: a backfill that closes a hole means the next
            # live trade is contiguous; ids at or below the last seen are ignored (overlap / duplicates)
            det = self.idgaps.setdefault((ev.source, ev.symbol),
                                         IdGapDetector(ev.source, ev.asset, "trades", RECOVERABLE.get(ev.source, False)))
            g = det.observe(ev.payload.get("trade_id"), ev.series_ts_ms)
        elif ev.event_type == EventType.HEARTBEAT and ev.source == "coinbase" and ev.payload.get("last_trade_id"):
            # A heartbeat's last_trade_id is checked one heartbeat LATER (~1 s): a trade and the heartbeat
            # that mentions it can be delivered in either order, so an immediate check would invent gaps.
            det = self.idgaps.setdefault((ev.source, ev.symbol), IdGapDetector(ev.source, ev.asset, "trades", True))
            prev = self._hb_pending.get((ev.source, ev.symbol))
            self._hb_pending[(ev.source, ev.symbol)] = (int(ev.payload["last_trade_id"]), ev.series_ts_ms)
            if prev is not None and det.last_id is not None and prev[0] > det.last_id:
                lt, hb_ts = prev
                start = det.last_ts or hb_ts
                g = Gap(ev.source, ev.asset, "trades", "TRADE_ID", start, hb_ts, max(hb_ts - start, 0),
                        lt - det.last_id, True, f"heartbeat last_trade_id {lt} > last received {det.last_id}")
                det.last_id, det.last_ts = lt, hb_ts
        if g is None:
            return 0
        self._gap(Gap(**dict(g.to_dict(), known_at_ms=wall, ingest_seq=self.seq())))
        return 1

    def _settlement_capture(self, ev):
        if self.settlement_store is None:
            return
        p = ev.payload
        if ev.event_type == EventType.INDEX_VALUE:
            self._settle_buf.append(("observation", SettlementObservation.from_dict(p["observation"])))
        elif ev.event_type == EventType.PUBLISHED_AVERAGE:
            self._settle_buf.append(("published_average", PublishedAverage(p["index_id"], p["kind"], p["value"],
                                                                           ev.event_ts_ms, ev.receive_ts_ms, ev.source)))
        elif ev.event_type == EventType.MARKET_STATE and p["ticker"] not in self._markets_seen:
            self._markets_seen.add(p["ticker"])
            self._settle_buf.append(("market", SettlementMarket(
                ticker=p["ticker"], asset=ev.asset, close_ts_ms=p["close_ts_ms"], index_id=ASSET_INDEX[ev.asset],
                strike=p["strike"], strike_source=p["strike_source"], open_ts_ms=p["open_ts_ms"],
                series=p["ticker"].split("-")[0], metadata_source="kalshi_market_api")))
        elif ev.event_type == EventType.RESOLUTION:
            self._settle_buf.append(("resolution", OfficialResolution(p["ticker"], p["result"], p["expiration_value"],
                                                                      "kalshi_market_api")))

    def _live(self, ev):
        if not self.live_features:
            return
        if ev.event_type == EventType.MARKET_STATE:
            p = ev.payload
            self.current_market[ev.asset] = SettlementMarket(
                ticker=p["ticker"], asset=ev.asset, close_ts_ms=p["close_ts_ms"], index_id=ASSET_INDEX[ev.asset],
                strike=p["strike"], strike_source=p["strike_source"], open_ts_ms=p["open_ts_ms"])
        for a, eng in self.engines.items():
            if ev.asset not in (a, "*"):
                continue
            try:
                eng.ingest(ev)
            except CausalityError:                     # wall clock stepped backwards: restart the status engine
                self.engines[a] = FeatureEngine(a)
                self.manifest.notes.append(f"live feature engine {a} restarted after a clock step")
        if ev.event_type in (EventType.INDEX_VALUE, EventType.QUOTE, EventType.TRADE):
            self.last_values[f"{ev.source}:{ev.symbol}:{ev.event_type.value}"] = \
                ev.payload.get("value", ev.payload.get("mid", ev.payload.get("price")))

    # ---------------- periodic / end ----------------
    def tick(self, force=False):
        with self.lock:
            mono = self.clock.mono_ns()
            if not force and self._last_manifest_mono is not None and \
                    (mono - self._last_manifest_mono) / 1e9 < self.manifest_every_s:
                return
            self._last_manifest_mono = mono
            self.writer.flush()
            self._flush_settlement()
            self._update_manifest()
            self.manifest.write(self.dir)

    def _flush_settlement(self):
        if self.settlement_store is None or not self._settle_buf:
            return
        session = None
        if not self._settle_session_written:
            session = {"tool": "collect_market_data", "market_data_session": self.session_id, "synthetic": self.synthetic,
                       "app_version": APP_VERSION}
            self._settle_session_written = True
        self.settlement_store.append(self._settle_buf, session=session)
        self._settle_buf = []

    def _update_manifest(self):
        self.manifest.store = {"records": self.writer.records, "bytes_compressed": self.writer.bytes_compressed,
                               "flushes": self.writer.flushes, "counts": dict(self.counts),
                               "last_ingest_seq": self.seq.value}

    def close(self, status="COMPLETED", health=None):
        with self.lock:
            self.writer.flush()
            self._flush_settlement()
            self._update_manifest()
            if health:
                self.manifest.notes.append({"final_health": health})
            self.manifest.ended_utc = utc_iso(self.clock.wall_ms())
            self.manifest.status = status
            self.manifest.write(self.dir)
            self.writer.close()

    def status(self, health=None):
        with self.lock:
            now = self.clock.wall_ms()
            out = {"session_id": self.session_id, "now_utc": utc_iso(now), "research_only": True,
                   "counts": dict(self.counts), "store": {"records": self.writer.records,
                                                          "bytes_compressed": self.writer.bytes_compressed},
                   "sources": health or {}, "assets": {}}
            for a, eng in self.engines.items():
                m = self.current_market.get(a)
                try:
                    row = eng.features_at(max(now, eng.max_receive or now), m)
                except CausalityError:
                    continue
                pick = ("price.cf.last", "price.ref.last", "price.coinbase.mid", "price.kraken.mid", "vol.cf.rv.60s",
                        "vol.coinbase.rv.60s", "vol.ref.rv.5m", "cfspot.cf_minus_ref_bps", "xex.dispersion_bps",
                        "settle.accumulated_mean", "settle.coverage_elapsed", "settle.quality", "settle.seconds_remaining",
                        "kalshi.yes_mid", "strike.seconds_remaining")
                out["assets"][a] = {"market": m.ticker if m else None,
                                    "features": {k: {"value": row.values.get(k), "status": row.status[k].value}
                                                 for k in pick if k in row.status}}
            return out
