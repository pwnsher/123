"""
Causal research FEATURE DATASET from captured sessions. RESEARCH_ONLY.

    RAW EVENTS -> NORMALIZATION -> EVENT STORE -> CAUSAL REPLAY -> FEATURE ENGINE -> FEATURE DATASET

build_dataset() makes ONE pass over the replayed events in availability order (receive_ts, ingest_seq)
with one incremental FeatureEngine per asset. Checkpoints are (market, close - s) for s in
settlement.checkpoints.DEFAULT_CHECKPOINTS_S. A checkpoint at T is emitted immediately BEFORE the first
event received after T, so the engine holds exactly the events with receive_ts <= T (and gaps known by
T). The batch path engine.compute_at() is tested equal to it at every checkpoint.

A checkpoint is skipped (and counted, with the reason) when
    market_unknown   the market's metadata (strike / close) had not been received by T
    before_data      T precedes the first captured event
    after_data       T is after the last captured event (nothing observed there)

LABELS are separate: computed after the pass from the final settlement reconstruction and the official
RESOLUTION events (settlement.checkpoints.labels_for). Feature rows never contain them and the engine
cannot ingest RESOLUTION events. Writers put features, labels and provenance in separate files.
"""
import csv
import hashlib
import json
import os
import tempfile

from market_data import APP_VERSION, FEATURE_SET_VERSION, MARKET_DATA_SCHEMA_VERSION, RESEARCH_ONLY
from market_data.features.definitions import FEATURE_NAMES, FEATURES, HORIZONS_MS, FeatureConfig
from market_data.features.engine import FeatureEngine
from market_data.manifest import fingerprints, utc_iso
from market_data.types import EventType
from settlement.assets import ASSET_INDEX
from settlement.checkpoints import DEFAULT_CHECKPOINTS_S, labels_for
from settlement.policy import reconstruction_policy, window_policy
from settlement.types import OfficialResolution, SettlementMarket, SettlementObservation

DEFAULT_SOURCES = ("cf", "coinbase", "kraken", "kalshi")


def availability_key(ev):
    return (ev.receive_ts_ms, ev.ingest_seq)


def market_from_state(ev):
    p = ev.payload
    return SettlementMarket(ticker=p["ticker"], asset=ev.asset, close_ts_ms=p["close_ts_ms"],
                            index_id=ASSET_INDEX[ev.asset], strike=p["strike"], strike_source=p["strike_source"] or "",
                            open_ts_ms=p["open_ts_ms"], series=p["ticker"].split("-")[0],
                            metadata_source="kalshi_market_api")


def discover_markets(events, assets):
    """ticker -> (market, known_at_ms = receive time of its first MARKET_STATE)."""
    out = {}
    for ev in sorted((e for e in events if e.event_type == EventType.MARKET_STATE and e.asset in assets),
                     key=availability_key):
        if ev.payload["ticker"] not in out:
            out[ev.payload["ticker"]] = (market_from_state(ev), ev.receive_ts_ms)
    return out


def schedule(markets, checkpoints_s=DEFAULT_CHECKPOINTS_S):
    rows = []
    for ticker, (m, known_at) in markets.items():
        for s in checkpoints_s:
            if s < 0:
                raise ValueError("checkpoints are seconds BEFORE close (>= 0)")
            rows.append((m.close_ts_ms - int(round(s * 1000)), m.asset, ticker, s))
    rows.sort()
    return rows


class Dataset:
    def __init__(self):
        self.rows = []            # dict(key..., values=..., status=...)
        self.labels = {}          # ticker -> label dict
        self.skipped = {}         # reason -> count
        self.provenance = {}


def build_dataset(loaded, assets, checkpoints_s=DEFAULT_CHECKPOINTS_S, config=None, sources_enabled=DEFAULT_SOURCES,
                  session_manifests=None):
    config = config or FeatureConfig()
    assets = list(assets)
    ds = Dataset()
    events = sorted(loaded.events, key=availability_key)
    gaps = sorted(loaded.gaps, key=lambda g: ((g.known_at_ms if g.known_at_ms is not None else g.end_ts_ms),
                                              g.ingest_seq or 0))
    markets = discover_markets(events, assets)
    sched = schedule(markets, checkpoints_s)
    engines = {a: FeatureEngine(a, config, sources_enabled) for a in assets}
    first_rx = events[0].receive_ts_ms if events else None
    last_rx = events[-1].receive_ts_ms if events else None
    gi = 0
    ci = 0

    def emit_until(limit_exclusive):
        nonlocal ci, gi
        while ci < len(sched) and (limit_exclusive is None or sched[ci][0] < limit_exclusive):
            t, asset, ticker, s = sched[ci]
            ci += 1
            while gi < len(gaps) and (gaps[gi].known_at_ms if gaps[gi].known_at_ms is not None else gaps[gi].end_ts_ms) <= t:
                for eng in engines.values():
                    eng.ingest_gap(gaps[gi])
                gi += 1
            m, known_at = markets[ticker]
            reason = None
            if first_rx is None or t < first_rx:
                reason = "before_data"
            elif t > last_rx:
                reason = "after_data"
            elif known_at > t:
                reason = "market_unknown"
            if reason:
                ds.skipped[reason] = ds.skipped.get(reason, 0) + 1
                continue
            row = engines[asset].features_at(t, m)
            ds.rows.append({"market_ticker": ticker, "asset": asset, "checkpoint_seconds_remaining": s,
                            "checkpoint_ts_ms": t, "close_ts_ms": m.close_ts_ms, "strike": m.strike,
                            "values": row.values, "status": {k: v.value for k, v in row.status.items()}})

    for ev in events:
        emit_until(ev.receive_ts_ms)                  # every checkpoint T < this receive time sees exactly <= T
        for eng in engines.values():
            eng.ingest(ev)
    emit_until(None)

    # ---------------- labels (separate; all data allowed) ----------------
    obs_by_index, resolutions = {}, {}
    for ev in events:
        if ev.event_type == EventType.INDEX_VALUE:
            obs_by_index.setdefault(ev.symbol, []).append(SettlementObservation.from_dict(ev.payload["observation"]))
        elif ev.event_type == EventType.RESOLUTION:
            p = ev.payload
            resolutions[p["ticker"]] = OfficialResolution(p["ticker"], p["result"], p["expiration_value"],
                                                          "kalshi_market_api")
    for ticker, (m, _k) in markets.items():
        ds.labels[ticker] = labels_for(m, obs_by_index.get(m.index_id, []), resolutions.get(ticker))
    ds.provenance = provenance(loaded, assets, checkpoints_s, config, sources_enabled, ds, session_manifests)
    return ds


def provenance(loaded, assets, checkpoints_s, config, sources_enabled, ds, session_manifests=None):
    wp, rp = window_policy(), reconstruction_policy()
    return {
        "research_only": RESEARCH_ONLY,
        "generated_utc": utc_iso(),
        "app_version": APP_VERSION,
        "feature_set_version": FEATURE_SET_VERSION,
        "market_data_schema_version": MARKET_DATA_SCHEMA_VERSION,
        "sessions": list(loaded.session_ids),
        "session_fingerprints": {m.get("session_id"): m.get("fingerprints") for m in (session_manifests or [])},
        "assets": list(assets),
        "sources_enabled": list(sources_enabled),
        "checkpoints_s": list(checkpoints_s),
        "windows_ms": dict(HORIZONS_MS),
        "config": config.to_dict(),
        "causal_rule": "event usable at T iff receive_ts_ms <= T; single pass in (receive_ts, ingest_seq) order",
        "settlement_window_policy": wp.policy_id,
        "settlement_window_policy_verified": bool(getattr(wp, "verified", False)),
        "settlement_reconstruction_policy": rp.policy_id,
        "fingerprints": fingerprints(),
        "feature_definitions": [dict(f.__dict__) for f in FEATURES],
        "feature_names_sha256": hashlib.sha256("\n".join(FEATURE_NAMES).encode()).hexdigest(),
        "inputs": {"events": len(loaded.events), "gaps": len(loaded.gaps), "parse_failures": len(loaded.failures),
                   "corrupt_records": len(loaded.corrupt), "invalid_records": len(loaded.invalid)},
        "rows": len(ds.rows),
        "markets_labelled": len(ds.labels),
        "skipped_checkpoints": dict(ds.skipped),
        "labels_are_separate": True,
    }


def rows_digest(rows):
    h = hashlib.sha256()
    for r in rows:
        h.update(json.dumps(r, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())
        h.update(b"\n")
    return h.hexdigest()


def _atomic_text(path, write_fn):
    d = os.path.dirname(os.path.abspath(path))
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            write_fn(f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


KEY_COLS = ("market_ticker", "asset", "checkpoint_seconds_remaining", "checkpoint_ts_ms", "close_ts_ms", "strike")


def write_dataset(ds, out_dir, fmt="csv"):
    """features.<csv|jsonl> (values + <name>__status mask), labels.jsonl, provenance.json — atomically."""
    os.makedirs(out_dir, exist_ok=True)
    feats = os.path.join(out_dir, f"features.{fmt}")
    if fmt == "csv":
        def w(f):
            cw = csv.writer(f, lineterminator="\n")
            cw.writerow(list(KEY_COLS) + list(FEATURE_NAMES) + [f"{n}__status" for n in FEATURE_NAMES])
            for r in ds.rows:
                cw.writerow([r[k] for k in KEY_COLS] +
                            ["" if r["values"][n] is None else repr(r["values"][n]) if isinstance(r["values"][n], float)
                             else r["values"][n] for n in FEATURE_NAMES] +
                            [r["status"][n] for n in FEATURE_NAMES])
    elif fmt == "jsonl":
        def w(f):
            for r in ds.rows:
                f.write(json.dumps(r, sort_keys=True, allow_nan=False) + "\n")
    else:
        raise ValueError("fmt must be csv or jsonl")
    _atomic_text(feats, w)
    _atomic_text(os.path.join(out_dir, "labels.jsonl"),
                 lambda f: [f.write(json.dumps({"market_ticker": t, **lab}, sort_keys=True, allow_nan=False) + "\n")
                            for t, lab in sorted(ds.labels.items())])
    prov = dict(ds.provenance, features_file=os.path.basename(feats), rows_sha256=rows_digest(ds.rows))
    _atomic_text(os.path.join(out_dir, "provenance.json"), lambda f: json.dump(prov, f, indent=2, sort_keys=True))
    return feats
