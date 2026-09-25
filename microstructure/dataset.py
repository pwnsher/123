"""
JOINT research dataset: Step 2 (settlement labels) + Step 3 (spot / CF / Kalshi) + Step 4 (perp) + Step 5
(microstructure) features at the SAME checkpoint T under the SAME causal rule. RESEARCH ONLY.

    step3 = market_data.replay.load_sessions([d]); perp = perp_data.replay.load_perp_sessions([d])
    micro = microstructure.replay.load_micro_sessions([d])
    ds = build_joint_dataset(step3, perp, micro, ["BTC"])
    write_joint_dataset(ds, "analysis_output/joint_btc")     # features.csv | labels.jsonl | micro_labels.jsonl | provenance

ONE pass over the merged stream in availability order (receive_ts, ingest_seq, family). At checkpoint T - emitted
immediately before the first event received after T - every engine has ingested exactly the events with
receive_ts <= T. No forward fill across the join; each family keeps its own statuses. LABELS are separate files:
    labels.jsonl        Step-2 settlement labels (final settlement reconstruction + official result)
    micro_labels.jsonl  POST-EVENT price-impact labels (forward mid moves after T; available_at_ms > T)
Neither is ever ingested by an engine. The single pass is tested equal to the batch paths at every checkpoint.
"""
import csv
import hashlib
import json

from market_data.features.dataset import _atomic_text, discover_markets, rows_digest, schedule
from market_data.features.definitions import FEATURE_NAMES as S3_NAMES, FeatureConfig
from market_data.features.engine import FeatureEngine
from market_data.manifest import utc_iso
from market_data.types import EventType
from microstructure import APP_VERSION, MICRO_FEATURE_SET_VERSION, MICRO_SCHEMA_VERSION, RESEARCH_ONLY
from microstructure.features.definitions import (FEATURE_NAMES as M5_NAMES, FEATURES as M5_FEATURES, MicroFeatureConfig,
                                                 counts_by_family, names_by_family)
from microstructure.features.engine import MicroFeatureEngine, event_key
from microstructure.labels import HORIZONS_MS, checkpoint_forward_labels
from microstructure.manifest import fingerprints
from perp_data.features.definitions import FEATURE_NAMES as P4_NAMES, PerpFeatureConfig
from perp_data.features.engine import PerpFeatureEngine
from perp_data.venues import PERP_VENUES
from settlement.checkpoints import DEFAULT_CHECKPOINTS_S, labels_for
from settlement.types import OfficialResolution, SettlementObservation

assert not (set(S3_NAMES) | set(P4_NAMES)) & set(M5_NAMES), "feature names must not collide across steps"
KEY_COLS = ("market_ticker", "asset", "checkpoint_seconds_remaining", "checkpoint_ts_ms", "close_ts_ms", "strike")


class JointDataset:
    def __init__(self):
        self.rows = []
        self.labels = {}
        self.micro_labels = []
        self.skipped = {}
        self.provenance = {}


def build_joint_dataset(step3, perp, micro, assets, checkpoints_s=DEFAULT_CHECKPOINTS_S, s3_config=None, p4_config=None,
                        m5_config=None, include_step3=True, include_perp=True, label_horizons_ms=HORIZONS_MS):
    assets = list(assets)
    ds = JointDataset()
    s3_events = list(step3.events)
    p4_events = list(perp.events) if (perp is not None and include_perp) else []
    m5_events = list(micro.events) if micro is not None else []
    events = sorted(s3_events + p4_events + m5_events, key=event_key)
    gaps = sorted(list(step3.gaps) + (list(perp.gaps) if (perp is not None and include_perp) else []),
                  key=lambda g: ((g.known_at_ms if g.known_at_ms is not None else g.end_ts_ms), g.ingest_seq or 0))
    markets = discover_markets(s3_events, assets)
    sched = schedule(markets, checkpoints_s)
    e3 = {a: FeatureEngine(a, s3_config or FeatureConfig()) for a in assets} if include_step3 else {}
    e4 = {a: PerpFeatureEngine(a, p4_config or PerpFeatureConfig(), PERP_VENUES) for a in assets} if include_perp else {}
    e5 = {a: MicroFeatureEngine(a, m5_config or MicroFeatureConfig()) for a in assets}
    first_rx = events[0].receive_ts_ms if events else None
    last_rx = events[-1].receive_ts_ms if events else None
    state = {"ci": 0, "gi": 0}
    emitted_t = {a: [] for a in assets}

    def emit_until(limit):
        while state["ci"] < len(sched) and (limit is None or sched[state["ci"]][0] < limit):
            t, asset, ticker, s = sched[state["ci"]]
            state["ci"] += 1
            while state["gi"] < len(gaps) and (gaps[state["gi"]].known_at_ms if gaps[state["gi"]].known_at_ms is not None
                                               else gaps[state["gi"]].end_ts_ms) <= t:
                for eng in list(e3.values()) + list(e4.values()):
                    eng.ingest_gap(gaps[state["gi"]])
                state["gi"] += 1
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
            r5 = e5[asset].features_at(t, m)
            row = {"market_ticker": ticker, "asset": asset, "checkpoint_seconds_remaining": s, "checkpoint_ts_ms": t,
                   "close_ts_ms": m.close_ts_ms, "strike": m.strike,
                   "micro_values": r5.values, "micro_status": {k: v.value for k, v in r5.status.items()}}
            if include_perp:
                r4 = e4[asset].features_at(t, m)
                row["perp_values"], row["perp_status"] = r4.values, {k: v.value for k, v in r4.status.items()}
            if include_step3:
                r3 = e3[asset].features_at(t, m)
                row["step3_values"], row["step3_status"] = r3.values, {k: v.value for k, v in r3.status.items()}
            ds.rows.append(row)
            emitted_t[asset].append((t, ticker))

    for ev in events:
        emit_until(ev.receive_ts_ms)
        fam = getattr(ev, "family", None)
        if fam == "micro":
            for eng in e5.values():
                eng.ingest(ev)
            continue
        for eng in e5.values():
            eng.ingest(ev)                       # Step-3 spot trades / Step-4 perp trades + contract values
        for eng in e4.values():
            eng.ingest(ev)
        if fam != "perp":
            for eng in e3.values():
                eng.ingest(ev)
    emit_until(None)

    obs_by_index, resolutions = {}, {}
    for ev in s3_events:
        if ev.event_type == EventType.INDEX_VALUE:
            obs_by_index.setdefault(ev.symbol, []).append(SettlementObservation.from_dict(ev.payload["observation"]))
        elif ev.event_type == EventType.RESOLUTION:
            p = ev.payload
            resolutions[p["ticker"]] = OfficialResolution(p["ticker"], p["result"], p["expiration_value"], "kalshi_market_api")
    for ticker, (m, _k) in markets.items():
        ds.labels[ticker] = labels_for(m, obs_by_index.get(m.index_id, []), resolutions.get(ticker))
    for a in assets:
        ts = [t for t, _ in emitted_t[a]]
        if ts:
            fl = checkpoint_forward_labels(s3_events + p4_events + m5_events, a, ts, label_horizons_ms)
            for (t, tk), lab in zip(emitted_t[a], fl):
                ds.micro_labels.append(dict(lab, market_ticker=tk))
    ds.provenance = _provenance(step3, perp, micro, assets, checkpoints_s, s3_config, p4_config, m5_config, ds,
                                include_step3, include_perp, label_horizons_ms)
    return ds


def _provenance(step3, perp, micro, assets, checkpoints_s, s3c, p4c, m5c, ds, inc3, inc4, horizons):
    return {
        "research_only": RESEARCH_ONLY,
        "generated_utc": utc_iso(),
        "app_version": APP_VERSION,
        "micro_feature_set_version": MICRO_FEATURE_SET_VERSION,
        "micro_schema_version": MICRO_SCHEMA_VERSION,
        "step3_sessions": list(step3.session_ids),
        "perp_sessions": list(perp.session_ids) if perp is not None else [],
        "micro_sessions": list(micro.session_ids) if micro is not None else [],
        "micro_session_fingerprints": {(m or {}).get("session_id"): (m or {}).get("fingerprints")
                                       for m in (micro.manifests if micro is not None else [])},
        "assets": list(assets),
        "checkpoints_s": list(checkpoints_s),
        "step3_config": (s3c or FeatureConfig()).to_dict(),
        "perp_config": (p4c or PerpFeatureConfig()).to_dict(),
        "micro_config": (m5c or MicroFeatureConfig()).to_dict(),
        "causal_rule": "an event is usable at T iff receive_ts_ms <= T; one pass over the merged Step-3/4/5 stream in "
                       "(receive_ts, ingest_seq, family) order; no forward fill across the join",
        "fingerprints": fingerprints(),
        "micro_feature_definitions": [dict(f.__dict__) for f in M5_FEATURES],
        "micro_feature_families": names_by_family(),
        "micro_feature_counts_by_family": counts_by_family(),
        "micro_feature_names_sha256": hashlib.sha256("\n".join(M5_NAMES).encode()).hexdigest(),
        "step3_included": inc3, "perp_included": inc4,
        "inputs": {"step3_events": len(step3.events), "perp_events": len(perp.events) if perp is not None else 0,
                   "micro_events": len(micro.events) if micro is not None else 0,
                   "micro_parse_failures": len(micro.failures) if micro is not None else 0,
                   "corrupt_records": len(step3.corrupt) + (len(perp.corrupt) if perp is not None else 0) +
                   (len(micro.corrupt) if micro is not None else 0)},
        "rows": len(ds.rows), "markets_labelled": len(ds.labels), "skipped_checkpoints": dict(ds.skipped),
        "labels_are_separate": True,
        "post_event_labels": {"file": "micro_labels.jsonl", "horizons_ms": list(horizons),
                              "note": "forward mid moves AFTER T - research labels, never features"},
        "feeds_production": False,
        "feeds_existing_perp_veto": False,
    }


def write_joint_dataset(ds, out_dir, fmt="csv"):
    import os
    os.makedirs(out_dir, exist_ok=True)
    s3 = S3_NAMES if (ds.rows and "step3_values" in ds.rows[0]) else ()
    p4 = P4_NAMES if (ds.rows and "perp_values" in ds.rows[0]) else ()
    feats = os.path.join(out_dir, f"features.{fmt}")
    if fmt == "csv":
        def w(f):
            cw = csv.writer(f, lineterminator="\n")
            names = list(s3) + list(p4) + list(M5_NAMES)
            cw.writerow(list(KEY_COLS) + names + [f"{n}__status" for n in names])
            cell = lambda x: "" if x is None else repr(x) if isinstance(x, float) else x  # noqa: E731
            for r in ds.rows:
                vals = [cell(r["step3_values"][n]) for n in s3] + [cell(r["perp_values"][n]) for n in p4] + \
                       [cell(r["micro_values"][n]) for n in M5_NAMES]
                sts = [r["step3_status"][n] for n in s3] + [r["perp_status"][n] for n in p4] + [r["micro_status"][n] for n in M5_NAMES]
                cw.writerow([r[k] for k in KEY_COLS] + vals + sts)
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
    _atomic_text(os.path.join(out_dir, "micro_labels.jsonl"),
                 lambda f: [f.write(json.dumps(lab, sort_keys=True, allow_nan=False) + "\n") for lab in ds.micro_labels])
    prov = dict(ds.provenance, features_file=os.path.basename(feats), rows_sha256=rows_digest(ds.rows))
    _atomic_text(os.path.join(out_dir, "provenance.json"), lambda f: json.dump(prov, f, indent=2, sort_keys=True))
    return feats
