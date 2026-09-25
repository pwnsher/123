"""
JOINT research dataset: Step-3 (spot / CF / Kalshi / settlement state) + Step-4 (perp) features at the SAME
checkpoint T, under the SAME causal rule. RESEARCH ONLY.

    step3 = market_data.replay.load_sessions([session_dir])
    perp  = perp_data.replay.load_perp_sessions([session_dir])
    ds = build_research_dataset(step3, perp, ["BTC"])
    write_research_dataset(ds, "analysis_output/research_btc")      # features | labels | provenance, separate

ONE pass over the MERGED event stream in availability order (receive_ts, ingest_seq, family). At checkpoint T
(close - s for s in settlement.checkpoints.DEFAULT_CHECKPOINTS_S, per Kalshi market) - emitted immediately
before the first event received after T - the Step-3 engine and the perp engine have ingested exactly the
events with receive_ts <= T (and gaps known by T). There is no forward fill across the join: each side
reports its own statuses. Labels are computed separately from the final settlement reconstruction and the
official Kalshi result (never ingested by either engine). The single pass is tested equal to the two batch
paths (market_data compute_at + perp compute_at) at every checkpoint.
"""
import csv
import hashlib
import json
import os

from market_data.features.dataset import (_atomic_text, discover_markets, rows_digest, schedule)
from market_data.features.definitions import FEATURE_NAMES as S3_NAMES, FeatureConfig
from market_data.features.engine import FeatureEngine
from market_data.manifest import utc_iso
from market_data.types import EventType
from perp_data import APP_VERSION, PERP_FEATURE_SET_VERSION, PERP_SCHEMA_VERSION, RESEARCH_ONLY
from perp_data.features.definitions import FEATURE_NAMES as P4_NAMES, FEATURES as P4_FEATURES, PerpFeatureConfig, names_by_family
from perp_data.features.engine import PerpFeatureEngine, event_key
from perp_data.manifest import fingerprints
from perp_data.venues import PERP_VENUES
from settlement.checkpoints import DEFAULT_CHECKPOINTS_S, labels_for
from settlement.types import OfficialResolution, SettlementObservation

assert not set(S3_NAMES) & set(P4_NAMES), "Step-3 and Step-4 feature names must not collide"
KEY_COLS = ("market_ticker", "asset", "checkpoint_seconds_remaining", "checkpoint_ts_ms", "close_ts_ms", "strike")


class ResearchDataset:
    def __init__(self):
        self.rows = []
        self.labels = {}
        self.skipped = {}
        self.provenance = {}


def build_research_dataset(step3, perp, assets, checkpoints_s=DEFAULT_CHECKPOINTS_S, s3_config=None, p4_config=None,
                           venues=PERP_VENUES, include_step3=True):
    assets = list(assets)
    ds = ResearchDataset()
    s3_events = list(step3.events)
    p4_events = list(perp.events) if perp is not None else []
    events = sorted(s3_events + p4_events, key=event_key)
    gaps = sorted(list(step3.gaps) + (list(perp.gaps) if perp is not None else []),
                  key=lambda g: ((g.known_at_ms if g.known_at_ms is not None else g.end_ts_ms), g.ingest_seq or 0))
    markets = discover_markets(s3_events, assets)
    sched = schedule(markets, checkpoints_s)
    e3 = {a: FeatureEngine(a, s3_config or FeatureConfig()) for a in assets} if include_step3 else {}
    e4 = {a: PerpFeatureEngine(a, p4_config or PerpFeatureConfig(), venues) for a in assets}
    first_rx = events[0].receive_ts_ms if events else None
    last_rx = events[-1].receive_ts_ms if events else None
    state = {"ci": 0, "gi": 0}

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
            r4 = e4[asset].features_at(t, m)
            row = {"market_ticker": ticker, "asset": asset, "checkpoint_seconds_remaining": s, "checkpoint_ts_ms": t,
                   "close_ts_ms": m.close_ts_ms, "strike": m.strike,
                   "perp_values": r4.values, "perp_status": {k: v.value for k, v in r4.status.items()}}
            if include_step3:
                r3 = e3[asset].features_at(t, m)
                row["step3_values"], row["step3_status"] = r3.values, {k: v.value for k, v in r3.status.items()}
            ds.rows.append(row)

    for ev in events:
        emit_until(ev.receive_ts_ms)
        if ev.receive_ts_ms is not None:
            is_perp = getattr(ev, "family", None) == "perp"
            for eng in e4.values():
                eng.ingest(ev)
            if not is_perp:
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
    ds.provenance = _provenance(step3, perp, assets, checkpoints_s, s3_config, p4_config, venues, ds, include_step3)
    return ds


def _provenance(step3, perp, assets, checkpoints_s, s3_config, p4_config, venues, ds, include_step3):
    fam = names_by_family()
    return {
        "research_only": RESEARCH_ONLY,
        "generated_utc": utc_iso(),
        "app_version": APP_VERSION,
        "perp_feature_set_version": PERP_FEATURE_SET_VERSION,
        "perp_schema_version": PERP_SCHEMA_VERSION,
        "step3_sessions": list(step3.session_ids),
        "perp_sessions": list(perp.session_ids) if perp is not None else [],
        "perp_session_fingerprints": {(m or {}).get("session_id"): (m or {}).get("fingerprints")
                                      for m in (perp.manifests if perp is not None else [])},
        "perp_venues": list(venues),
        "assets": list(assets),
        "checkpoints_s": list(checkpoints_s),
        "step3_config": (s3_config or FeatureConfig()).to_dict(),
        "perp_config": (p4_config or PerpFeatureConfig()).to_dict(),
        "causal_rule": "an event is usable at T iff receive_ts_ms <= T; one pass over the merged stream in "
                       "(receive_ts, ingest_seq, family) order; no forward fill across the join",
        "fingerprints": fingerprints(),
        "perp_feature_definitions": [dict(f.__dict__) for f in P4_FEATURES],
        "perp_feature_families": fam,
        "perp_feature_names_sha256": hashlib.sha256("\n".join(P4_NAMES).encode()).hexdigest(),
        "step3_included": include_step3,
        "inputs": {"step3_events": len(step3.events), "perp_events": len(perp.events) if perp is not None else 0,
                   "gaps": len(step3.gaps) + (len(perp.gaps) if perp is not None else 0),
                   "perp_parse_failures": len(perp.failures) if perp is not None else 0,
                   "corrupt_records": len(step3.corrupt) + (len(perp.corrupt) if perp is not None else 0)},
        "rows": len(ds.rows), "markets_labelled": len(ds.labels), "skipped_checkpoints": dict(ds.skipped),
        "labels_are_separate": True,
        "feeds_existing_perp_veto": False,
    }


def write_research_dataset(ds, out_dir, fmt="csv"):
    os.makedirs(out_dir, exist_ok=True)
    s3 = S3_NAMES if (ds.rows and "step3_values" in ds.rows[0]) else ()
    feats = os.path.join(out_dir, f"features.{fmt}")
    if fmt == "csv":
        def w(f):
            cw = csv.writer(f, lineterminator="\n")
            names = list(s3) + list(P4_NAMES)
            cw.writerow(list(KEY_COLS) + names + [f"{n}__status" for n in names])
            cell = lambda x: "" if x is None else repr(x) if isinstance(x, float) else x  # noqa: E731
            for r in ds.rows:
                vals = [cell(r["step3_values"][n]) for n in s3] + [cell(r["perp_values"][n]) for n in P4_NAMES]
                sts = [r["step3_status"][n] for n in s3] + [r["perp_status"][n] for n in P4_NAMES]
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
    prov = dict(ds.provenance, features_file=os.path.basename(feats), rows_sha256=rows_digest(ds.rows))
    _atomic_text(os.path.join(out_dir, "provenance.json"), lambda f: json.dump(prov, f, indent=2, sort_keys=True))
    return feats
