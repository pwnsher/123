"""
Real-session DATA-QUALITY GATE (runs before any predictive analysis).

    report = validate_session(session_dir)      # PASS | DEGRADED | REJECT  (+ SYNTHETIC_ONLY for synthetic sessions)
    report["sources"]["coinbase:BTC"]["verdict"]

Session level and source/asset level. Checks:
    raw files readable        every segment of the Step-3 store, <session>/perp/ and <session>/micro/ decompresses;
                              CRC per line; corrupt lines anywhere but a truncated final member -> REJECT
    segment hashes            sha256 of every segment (the raw-store checksums the dataset fingerprint covers);
                              compared with a previous validation report when given (a changed file -> REJECT)
    monotonic receive order   receive_ts must not go backwards in arrival (ingest_seq) order
    feed gaps / reconnects    gap records (duration share per source / asset), reconnect rate
    backfills / duplicates    BACKFILLED events, duplicate market facts (dedup_key)
    source availability       share of 1-minute buckets with data per source / asset
    event counts              per source / event type
    timestamp sanity          venue event time more than future_tolerance_ms AFTER receipt (impossible), receive times
                              outside the session, non-epoch values
    asset / contract mapping  symbols map to the right asset for every venue; OKX contract values verified; Kalshi
                              tickers belong to their asset's series
    book validity             Step-5 books rebuilt with the frozen reconstructor: share of time READY, gaps, crossed
    settlement provenance     CF index observations present from TRUSTED settlement sources
    label completeness        markets discovered vs markets with an official Kalshi resolution
    label leakage             no label name is a feature name; engines never ingest RESOLUTION (structural check)
Rejected sessions / sources must not enter research datasets (feature_eval.dataset enforces it). Synthetic sessions
are SYNTHETIC_ONLY: never real evidence.
"""
import hashlib
import json
import os
from dataclasses import asdict, dataclass

from market_data.storage import read_session, segment_files

VERDICTS = ("PASS", "DEGRADED", "REJECT")
UNAVAILABLE = "UNAVAILABLE"      # Step 6.4: a source / book whose REQUIRED data was terminally access-denied (HTTP 403 /
                                 # 451). Excluded from research inputs exactly like REJECT (features MISSING, never zero);
                                 # it degrades the session like any non-PASS source, and it is never a data defect.
_RANK = {"PASS": 0, "DEGRADED": 1, "REJECT": 2}


@dataclass(frozen=True)
class QualityConfig:
    coverage_reject: float = 0.5
    coverage_degraded: float = 0.9
    gap_share_degraded: float = 0.1
    gap_share_reject: float = 0.5
    future_tolerance_ms: int = 2000
    future_share_degraded: float = 0.001
    future_share_reject: float = 0.01
    regress_share_reject: float = 0.01
    duplicate_share_degraded: float = 0.05
    reconnects_per_hour_degraded: float = 6.0
    book_ready_degraded: float = 0.9
    book_ready_reject: float = 0.5
    min_session_minutes: float = 15.0
    min_epoch_ms: int = 1_600_000_000_000
    max_epoch_ms: int = 4_102_444_800_000

    def to_dict(self):
        return asdict(self)


def worst(*vs):
    return max(vs, key=lambda v: _RANK[v]) if vs else "PASS"


def _sha_file(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def is_synthetic(session_dir):
    """Any synthetic marker makes the whole session SYNTHETIC_ONLY."""
    base = os.path.basename(os.path.normpath(session_dir))
    reasons = []
    if base.upper().startswith("SYNTHETIC"):
        reasons.append("session id")
    for rel in ("manifest.json", os.path.join("perp", "manifest.json"), os.path.join("micro", "manifest.json")):
        p = os.path.join(session_dir, rel)
        if not os.path.exists(p):
            continue
        try:
            with open(p, encoding="utf-8") as f:
                m = json.load(f)
        except (OSError, ValueError):
            continue
        if m.get("synthetic") is True or "_synthetic" in (m.get("sources") or {}):
            reasons.append(rel)
        if any("SYNTHETIC" in str(n) for n in (m.get("notes") or [])):
            reasons.append(f"{rel} notes")
        if str(m.get("session_id", "")).upper().startswith("SYNTHETIC"):
            reasons.append(f"{rel} session_id")
    return sorted(set(reasons))


def store_dirs(session_dir):
    out = {"step3": session_dir}
    for sub in ("perp", "micro"):
        d = os.path.join(session_dir, sub)
        if os.path.isdir(d):
            out[sub] = d
    return out


def raw_checksums(session_dir):
    """{store/segment: sha256} of every raw segment (+ manifests) - the raw-store checksums of a session."""
    out = {}
    for store, d in store_dirs(session_dir).items():
        for p in segment_files(d):
            out[f"{store}/{os.path.basename(p)}"] = _sha_file(p)
    return dict(sorted(out.items()))


def _src_key(src, asset):
    return f"{src}:{asset}"


def validate_session(session_dir, config=None, previous=None, coinbase_status=None):
    """previous: an earlier report of the same session (segment hashes are compared). coinbase_status: the verdict of
    feature_eval.coinbase_seq (Coinbase book data fails closed when it CONTRADICTS the current policy)."""
    from market_data.features.dataset import discover_markets
    from market_data.replay import load_sessions
    from market_data.types import EventType, IngestMode
    from microstructure.reconstruction import BookStatus
    from microstructure.replay import load_micro_sessions, rebuild_books
    from perp_data.replay import load_perp_sessions
    from perp_data.types import PerpEventType
    from settlement.assets import SERIES_ASSET
    from settlement.policy import TRUSTED_SOURCES
    cfg = config or QualityConfig()
    rep = {"session_dir": os.path.abspath(session_dir), "session_id": os.path.basename(os.path.normpath(session_dir)),
           "config": cfg.to_dict(), "checks": {}, "sources": {}, "problems": [], "verdict": "PASS"}
    syn = is_synthetic(session_dir)
    rep["synthetic"] = bool(syn)
    rep["synthetic_markers"] = syn
    if not os.path.exists(os.path.join(session_dir, "manifest.json")):
        rep["verdict"] = "REJECT"
        rep["problems"].append("no Step-3 manifest.json: provenance unknown")
        return _final(rep)
    # ---- raw files / segment hashes ----
    sums = raw_checksums(session_dir)
    rep["raw_checksums"] = sums
    rep["raw_store_sha256"] = hashlib.sha256(json.dumps(sums, sort_keys=True).encode()).hexdigest()
    if previous is not None:
        changed = sorted(k for k in set(sums) | set(previous.get("raw_checksums", {}))
                         if sums.get(k) != previous.get("raw_checksums", {}).get(k))
        rep["checks"]["segment_hashes_vs_previous"] = {"changed": changed}
        if changed:
            rep["problems"].append(f"segment files changed since the previous validation: {changed[:5]}")
            rep["verdict"] = "REJECT"
    corrupt = {}
    for store, d in store_dirs(session_dir).items():
        r = read_session(d)
        segs = r.segments
        bad = [c for c in r.corrupt if not (c[0] == (segs[-1] if segs else None) and "truncated" in c[2])]
        tail = [c for c in r.corrupt if c not in bad]
        corrupt[store] = {"segments": len(segs), "corrupt_lines": len(bad), "truncated_tail": len(tail),
                          "examples": [list(c) for c in bad[:3]]}
        if bad:
            rep["problems"].append(f"{store}: {len(bad)} corrupt records (CRC / JSON / member) - REJECT")
            rep["verdict"] = "REJECT"
        elif tail:
            rep["problems"].append(f"{store}: truncated final gzip member (crash during write) - DEGRADED")
            rep["verdict"] = worst(rep["verdict"], "DEGRADED")
    rep["checks"]["raw_files"] = corrupt
    s3 = load_sessions([session_dir])
    p4 = load_perp_sessions([session_dir]) if "perp" in store_dirs(session_dir) else None
    m5 = load_micro_sessions([session_dir]) if "micro" in store_dirs(session_dir) else None
    fams = [("step3", s3.events, s3.gaps), ("perp", p4.events if p4 else [], p4.gaps if p4 else []),
            ("micro", m5.events if m5 else [], m5.gaps if m5 else [])]
    all_ev = [e for _f, evs, _g in fams for e in evs]
    if not all_ev:
        rep["problems"].append("no events")
        rep["verdict"] = "REJECT"
        return _final(rep)
    t0 = min(e.receive_ts_ms for e in all_ev)
    t1 = max(e.receive_ts_ms for e in all_ev)
    span_min = (t1 - t0) / 60_000
    rep["span"] = {"first_receive_ms": t0, "last_receive_ms": t1, "minutes": round(span_min, 2)}
    if span_min < cfg.min_session_minutes:
        rep["problems"].append(f"session only {span_min:.1f} min (< {cfg.min_session_minutes}) - DEGRADED")
        rep["verdict"] = worst(rep["verdict"], "DEGRADED")
    # ---- per source / asset ----
    per = {}

    def acc(k):
        return per.setdefault(k, {"events": 0, "by_type": {}, "minutes": set(), "future": 0, "regress": 0,
                                  "bad_epoch": 0, "backfilled": 0, "dup": 0, "gap_ms": 0, "mapping_errors": 0})
    seen_keys = set()
    for fam, evs, gaps in fams:
        last_rx = {}
        for e in sorted(evs, key=lambda e: e.ingest_seq):
            a = acc(_src_key(e.source, e.asset))
            a["events"] += 1
            et = getattr(e.event_type, "value", str(e.event_type))
            a["by_type"][et] = a["by_type"].get(et, 0) + 1
            a["minutes"].add((e.receive_ts_ms - t0) // 60_000)
            if not (cfg.min_epoch_ms <= e.receive_ts_ms <= cfg.max_epoch_ms):
                a["bad_epoch"] += 1
            if e.event_ts_ms is not None and e.event_ts_ms - e.receive_ts_ms > cfg.future_tolerance_ms:
                a["future"] += 1
            prev = last_rx.get(e.source)
            if prev is not None and e.receive_ts_ms < prev:
                a["regress"] += 1
            last_rx[e.source] = max(prev or 0, e.receive_ts_ms)
            if getattr(e, "mode", None) == IngestMode.BACKFILLED:
                a["backfilled"] += 1
            dk = e.dedup_key()
            if dk is not None:
                if dk in seen_keys:
                    a["dup"] += 1
                seen_keys.add(dk)
            a["mapping_errors"] += _mapping_error(e, SERIES_ASSET)
        for g in gaps:
            acc(_src_key(g.source, g.asset))["gap_ms"] += max(0, g.duration_ms or 0)
    # reconnects (and, Step 6.4, terminally unavailable streams / books) from the manifests
    reconnects, unavailable = {}, {}
    for rel in ("manifest.json", os.path.join("perp", "manifest.json"), os.path.join("micro", "manifest.json")):
        p = os.path.join(session_dir, rel)
        if os.path.exists(p):
            try:
                with open(p, encoding="utf-8") as f:
                    man = json.load(f)
                for k, v in (man.get("reconnects") or {}).items():
                    reconnects[k] = reconnects.get(k, 0) + int(v)
                for k, v in (man.get("unavailable") or {}).items():
                    unavailable[k] = v
            except (OSError, ValueError):
                pass
    rep["checks"]["unavailable"] = {k: {"status": (v or {}).get("status"), "kind": (v or {}).get("kind"),
                                        "endpoint": (v or {}).get("endpoint")} for k, v in sorted(unavailable.items())}
    hours = max(span_min / 60.0, 1e-9)
    total_minutes = max(1, int(span_min) + 1)
    for k, a in sorted(per.items()):
        src, asset = k.split(":", 1)
        v, why = "PASS", []
        cov = len(a["minutes"]) / total_minutes if asset != "*" else None
        n = max(a["events"], 1)
        if cov is not None and cov < cfg.coverage_reject:
            v, why = worst(v, "REJECT"), why + [f"availability {cov:.0%} < {cfg.coverage_reject:.0%}"]
        elif cov is not None and cov < cfg.coverage_degraded:
            v, why = worst(v, "DEGRADED"), why + [f"availability {cov:.0%}"]
        gshare = a["gap_ms"] / max(1, t1 - t0)
        if gshare > cfg.gap_share_reject:
            v, why = worst(v, "REJECT"), why + [f"gaps {gshare:.0%} of the session"]
        elif gshare > cfg.gap_share_degraded:
            v, why = worst(v, "DEGRADED"), why + [f"gaps {gshare:.0%} of the session"]
        fshare = a["future"] / n
        if fshare > cfg.future_share_reject:
            v, why = worst(v, "REJECT"), why + [f"{a['future']} impossible future timestamps"]
        elif fshare > cfg.future_share_degraded:
            v, why = worst(v, "DEGRADED"), why + [f"{a['future']} future timestamps"]
        if a["bad_epoch"]:
            v, why = worst(v, "REJECT"), why + [f"{a['bad_epoch']} non-epoch receive times"]
        if a["regress"] / n > cfg.regress_share_reject:
            v, why = worst(v, "REJECT"), why + [f"{a['regress']} receive-order regressions"]
        elif a["regress"]:
            v, why = worst(v, "DEGRADED"), why + [f"{a['regress']} receive-order regressions (clock steps)"]
        if a["dup"] / n > cfg.duplicate_share_degraded:
            v, why = worst(v, "DEGRADED"), why + [f"{a['dup']} duplicate facts"]
        if a["mapping_errors"]:
            v, why = worst(v, "REJECT"), why + [f"{a['mapping_errors']} asset / contract mapping errors"]
        rc = reconnects.get(src, 0)
        if rc / hours > cfg.reconnects_per_hour_degraded:
            v, why = worst(v, "DEGRADED"), why + [f"{rc} reconnects ({rc / hours:.1f}/h)"]
        if k in unavailable:                     # required data terminally access-denied: UNAVAILABLE, not a defect
            v, why = UNAVAILABLE, [f"required endpoint access-denied (HTTP {unavailable[k].get('status')}): "
                                   "UNAVAILABLE for the session"]
        rep["sources"][k] = {"verdict": v, "reasons": why, "events": a["events"], "by_type": dict(sorted(a["by_type"].items())),
                             "availability": None if cov is None else round(cov, 4), "gap_share": round(gshare, 4),
                             "future_timestamps": a["future"], "receive_regressions": a["regress"],
                             "backfilled": a["backfilled"], "duplicates": a["dup"], "reconnects": rc,
                             "mapping_errors": a["mapping_errors"]}
    # ---- book validity (Step 5) ----
    books = {}
    if m5 is not None and m5.events:
        rc_ = rebuild_books(m5.events)
        for key, tr in sorted(rc_.tracks.items()):
            first = next((e.receive_ts_ms for e in m5.events if (e.source, e.payload.get("book")) == key), t0)
            span = max(1, t1 - first)
            ready = sum((min(hi if hi is not None else t1, t1) - max(lo, first)) for lo, hi in tr.intervals
                        if (hi if hi is not None else t1) > first)
            share = max(0.0, min(1.0, ready / span))
            v = "PASS" if share >= cfg.book_ready_degraded else ("DEGRADED" if share >= cfg.book_ready_reject else "REJECT")
            why = [] if v == "PASS" else [f"book valid {share:.0%} of its span"]
            if tr.base == BookStatus.UNAVAILABLE:
                v, why = UNAVAILABLE, ["required REST snapshot access-denied: book UNAVAILABLE for the session "
                                       "(no snapshot fabricated; features MISSING)"]
            if key[0] == "coinbase_l2" and coinbase_status in ("CONTRADICTS_CURRENT_POLICY",):
                v, why = "REJECT", why + ["Coinbase sequence semantics contradict the reconstruction policy: fail closed"]
            books[f"{key[0]}:{key[1]}"] = {"verdict": v, "reasons": why, "valid_share": round(share, 4),
                                           "resnapshots": tr.counts["resnapshots"], "gaps": tr.counts["gaps"],
                                           "invalid": tr.counts["invalid"], "crossed": tr.counts["crossed"],
                                           "checksum_ok": tr.counts["checksum_ok"], "checksum_fail": tr.counts["checksum_fail"]}
    rep["checks"]["books"] = books
    # ---- settlement provenance / labels ----
    cf = [e for e in s3.events if e.event_type == EventType.INDEX_VALUE]
    trusted = sum(1 for e in cf if ((e.payload.get("observation") or {}).get("source") in TRUSTED_SOURCES))
    markets = discover_markets(s3.events, sorted({e.asset for e in s3.events if e.asset in SERIES_ASSET.values()}))
    resolved = {e.payload.get("ticker") for e in s3.events if e.event_type == EventType.RESOLUTION}
    rep["checks"]["settlement_provenance"] = {"cf_observations": len(cf), "from_trusted_sources": trusted,
                                              "untrusted": len(cf) - trusted}
    if cf and trusted < len(cf):
        rep["problems"].append(f"{len(cf) - trusted} CF observations from untrusted sources (settlement features DEGRADED)")
        rep["verdict"] = worst(rep["verdict"], "DEGRADED")
    rep["checks"]["labels"] = {"markets": len(markets), "with_official_resolution": len(set(markets) & resolved),
                               "unresolved": sorted(set(markets) - resolved)[:20]}
    rep["checks"]["label_leakage"] = structural_leakage_check()
    if not rep["checks"]["label_leakage"]["ok"]:
        rep["verdict"] = "REJECT"
        rep["problems"].append("label names overlap feature names")
    # ---- OKX contract values ----
    if p4 is not None:
        inst = [e for e in p4.events if e.event_type == PerpEventType.INSTRUMENT and e.source == "okx_swap"]
        unver = [e.symbol for e in inst if not e.payload.get("verified")]
        rep["checks"]["contract_values"] = {"okx_instruments": len(inst), "unverified": sorted(set(unver))}
    # ---- session verdict ----
    srcv = [v["verdict"] for v in rep["sources"].values()] + [b["verdict"] for b in books.values()]
    if srcv and all(v == "REJECT" for v in srcv):
        rep["verdict"] = "REJECT"
        rep["problems"].append("every source rejected")
    elif any(v != "PASS" for v in srcv):
        rep["verdict"] = worst(rep["verdict"], "DEGRADED")
    return _final(rep)


def _mapping_error(e, series_asset):
    """1 when the symbol does not map to the event's asset for its venue (a mis-labelled stream)."""
    from market_data.sources.coinbase import SYMBOLS as CB
    from microstructure.venues import VENUES as MV
    from perp_data.venues import VENUES as PV
    src, sym, asset = e.source, str(e.symbol), e.asset
    if asset in ("*", None):
        return 0
    if src == "coinbase" and sym in CB.values():
        return int(CB.get(asset) != sym)
    if src in MV and MV[src].symbols and sym in MV[src].symbols.values():
        return int(MV[src].symbols.get(asset) != sym)
    if src in PV and PV[src].symbols and sym in PV[src].symbols.values():
        return int(PV[src].symbols.get(asset) != sym)
    if src in ("kalshi", "kalshi_ws") and sym.startswith("KX"):
        return int(series_asset.get(sym.split("-", 1)[0]) not in (asset, None))
    return 0


def structural_leakage_check():
    """No label field may be a feature name, and no engine ingests the official resolution."""
    from feature_eval.universe import build_universe
    from settlement.checkpoints import LABEL_FIELDS
    names = {r["name"] for r in build_universe()["records"]}
    bad = sorted(n for n in names if n in LABEL_FIELDS or n.startswith(("y_", "label.", "micro_label.")) or
                 n in ("official_result", "result", "expiration_value"))
    return {"ok": not bad, "overlaps": bad}


def _final(rep):
    if rep.get("synthetic"):
        rep["real_data_verdict"] = "SYNTHETIC_ONLY"
    else:
        rep["real_data_verdict"] = rep["verdict"]
    rep["usable_for_research"] = (not rep.get("synthetic")) and rep["verdict"] != "REJECT"
    return rep


def discover_sessions(root):
    """Session directories under root (each holds a Step-3 manifest.json)."""
    if os.path.exists(os.path.join(root, "manifest.json")):
        return [root]
    if not os.path.isdir(root):
        return []
    return sorted(os.path.join(root, n) for n in os.listdir(root)
                  if os.path.exists(os.path.join(root, n, "manifest.json")))
