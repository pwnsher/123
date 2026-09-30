"""
Step-6 RESEARCH MATRIX: frozen Step 2-5 features + executable Kalshi state + legacy probability + gated labels, one
row per (market, frozen checkpoint), built only from sessions that passed the data-quality gate.

    m = build_matrix(session_dirs, assets)                 # -> ResearchMatrix (+ quality / gates / fingerprint)
    save_cache(m, cache_root); m2 = load_cache(path)       # immutable derived matrix; fails if raw files changed

Frozen checkpoint grid (seconds before close): CHECKPOINT_GRID_S = 600, 480, 360, 300, 240, 180, 120, 90, 60, 30.
(The Step-2 default grid also has 0 s; T = close is excluded: nothing can be executed at the close.)

Per session, ONE causal pass: microstructure.dataset.build_joint_dataset (Steps 2-5 engines, receive_ts <= T, the
single pass already tested equal to every batch path). Then, from the same session's events:
    legacy_p_up      the frozen production probability (feature_eval.legacy: the UNMODIFIED evaluate())
    execution state  the Kalshi book at T from the Step-5 websocket book when READY (full YES-bid / NO-bid ladders),
                     else the Step-3 polled depth-10 book (<= 5 s old); executable ask ladders are derived in YES / NO
                     terms (YES asks = 100 - NO bids). Top-of-book without sizes never produces an executable VWAP.
    labels           Step-2 labels_for + feature_eval.labels (OFFICIAL_RESULT / RECONSTRUCTED_VERIFIED /
                     SETTLEMENT_UNVERIFIED / LABEL_CONFLICT / UNLABELED)
Quality: REJECTED sessions never enter; features of REJECTED sources / assets / books are blanked with status
EXCLUDED_BY_QUALITY. Synthetic sessions build only a synthetic_only matrix; real and synthetic are never mixed.
"""
import bisect
import gzip
import hashlib
import json
import os

from feature_eval import FEATURE_UNIVERSE_VERSION, LABEL_VERSION, STEP6_SCHEMA_VERSION

CHECKPOINT_GRID_S = (600, 480, 360, 300, 240, 180, 120, 90, 60, 30)
LADDER_LEVELS = 10
REST_BOOK_MAX_AGE_MS = 5000
EXCLUDED = "EXCLUDED_BY_QUALITY"


class ResearchMatrix:
    def __init__(self, columns, rows, meta):
        self.columns = list(columns)
        self.col_index = {c: i for i, c in enumerate(self.columns)}
        self.rows = rows
        self.meta = meta

    @property
    def synthetic_only(self):
        return bool(self.meta.get("synthetic_only"))

    @property
    def fingerprint(self):
        return self.meta["dataset_fingerprint"]


def _sha(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def feature_sources(rec):
    """Session source names a feature depends on (for quality exclusion)."""
    s = rec["source"]
    if rec["layer"] in ("STEP2_SETTLEMENT",):
        return {"cf_via_kalshi", "cf_direct"}
    if rec["layer"] == "STEP3_SPOT":
        return {"cf": {"cf_via_kalshi", "cf_direct"}, "coinbase": {"coinbase"}, "kraken": {"kraken"}, "kalshi": {"kalshi"},
                "ref": {"coinbase", "kraken"}}.get(s, set())
    if rec["layer"] == "STEP4_PERP":
        return {s} if s != "*" else set()
    return {s} if s not in ("*",) else set()


def dataset_fingerprint(session_meta, universe_fp, settlement_status, checkpoint_grid, assets, source_exclusions, extra=None):
    """Deterministic: covers every session id and its raw-store checksums, the feature universe, the settlement
    convention status / label version, the versioned contract settlement rule set (Step 6.1), the checkpoint grid,
    source and asset inclusion and the date range."""
    from settlement.rules import RULE_SET_VERSION, rule_set_fingerprint
    body = {"schema": STEP6_SCHEMA_VERSION, "sessions": sorted(
                ({"session_id": s["session_id"], "raw_store_sha256": s["raw_store_sha256"]} for s in session_meta),
                key=lambda x: x["session_id"]),
            "feature_universe": universe_fp, "feature_universe_version": FEATURE_UNIVERSE_VERSION,
            "label_version": LABEL_VERSION, "settlement": settlement_status,
            "settlement_rule_set": {"version": RULE_SET_VERSION, "fingerprint": rule_set_fingerprint()},
            "checkpoint_grid_s": list(checkpoint_grid),
            "assets": sorted(assets), "source_exclusions": sorted(source_exclusions),
            "date_range": [min((s.get("first_close_ms") or 0) for s in session_meta) if session_meta else None,
                           max((s.get("last_close_ms") or 0) for s in session_meta) if session_meta else None],
            "extra": extra or {}}
    # Step 6.2: the per-market contract rule text hashes / statuses used for the labels (a changed snapshot changes
    # the dataset)
    body["market_rules"] = sorted([tk, p.get("rule_text_sha256"), p.get("rule_status"), p.get("rule_fingerprint")]
                                  for s in session_meta for tk, p in (s.get("rule_provenance") or {}).items())
    return _sha(body), body


def _ladder_from_book(bids, asks):
    """YES-terms book (bids = YES bids, asks = YES asks) -> executable ask ladders in each side's own cents."""
    yes_asks = sorted(([float(p), float(q)] for p, q in asks), key=lambda x: x[0])[:LADDER_LEVELS]
    no_asks = sorted(([round(100.0 - float(p), 6), float(q)] for p, q in bids), key=lambda x: x[0])[:LADDER_LEVELS]
    return {"yes_ask_ladder": yes_asks, "no_ask_ladder": no_asks,
            "yes_bid": max((float(p) for p, _ in bids), default=None), "yes_ask": yes_asks[0][0] if yes_asks else None}


def execution_states(m5_events, s3_events, checkpoints_by_ticker):
    """{(ticker, T): execution state} - Step-5 websocket book when READY at T, else the Step-3 polled depth-10 book."""
    from market_data.types import EventType, MarketEvent
    from microstructure.reconstruction import BookReconstructor, BookStatus
    out = {}
    want = sorted((t, tk) for tk, ts in checkpoints_by_ticker.items() for t in ts)
    if m5_events:
        evs = sorted((e for e in m5_events if e.source == "kalshi_ws"), key=lambda e: (e.receive_ts_ms, e.ingest_seq))
        rc = BookReconstructor(warmup_ms=0)
        i = 0
        for t, tk in want:
            while i < len(evs) and evs[i].receive_ts_ms <= t:
                rc.apply(evs[i])
                i += 1
            key = ("kalshi_ws", tk)
            if key in rc.tracks and rc.status(key, t) == BookStatus.READY:
                b = rc.tracks[key].book
                st = _ladder_from_book(b.top("bid", 50), b.top("ask", 50))
                st["source"] = "kalshi_ws_book"
                out[(tk, t)] = st
    books = {}
    for e in s3_events:
        if isinstance(e, MarketEvent) and e.source == "kalshi" and e.event_type == EventType.BOOK:
            books.setdefault(e.symbol, []).append(e)
    for tk in books:
        books[tk].sort(key=lambda e: (e.receive_ts_ms, e.ingest_seq))
    for t, tk in want:
        if (tk, t) in out or tk not in books:
            continue
        rx = [e.receive_ts_ms for e in books[tk]]
        j = bisect.bisect_right(rx, t) - 1
        if j >= 0 and t - rx[j] <= REST_BOOK_MAX_AGE_MS:
            p = books[tk][j].payload
            st = _ladder_from_book(p["bids"], p["asks"])
            st["source"] = "kalshi_rest_depth10"
            st["age_ms"] = t - rx[j]
            out[(tk, t)] = st
    return out


def _quotes_asof(s3_events):
    """Kalshi MARKET_STATE quotes per ticker (for the legacy evaluate() input)."""
    from market_data.types import EventType, MarketEvent
    q = {}
    for e in s3_events:
        if isinstance(e, MarketEvent) and e.event_type == EventType.MARKET_STATE:
            q.setdefault(e.symbol, []).append((e.receive_ts_ms, e.payload))
    for v in q.values():
        v.sort(key=lambda x: x[0])
    return q


def market_contract_snapshots(s3_events, rejected_sources):
    """{ticker: [contract snapshots]}, {ticker: [(receive_ts_ms, snapshot)]} from MARKET_STATE / RESOLUTION payloads of
    the Kalshi market API (Step 6.2). A REJECTED kalshi:<asset> pair contributes nothing."""
    from market_data.types import EventType
    snaps, fees = {}, {}
    for ev in s3_events:
        if ev.event_type not in (EventType.MARKET_STATE, EventType.RESOLUTION) or ev.source != "kalshi":
            continue
        if f"{ev.source}:{ev.asset}" in rejected_sources:
            continue
        c = (ev.payload or {}).get("contract")
        if not isinstance(c, dict):
            continue
        tk = ev.payload.get("ticker")
        snaps.setdefault(tk, []).append(c)
        fees.setdefault(tk, []).append((ev.receive_ts_ms, c))
    for v in fees.values():
        v.sort(key=lambda x: x[0])
    return snaps, fees


def fee_context_asof(captures, t_ms):
    """The newest captured fee metadata (market / event) RECEIVED at or before t_ms, or None."""
    last = None
    for rx, c in captures:
        if rx > t_ms:
            break
        last = c
    if last is None:
        return None
    return {"event_ticker": last.get("event_ticker"), "series_ticker": last.get("series_ticker"),
            "fee_metadata": dict(last.get("fee_metadata") or {}), "fee_metadata_state": last.get("fee_metadata_state"),
            "capture_ts_ms": last.get("capture_ts_ms")}


SETTLEMENT_OBSERVATION_SOURCES = ("cf_via_kalshi", "cf_direct")     # the CF RTI capture paths
RESOLUTION_SOURCES = ("kalshi",)                                     # the Kalshi market API (read-only GET)


def settlement_inputs(s3_events, rejected_sources):
    """Settlement observations and official resolutions that may build LABELS. A source/asset pair whose quality verdict
    is REJECT contributes nothing - not to the convention verification, the reconstructed values / outcomes, nor the
    official result labels - exactly as its feature columns are excluded. A session is not rejected for this: other,
    independent sources still contribute. Events from any other path (not a CF RTI capture / the Kalshi market API)
    are untrusted for labels. -> (observations by index id, {ticker: OfficialResolution}, exclusion report)"""
    from market_data.types import EventType
    from settlement.types import OfficialResolution, SettlementObservation
    obs_by_index, resolutions = {}, {}
    rep = {"index_values_used": 0, "index_values_rejected_source": 0, "index_values_untrusted_path": 0,
           "resolutions_used": 0, "resolutions_rejected_source": 0, "resolutions_untrusted_path": 0,
           "rejected_pairs": sorted(rejected_sources)}
    for ev in s3_events:
        key = f"{ev.source}:{ev.asset}"
        if ev.event_type == EventType.INDEX_VALUE:
            if ev.source not in SETTLEMENT_OBSERVATION_SOURCES:
                rep["index_values_untrusted_path"] += 1
            elif key in rejected_sources:
                rep["index_values_rejected_source"] += 1
            else:
                obs_by_index.setdefault(ev.symbol, []).append(SettlementObservation.from_dict(ev.payload["observation"]))
                rep["index_values_used"] += 1
        elif ev.event_type == EventType.RESOLUTION:
            if ev.source not in RESOLUTION_SOURCES:
                rep["resolutions_untrusted_path"] += 1
            elif key in rejected_sources:
                rep["resolutions_rejected_source"] += 1
            else:
                p = ev.payload
                resolutions[p["ticker"]] = OfficialResolution(p["ticker"], p["result"], p["expiration_value"],
                                                              "kalshi_market_api")
                rep["resolutions_used"] += 1
    return obs_by_index, resolutions, rep


def build_session_rows(session_dir, assets, columns, rec_by_name, quality, coinbase_status, gate, checkpoints_s,
                       include_perp=True, include_micro=True):
    from market_data.replay import load_sessions
    from microstructure.dataset import build_joint_dataset
    from microstructure.replay import load_micro_sessions
    from perp_data.replay import load_perp_sessions
    from settlement.checkpoints import labels_for
    from settlement.market_rules import with_snapshots
    from feature_eval.labels import market_label
    from feature_eval.legacy import LegacyEvaluator, tapes_from_events
    s3 = load_sessions([session_dir])
    p4 = load_perp_sessions([session_dir]) if (include_perp and os.path.isdir(os.path.join(session_dir, "perp"))) else None
    m5 = load_micro_sessions([session_dir]) if (include_micro and os.path.isdir(os.path.join(session_dir, "micro"))) else None
    empty = type("E", (), {"events": [], "gaps": [], "session_ids": [], "manifests": [], "failures": [], "corrupt": []})()
    ds = build_joint_dataset(s3, p4, m5 if m5 is not None else empty, assets, checkpoints_s,
                             include_perp=p4 is not None)
    # rejected sources / books -> excluded
    # Step 6.4: UNAVAILABLE (terminally access-denied) sources / books are excluded exactly like REJECT ones
    rejected = {k for k, v in quality.get("sources", {}).items() if v["verdict"] in ("REJECT", "UNAVAILABLE")}
    for bk, bv in quality.get("checks", {}).get("books", {}).items():
        if bv["verdict"] in ("REJECT", "UNAVAILABLE"):
            venue, sym = bk.split(":", 1)
            from microstructure.venues import VENUES
            asset = next((a for a, s in VENUES[venue].symbols.items() if s == sym), None) if venue in VENUES else None
            if asset:
                rejected.add(f"{venue}:{asset}")
    excl_cols = {}
    for a in assets:
        excl_cols[a] = [i for i, c in enumerate(columns)
                        if any(f"{s}:{a}" in rejected for s in feature_sources(rec_by_name[c]))]
    # labels: only settlement observations / resolutions from sources that passed quality (Step 6.1)
    obs_by_index, resolutions, label_exclusions = settlement_inputs(s3.events, rejected)
    from market_data.features.dataset import discover_markets
    markets = discover_markets(s3.events, assets)
    # Step 6.2: each market's own captured contract-rule snapshots (a rejected Kalshi source contributes none)
    snaps, fee_ctx = market_contract_snapshots(s3.events, rejected)
    markets = {tk: (with_snapshots(m, snaps.get(tk, [])), k) for tk, (m, k) in markets.items()}
    labels = {tk: labels_for(m, obs_by_index.get(m.index_id, []), resolutions.get(tk)) for tk, (m, _k) in markets.items()}
    rule_prov = {tk: {"rule_text_sha256": lb.get("settlement_rule_text_sha256"),
                      "rule_status": lb.get("settlement_rule_status"),
                      "rule_fingerprint": lb.get("settlement_rule_fingerprint")} for tk, lb in sorted(labels.items())}
    # execution + legacy
    cps = {}
    for r in ds.rows:
        cps.setdefault(r["market_ticker"], []).append(r["checkpoint_ts_ms"])
    exe = execution_states(m5.events if m5 is not None else [], s3.events, cps)
    quotes = _quotes_asof(s3.events)
    tapes = tapes_from_events(s3.events, assets)
    rows = []
    with LegacyEvaluator() as le:
        for r in ds.rows:
            tk, t, a = r["market_ticker"], r["checkpoint_ts_ms"], r["asset"]
            vals, sts = {}, {}
            for part in ("step3", "perp", "micro"):
                if f"{part}_values" in r:
                    vals.update(r[f"{part}_values"])
                    sts.update(r[f"{part}_status"])
            x = [vals.get(c) for c in columns]
            s = [sts.get(c, "UNAVAILABLE") for c in columns]
            for i in excl_cols[a]:
                x[i], s[i] = None, EXCLUDED
            q = None
            qs = quotes.get(tk, [])
            j = bisect.bisect_right([u[0] for u in qs], t) - 1
            if j >= 0:
                q = qs[j][1]
            p_up, info = le.p_up(a, t, tk, r["strike"], r["close_ts_ms"], q, tapes.get(a))
            y, src = market_label(labels.get(tk, {}), gate.get("status"))
            rows.append({"market_ticker": tk, "asset": a, "checkpoint_s": r["checkpoint_seconds_remaining"],
                         "checkpoint_ts_ms": t, "close_ts_ms": r["close_ts_ms"], "strike": r["strike"],
                         "session_id": quality.get("session_id"), "x": x, "st": s,
                         "legacy_p_up": p_up, "legacy_status": info.get("legacy_status"),
                         "execution": exe.get((tk, t)), "fee_context": fee_context_asof(fee_ctx.get(tk, []), t),
                         "y": y, "label_source": src,
                         "official_result": labels.get(tk, {}).get("official_result"),
                         "synthetic": bool(quality.get("synthetic"))})
    closes = [m.close_ts_ms for m, _ in markets.values()]
    meta = {"session_id": quality.get("session_id"), "rows": len(rows), "markets": len(markets),
            "first_close_ms": min(closes) if closes else None, "last_close_ms": max(closes) if closes else None,
            "rejected_sources": sorted(rejected), "raw_store_sha256": quality.get("raw_store_sha256"),
            "settlement_label_exclusions": label_exclusions, "rule_provenance": rule_prov,
            "synthetic": bool(quality.get("synthetic")), "quality_verdict": quality.get("verdict"),
            "settlement_inputs": {"markets": [m for m, _ in markets.values()], "resolutions": resolutions,
                                  "observations": [o for v in obs_by_index.values() for o in v]}}
    return rows, meta


def build_matrix(session_dirs, assets, checkpoints_s=CHECKPOINT_GRID_S, quality_reports=None, coinbase_status=None,
                 synthetic_mode=False, universe=None, include_perp=True, include_micro=True):
    """Validated sessions -> ResearchMatrix. Rejected sessions are skipped and listed; synthetic and real never mix."""
    from feature_eval.labels import convention_gate
    from feature_eval.quality import validate_session
    from feature_eval.universe import build_universe
    uni = universe or build_universe()
    recs = uni["records"]
    columns = [r["name"] for r in recs]
    rec_by_name = {r["name"]: r for r in recs}
    reports = dict(quality_reports or {})
    used, skipped = [], []
    for d in session_dirs:
        q = reports.get(d) or validate_session(d, coinbase_status=coinbase_status)
        reports[d] = q
        if q["verdict"] == "REJECT":
            skipped.append({"session": q["session_id"], "reason": "REJECT", "problems": q["problems"][:5]})
            continue
        if bool(q.get("synthetic")) != bool(synthetic_mode):
            skipped.append({"session": q["session_id"], "reason": "SYNTHETIC_SESSION_IN_REAL_MODE" if q.get("synthetic")
                            else "REAL_SESSION_IN_SYNTHETIC_MODE"})
            continue
        used.append(d)
    all_rows, metas = [], []
    set_inputs = {"markets": [], "resolutions": {}, "observations": []}
    gate_first = {"status": "SYNTHETIC_ONLY" if synthetic_mode else "SETTLEMENT_UNVERIFIED"}
    # the settlement gate needs every session's markets; build rows twice would be costly, so labels use a
    # provisional gate and are re-labelled below once the pooled gate is known
    for d in used:
        rows, meta = build_session_rows(d, assets, columns, rec_by_name, reports[d], coinbase_status, gate_first,
                                        checkpoints_s, include_perp, include_micro)
        si = meta.pop("settlement_inputs")
        set_inputs["markets"] += si["markets"]
        set_inputs["resolutions"].update(si["resolutions"])
        set_inputs["observations"] += si["observations"]
        all_rows += rows
        metas.append(meta)
    gate = convention_gate(set_inputs["markets"], set_inputs["resolutions"], set_inputs["observations"],
                           synthetic=synthetic_mode)
    for r in all_rows:
        if r["label_source"] in ("SETTLEMENT_UNVERIFIED",) and gate["status"] == "VERIFIED":
            r["label_source"] = "RECONSTRUCTED_VERIFIED"
    # de-duplicate markets captured by overlapping sessions (the first session in chronological order wins)
    seen, rows = set(), []
    for r in sorted(all_rows, key=lambda r: (r["close_ts_ms"], r["market_ticker"], -r["checkpoint_s"], r["session_id"] or "")):
        k = (r["market_ticker"], r["checkpoint_s"])
        if k in seen:
            continue
        seen.add(k)
        rows.append(r)
    excl = sorted({f"{m['session_id']}|{s}" for m in metas for s in m["rejected_sources"]})
    fp, body = dataset_fingerprint(metas, uni["fingerprint"], {"gate_status": gate["status"], "convention": gate.get("convention"),
                                                                "label_window_policy": gate["label_window_policy"]},
                                   checkpoints_s, assets, excl, {"include_perp": include_perp, "include_micro": include_micro,
                                                                  "synthetic_mode": synthetic_mode,
                                                                  "coinbase_sequence_status": coinbase_status})
    meta = {"dataset_fingerprint": fp, "fingerprint_body": body, "feature_universe_fingerprint": uni["fingerprint"],
            "sessions_used": [m["session_id"] for m in metas], "sessions_skipped": skipped, "session_meta": metas,
            "settlement_gate": gate, "coinbase_sequence_status": coinbase_status, "synthetic_only": bool(synthetic_mode),
            "checkpoint_grid_s": list(checkpoints_s), "assets": list(assets), "rows": len(rows),
            "quality_reports": {os.path.basename(os.path.normpath(k)): {"verdict": v["verdict"],
                                                                        "real_data_verdict": v["real_data_verdict"]}
                                for k, v in reports.items()}}
    return ResearchMatrix(columns, rows, meta)


# ---------------- immutable cache ----------------
def save_cache(matrix, cache_root):
    d = os.path.join(cache_root, matrix.fingerprint)
    os.makedirs(d, exist_ok=True)
    p = os.path.join(d, "matrix.jsonl.gz")
    if os.path.exists(p):
        return d                                         # immutable: an existing matrix is never overwritten
    tmp = p + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        f.write(json.dumps({"columns": matrix.columns}) + "\n")
        for r in matrix.rows:
            f.write(json.dumps(r, sort_keys=True, allow_nan=False) + "\n")
    os.replace(tmp, p)
    meta = dict(matrix.meta)
    meta["matrix_sha256"] = _file_sha(p)
    with open(os.path.join(d, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=1, sort_keys=True, default=str)
    return d


def _file_sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


class CacheInvalid(ValueError):
    pass


def load_cache(cache_dir, session_dirs=None):
    """Load a cached matrix; when session_dirs are given, every raw-store checksum must still match (else CacheInvalid)."""
    with open(os.path.join(cache_dir, "meta.json"), encoding="utf-8") as f:
        meta = json.load(f)
    p = os.path.join(cache_dir, "matrix.jsonl.gz")
    if _file_sha(p) != meta.get("matrix_sha256"):
        raise CacheInvalid("the cached matrix file changed after it was written")
    if session_dirs is not None:
        from feature_eval.quality import raw_checksums
        want = {s["session_id"]: s["raw_store_sha256"] for s in meta["session_meta"]}
        for d in session_dirs:
            sid = os.path.basename(os.path.normpath(d))
            if sid in want:
                got = hashlib.sha256(json.dumps(raw_checksums(d), sort_keys=True).encode()).hexdigest()
                if got != want[sid]:
                    raise CacheInvalid(f"raw store of {sid} changed since the matrix was built")
    with gzip.open(p, "rt", encoding="utf-8") as f:
        cols = json.loads(f.readline())["columns"]
        rows = [json.loads(ln) for ln in f if ln.strip()]
    return ResearchMatrix(cols, rows, meta)
