#!/usr/bin/env python3
"""
Import CAPTURED settlement data into the offline settlement store (no network here).

    py scripts/settlement_import.py --kind kalshi-ws-jsonl   --input capture.jsonl
    py scripts/settlement_import.py --kind cf-ws-jsonl       --input capture.jsonl
    py scripts/settlement_import.py --kind cf-rest-json      --input brti_history.json --index-id BRTI
    py scripts/settlement_import.py --kind kalshi-markets-json --input settled_markets.json
    py scripts/settlement_import.py --kind perp-telemetry-csv --input kalshi_perp_telemetry.csv   (PROXY only)
    add --inspect to validate and print schema fingerprints without writing anything.

Websocket capture files are JSON lines. Preferred line format (receive time recorded at capture):
    {"receive_ts_ms": 1790000000123, "seq": 42, "message": <the raw message object or text>}
A bare message per line is accepted, but then receive time is unknown (RECEIVE_TIME_MISSING).
Markets: a GET /markets response ({"markets": [...]}), a list of market objects, or {"market": {...}}.
"""
import argparse
import hashlib
import json
import os
import sys

import _settlement_cli as cli  # noqa: F401  (path setup)
from settlement.cache import SettlementStore
from settlement.cf_history import parse_cfb_historical
from settlement.cf_live import parse_cfb_frame, parse_kalshi_cfb_message
from settlement.kalshi_markets import parse_market
from settlement.proxy import load_perp_reference_csv
from settlement.synthetic import as_synthetic

KINDS = ("kalshi-ws-jsonl", "cf-ws-jsonl", "cf-rest-json", "kalshi-markets-json", "perp-telemetry-csv")


def _sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _ws_lines(path):
    with open(path, encoding="utf-8") as f:
        for n, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                yield n, None, None, line          # corrupt: reported by the parser
                continue
            if isinstance(rec, dict) and "message" in rec and ("receive_ts_ms" in rec or "seq" in rec):
                yield n, rec.get("receive_ts_ms"), rec.get("seq", n), rec["message"]
            else:
                yield n, None, n, rec


def parse_file(kind, path, index_id=None):
    """Returns (records [(kind, obj)], issues)."""
    recs, issues = [], []
    if kind in ("kalshi-ws-jsonl", "cf-ws-jsonl"):
        for n, rts, seq, msg in _ws_lines(path):
            if kind == "kalshi-ws-jsonl":
                obs, avgs, iss = parse_kalshi_cfb_message(msg, rts, seq)
                recs += [("published_average", a) for a in avgs]
            else:
                if isinstance(msg, str):
                    try:
                        msg = json.loads(msg)
                    except ValueError:
                        msg = None
                obs, iss = parse_cfb_frame(msg if isinstance(msg, dict) else {}, rts, seq)
            if obs is not None:
                recs.append(("observation", obs))
            issues += iss
    elif kind == "cf-rest-json":
        if not index_id:
            raise SystemExit("--index-id is required for cf-rest-json (e.g. BRTI)")
        with open(path, encoding="utf-8") as f:
            obs, iss = parse_cfb_historical(json.load(f), index_id)
        recs += [("observation", o) for o in obs]
        issues += iss
    elif kind == "kalshi-markets-json":
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        items = doc.get("markets") if isinstance(doc, dict) and "markets" in doc else (doc if isinstance(doc, list) else [doc])
        for it in items:
            m, r, iss = parse_market(it)
            if m is not None:
                recs.append(("market", m))
            if r is not None:
                recs.append(("resolution", r))
            issues += iss
    elif kind == "perp-telemetry-csv":
        obs, iss = load_perp_reference_csv(path)
        recs += [("observation", o) for o in obs]
        issues += iss
    return recs, issues


def main(argv=None):
    ap = argparse.ArgumentParser(description="Import captured settlement data (offline).")
    ap.add_argument("--kind", required=True, choices=KINDS)
    ap.add_argument("--input", required=True)
    ap.add_argument("--store", default=cli.DEFAULT_STORE)
    ap.add_argument("--index-id", default=None)
    ap.add_argument("--inspect", action="store_true", help="validate + print fingerprints; write nothing")
    ap.add_argument("--tag-synthetic", action="store_true", help="re-tag observations as synthetic (demos/tests)")
    a = ap.parse_args(argv)
    recs, issues = parse_file(a.kind, a.input, a.index_id)
    if a.tag_synthetic:
        obs = as_synthetic([o for k, o in recs if k == "observation"])
        recs = [(k, o) for k, o in recs if k != "observation"] + [("observation", o) for o in obs]
    fps = sorted({getattr(o, "schema_fingerprint", "") for _k, o in recs if getattr(o, "schema_fingerprint", "")})
    counts = {}
    for k, _o in recs:
        counts[k] = counts.get(k, 0) + 1
    ikinds = {}
    for i in issues:
        ikinds[i.kind] = ikinds.get(i.kind, 0) + 1
    print(json.dumps({"input": os.path.basename(a.input), "kind": a.kind, "records": counts, "issues": ikinds,
                      "observed_schema_fingerprints": fps}, sort_keys=True))
    if a.inspect:
        for i in issues[:20]:
            print(f"  {i.kind}: {i.detail} ({i.location})")
        return 0
    session = {"tool": "settlement_import", "kind": a.kind, "input_file": os.path.basename(a.input),
               "input_sha256": _sha(a.input), "records": counts, "issue_counts": ikinds,
               "observed_schema_fingerprints": fps, "synthetic": bool(a.tag_synthetic)}
    n = SettlementStore(a.store).append(recs + [("issue", i) for i in issues], session=session)
    print(f"appended {n} lines to {a.store}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
