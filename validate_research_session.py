#!/usr/bin/env python3
"""
validate_research_session.py - Step-6 REAL-data quality validator for one captured research session. READ ONLY.

    py validate_research_session.py market_data_sessions/<session_id>
    py validate_research_session.py market_data_sessions/<session_id> --json analysis_output/quality_<id>.json

Verdict per session and per source/asset: PASS / DEGRADED / REJECT (+ SYNTHETIC_ONLY as the real-data verdict of a
synthetic session). Checks: raw files and segment checksums, receive ordering, gaps / reconnects / backfills, source
availability, event counts, duplicates, timestamp sanity and impossible future timestamps, asset / contract mapping,
book validity, settlement provenance, label completeness, label-to-feature leakage, and the empirical Coinbase
sequence semantics (UNVERIFIED_REAL_FEED until real evidence exists; fails closed on a contradiction).
Exit code 2 when the session is REJECTED, else 0.
"""
import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from feature_eval.coinbase_seq import analyse_sessions  # noqa: E402
from feature_eval.quality import validate_session       # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser(description="Validate one research session (read only).")
    ap.add_argument("session")
    ap.add_argument("--json", default=None, help="also write the full report here")
    ap.add_argument("--previous", default=None, help="an earlier JSON report of the same session (segment hashes)")
    a = ap.parse_args(argv)
    if not os.path.isdir(a.session):
        print(f"not a directory: {a.session}")
        return 2
    prev = None
    if a.previous:
        with open(a.previous, encoding="utf-8") as f:
            prev = json.load(f)
    cb = analyse_sessions([a.session])
    rep = validate_session(a.session, previous=prev, coinbase_status=cb["status"])
    rep["coinbase_sequence"] = cb
    print(f"session {rep['session_id']}: {rep['verdict']}  (real-data verdict {rep['real_data_verdict']}; "
          f"usable for research: {rep['usable_for_research']})")
    if rep.get("span"):
        print(f"  span {rep['span']['minutes']} min")
    for k, v in sorted(rep.get("sources", {}).items()):
        print(f"  {k:32s} {v['verdict']:9s} events={v['events']:<8d} {'; '.join(v['reasons'])}")
    for k, v in sorted(rep.get("checks", {}).get("books", {}).items()):
        print(f"  book {k:27s} {v['verdict']:9s} valid={v['valid_share']:.0%} {'; '.join(v['reasons'])}")
    lab = rep.get("checks", {}).get("labels", {})
    if lab:
        print(f"  labels: {lab.get('with_official_resolution')} / {lab.get('markets')} markets with an official result")
    print(f"  coinbase sequence semantics: {cb['status']}")
    for p in rep.get("problems", []):
        print(f"  problem: {p}")
    if a.json:
        os.makedirs(os.path.dirname(os.path.abspath(a.json)), exist_ok=True)
        with open(a.json, "w", encoding="utf-8") as f:
            json.dump(rep, f, indent=1, sort_keys=True, default=str)
    return 2 if rep["verdict"] == "REJECT" else 0


if __name__ == "__main__":
    sys.exit(main())
