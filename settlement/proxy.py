"""
PROXY source: the Kalshi perp `reference_price` already recorded by perp_telemetry.py
(kalshi_perp_telemetry.csv, column index_price).

It is described by the perps API as the CF Benchmarks index SCALED PER PERP CONTRACT, sampled by
the watcher about every 4 s. It is therefore NOT the RTI value Kalshi settles on, and the settlement
engine never samples it: observations carry source "kalshi_perp_reference_price" (policy.PROXY_SOURCES)
and an index id "PERP_REF:<COIN>" that can never equal a CF index id. It is loaded only so the overlap
tool can describe it next to real CF data. If it were the only data, quality would be PROXY_SOURCE.
"""
import csv

from settlement.schemas import PERP_TELEMETRY_CSV, parse_epoch_ms, parse_value, structure_fingerprint
from settlement.types import ParseIssue, SettlementObservation

SOURCE = "kalshi_perp_reference_price"


def proxy_index_id(coin):
    return f"PERP_REF:{coin}"


def load_perp_reference_csv(path):
    obs, issues = [], []
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        header = r.fieldnames or []
        missing = [fs.path for fs in PERP_TELEMETRY_CSV.fields if fs.required and fs.path not in header]
        fp = structure_fingerprint({h: "" for h in header})
        if missing:
            return [], [ParseIssue(SOURCE, "SCHEMA_MISMATCH", f"missing columns {missing}", location=path)]
        seen = set()
        for i, row in enumerate(r, start=2):
            if not row.get("index_price") or not row.get("index_ts_ms"):
                continue                                           # blank = not available (telemetry convention)
            try:
                ts_raw = int(row["index_ts_ms"])
            except ValueError:
                issues.append(ParseIssue(SOURCE, "INVALID_TIMESTAMP", row["index_ts_ms"][:40], location=f"line {i}"))
                continue
            ts, err = parse_epoch_ms(ts_raw)
            if err:
                issues.append(ParseIssue(SOURCE, "INVALID_TIMESTAMP", err, location=f"line {i}"))
                continue
            val, err = parse_value(row["index_price"])
            if err:
                issues.append(ParseIssue(SOURCE, "MALFORMED_VALUE", err, event_ts_ms=ts, location=f"line {i}"))
                continue
            key = (row["coin"], ts, val)
            if key in seen:
                continue                                           # the same snapshot joined to several rows
            seen.add(key)
            obs.append(SettlementObservation(asset=row["coin"], index_id=proxy_index_id(row["coin"]), source=SOURCE,
                                             value=val, event_ts_ms=ts, receive_ts_ms=None, seq=i,
                                             schema_id=f"{PERP_TELEMETRY_CSV.schema_id}@{PERP_TELEMETRY_CSV.version}",
                                             schema_fingerprint=fp))
    return obs, issues
