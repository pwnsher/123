"""
HISTORICAL CF Benchmarks observations: parser for a captured REST historical-values response
(CF Benchmarks directly, or the same payload through Kalshi's authenticated REST passthrough).

History has no local receive time: receive_ts_ms stays None and causal availability falls back to
event time + the reconstruction policy's explicit assumed_publication_lag_ms (flagged
RECEIVE_TIME_MISSING). seq records the element order in the response, for determinism only.

Step 6.3: decode the raw response with settlement.schemas.loads_exact (parse_cfb_historical_text does). A value
that already went through a binary float is rejected per element (VALUE_PRECISION_LOST), never accepted lossily.
"""
from settlement.assets import INDEX_ASSET
from settlement.cf_live import schema_id_for, value_gate, value_text
from settlement.schemas import (CFB_REST_ELEMENT, CFB_REST_HISTORICAL, loads_exact, parse_epoch_ms, parse_value,
                                validate)
from settlement.types import ParseIssue, SettlementObservation


def parse_cfb_historical(payload, index_id, source="cfb_rest_history"):
    """Returns ([SettlementObservation], [ParseIssue]). A top-level schema mismatch rejects the whole
    payload; a bad element rejects only that element (reported)."""
    chk = validate(payload, CFB_REST_HISTORICAL)
    if not chk.ok:
        return [], [ParseIssue(source, "SCHEMA_MISMATCH", "; ".join(chk.problems)[:300], index_id=index_id)]
    obs, issues = [], []
    for i, el in enumerate(payload["payload"]):
        e = validate(el, CFB_REST_ELEMENT)
        if not e.ok:
            issues.append(ParseIssue(source, "SCHEMA_MISMATCH", "; ".join(e.problems)[:200], index_id=index_id,
                                     location=f"payload[{i}]"))
            continue
        if e.extra:
            issues.append(ParseIssue(source, "SCHEMA_EXTRA_FIELDS", ",".join(e.extra)[:200], index_id=index_id,
                                     location=f"payload[{i}]"))
        ts, err = parse_epoch_ms(el["time"])
        if err:
            issues.append(ParseIssue(source, "INVALID_TIMESTAMP", err, index_id=index_id, location=f"payload[{i}]"))
            continue
        val, err = parse_value(el["value"])
        if err:
            issues.append(ParseIssue(source, "MALFORMED_VALUE", err, event_ts_ms=ts, index_id=index_id,
                                     location=f"payload[{i}]"))
            continue
        lost = value_gate(el["value"], source, event_ts_ms=ts, index_id=index_id, location=f"payload[{i}]")
        if lost is not None:
            issues.append(lost)
            continue
        obs.append(SettlementObservation(asset=INDEX_ASSET.get(index_id, "?"), index_id=index_id, source=source,
                                         value=val, event_ts_ms=ts, receive_ts_ms=None, seq=i,
                                         schema_id=schema_id_for(CFB_REST_HISTORICAL, el["value"]),
                                         schema_fingerprint=chk.fingerprint, value_text=value_text(el["value"])))
    return obs, issues


def parse_cfb_historical_text(text, index_id, source="cfb_rest_history"):
    """The RAW response text (str / bytes) -> parse_cfb_historical, decoded token-preserving (Step 6.3)."""
    try:
        payload = loads_exact(text)
    except ValueError as e:
        return [], [ParseIssue(source, "CORRUPT_RECORD", f"invalid JSON: {e}"[:200], index_id=index_id)]
    return parse_cfb_historical(payload, index_id, source)
