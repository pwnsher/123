"""
LIVE CF Benchmarks observations: parsers for captured websocket messages (no network here).

Two live shapes are supported:
  * a raw CF Benchmarks websocket value frame   -> source "cfb_ws"
  * Kalshi's authenticated `cfbenchmarks_value` channel message, which wraps the raw upstream frame
    as a JSON string in msg.data and adds Kalshi-computed averages  -> source "cfb_ws_via_kalshi"
    The Kalshi averages are kept as PublishedAverage records (validation targets, never inputs).

The caller supplies receive_ts_ms (local receipt, UTC ms) and seq (arrival counter). The frame's
own `time` is the EVENT time; the two are never substituted for each other.
"""
import json
from dataclasses import dataclass, asdict
from typing import Optional

from settlement.assets import INDEX_ASSET
from settlement.schemas import (CFB_WS_VALUE, KALSHI_WS_CFB_VALUE, parse_epoch_ms, parse_value, validate)
from settlement.types import ParseIssue, SettlementObservation


@dataclass(frozen=True)
class PublishedAverage:
    """An average published by the data provider (e.g. Kalshi's final-minute average). VALIDATION ONLY."""
    index_id: str
    kind: str                         # "kalshi_avg_60s" | "kalshi_last_60s_windowed_average_15min"
    value: float
    event_ts_ms: int                  # time of the frame that carried it
    receive_ts_ms: Optional[int]
    source: str
    schema_fingerprint: str = ""

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


def parse_cfb_frame(frame, receive_ts_ms=None, seq=None, source="cfb_ws"):
    """One CF websocket value frame (dict). Returns (observation | None, [ParseIssue])."""
    chk = validate(frame, CFB_WS_VALUE)
    if not chk.ok:
        return None, [ParseIssue(source, "SCHEMA_MISMATCH", "; ".join(chk.problems)[:300],
                                 index_id=frame.get("id") if isinstance(frame, dict) and isinstance(frame.get("id"), str) else None,
                                 location=f"seq={seq}", receive_ts_ms=receive_ts_ms)]
    issues = []
    if chk.extra:
        issues.append(ParseIssue(source, "SCHEMA_EXTRA_FIELDS", ",".join(chk.extra)[:300], index_id=frame["id"],
                                 location=f"seq={seq}", receive_ts_ms=receive_ts_ms))
    ts, err = parse_epoch_ms(frame["time"])
    if err:
        return None, issues + [ParseIssue(source, "INVALID_TIMESTAMP", err, index_id=frame["id"], location=f"seq={seq}",
                                          receive_ts_ms=receive_ts_ms)]
    val, err = parse_value(frame["value"])
    if err:
        return None, issues + [ParseIssue(source, "MALFORMED_VALUE", err, event_ts_ms=ts, index_id=frame["id"],
                                          location=f"seq={seq}", receive_ts_ms=receive_ts_ms)]
    amend = None
    if frame.get("amendTime") is not None:
        amend, err = parse_epoch_ms(frame["amendTime"])
        if err:
            return None, issues + [ParseIssue(source, "INVALID_TIMESTAMP", "amendTime: " + err, event_ts_ms=ts,
                                              index_id=frame["id"], location=f"seq={seq}", receive_ts_ms=receive_ts_ms)]
    obs = SettlementObservation(
        asset=INDEX_ASSET.get(frame["id"], "?"), index_id=frame["id"], source=source, value=val, event_ts_ms=ts,
        receive_ts_ms=receive_ts_ms, amend_ts_ms=amend, seq=seq,
        repeat_of_previous=frame.get("repeatOfPreviousValue") if isinstance(frame.get("repeatOfPreviousValue"), bool) else None,
        schema_id=f"{CFB_WS_VALUE.schema_id}@{CFB_WS_VALUE.version}", schema_fingerprint=chk.fingerprint)
    return obs, issues


def _avg_value(x):
    if isinstance(x, dict):
        x = x.get("value")
    v, err = parse_value(x)
    return v if err is None else None


def parse_kalshi_cfb_message(message, receive_ts_ms=None, seq=None):
    """One Kalshi `cfbenchmarks_value` websocket message (dict or JSON text).
    Returns (observation | None, [PublishedAverage], [ParseIssue])."""
    src = "cfb_ws_via_kalshi"
    if isinstance(message, (str, bytes)):
        try:
            message = json.loads(message)
        except ValueError as e:
            return None, [], [ParseIssue(src, "CORRUPT_RECORD", f"invalid JSON: {e}"[:200], location=f"seq={seq}",
                                         receive_ts_ms=receive_ts_ms)]
    chk = validate(message, KALSHI_WS_CFB_VALUE)
    if not chk.ok:
        return None, [], [ParseIssue(src, "SCHEMA_MISMATCH", "; ".join(chk.problems)[:300], location=f"seq={seq}",
                                     receive_ts_ms=receive_ts_ms)]
    try:
        frame = json.loads(message["msg"]["data"])
    except ValueError as e:
        return None, [], [ParseIssue(src, "SCHEMA_MISMATCH", f"msg.data is not JSON: {e}"[:200], location=f"seq={seq}",
                                     receive_ts_ms=receive_ts_ms)]
    obs, issues = parse_cfb_frame(frame, receive_ts_ms, seq if seq is not None else message.get("seq"), source=src)
    avgs = []
    if obs is not None:
        msg = message["msg"]
        for key, kind in (("avg_60s_data", "kalshi_avg_60s"),
                          ("last_60s_windowed_average_15min", "kalshi_last_60s_windowed_average_15min")):
            v = _avg_value(msg.get(key))
            if v is not None:
                avgs.append(PublishedAverage(obs.index_id, kind, v, obs.event_ts_ms, receive_ts_ms, src, chk.fingerprint))
    return obs, avgs, issues
