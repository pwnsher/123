"""
Expected schemas of every external settlement source, validation, and structure fingerprints.

Unknown or changed payloads are never silently reinterpreted:
    * a missing required field or a wrong type        -> SCHEMA_MISMATCH (record rejected, issue logged)
    * an unexpected extra field (strict specs only)   -> accepted, flagged SCHEMA_EXTRA_FIELDS, and the
                                                          new structure fingerprint is recorded
Every parsed record carries the fingerprint of the structure it came from, so a schema change is
visible in the cache and in every reconstruction's provenance.

VERIFICATION STATUS: the CF Benchmarks and Kalshi CF-feed specs below were written from the public
documentation descriptions available while building (the documentation pages themselves were not
reachable from the build environment). They are marked UNVERIFIED; the first real capture must be
checked with `py scripts/settlement_import.py --inspect <file>`, which prints the observed fingerprint.
The Kalshi market-object fields are the ones this repository already reads in production
(kalshi_dashboard.current_market / strike_of / book_of, label_binary_outcomes).
"""
import hashlib
import json
import math
from dataclasses import dataclass, field
from typing import Tuple

EPOCH_MS_MIN = 1_230_768_000_000      # 2009-01-01: anything earlier is a units error (seconds, not ms)
EPOCH_MS_MAX = 4_102_444_800_000      # 2100-01-01

VERIFIED = "VERIFIED_AGAINST_REPOSITORY_CODE"
UNVERIFIED = "UNVERIFIED_FROM_PUBLIC_DOC_DESCRIPTIONS"


def _jtype(v):
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, list):
        return "list"
    if isinstance(v, dict):
        return "dict"
    return type(v).__name__


def _paths(obj, prefix=""):
    out = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else str(k)
            out.add(f"{p}:{_jtype(v)}")
            out |= _paths(v, p)
    elif isinstance(obj, list):
        for v in obj[:50]:                        # element structure; bounded for huge arrays
            out.add(f"{prefix}[]:{_jtype(v)}")
            out |= _paths(v, prefix + "[]")
    return out


def structure_fingerprint(obj):
    """Order-independent fingerprint of a JSON structure (keys + value types, not values)."""
    return hashlib.sha256(json.dumps(sorted(_paths(obj)), separators=(",", ":")).encode()).hexdigest()[:16]


@dataclass(frozen=True)
class FieldSpec:
    path: str                    # dotted path; "[]" addresses list elements
    types: Tuple[str, ...]
    required: bool = True
    const: object = None


@dataclass(frozen=True)
class SchemaSpec:
    schema_id: str
    version: int
    fields: Tuple[FieldSpec, ...]
    timestamp_field: str
    value_field: str
    strict_extra: bool
    verification: str
    notes: str = ""

    def known_paths(self):
        out = set()
        for f in self.fields:
            parts = f.path.split(".")
            for i in range(1, len(parts) + 1):
                out.add(".".join(parts[:i]))
        return out


@dataclass(frozen=True)
class SchemaCheck:
    ok: bool
    schema_id: str
    fingerprint: str
    missing: Tuple[str, ...] = ()
    wrong_type: Tuple[str, ...] = ()
    extra: Tuple[str, ...] = ()
    problems: Tuple[str, ...] = field(default_factory=tuple)


def _get(obj, path):
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return False, None
        cur = cur[part]
    return True, cur


def validate(obj, spec):
    """Validate one payload object against a spec (list-element fields: see validate_list)."""
    fp = structure_fingerprint(obj)
    if not isinstance(obj, dict):
        return SchemaCheck(False, spec.schema_id, fp, problems=(f"expected an object, got {_jtype(obj)}",))
    missing, wrong, problems = [], [], []
    for f in spec.fields:
        if "[]" in f.path:
            continue
        present, v = _get(obj, f.path)
        if not present:
            if f.required:
                missing.append(f.path)
            continue
        if _jtype(v) not in f.types:
            wrong.append(f"{f.path}:{_jtype(v)}")
        elif f.const is not None and v != f.const:
            wrong.append(f"{f.path}=={v!r} (expected {f.const!r})")
    known = spec.known_paths()
    extra = sorted(k for k in obj if isinstance(k, str) and k not in known)
    problems += [f"missing {m}" for m in missing] + [f"type {w}" for w in wrong]
    return SchemaCheck(not missing and not wrong, spec.schema_id, fp, tuple(missing), tuple(wrong),
                       tuple(extra) if spec.strict_extra else (), tuple(problems))


# ───────────────────────── specs ─────────────────────────
NUM = ("int", "float", "str")          # numeric values may arrive as JSON numbers or decimal strings
INT_TS = ("int",)

CFB_WS_VALUE = SchemaSpec(
    "cfb_ws_value", 1,
    (FieldSpec("type", ("str",), True, "value"), FieldSpec("id", ("str",)), FieldSpec("value", NUM),
     FieldSpec("time", INT_TS), FieldSpec("amendTime", ("int", "null"), False),
     FieldSpec("repeatOfPreviousValue", ("bool",), False)),
    timestamp_field="time", value_field="value", strict_extra=True, verification=UNVERIFIED,
    notes="CF Benchmarks websocket 'value' frame: {type:'value', id, value, time(ms), amendTime?, "
          "repeatOfPreviousValue?}. Also the raw upstream frame inside Kalshi's cfbenchmarks_value channel.")

KALSHI_WS_CFB_VALUE = SchemaSpec(
    "kalshi_ws_cfbenchmarks_value", 1,
    (FieldSpec("type", ("str",)), FieldSpec("msg", ("dict",)), FieldSpec("msg.data", ("str",)),
     FieldSpec("sid", ("int",), False), FieldSpec("seq", ("int",), False),
     FieldSpec("msg.avg_60s_data", ("dict", "null"), False),
     FieldSpec("msg.avg_60s_data.value", NUM + ("null",), False),
     FieldSpec("msg.last_60s_windowed_average_15min", ("dict", "str", "int", "float", "null"), False)),
    timestamp_field="msg.data->time", value_field="msg.data->value", strict_extra=False, verification=UNVERIFIED,
    notes="Kalshi authenticated websocket channel carrying the raw upstream CF frame as a JSON string in "
          "msg.data plus Kalshi-computed trailing 60-s and quarter-hour final-minute averages.")

CFB_REST_HISTORICAL = SchemaSpec(
    "cfb_rest_historical_values", 1,
    (FieldSpec("payload", ("list",)), FieldSpec("payload[].value", NUM), FieldSpec("payload[].time", INT_TS)),
    timestamp_field="payload[].time", value_field="payload[].value", strict_extra=False, verification=UNVERIFIED,
    notes="CF Benchmarks REST historical values (directly or via Kalshi's REST passthrough): "
          "{payload:[{value, time(ms)}, ...]} sorted by time ascending.")

CFB_REST_ELEMENT = SchemaSpec(
    "cfb_rest_historical_values.element", 1,
    (FieldSpec("value", NUM), FieldSpec("time", INT_TS)),
    timestamp_field="time", value_field="value", strict_extra=True, verification=UNVERIFIED)

KALSHI_MARKET = SchemaSpec(
    "kalshi_market", 2,
    (FieldSpec("ticker", ("str",)), FieldSpec("close_time", ("str",)),
     FieldSpec("open_time", ("str",), False), FieldSpec("floor_strike", ("int", "float", "null"), False),
     FieldSpec("cap_strike", ("int", "float", "null"), False), FieldSpec("strike", ("int", "float", "null"), False),
     FieldSpec("result", ("str", "null"), False), FieldSpec("expiration_value", NUM + ("null",), False),
     FieldSpec("status", ("str",), False), FieldSpec("event_ticker", ("str",), False),
     FieldSpec("rules_primary", ("str",), False)),
    timestamp_field="close_time", value_field="expiration_value", strict_extra=False, verification=VERIFIED,
    notes="Kalshi GET /markets and /markets/{ticker} market object. ticker, close_time, floor_strike|cap_strike|"
          "strike and result are read by the existing code; expiration_value is the settled index value.")

PERP_TELEMETRY_CSV = SchemaSpec(
    "kalshi_perp_telemetry_csv", 3,
    (FieldSpec("coin", ("str",)), FieldSpec("index_price", ("str",)), FieldSpec("index_ts_ms", ("str",)),
     FieldSpec("index_source", ("str",)), FieldSpec("perp_contract_size", ("str",), False),
     FieldSpec("perp_underlying_multiplier", ("str",), False)),
    timestamp_field="index_ts_ms", value_field="index_price", strict_extra=False, verification=VERIFIED,
    notes="PROXY ONLY: Kalshi perp reference_price, 'CF Benchmarks index SCALED PER CONTRACT' (perp_telemetry.py).")

SPECS = {s.schema_id: s for s in (CFB_WS_VALUE, KALSHI_WS_CFB_VALUE, CFB_REST_HISTORICAL, CFB_REST_ELEMENT,
                                  KALSHI_MARKET, PERP_TELEMETRY_CSV)}


# ───────────────────────── value helpers ─────────────────────────
def parse_value(v):
    """Finite positive float from a JSON number or decimal string; else (None, reason)."""
    if isinstance(v, bool) or v is None:
        return None, "value missing or boolean"
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None, f"non-numeric value {str(v)[:40]!r}"
    if not math.isfinite(x):
        return None, "non-finite value"
    if x <= 0:
        return None, "non-positive value"
    return x, None


def parse_epoch_ms(v):
    """Integer epoch milliseconds within [2009, 2100); rejects seconds-scale and fractional values."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None, f"timestamp not a number: {str(v)[:40]!r}"
    if isinstance(v, float) and (not math.isfinite(v) or v != int(v)):
        return None, "timestamp not an integer millisecond"
    x = int(v)
    if not EPOCH_MS_MIN <= x < EPOCH_MS_MAX:
        return None, f"timestamp {x} outside plausible epoch-ms range (seconds instead of ms?)"
    return x, None


def parse_iso_utc_ms(s):
    """ISO-8601 with an explicit offset or Z -> epoch ms. Naive times are REJECTED (no guessing)."""
    import datetime as dt
    if not isinstance(s, str) or not s:
        return None, "missing timestamp"
    try:
        d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None, f"unparseable timestamp {s[:40]!r}"
    if d.tzinfo is None:
        return None, f"timestamp without timezone {s[:40]!r}"
    return int(round(d.timestamp() * 1000)), None


def iso_utc(ms):
    import datetime as dt
    return dt.datetime.fromtimestamp(ms / 1000.0, dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
