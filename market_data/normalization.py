"""
Deterministic normalization helpers shared by the source adapters.

* Prices must be finite and > 0; sizes finite and >= 0. Anything else is a parse failure, never a 0.
* Timestamps: ISO-8601 strings must carry a timezone (Z or offset); any number of fractional digits is
  accepted (Python 3.10 fromisoformat only accepts 3 or 6), and the result is integer UTC epoch ms.
  Numeric epochs must be plausible epoch MILLISECONDS (2009-2100), which catches seconds/µs mix-ups.
* Symbols: one explicit map per venue; unknown symbols are rejected, never guessed.
"""
import datetime as dt
import math
import re

from settlement.schemas import parse_epoch_ms

ASSETS = ("BTC", "ETH", "SOL", "XRP")
_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
_ISO = re.compile(r"^(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:?\d{2})$")


class NormalizationError(ValueError):
    pass


def price(v, name="price"):
    if isinstance(v, bool) or v is None or v == "":
        raise NormalizationError(f"{name} missing")
    try:
        x = float(v)
    except (TypeError, ValueError):
        raise NormalizationError(f"{name} not numeric: {str(v)[:30]!r}")
    if not math.isfinite(x) or x <= 0:
        raise NormalizationError(f"{name} not a positive finite number: {x}")
    return x


def size(v, name="size"):
    if isinstance(v, bool) or v is None or v == "":
        raise NormalizationError(f"{name} missing")
    try:
        x = float(v)
    except (TypeError, ValueError):
        raise NormalizationError(f"{name} not numeric: {str(v)[:30]!r}")
    if not math.isfinite(x) or x < 0:
        raise NormalizationError(f"{name} not a non-negative finite number: {x}")
    return x


def optional_price(v, name="price"):
    return None if v is None or v == "" else price(v, name)


def optional_size(v, name="size"):
    return None if v is None or v == "" else size(v, name)


def iso_ms(s, name="time"):
    """RFC3339 / ISO-8601 with an explicit zone -> UTC epoch ms (sub-ms digits truncated)."""
    if not isinstance(s, str):
        raise NormalizationError(f"{name} not a string")
    m = _ISO.match(s.strip())
    if not m:
        raise NormalizationError(f"{name} not an ISO-8601 time with a zone: {s[:40]!r}")
    base, frac, zone = m.groups()
    frac = (frac or "")[:6].ljust(6, "0")
    zone = "+00:00" if zone == "Z" else (zone if ":" in zone else zone[:3] + ":" + zone[3:])
    try:
        d = dt.datetime.fromisoformat(f"{base.replace(' ', 'T')}.{frac}{zone}")
    except ValueError as e:
        raise NormalizationError(f"{name}: {e}")
    delta = d - _EPOCH                                   # exact integer arithmetic (no float rounding)
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def epoch_ms(v, name="time"):
    x, err = parse_epoch_ms(v)
    if err:
        raise NormalizationError(f"{name}: {err}")
    return x


def mid(bid, ask):
    """Midpoint only when both sides exist and the book is not crossed; else None."""
    if bid is None or ask is None or ask < bid:
        return None
    return (bid + ask) / 2.0
