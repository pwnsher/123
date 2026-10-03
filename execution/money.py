"""
Fixed-point money / price / quantity helpers (Decimal only) and the explicit UNKNOWN value.

* Prices are DOLLARS per contract in (0, 1] with at most 4 decimal places (Kalshi sub-penny steps).
* Contract quantities are non-negative with at most 2 decimal places (Kalshi fixed-point counts).
* A float is NEVER accepted as a money / price / quantity input (binary rounding bugs); text, int or Decimal only.
* UNKNOWN is a distinct value: it is never zero, never compares equal to a number and never takes part in arithmetic.
"""
from decimal import Decimal, InvalidOperation

PRICE_PLACES = 4
QTY_PLACES = 2
MONEY_PLACES = 6


class _Unknown:
    """The single UNKNOWN value (missing fee / mark / position): never zero, never arithmetic."""
    __slots__ = ()
    _inst = None

    def __new__(cls):
        if cls._inst is None:
            cls._inst = super().__new__(cls)
        return cls._inst

    def __repr__(self):
        return "UNKNOWN"

    def __bool__(self):
        raise TypeError("UNKNOWN has no truth value")

    def __eq__(self, other):
        return other is self

    def __hash__(self):
        return hash("execution.UNKNOWN")

    def _no(self, *_a):
        raise TypeError("UNKNOWN takes part in no arithmetic (it is never zero)")
    __add__ = __radd__ = __sub__ = __rsub__ = __mul__ = __rmul__ = __truediv__ = __rtruediv__ = _no
    __lt__ = __le__ = __gt__ = __ge__ = _no


UNKNOWN = _Unknown()


def is_unknown(v):
    return v is UNKNOWN


def dec(v, name="value"):
    """Exact Decimal from str / int / Decimal. Floats, bools, None and non-finite values are refused."""
    if isinstance(v, bool) or v is None or isinstance(v, float) or v is UNKNOWN:
        raise ValueError(f"{name}: {v!r} is not an exact decimal (floats / None / UNKNOWN refused)")
    if isinstance(v, Decimal):
        d = v
    elif isinstance(v, int):
        d = Decimal(v)
    elif isinstance(v, str):
        try:
            d = Decimal(v.strip())
        except InvalidOperation:
            raise ValueError(f"{name}: {v!r} is not a decimal")
    else:
        raise ValueError(f"{name}: unsupported type {type(v).__name__}")
    if not d.is_finite():
        raise ValueError(f"{name}: not finite")
    return d


def _places(d):
    e = d.normalize().as_tuple().exponent
    return max(0, -e) if isinstance(e, int) else 99


def price(v, name="price"):
    d = dec(v, name)
    if not (Decimal(0) < d <= Decimal(1)):
        raise ValueError(f"{name}: {d} outside (0, 1] dollars")
    if _places(d) > PRICE_PLACES:
        raise ValueError(f"{name}: {d} has more than {PRICE_PLACES} decimal places")
    return d


def qty(v, name="quantity", positive=False):
    d = dec(v, name)
    if d < 0 or (positive and d == 0):
        raise ValueError(f"{name}: {d} must be {'> 0' if positive else '>= 0'}")
    if _places(d) > QTY_PLACES:
        raise ValueError(f"{name}: {d} has more than {QTY_PLACES} decimal places")
    return d


def money_or_unknown(v, name="amount", allow_negative=True):
    """A money amount, or UNKNOWN. None is refused: a missing value must be stated as UNKNOWN explicitly."""
    if v is UNKNOWN:
        return UNKNOWN
    d = dec(v, name)
    if not allow_negative and d < 0:
        raise ValueError(f"{name}: {d} is negative")
    return d


def prob(v, name="probability"):
    d = dec(v, name)
    if not (Decimal(0) <= d <= Decimal(1)):
        raise ValueError(f"{name}: {d} outside [0, 1]")
    return d


def canon(d):
    """Deterministic, versioned textual form of a Decimal: no exponent, no trailing zeros, '-0' -> '0'."""
    if d is UNKNOWN:
        return "UNKNOWN"
    d = dec(d)
    if d == 0:
        return "0"
    t = format(d.normalize(), "f")
    if "." in t:
        t = t.rstrip("0").rstrip(".")
    return t


def jsonable(v):
    if v is UNKNOWN:
        return "UNKNOWN"
    if isinstance(v, Decimal):
        return canon(v)
    return v


def from_jsonable(v, kind="money"):
    """Inverse of jsonable for money-like fields ("UNKNOWN" -> UNKNOWN; text -> Decimal; None stays None)."""
    if v is None:
        return None
    if v == "UNKNOWN":
        return UNKNOWN
    return dec(v, kind)
