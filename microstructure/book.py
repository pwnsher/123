"""
LocalBook: a deterministic PRICE-LEVEL order book (aggregate size per price; no order identities, so no queue
position). Bids and asks are dicts price -> size plus ascending price lists kept in sync with bisect, so the best
levels, the top N and the rank of a price are cheap.

    set_level(side, px, qty)   ABSOLUTE size (qty <= 0 deletes the level)          -> (old, new)
    add_level(side, px, dq)    RELATIVE size change (Kalshi); a negative result raises NegativeLevel
    load(bids, asks)           replace the whole book (snapshot)
    truncate(depth)            keep only the best `depth` levels per side (Kraken's documented rule)
    rank(side, px)             number of strictly better levels on that side (0 = best)

Prices are floats parsed from the venue's strings (the same string always gives the same float); Kalshi prices
are YES cents rounded to 1e-6. That float book serves the FEATURES only. A venue checksum (Kraken) is computed
from ExactBook, a parallel Decimal-only book fed with the exact wire decimals: no float ever reaches it.
"""
import bisect
import zlib
from decimal import ROUND_HALF_EVEN, Context, Decimal

EPS = 1e-12


class NegativeLevel(ValueError):
    pass


class LocalBook:
    __slots__ = ("bids", "asks", "bpx", "apx")

    def __init__(self):
        self.bids, self.asks = {}, {}
        self.bpx, self.apx = [], []                 # ascending price lists

    def clear(self):
        self.bids.clear(); self.asks.clear()
        self.bpx.clear(); self.apx.clear()

    def _side(self, side):
        return (self.bids, self.bpx) if side == "bid" else (self.asks, self.apx)

    def set_level(self, side, px, qty):
        d, lst = self._side(side)
        old = d.get(px, 0.0)
        if qty <= EPS:
            if px in d:
                del d[px]
                del lst[bisect.bisect_left(lst, px)]
            return old, 0.0
        if px not in d:
            bisect.insort(lst, px)
        d[px] = qty
        return old, qty

    def add_level(self, side, px, dq):
        d, _lst = self._side(side)
        old = d.get(px, 0.0)
        new = old + dq
        if new < -1e-9:
            raise NegativeLevel(f"{side} {px}: {old} + {dq} < 0")
        return old, self.set_level(side, px, new if new > EPS else 0.0)[1]

    def load(self, bids, asks):
        self.clear()
        for px, q in bids:
            if q > EPS:
                self.bids[px] = q
        for px, q in asks:
            if q > EPS:
                self.asks[px] = q
        self.bpx[:] = sorted(self.bids)
        self.apx[:] = sorted(self.asks)

    def truncate(self, depth):
        while len(self.bpx) > depth:
            del self.bids[self.bpx.pop(0)]
        while len(self.apx) > depth:
            del self.asks[self.apx.pop()]

    # ---------- reads ----------
    def best_bid(self):
        return (self.bpx[-1], self.bids[self.bpx[-1]]) if self.bpx else None

    def best_ask(self):
        return (self.apx[0], self.asks[self.apx[0]]) if self.apx else None

    def top(self, side, n):
        if side == "bid":
            return [(p, self.bids[p]) for p in reversed(self.bpx[-n:])] if n else []
        return [(p, self.asks[p]) for p in self.apx[:n]]

    def rank(self, side, px):
        if side == "bid":
            return len(self.bpx) - bisect.bisect_right(self.bpx, px)
        return bisect.bisect_left(self.apx, px)

    def crossed(self):
        return bool(self.bpx and self.apx and self.bpx[-1] >= self.apx[0])

    def levels(self):
        return len(self.bpx), len(self.apx)

    def copy_top(self, n):
        return self.top("bid", n), self.top("ask", n)


class ExactBook(LocalBook):
    """A Decimal-ONLY price-level book: the state a venue checksum is computed from (Kraken). Prices and sizes are
    the exact decimals received on the wire; a float is refused, so no binary-float round trip can reach the
    checksum. (The generic float LocalBook is kept beside it for the feature engine.)"""
    __slots__ = ()

    def set_level(self, side, px, qty):
        if type(px) is not Decimal or type(qty) is not Decimal:
            raise TypeError(f"ExactBook accepts Decimal only, got {type(px).__name__} / {type(qty).__name__}")
        return super().set_level(side, px, qty)

    def add_level(self, side, px, dq):
        raise TypeError("ExactBook holds ABSOLUTE sizes only")

    def load(self, bids, asks):
        for px, q in list(bids) + list(asks):
            if type(px) is not Decimal or type(q) is not Decimal:
                raise TypeError("ExactBook accepts Decimal only")
        super().load(bids, asks)


_CTX = Context(prec=80, rounding=ROUND_HALF_EVEN)


def exact_decimal(v, name="value"):
    """Wire value -> Decimal without any binary-float step. Accepts Decimal (json parse_float=Decimal), int and str;
    a float has already lost the wire digits and is refused."""
    if isinstance(v, bool) or v is None:
        raise TypeError(f"{name}: not a number: {v!r}")
    if isinstance(v, float):
        raise TypeError(f"{name}: float on the checksum path (decimal exactness lost): {v!r}")
    d = v if isinstance(v, Decimal) else Decimal(str(v) if isinstance(v, int) else v)
    if not d.is_finite() or d < 0:
        raise ValueError(f"{name}: not a finite non-negative decimal: {v!r}")
    return d


class ChecksumPrecisionError(ValueError):
    pass


def _fmt_exact(x, prec):
    """Exact decimal formatted with `prec` decimals, '.' removed, leading zeros stripped. Only zero PADDING is ever
    applied: a wire value with more decimals than the instrument precision raises (never rounded silently)."""
    q = x.quantize(Decimal(1).scaleb(-prec), context=_CTX)
    if q.compare(x, context=_CTX) != 0:
        raise ChecksumPrecisionError(f"{x} has more than {prec} decimals")
    return format(q, "f").replace(".", "").lstrip("0")


def kraken_checksum_string(book, price_precision, qty_precision):
    """Kraken v2 book checksum input: top 10 asks (low -> high) then top 10 bids (high -> low); every price and qty
    formatted with the pair's precision, '.' removed, leading zeros stripped, concatenated. `book` must be an
    ExactBook (Decimal state)."""
    if not isinstance(book, ExactBook):
        raise TypeError("the Kraken checksum is computed from the exact-decimal book only")
    s = "".join(_fmt_exact(p, price_precision) + _fmt_exact(q, qty_precision) for p, q in book.top("ask", 10))
    s += "".join(_fmt_exact(p, price_precision) + _fmt_exact(q, qty_precision) for p, q in book.top("bid", 10))
    return s


def kraken_checksum(book, price_precision, qty_precision):
    """CRC32 (unsigned 32-bit) of kraken_checksum_string."""
    return zlib.crc32(kraken_checksum_string(book, price_precision, qty_precision).encode()) & 0xFFFFFFFF
