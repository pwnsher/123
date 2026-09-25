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
are YES cents rounded to 1e-6.
"""
import bisect
import zlib

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


def kraken_checksum(book, price_precision, qty_precision):
    """Kraken v2 book checksum: top 10 asks (low -> high) then top 10 bids (high -> low); each price and qty
    formatted with the pair's precision, '.' removed, leading zeros stripped, concatenated; CRC32 (unsigned)."""
    def fmt(x, prec):
        return f"{x:.{prec}f}".replace(".", "").lstrip("0")
    s = "".join(fmt(p, price_precision) + fmt(q, qty_precision) for p, q in book.top("ask", 10))
    s += "".join(fmt(p, price_precision) + fmt(q, qty_precision) for p, q in book.top("bid", 10))
    return zlib.crc32(s.encode()) & 0xFFFFFFFF
