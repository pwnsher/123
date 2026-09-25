"""
Kalshi binary-market price conventions — explicit units, no implicit mixing.

A Kalshi market has two complementary contracts, YES and NO, each paying 100 cents if its outcome occurs.
The public order book lists BIDS ONLY, per side:
    yes bids  [price_cents, qty]   people willing to BUY YES at that price
    no bids   [price_cents, qty]   people willing to BUY NO  at that price
A NO bid at p cents is economically a YES OFFER at (100 - p): buying NO at p == selling YES at 100 - p.
Therefore (all in CENTS, the unit named YES_CENTS / NO_CENTS below):
    YES ask = 100 - best NO bid          NO ask = 100 - best YES bid
    YES bid = best YES bid               NO bid = best NO bid
    YES spread = YES ask - YES bid = 100 - YES bid - NO bid = NO spread   (identical by construction)
    size available at YES ask = size of the best NO bid;  at NO ask = size of the best YES bid
Probability units are only produced by cents_to_prob (cents / 100). Dollar strings from the API ("0.4500") are
converted with dollars_to_cents. Helpers refuse values outside [0, 100] cents / [0, 1] probability.

This module never treats the Kalshi price as a calibrated probability: "implied" means only "the price scaled to
[0, 1]".
"""
from market_data.normalization import NormalizationError

CENTS_PER_DOLLAR = 100.0


def _cents(x, name):
    try:
        v = float(x)
    except (TypeError, ValueError):
        raise NormalizationError(f"{name} not numeric: {x!r}")
    if not (0.0 <= v <= 100.0) or v != v:
        raise NormalizationError(f"{name} outside [0, 100] cents: {v}")
    return v


def dollars_to_cents(d, name="price_dollars"):
    try:
        v = float(d) * CENTS_PER_DOLLAR
    except (TypeError, ValueError):
        raise NormalizationError(f"{name} not numeric: {d!r}")
    return round(_cents(v, name), 6)


def cents_to_prob(c, name="price_cents"):
    return _cents(c, name) / 100.0


def prob_to_cents(p, name="probability"):
    try:
        v = float(p)
    except (TypeError, ValueError):
        raise NormalizationError(f"{name} not numeric: {p!r}")
    if not (0.0 <= v <= 1.0) or v != v:
        raise NormalizationError(f"{name} outside [0, 1]: {v}")
    return v * 100.0


def yes_ask_from_no_bid(no_bid_cents):
    return None if no_bid_cents is None else round(100.0 - _cents(no_bid_cents, "no_bid_cents"), 6)


def no_ask_from_yes_bid(yes_bid_cents):
    return None if yes_bid_cents is None else round(100.0 - _cents(yes_bid_cents, "yes_bid_cents"), 6)


def no_bid_from_yes_ask(yes_ask_cents):
    return None if yes_ask_cents is None else round(100.0 - _cents(yes_ask_cents, "yes_ask_cents"), 6)


def yes_bid_from_no_ask(no_ask_cents):
    return None if no_ask_cents is None else round(100.0 - _cents(no_ask_cents, "no_ask_cents"), 6)


def native_to_yes_terms(side, price_cents):
    """A Kalshi book entry (side 'yes' | 'no', bid price in that side's cents) -> (book side, YES-cents price)."""
    s = str(side).lower()
    if s == "yes":
        return "bid", _cents(price_cents, "yes bid")
    if s == "no":
        return "ask", yes_ask_from_no_bid(price_cents)
    raise NormalizationError(f"unknown Kalshi book side {side!r}")


def executable_state(yes_book_bids, yes_book_asks):
    """Executable prices and sizes from a YES-terms book (bids / asks sorted best first, [cents, contracts]).
    Returns a dict of YES bid / ask, NO bid / ask (cents) and the size available at each, None when absent."""
    yb = yes_book_bids[0] if yes_book_bids else None
    ya = yes_book_asks[0] if yes_book_asks else None
    return {"yes_bid_cents": yb[0] if yb else None, "yes_bid_size": yb[1] if yb else None,
            "yes_ask_cents": ya[0] if ya else None, "yes_ask_size": ya[1] if ya else None,
            "no_bid_cents": no_bid_from_yes_ask(ya[0]) if ya else None, "no_bid_size": ya[1] if ya else None,
            "no_ask_cents": no_ask_from_yes_bid(yb[0]) if yb else None, "no_ask_size": yb[1] if yb else None}
