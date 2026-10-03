"""
Position / accounting ledger (exact Decimal; LEDGER_VERSION "ledger_v1").

One ExecutionLedger per execution (its filled exposure); market_position() aggregates them per
(asset, market_ticker, side). Rules:
    * fills are keyed by fill_id: a repeated notification with identical content is ignored (never double-counted);
      the same fill_id with DIFFERENT content raises FillConflict (contradictory evidence -> reconciliation);
    * fees are keyed by fee_id the same way; every fill expects a fee record - a fill without one, or a fee reported as
      UNKNOWN, makes fees_paid UNKNOWN (never zero);
    * a missing mark is UNKNOWN (never zero) -> unrealized PnL UNKNOWN;
    * average_entry_price = sum(qty * price) / sum(qty), exact (2 @ 0.40 + 3 @ 0.50 -> 0.46);
    * realized / unrealized PnL are reported as AUTHORITATIVE only when accounting is complete.
Settlement: a binary contract pays 1 per contract to the winning side, 0 otherwise. Only the FILLED quantity settles;
after settlement open_size is 0.
"""
from decimal import Decimal

from execution.money import UNKNOWN, dec, price as _price, qty as _qty

LEDGER_VERSION = "ledger_v1"
ZERO = Decimal(0)


class FillConflict(RuntimeError):
    """The same fill / fee id was reported twice with different content."""


class ExecutionLedger:
    def __init__(self, execution_key, market_ticker, asset, side, requested_size, max_limit_price):
        self.execution_key, self.market_ticker, self.asset, self.side = execution_key, market_ticker, asset, side
        self.requested_size = _qty(requested_size, "requested_size", positive=True)
        self.max_limit_price = _price(max_limit_price, "max_limit_price")
        self._fills = []                 # [(fill_id, qty, price)] in arrival order (each id at most once)
        self._fill_index = {}            # fill_id -> (qty, price)
        self._fees = []                  # [(fee_id, Decimal | UNKNOWN)]
        self._fee_index = {}             # fee_id -> amount
        self.fee_for_fill = {}           # fill_id -> fee_id
        self.mark_price = UNKNOWN
        self.settlement_status = "NONE"  # NONE | PENDING | SETTLED
        self.settlement_result = None    # "yes" | "no"
        self.settlement_price = None     # payout per contract (1 or 0)
        self.settlement_value = None     # the official expiration value, when given (reference only)

    # ---------------- inputs ----------------
    def add_fill(self, fill_id, qty, price):
        q, p = _qty(qty, "fill qty", positive=True), _price(price, "fill price")
        if fill_id in self._fill_index:
            if self._fill_index[fill_id] != (q, p):
                raise FillConflict(f"fill {fill_id} reported as {self._fill_index[fill_id]} and {(q, p)}")
            return "DUPLICATE"
        self._fill_index[fill_id] = (q, p)
        self._fills.append((fill_id, q, p))
        return "ADDED"

    def add_fee(self, fee_id, amount, fill_id=None):
        a = UNKNOWN if amount is UNKNOWN else dec(amount, "fee")
        if a is not UNKNOWN and a < 0:
            raise ValueError("a fee cannot be negative")
        if fee_id in self._fee_index:
            if self._fee_index[fee_id] != a:
                raise FillConflict(f"fee {fee_id} reported as {self._fee_index[fee_id]!r} and {a!r}")
            return "DUPLICATE"
        self._fee_index[fee_id] = a
        self._fees.append((fee_id, a))
        if fill_id is not None:
            self.fee_for_fill[fill_id] = fee_id
        return "ADDED"

    def set_mark(self, mark):
        self.mark_price = UNKNOWN if mark is UNKNOWN else _price(mark, "mark")

    def settle(self, result, settlement_value=None):
        if result not in ("yes", "no"):
            raise ValueError("settlement result must be 'yes' or 'no'")
        self.settlement_result = result
        self.settlement_price = Decimal(1) if (result == "yes") == (self.side == "YES") else ZERO
        self.settlement_value = settlement_value
        self.settlement_status = "SETTLED"

    # ---------------- derived ----------------
    def copy(self):
        c = ExecutionLedger(self.execution_key, self.market_ticker, self.asset, self.side, self.requested_size,
                            self.max_limit_price)
        c._fills, c._fill_index = list(self._fills), dict(self._fill_index)
        c._fees, c._fee_index, c.fee_for_fill = list(self._fees), dict(self._fee_index), dict(self.fee_for_fill)
        c.mark_price, c.settlement_status = self.mark_price, self.settlement_status
        c.settlement_result, c.settlement_price, c.settlement_value = (self.settlement_result, self.settlement_price,
                                                                       self.settlement_value)
        return c

    def fill_ids(self):
        return [fid for fid, _q, _p in self._fills]

    def fill_values(self):
        return [(q, p) for _fid, q, p in self._fills]

    @property
    def filled_size(self):
        return sum((q for _fid, q, _p in self._fills), ZERO)

    @property
    def remaining_size(self):
        return max(ZERO, self.requested_size - self.filled_size)

    @property
    def open_size(self):
        return ZERO if self.settlement_status == "SETTLED" else self.filled_size

    @property
    def entry_notional(self):
        return sum((q * p for _fid, q, p in self._fills), ZERO)

    @property
    def average_entry_price(self):
        f = self.filled_size
        return None if f == 0 else self.entry_notional / f

    @property
    def fees_known(self):
        if any(a is UNKNOWN for _fid, a in self._fees):
            return False
        return all(fid in self.fee_for_fill for fid, _q, _p in self._fills)

    @property
    def fees_paid(self):
        if not self.fees_known:
            return UNKNOWN
        return sum((a for _fid, a in self._fees), ZERO)

    @property
    def realized_pnl(self):
        if self.settlement_status != "SETTLED":
            return ZERO if self.filled_size == 0 else UNKNOWN
        fees = self.fees_paid
        if fees is UNKNOWN:
            return UNKNOWN
        return self.filled_size * self.settlement_price - self.entry_notional - fees

    @property
    def unrealized_pnl(self):
        if self.settlement_status == "SETTLED" or self.filled_size == 0:
            return ZERO
        if self.mark_price is UNKNOWN or not self.fees_known:
            return UNKNOWN
        return self.filled_size * self.mark_price - self.entry_notional - self.fees_paid

    @property
    def accounting_complete(self):
        if not self.fees_known:
            return False
        if self.filled_size == 0 or self.settlement_status == "SETTLED":
            return True
        return self.mark_price is not UNKNOWN

    def snapshot(self):
        return {"execution_key": self.execution_key, "market_ticker": self.market_ticker, "asset": self.asset,
                "side": self.side, "requested_size": self.requested_size, "filled_size": self.filled_size,
                "open_size": self.open_size, "average_entry_price": self.average_entry_price,
                "entry_notional": self.entry_notional, "fees_paid": self.fees_paid, "mark_price": self.mark_price,
                "realized_pnl": self.realized_pnl, "unrealized_pnl": self.unrealized_pnl,
                "settlement_status": self.settlement_status, "settlement_price": self.settlement_price,
                "settlement_result": self.settlement_result, "accounting_complete": self.accounting_complete,
                "pnl_authoritative": self.accounting_complete}


def market_position(ledgers, asset, market_ticker, side):
    """Open filled exposure of one (asset, market_ticker, side) across executions."""
    return sum((lg.open_size for lg in ledgers if lg.asset == asset and lg.market_ticker == market_ticker
                and lg.side == side), ZERO)
