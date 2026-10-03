"""
PaperExecutionAdapter + PaperVenue: a deterministic, scripted, IN-PROCESS simulation of an exchange. No network, no
real order. It implements the same ExecutionAdapter interface the eventual live adapter will implement.

PaperVenue is the simulated "outside world": it survives an engine / adapter restart (keep the object, or save() /
load() it as JSON for a cross-process demonstration). Tests script it exactly:

    submit behaviour queue   venue.script_submit("ACCEPT" | "REJECT" | "LOST_ACK" | "TIMEOUT_NOT_RECEIVED" |
                                                 "CRASH_AFTER_ACCEPT")
                             LOST_ACK             the venue ACCEPTS the order, the response is lost (ambiguous)
                             TIMEOUT_NOT_RECEIVED the venue never received it, the caller sees a timeout (ambiguous)
                             CRASH_AFTER_ACCEPT   the venue accepts, then the calling process dies (SimulatedCrash)
    cancel behaviour queue   venue.script_cancel("OK" | "LOST_ACK" | "AMBIGUOUS_NOT_DONE" | "FILL_THEN_CANCEL")
    fills                    venue.fill(coid, qty, price, fee=...)   (partial / multiple / delayed: call it as needed)
                             venue.duplicate_last_fill(coid)          (a repeated fill notification)
    venue-side endings       venue.venue_cancel(coid) / venue.venue_expire(coid)
    queries                  venue.fail_queries(n) (adapter timeout), venue.position_mode = "LIVE" | "STALE" |
                             "FAIL" | ("CONFLICT", side, qty), venue.lookup_authoritative (a not-found proves absence?)
Every submit attempt is counted per client_order_id (venue.submit_attempts) so tests can prove no duplicate order.
Fill ids, order ids and event ids are deterministic. Realistic economics (slippage, latency, queue position) are NOT
modelled here (Step 6.7).
"""
import hashlib
import json
from decimal import Decimal

from execution.adapter import (AdapterUnavailable, AmbiguousOutcome, ExecutionAdapter, FillReport, OrderLookup,
                               OrderView, PositionReport, SubmitRejected)
from execution.faults import NO_FAULTS, SimulatedCrash
from execution.money import UNKNOWN, canon, dec

PAPER_PROTOCOL_VERSION = "paper_venue_v1"
SUBMIT_MODES = ("ACCEPT", "REJECT", "LOST_ACK", "TIMEOUT_NOT_RECEIVED", "CRASH_AFTER_ACCEPT")
CANCEL_MODES = ("OK", "LOST_ACK", "AMBIGUOUS_NOT_DONE", "FILL_THEN_CANCEL")


def _h(*parts):
    return hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()


class PaperVenue:
    def __init__(self):
        self.orders = {}                 # client_order_id -> order dict
        self.fills = {}                  # client_order_id -> [fill dict] (notifications, may repeat)
        self.submit_attempts = {}        # client_order_id -> number of submit calls that reached the adapter
        self.submit_script, self.cancel_script = [], []
        self.query_failures = 0
        self.position_mode = "LIVE"
        self.position_snapshot = {}      # market -> (yes, no) captured for STALE responses
        self.lookup_authoritative = False
        self.balance = Decimal("1000")
        self.event_n = 0

    # ---------------- scripting ----------------
    def script_submit(self, *modes):
        for m in modes:
            if m not in SUBMIT_MODES:
                raise ValueError(f"unknown submit mode {m!r}")
        self.submit_script.extend(modes)
        return self

    def script_cancel(self, *modes):
        for m in modes:
            if (m[0] if isinstance(m, tuple) else m) not in CANCEL_MODES:
                raise ValueError(f"unknown cancel mode {m!r}")
        self.cancel_script.extend(modes)
        return self

    def fail_queries(self, n):
        self.query_failures = int(n)
        return self

    def _event(self, what):
        self.event_n += 1
        return f"pv-{self.event_n}-{what}"

    # ---------------- venue actions ----------------
    def _accept(self, req):
        coid = req.client_order_id
        if coid in self.orders:                          # a venue-side idempotency key: never a second order
            return self.orders[coid]
        o = {"order_id": "po-" + _h("order", coid)[:20], "client_order_id": coid, "market_ticker": req.market_ticker,
             "side": req.side, "count": canon(req.count), "limit_price": canon(req.limit_price), "status": "RESTING",
             "time_in_force": req.time_in_force, "expires_at": req.expires_at}
        self.orders[coid] = o
        self.fills.setdefault(coid, [])
        return o

    def filled(self, coid):
        seen, tot = set(), Decimal(0)
        for f in self.fills.get(coid, []):
            if f["fill_id"] not in seen:
                seen.add(f["fill_id"])
                tot += dec(f["qty"])
        return tot

    def fill(self, coid, qty, price, fee="0.01", ts_ms=1_790_000_000_000, force=False):
        """The venue fills (part of) a resting order at `price` (<= the limit unless force=True, for mismatch tests)."""
        o = self.orders[coid]
        q, p = dec(qty), dec(price)
        if not force:
            if o["status"] != "RESTING":
                raise ValueError(f"order {coid} is {o['status']}")
            if p > dec(o["limit_price"]) or self.filled(coid) + q > dec(o["count"]):
                raise ValueError("a paper fill must respect the order's limit and size")
        n = len({f["fill_id"] for f in self.fills[coid]})
        fid = "pf-" + _h("fill", coid, n)[:20]
        self.fills[coid].append({"fill_id": fid, "qty": canon(q), "price": canon(p),
                                 "fee": "UNKNOWN" if fee is UNKNOWN else canon(dec(fee)), "fee_id": "fee-" + fid,
                                 "ts_ms": int(ts_ms), "order_id": o["order_id"]})
        if self.filled(coid) == dec(o["count"]):
            o["status"] = "FILLED"
        return fid

    def duplicate_last_fill(self, coid):
        self.fills[coid].append(dict(self.fills[coid][-1]))

    def venue_cancel(self, coid):
        if self.orders[coid]["status"] == "RESTING":
            self.orders[coid]["status"] = "CANCELLED"

    def venue_expire(self, coid):
        if self.orders[coid]["status"] == "RESTING":
            self.orders[coid]["status"] = "EXPIRED"

    def position(self, market_ticker):
        yes = no = Decimal(0)
        for coid, o in self.orders.items():
            if o["market_ticker"] == market_ticker:
                if o["side"] == "YES":
                    yes += self.filled(coid)
                else:
                    no += self.filled(coid)
        return yes, no

    def capture_position(self, market_ticker):
        self.position_snapshot[market_ticker] = self.position(market_ticker)

    # ---------------- persistence (the venue survives our restarts) ----------------
    def save(self, path):
        state = {k: getattr(self, k) for k in ("orders", "fills", "submit_attempts", "submit_script", "cancel_script",
                                               "query_failures", "position_mode", "lookup_authoritative", "event_n")}
        state["position_snapshot"] = {k: [canon(a), canon(b)] for k, (a, b) in self.position_snapshot.items()}
        state["balance"] = canon(self.balance)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f, sort_keys=True, indent=1)

    @classmethod
    def load(cls, path):
        with open(path, encoding="utf-8") as f:
            st = json.load(f)
        v = cls()
        for k in ("orders", "fills", "submit_attempts", "submit_script", "cancel_script", "query_failures",
                  "position_mode", "lookup_authoritative", "event_n"):
            setattr(v, k, st[k])
        if isinstance(v.position_mode, list):
            v.position_mode = tuple(v.position_mode)
        v.position_snapshot = {k: (dec(a), dec(b)) for k, (a, b) in st["position_snapshot"].items()}
        v.balance = dec(st["balance"])
        return v


def _view(o, venue, event_id=""):
    return OrderView(order_id=o["order_id"], client_order_id=o["client_order_id"], market_ticker=o["market_ticker"],
                     side=o["side"], count=dec(o["count"]), limit_price=dec(o["limit_price"]), status=o["status"],
                     filled=venue.filled(o["client_order_id"]), adapter_event_id=event_id)


class PaperExecutionAdapter(ExecutionAdapter):
    name = "paper"
    writable_venue = False

    def __init__(self, venue, faults=None):
        self.venue = venue
        self.faults = faults or NO_FAULTS

    def _query(self):
        if self.venue.query_failures > 0:
            self.venue.query_failures -= 1
            raise AdapterUnavailable("paper adapter timeout (scripted)")

    def submit_order(self, request):
        v = self.venue
        coid = request.client_order_id
        v.submit_attempts[coid] = v.submit_attempts.get(coid, 0) + 1
        mode = v.submit_script.pop(0) if v.submit_script else "ACCEPT"
        if mode == "REJECT":
            raise SubmitRejected("paper venue rejected the order (scripted)")
        if mode == "TIMEOUT_NOT_RECEIVED":
            raise AmbiguousOutcome("paper adapter timeout: the outcome of the submit is unknown")
        o = v._accept(request)
        if mode == "CRASH_AFTER_ACCEPT":
            raise SimulatedCrash("during_submit")
        self.faults.hit("during_submit")
        if mode == "LOST_ACK":
            raise AmbiguousOutcome("paper venue accepted, but the acknowledgement was lost")
        return _view(o, v, v._event("ack"))

    def cancel_order(self, client_order_id):
        v = self.venue
        mode = v.cancel_script.pop(0) if v.cancel_script else "OK"
        o = v.orders.get(client_order_id)
        if o is None:
            raise AmbiguousOutcome("cancel for an order the paper venue does not know")
        if isinstance(mode, tuple) and mode[0] == "FILL_THEN_CANCEL":
            v.fill(client_order_id, mode[1], mode[2])
            mode = "OK"
        elif mode == "FILL_THEN_CANCEL":
            v.fill(client_order_id, dec(o["count"]) - v.filled(client_order_id), o["limit_price"])
            mode = "OK"
        if mode == "AMBIGUOUS_NOT_DONE":
            raise AmbiguousOutcome("paper adapter timeout: the outcome of the cancel is unknown")
        v.venue_cancel(client_order_id)
        if mode == "LOST_ACK":
            raise AmbiguousOutcome("paper venue cancelled, but the response was lost")
        return _view(o, v, v._event("cancel"))

    def get_order(self, client_order_id):
        self._query()
        o = self.venue.orders.get(client_order_id)
        if o is None:
            return OrderLookup(None, authoritative=bool(self.venue.lookup_authoritative))
        return OrderLookup(_view(o, self.venue, self.venue._event("lookup")), authoritative=True)

    def get_fills(self, client_order_id):
        self._query()
        out = []
        for f in self.venue.fills.get(client_order_id, []):
            out.append(FillReport(fill_id=f["fill_id"], order_id=f["order_id"], client_order_id=client_order_id,
                                  qty=dec(f["qty"]), price=dec(f["price"]),
                                  fee=UNKNOWN if f["fee"] == "UNKNOWN" else dec(f["fee"]), fee_id=f["fee_id"],
                                  ts_ms=f["ts_ms"]))
        return out

    def get_positions(self, market_ticker):
        self._query()
        mode = self.venue.position_mode
        if mode == "FAIL":
            raise AdapterUnavailable("paper position query failed (scripted)")
        if mode == "STALE":
            yes, no = self.venue.position_snapshot.get(market_ticker, (Decimal(0), Decimal(0)))
            return PositionReport(market_ticker, yes, no, stale=True)
        if isinstance(mode, tuple) and mode[0] == "CONFLICT":       # ("CONFLICT", side, qty): a contradicting report
            yes, no = self.venue.position(market_ticker)
            return PositionReport(market_ticker, dec(mode[2]) if mode[1] == "YES" else yes,
                                  dec(mode[2]) if mode[1] == "NO" else no)
        yes, no = self.venue.position(market_ticker)
        return PositionReport(market_ticker, yes, no)

    def get_balance(self):
        self._query()
        return self.venue.balance
