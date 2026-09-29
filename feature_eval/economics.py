"""
Research-only EXECUTABLE ECONOMICS for unseen predictions (SIMULATED - no order is ever placed).

For every out-of-sample prediction with a captured Kalshi book at T, for each PREDECLARED hypothetical size:
    YES:  net_edge = P_cal(YES) - VWAP_yes_ask(size) / 100 - taker_fee_per_contract(VWAP)
    NO:   net_edge = P_cal(NO)  - VWAP_no_ask(size)  / 100 - taker_fee_per_contract(VWAP)
All in probability units per contract (1.00 = the $1 payout). VWAP walks the captured ask ladder (YES asks = 100 - NO
bids); if the captured depth cannot fill the size the result is NOT_EXECUTABLE - depth is never extrapolated.
The primary evaluation is IMMEDIATE (taker) execution against the captured book. Maker economics are NOT evaluated
(resting-order fill probability is unknown from the displayed book): only the fee arithmetic is reported.

FEES are versioned configuration, never a hidden constant (Step 6.1 hardening). A FeeModel holds FeeSchedules; each
records its source / version, effective dates, the series it covers (or the general "*" scope with explicit
exclusions), taker and maker rates and rounding. A trade uses the schedule whose effective range covers the trade
timestamp and whose scope covers the market's series (a series-specific schedule beats the general one). Its fee
status is VERIFIED only when that schedule is marked verified AND its effective range covers the trade time; anything
else is FEE_UNVERIFIED (unknown series, unknown / uncovered date, unverified schedule). The general formula is NEVER
verified globally. Default: Kalshi's general trading-fee formula fee = ceil_to_cent(rate * C * P * (1 - P)) per order,
taker rate 0.07, maker 0.0175, UNVERIFIED - the official schedule (kalshi.com/docs/kalshi-fee-schedule.pdf) could
not be read from the build environment and public summaries disagree on whether crypto uses a different multiplier.
The model fingerprint (every schedule) is part of every economic experiment.

Any extra uncertainty buffer is reported separately (uncertainty_buffer), never folded into the probability.
"""
import hashlib
import json
import math
from dataclasses import asdict, dataclass
from typing import Optional, Tuple

RESEARCH_SIZES = (1, 10, 50)


@dataclass(frozen=True)
class FeeSchedule:
    schedule_id: str
    version: str
    source: str
    source_url: str
    effective_from_ms: Optional[int]             # None = unknown start
    effective_to_ms: Optional[int]               # None = open ended
    series_scope: Tuple[str, ...] = ("*",)       # series prefixes, or "*" (general schedule)
    excluded_series: Tuple[str, ...] = ()        # series with their own (special) schedules
    taker_rate: float = 0.07
    maker_rate: float = 0.0175
    rounding: str = "ceil_to_cent_per_order"
    verified: bool = False
    note: str = ""

    def covers_series(self, ticker):
        t = str(ticker).upper()
        if any(t.startswith(x) for x in self.excluded_series):
            return False
        return "*" in self.series_scope or any(t.startswith(x) for x in self.series_scope)

    def covers_time(self, ts_ms):
        if ts_ms is None or self.effective_from_ms is None:
            return False                                          # an unknown date is never covered
        return ts_ms >= self.effective_from_ms and (self.effective_to_ms is None or ts_ms < self.effective_to_ms)


GENERAL_UNVERIFIED = FeeSchedule(
    schedule_id="kalshi_general_fee_formula", version="v1",
    source="Kalshi fee schedule: general trading fee = round up(rate x C x P x (1 - P)) per order (not read: kalshi.com "
           "unreachable from the build environment)", source_url="https://kalshi.com/docs/kalshi-fee-schedule.pdf",
    effective_from_ms=None, effective_to_ms=None, verified=False,
    note="15-minute crypto series may carry a special schedule / multiplier: not confirmed")


@dataclass(frozen=True)
class FeeModel:
    version: str = "fee_model_v2"
    schedules: Tuple[FeeSchedule, ...] = (GENERAL_UNVERIFIED,)

    def to_dict(self):
        return {"version": self.version, "schedules": [asdict(x) for x in self.schedules]}

    def fingerprint(self):
        return hashlib.sha256(json.dumps(self.to_dict(), sort_keys=True).encode()).hexdigest()

    def resolve(self, ticker, ts_ms=None):
        """-> (schedule | None, status). Series-specific schedules first; VERIFIED only for a verified schedule whose
        effective range covers ts_ms."""
        cands = [x for x in self.schedules if x.covers_series(ticker)]
        cands.sort(key=lambda x: ("*" in x.series_scope, x.schedule_id))
        timed = [x for x in cands if x.covers_time(ts_ms)]
        if timed:
            x = timed[0]
            return x, ("VERIFIED" if x.verified else "FEE_UNVERIFIED")
        undated = [x for x in cands if x.effective_from_ms is None]
        if undated:
            return undated[0], "FEE_UNVERIFIED"
        return None, "FEE_UNKNOWN"

    def fee_dollars(self, contracts, price_prob, ticker="", maker=False, ts_ms=None):
        x, _st = self.resolve(ticker, ts_ms)
        if x is None:
            return None
        raw = (x.maker_rate if maker else x.taker_rate) * contracts * price_prob * (1 - price_prob)
        if x.rounding == "ceil_to_cent_per_order":
            return math.ceil(round(raw * 100, 9)) / 100.0
        return raw

    def status(self, ticker, ts_ms=None):
        return self.resolve(ticker, ts_ms)[1]


def vwap(ladder, size):
    """ladder: [[price_cents, qty]] best first. -> (vwap_cents, filled_qty) or (None, available) if not fillable."""
    need, cost, avail = size, 0.0, 0.0
    for px, q in ladder or []:
        avail += q
        take = min(q, need)
        cost += take * px
        need -= take
        if need <= 1e-12:
            return cost / size, size
    return None, avail


def side_economics(p_side, ladder, size, fee_model, ticker, uncertainty_buffer=0.0, ts_ms=None):
    v, filled = vwap(ladder, size)
    if v is None:
        return {"status": "NOT_EXECUTABLE", "available_qty": filled, "size": size}
    price = v / 100.0
    fee = fee_model.fee_dollars(size, price, ticker, ts_ms=ts_ms)
    if fee is None:
        return {"status": "FEE_UNKNOWN", "size": size, "vwap_cents": v, "price": price}
    fee_pc = fee / size
    raw = p_side - price
    return {"status": "EXECUTABLE", "size": size, "vwap_cents": v, "price": price, "raw_edge": raw,
            "fee_per_contract": fee_pc, "fee_status": fee_model.status(ticker, ts_ms), "net_executable_edge": raw - fee_pc,
            "net_edge_after_buffer": raw - fee_pc - uncertainty_buffer, "uncertainty_buffer": uncertainty_buffer}


def row_economics(p_yes, execution, fee_model, ticker, sizes=RESEARCH_SIZES, ts_ms=None):
    if not execution:
        return {"status": "NO_BOOK"}
    fav = "YES" if p_yes >= 0.5 else "NO"
    out = {"favoured_side": fav, "book_source": execution.get("source"), "sides": {}}
    for side, p, lad in (("YES", p_yes, execution.get("yes_ask_ladder")), ("NO", 1 - p_yes, execution.get("no_ask_ladder"))):
        out["sides"][side] = {str(s): side_economics(p, lad, s, fee_model, ticker, ts_ms=ts_ms) for s in sizes}
    out["status"] = "OK"
    return out


def evaluate(preds, fee_model=None, sizes=RESEARCH_SIZES, later_bids=None):
    """preds: [{market, p_yes, y, execution, checkpoint_s, ticker, ts_ms}] (OUT-OF-SAMPLE only). Hypothetical fixed-size
    trades on the favoured side where net executable edge > 0. later_bids: {(market, checkpoint_s): [(later cp_s,
    yes_bid_cents, no_bid_cents)]} for grid-level max adverse / favourable excursion."""
    fm = fee_model or FeeModel()
    res = {"fee_model": fm.to_dict(), "fee_model_fingerprint": fm.fingerprint(),
           "status": None,
           "note": "SIMULATED research economics from captured books - not realized profit; no order was placed",
           "maker": {"status": "NOT_EVALUATED", "reason": "resting-order fill probability is unknown from the displayed "
                     "book; maker EV needs an explicit fill model (a later execution stage)"},
           "by_size": {}}
    for s in sizes:
        rows = []
        for pr in preds:
            eco = row_economics(pr["p_yes"], pr.get("execution"), fm, pr.get("ticker", ""), (s,), ts_ms=pr.get("ts_ms"))
            if eco["status"] != "OK":
                continue
            side = eco["favoured_side"]
            e = eco["sides"][side][str(s)]
            if e["status"] != "EXECUTABLE":
                rows.append({"executable": False})
                continue
            won = (pr["y"] == 1) if side == "YES" else (pr["y"] == 0)
            pnl = ((1.0 if won else 0.0) - e["price"] - e["fee_per_contract"]) if e["net_executable_edge"] > 0 else None
            mae = mfe = None
            if later_bids is not None and e["net_executable_edge"] > 0:
                path = later_bids.get((pr["market"], pr["checkpoint_s"])) or []
                marks = [((yb if side == "YES" else nb) / 100.0) for _cp, yb, nb in path
                         if (yb if side == "YES" else nb) is not None]
                if marks:
                    mae = min(m - e["price"] for m in marks)
                    mfe = max(m - e["price"] for m in marks)
            rows.append({"executable": True, "fee_status": e["fee_status"], "raw": e["raw_edge"],
                         "net": e["net_executable_edge"], "pnl": pnl,
                         "market": pr["market"], "mae": mae, "mfe": mfe, "price": e["price"]})
        ex = [r for r in rows if r["executable"]]
        trades = [r for r in ex if r["pnl"] is not None]
        nets = sorted(r["net"] for r in ex)

        def q(v, a):
            return v[int(a * (len(v) - 1))] if v else None
        res["by_size"][str(s)] = {
            "predictions_with_book": len(rows), "executable": len(ex), "not_executable": len(rows) - len(ex),
            "average_raw_edge": sum(r["raw"] for r in ex) / len(ex) if ex else None,
            "average_net_edge": sum(r["net"] for r in ex) / len(ex) if ex else None,
            "net_edge_quantiles": {k: q(nets, a) for k, a in (("p05", .05), ("p25", .25), ("p50", .5), ("p75", .75), ("p95", .95))},
            "hypothetical_trades_positive_net_edge": len(trades),
            "hypothetical_trade_markets": len({r["market"] for r in trades}),
            "simulated_pnl_total_per_contract_size": sum(r["pnl"] for r in trades) * s if trades else 0.0,
            "simulated_return_per_contract": sum(r["pnl"] for r in trades) / len(trades) if trades else None,
            "simulated_win_rate": (sum(1 for r in trades if r["pnl"] > 0) / len(trades)) if trades else None,
            "max_adverse_excursion_grid": min((r["mae"] for r in trades if r["mae"] is not None), default=None),
            "max_favourable_excursion_grid": max((r["mfe"] for r in trades if r["mfe"] is not None), default=None),
            "excursion_note": "on the frozen checkpoint grid only (captured bids at later checkpoints), not tick level",
            "fee_status_counts": {k: sum(1 for r in ex if r["fee_status"] == k) for k in sorted({r["fee_status"] for r in ex})},
            "labels": "SIMULATED"}
    stats = {r_ for b in res["by_size"].values() for r_ in b["fee_status_counts"]}
    res["status"] = "SIMULATED_RESEARCH_ONLY" if stats == {"VERIFIED"} else "FEE_UNVERIFIED"
    res["fee_status_rule"] = ("VERIFIED only when every executable row's schedule is verified and its effective range "
                              "covers the trade timestamp; otherwise FEE_UNVERIFIED")
    return res
