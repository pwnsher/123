"""
Research-only EXECUTABLE ECONOMICS for unseen predictions (SIMULATED - no order is ever placed).

For every out-of-sample prediction with a captured Kalshi book at T, for each PREDECLARED hypothetical size:
    YES:  net_edge = P_cal(YES) - VWAP_yes_ask(size) / 100 - taker_fee_per_contract(VWAP)
    NO:   net_edge = P_cal(NO)  - VWAP_no_ask(size)  / 100 - taker_fee_per_contract(VWAP)
All in probability units per contract (1.00 = the $1 payout). VWAP walks the captured ask ladder (YES asks = 100 - NO
bids); if the captured depth cannot fill the size the result is NOT_EXECUTABLE - depth is never extrapolated.
The primary evaluation is IMMEDIATE (taker) execution against the captured book. Maker economics are NOT evaluated
(resting-order fill probability is unknown from the displayed book): only the fee arithmetic is reported.

FEES are versioned configuration, never a hidden constant. FeeModel records version, source, effective date,
maker / taker rates, rounding and market exceptions; its fingerprint is part of every economic experiment. Unless the
fee model is marked verified for the markets studied, the economic result is FEE_UNVERIFIED and cannot support any
promotion. Default (UNVERIFIED): Kalshi's general trading-fee formula fee = ceil_to_cent(rate * C * P * (1 - P)) per
order with taker rate 0.07 and maker rate 0.0175.

Any extra uncertainty buffer is reported separately (uncertainty_buffer), never folded into the probability.
"""
import hashlib
import json
import math
from dataclasses import asdict, dataclass, field

RESEARCH_SIZES = (1, 10, 50)


@dataclass(frozen=True)
class FeeModel:
    version: str = "kalshi_general_fee_formula_v1"
    source: str = "Kalshi fee schedule: general trading fee = round up(rate x C x P x (1 - P)) per order (to be confirmed)"
    effective_date: str = "UNVERIFIED"
    taker_rate: float = 0.07
    maker_rate: float = 0.0175
    rounding: str = "ceil_to_cent_per_order"
    market_exceptions: dict = field(default_factory=dict)       # series prefix -> {taker_rate, maker_rate, note}
    verified: bool = False

    def to_dict(self):
        return asdict(self)

    def fingerprint(self):
        return hashlib.sha256(json.dumps(self.to_dict(), sort_keys=True).encode()).hexdigest()

    def rate(self, ticker, maker=False):
        for pre, ex in sorted(self.market_exceptions.items()):
            if str(ticker).startswith(pre):
                return ex["maker_rate" if maker else "taker_rate"]
        return self.maker_rate if maker else self.taker_rate

    def fee_dollars(self, contracts, price_prob, ticker="", maker=False):
        raw = self.rate(ticker, maker) * contracts * price_prob * (1 - price_prob)
        if self.rounding == "ceil_to_cent_per_order":
            return math.ceil(round(raw * 100, 9)) / 100.0
        return raw


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


def side_economics(p_side, ladder, size, fee_model, ticker, uncertainty_buffer=0.0):
    v, filled = vwap(ladder, size)
    if v is None:
        return {"status": "NOT_EXECUTABLE", "available_qty": filled, "size": size}
    price = v / 100.0
    fee_pc = fee_model.fee_dollars(size, price, ticker) / size
    raw = p_side - price
    return {"status": "EXECUTABLE", "size": size, "vwap_cents": v, "price": price, "raw_edge": raw,
            "fee_per_contract": fee_pc, "net_executable_edge": raw - fee_pc,
            "net_edge_after_buffer": raw - fee_pc - uncertainty_buffer, "uncertainty_buffer": uncertainty_buffer}


def row_economics(p_yes, execution, fee_model, ticker, sizes=RESEARCH_SIZES):
    if not execution:
        return {"status": "NO_BOOK"}
    fav = "YES" if p_yes >= 0.5 else "NO"
    out = {"favoured_side": fav, "book_source": execution.get("source"), "sides": {}}
    for side, p, lad in (("YES", p_yes, execution.get("yes_ask_ladder")), ("NO", 1 - p_yes, execution.get("no_ask_ladder"))):
        out["sides"][side] = {str(s): side_economics(p, lad, s, fee_model, ticker) for s in sizes}
    out["status"] = "OK"
    return out


def evaluate(preds, fee_model=None, sizes=RESEARCH_SIZES, later_bids=None):
    """preds: [{market, p_yes, y, execution, checkpoint_s, ticker}] (OUT-OF-SAMPLE only). Hypothetical fixed-size
    trades on the favoured side where net executable edge > 0. later_bids: {(market, checkpoint_s): [(later cp_s,
    yes_bid_cents, no_bid_cents)]} for grid-level max adverse / favourable excursion."""
    fm = fee_model or FeeModel()
    res = {"fee_model": fm.to_dict(), "fee_model_fingerprint": fm.fingerprint(),
           "status": "SIMULATED_RESEARCH_ONLY" if fm.verified else "FEE_UNVERIFIED",
           "note": "SIMULATED research economics from captured books - not realized profit; no order was placed",
           "maker": {"status": "NOT_EVALUATED", "reason": "resting-order fill probability is unknown from the displayed "
                     "book; maker EV needs an explicit fill model (a later execution stage)"},
           "by_size": {}}
    for s in sizes:
        rows = []
        for pr in preds:
            eco = row_economics(pr["p_yes"], pr.get("execution"), fm, pr.get("ticker", ""), (s,))
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
            rows.append({"executable": True, "raw": e["raw_edge"], "net": e["net_executable_edge"], "pnl": pnl,
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
            "labels": "SIMULATED"}
    return res
