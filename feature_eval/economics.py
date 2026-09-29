"""
Research-only EXECUTABLE ECONOMICS for unseen predictions (SIMULATED - no order is ever placed).

For every out-of-sample prediction with a captured Kalshi book at T, for each PREDECLARED hypothetical size:
    YES:  net_edge = P_cal(YES) - VWAP_yes_ask(size) / 100 - taker_fee_per_contract(VWAP)
    NO:   net_edge = P_cal(NO)  - VWAP_no_ask(size)  / 100 - taker_fee_per_contract(VWAP)
All in probability units per contract (1.00 = the $1 payout). VWAP walks the captured ask ladder (YES asks = 100 - NO
bids); if the captured depth cannot fill the size the result is NOT_EXECUTABLE - depth is never extrapolated.
The primary evaluation is IMMEDIATE (taker) execution against the captured book. Maker economics are NOT evaluated
(resting-order fill probability is unknown from the displayed book): only the fee arithmetic is reported.

FEES (Step 6.2): exact decimal arithmetic (decimal.Decimal on the exact price / quantity text; never binary floats
patched with round()), with schedules VERSIONED BY EFFECTIVE TIME and their own rounding rule:
    kalshi_general_2026_07_07   from 2026-07-07 00:00 ET: taker fee = M x 0.07 x C x P x (1 - P), maker
                                M x 0.0175 x C x P x (1 - P); ROUND UP (fee + position cost) to a CENTICENT ($0.0001);
                                terms as supplied by the project owner from the official "Fee Schedule for July 2026 -
                                7.7.26 Update" (the PDF itself is unreachable from the build environment)
    kalshi_general_pre_2026_07_07  before 2026-07-07: the earlier behaviour, fee itself rounded up to a full CENT;
                                start unknown -> never FEE_VERIFIED, but its arithmetic is preserved for old periods
A trade uses the schedule whose effective interval covers the TRADE timestamp (never a later schedule applied
retroactively). The multiplier M comes from the captured Kalshi metadata: a market / event fee_multiplier_override
takes precedence over the (series / market) fee_multiplier; the schedule default applies only for pricing, never as
evidence. FEE_VERIFIED requires ALL of: schedule version known and verified, trade timestamp covered by an interval
with a known start, override state known (captured field, null or a value), multiplier known, rounding rule known.
Otherwise FEE_UNVERIFIED (or FEE_UNKNOWN when no schedule covers the trade). The 15-minute crypto series are NOT
hardcoded as general-fee markets: the captured fee_type / override decides, per event.
The model fingerprint (every schedule) is part of every economic experiment.

Any extra uncertainty buffer is reported separately (uncertainty_buffer), never folded into the probability.
"""
import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import ROUND_CEILING, Decimal
from typing import Optional, Tuple

RESEARCH_SIZES = (1, 10, 50)
CENT = Decimal("0.01")
CENTICENT = Decimal("0.0001")
ROUNDING_RULES = ("ROUND_UP_FEE_TO_CENT", "ROUND_UP_FEE_PLUS_COST_TO_CENTICENT")
JULY_2026_START_MS = 1_783_396_800_000               # 2026-07-07T00:00:00-04:00 (America/New_York)


def dec(v):
    """Exact Decimal of a price / quantity: a str / int / Decimal as is; a float via its shortest repr."""
    if isinstance(v, Decimal):
        return v
    if isinstance(v, float):
        return Decimal(repr(v))
    return Decimal(str(v))


def _ceil_to(x, inc):
    return (x / inc).to_integral_value(rounding=ROUND_CEILING) * inc


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
    taker_coefficient: str = "0.07"
    maker_coefficient: str = "0.0175"
    default_multiplier: str = "1"
    rounding: str = "ROUND_UP_FEE_TO_CENT"
    fee_type: str = "general"                    # the Kalshi fee type this schedule prices
    verified: bool = False
    source_sha256: str = ""
    note: str = ""

    def __post_init__(self):
        if self.rounding not in ROUNDING_RULES:
            raise ValueError(f"unknown fee rounding rule {self.rounding!r}")

    def covers_series(self, ticker):
        t = str(ticker).upper()
        if any(t.startswith(x) for x in self.excluded_series):
            return False
        return "*" in self.series_scope or any(t.startswith(x) for x in self.series_scope)

    def covers_time(self, ts_ms):
        """The trade time falls inside this schedule's interval (an unknown start covers earlier times for PRICING
        only; such a schedule can never make a fee FEE_VERIFIED)."""
        if ts_ms is None:
            return False
        return ((self.effective_from_ms is None or ts_ms >= self.effective_from_ms)
                and (self.effective_to_ms is None or ts_ms < self.effective_to_ms))


_JULY_TEXT = ("General taker fee M * 0.07 * C * P * (1-P); maker M * 0.0175 * C * P * (1-P); fee + positionCost rounded "
              "up to a centicent (Fee Schedule for July 2026 - 7.7.26 Update, as supplied by the project owner)")
KALSHI_GENERAL_2026_07_07 = FeeSchedule(
    schedule_id="kalshi_general_2026_07_07", version="7.7.26", source=_JULY_TEXT,
    source_url="https://kalshi.com/docs/kalshi-fee-schedule.pdf", effective_from_ms=JULY_2026_START_MS,
    effective_to_ms=None, rounding="ROUND_UP_FEE_PLUS_COST_TO_CENTICENT", verified=True,
    source_sha256=hashlib.sha256(_JULY_TEXT.encode()).hexdigest(),
    note="general schedule; specific listed products are excepted (the list is not encoded: the captured fee_type / "
         "override decides per event)")
KALSHI_GENERAL_PRE_2026_07_07 = FeeSchedule(
    schedule_id="kalshi_general_pre_2026_07_07", version="pre-7.7.26",
    source="earlier general schedule as previously modelled: fee = round up (0.07 x C x P x (1 - P)) to a cent",
    source_url="", effective_from_ms=None, effective_to_ms=JULY_2026_START_MS, rounding="ROUND_UP_FEE_TO_CENT",
    verified=False, note="historical arithmetic preserved for old periods; start and exceptions unknown -> unverified")
# kept for callers of the Step-6.1 name (the pre-July behaviour, unverified)
GENERAL_UNVERIFIED = KALSHI_GENERAL_PRE_2026_07_07


def fee_for_fills(fills, schedule, multiplier, maker=False):
    """Exact fee of ONE order made of fills [(price_dollars, contracts)] under one schedule. -> dict of Decimals."""
    coef = Decimal(schedule.maker_coefficient if maker else schedule.taker_coefficient)
    m = dec(multiplier)
    raw = sum((coef * m * dec(q) * dec(p) * (1 - dec(p)) for p, q in fills), Decimal(0))
    cost = sum((dec(q) * dec(p) for p, q in fills), Decimal(0))
    if schedule.rounding == "ROUND_UP_FEE_TO_CENT":
        fee = _ceil_to(raw, CENT)
    else:                                        # ROUND_UP_FEE_PLUS_COST_TO_CENTICENT
        fee = _ceil_to(raw + cost, CENTICENT) - cost
    return {"raw_fee": raw, "fee": fee, "position_cost": cost, "total": fee + cost, "rounding": schedule.rounding,
            "schedule_id": schedule.schedule_id, "multiplier": m}


def _num(v):
    if v is None or isinstance(v, bool):
        return None
    try:
        d = dec(v)
    except Exception:                            # noqa: BLE001
        return None
    return d if d.is_finite() and d > 0 else None


def resolve_multiplier(fee_context, schedule):
    """-> (multiplier Decimal, override_state, multiplier_known, reasons). An override takes precedence."""
    meta = (fee_context or {}).get("fee_metadata") or {}
    reasons = []
    if "fee_multiplier_override" in meta or "fee_type_override" in meta:
        ov_m, ov_t = meta.get("fee_multiplier_override"), meta.get("fee_type_override")
        override_state = "OVERRIDE" if (ov_m is not None or ov_t is not None) else "NO_OVERRIDE"
    else:
        override_state = "UNKNOWN"
        reasons.append("fee override state not captured")
    fee_type = meta.get("fee_type_override") if meta.get("fee_type_override") is not None else meta.get("fee_type")
    if fee_type is not None and str(fee_type).lower() != schedule.fee_type:
        reasons.append(f"captured fee type {fee_type!r} is not priced by schedule {schedule.schedule_id}")
    om = _num(meta.get("fee_multiplier_override"))
    if om is not None:
        return om, override_state, True, reasons
    bm = _num(meta.get("fee_multiplier"))
    if bm is not None:
        return bm, override_state, True, reasons
    reasons.append("fee multiplier not captured (schedule default used for pricing only)")
    return Decimal(schedule.default_multiplier), override_state, False, reasons


@dataclass(frozen=True)
class FeeModel:
    version: str = "fee_model_v3"
    schedules: Tuple[FeeSchedule, ...] = (KALSHI_GENERAL_2026_07_07, KALSHI_GENERAL_PRE_2026_07_07)

    def to_dict(self):
        return {"version": self.version, "schedules": [asdict(x) for x in self.schedules]}

    def fingerprint(self):
        return hashlib.sha256(json.dumps(self.to_dict(), sort_keys=True).encode()).hexdigest()

    def resolve(self, ticker, ts_ms=None):
        """The schedule whose effective interval covers the TRADE time and whose scope covers the series (series-
        specific first). -> schedule | None."""
        cands = [x for x in self.schedules if x.covers_series(ticker) and x.covers_time(ts_ms)]
        cands.sort(key=lambda x: ("*" in x.series_scope, x.schedule_id))
        return cands[0] if cands else None

    def fee(self, fills, ticker="", ts_ms=None, fee_context=None, maker=False):
        """Exact fee + status for one order. status FEE_VERIFIED only when every condition in the module doc holds."""
        x = self.resolve(ticker, ts_ms)
        if x is None:
            return {"status": "FEE_UNKNOWN", "reasons": ["no fee schedule covers this series / trade time"]}
        mult, ov_state, m_known, reasons = resolve_multiplier(fee_context, x)
        out = fee_for_fills(fills, x, mult, maker)
        verified = (x.verified and x.effective_from_ms is not None and ov_state != "UNKNOWN" and m_known
                    and x.rounding in ROUNDING_RULES and not any("not priced" in r for r in reasons))
        if not x.verified:
            reasons = reasons + [f"schedule {x.schedule_id} is not verified"]
        out.update(status="FEE_VERIFIED" if verified else "FEE_UNVERIFIED", override_state=ov_state,
                   multiplier_known=m_known, reasons=reasons, schedule_version=x.version,
                   schedule_effective=[x.effective_from_ms, x.effective_to_ms], schedule_source_sha256=x.source_sha256)
        return out

    def fee_dollars(self, contracts, price_prob, ticker="", maker=False, ts_ms=None, fee_context=None):
        """Single-price convenience: the exact fee as a float (None when no schedule covers the trade)."""
        r = self.fee([(dec(price_prob), dec(contracts))], ticker, ts_ms, fee_context, maker)
        return float(r["fee"]) if "fee" in r else None

    def status(self, ticker, ts_ms=None, fee_context=None):
        return self.fee([(Decimal("0.5"), Decimal(1))], ticker, ts_ms, fee_context)["status"]


def walk(ladder, size):
    """ladder [[price_cents, qty]] best first -> fills [(price_dollars Decimal, qty Decimal)] or None (not fillable)."""
    need, fills = dec(size), []
    for px, q in ladder or []:
        take = min(dec(q), need)
        if take > 0:
            fills.append((dec(px) / 100, take))
            need -= take
        if need <= 0:
            return fills
    return None


def vwap(ladder, size):
    """ladder: [[price_cents, qty]] best first. -> (vwap_cents, filled_qty) or (None, available) if not fillable."""
    fills = walk(ladder, size)
    if fills is None:
        return None, float(sum((dec(q) for _p, q in ladder or []), Decimal(0)))
    tot = sum((q for _p, q in fills), Decimal(0))
    return float(sum((p * 100 * q for p, q in fills), Decimal(0)) / tot), float(tot)


def side_economics(p_side, ladder, size, fee_model, ticker, uncertainty_buffer=0.0, ts_ms=None, fee_context=None):
    v, filled = vwap(ladder, size)
    if v is None:
        return {"status": "NOT_EXECUTABLE", "available_qty": filled, "size": size}
    price = v / 100.0
    f = fee_model.fee(walk(ladder, size), ticker, ts_ms, fee_context)
    if "fee" not in f:
        return {"status": "FEE_UNKNOWN", "size": size, "vwap_cents": v, "price": price, "fee_reasons": f["reasons"]}
    fee_pc = float(f["fee"] / dec(size))
    raw = p_side - price
    return {"status": "EXECUTABLE", "size": size, "vwap_cents": v, "price": price, "raw_edge": raw,
            "fee_per_contract": fee_pc, "fee": str(f["fee"]), "position_cost": str(f["position_cost"]),
            "fee_status": f["status"], "fee_schedule": f["schedule_id"], "fee_rounding": f["rounding"],
            "fee_multiplier": str(f["multiplier"]), "fee_override_state": f["override_state"],
            "net_executable_edge": raw - fee_pc, "net_edge_after_buffer": raw - fee_pc - uncertainty_buffer,
            "uncertainty_buffer": uncertainty_buffer}


def row_economics(p_yes, execution, fee_model, ticker, sizes=RESEARCH_SIZES, ts_ms=None, fee_context=None):
    if not execution:
        return {"status": "NO_BOOK"}
    fav = "YES" if p_yes >= 0.5 else "NO"
    out = {"favoured_side": fav, "book_source": execution.get("source"), "sides": {}}
    for side, p, lad in (("YES", p_yes, execution.get("yes_ask_ladder")), ("NO", 1 - p_yes, execution.get("no_ask_ladder"))):
        out["sides"][side] = {str(s): side_economics(p, lad, s, fee_model, ticker, ts_ms=ts_ms, fee_context=fee_context)
                              for s in sizes}
    out["status"] = "OK"
    return out


def evaluate(preds, fee_model=None, sizes=RESEARCH_SIZES, later_bids=None):
    """preds: [{market, p_yes, y, execution, checkpoint_s, ticker, ts_ms, fee_context}] (OUT-OF-SAMPLE only). Fixed-size
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
            eco = row_economics(pr["p_yes"], pr.get("execution"), fm, pr.get("ticker", ""), (s,), ts_ms=pr.get("ts_ms"),
                                fee_context=pr.get("fee_context"))
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
    res["status"] = "FEE_VERIFIED" if stats == {"FEE_VERIFIED"} else "FEE_UNVERIFIED"
    res["fee_status_rule"] = ("FEE_VERIFIED only when every executable row has: a verified schedule version whose "
                              "effective interval (with a known start) covers the trade timestamp, a known override "
                              "state, a known multiplier and a known rounding rule; otherwise FEE_UNVERIFIED")
    return res
