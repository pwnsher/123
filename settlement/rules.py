"""
Versioned CONTRACT SETTLEMENT RULES: how a reconstructed settlement mean becomes Kalshi's official expiration value
and outcome. Bound per SERIES and rule version; never one global comparator.

Kalshi's CRYPTO contract terms (kalshi-public-docs contract_terms/CRYPTO.pdf) define the comparison words:
"Above X" means strictly greater than X, "below X" strictly less than X, "at least X" means X or greater,
"exactly X" means equal to X to the number of decimal places specified. The current 15-minute crypto market pages
(reviewed by the project owner, 2026-09-29) say "Resolves Yes if ... is at least [target]" and that the official
final value is rounded (BTC and ETH: nearest 2 decimal places; XRP: nearest 4 decimal places). The build environment
could not open kalshi.com (egress blocked), so these entries carry that provenance explicitly.

    comparison_operator         GREATER_THAN_OR_EQUAL ("at least") | GREATER_THAN ("above") | ... | None (UNKNOWN)
    settlement_decimal_places   official rounding precision of the expiration value, or None (UNKNOWN)
    rounding_method             NEAREST (to settlement_decimal_places)
    tie_behavior                UNSPECIFIED: the documentation does not say how an exact .5 tie is rounded, so no
                                tie-breaking convention is invented (see settle_value)

UNKNOWN semantics FAIL CLOSED: no rounded settlement value and no reconstructed outcome (flag RULE_UNKNOWN). SOL is
UNKNOWN until its current market page is verified - its precision is NOT inferred from another asset. Official Kalshi
results do not depend on these rules and remain the authoritative payoff labels.

Versioning: every rule has a rule_id, a version, the observation date and an effective range. A market uses the rule
version whose range covers its close time, so old sessions stay tied to the rule that applied to them. The start of
v1 is not known (observed current on 2026-09-29): v1 is the earliest known version and markets closing before the
observation date carry RULE_OBSERVED_AFTER_CLOSE. The rule-set fingerprint enters the settlement and dataset
fingerprints.
"""
import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Optional

GTE, GT, LTE, LT = "GREATER_THAN_OR_EQUAL", "GREATER_THAN", "LESS_THAN_OR_EQUAL", "LESS_THAN"
COMPARISON_OPERATORS = (GTE, GT, LTE, LT)
ROUNDING_METHODS = ("NEAREST",)
TIE_BEHAVIORS = ("UNSPECIFIED", "HALF_UP", "HALF_EVEN")
RULE_SET_VERSION = "kalshi_crypto15m_rules_v1"
OBSERVED_2026_09_29_MS = 1_790_640_000_000          # 2026-09-29T00:00:00Z

CRYPTO_TERMS = ("Kalshi contract terms CRYPTO (kalshi-public-docs.s3.amazonaws.com/contract_terms/CRYPTO.pdf): "
                "'at least X' means X or greater; 'above X' strictly greater")
OWNER_REVIEW = ("current Kalshi 15-minute market page rules, reviewed by the project owner 2026-09-29 "
                "(kalshi.com not reachable from the build environment)")


@dataclass(frozen=True)
class SettlementRule:
    rule_id: str
    version: int
    series: str
    asset: str
    comparison_operator: Optional[str]
    settlement_decimal_places: Optional[int]
    rounding_method: Optional[str]
    tie_behavior: str
    window_policy_id: str
    reconstruction_policy_id: str
    rule_source: str
    rule_url: str
    observed_date: str
    observed_ts_ms: int
    effective_from_ms: Optional[int]
    effective_to_ms: Optional[int]
    status: str                                  # DOCUMENTED | UNVERIFIED
    note: str = ""

    def __post_init__(self):
        if self.comparison_operator is not None and self.comparison_operator not in COMPARISON_OPERATORS:
            raise ValueError(f"unknown comparison operator {self.comparison_operator!r}")
        if self.rounding_method is not None and self.rounding_method not in ROUNDING_METHODS:
            raise ValueError(f"unknown rounding method {self.rounding_method!r}")
        if self.tie_behavior not in TIE_BEHAVIORS:
            raise ValueError(f"unknown tie behaviour {self.tie_behavior!r}")
        if self.settlement_decimal_places is not None and not (0 <= self.settlement_decimal_places <= 12):
            raise ValueError("settlement_decimal_places out of range")

    @property
    def known(self):
        """Every piece needed to turn a mean into an official outcome is documented."""
        return (self.status == "DOCUMENTED" and self.comparison_operator is not None
                and self.settlement_decimal_places is not None and self.rounding_method is not None)

    def to_dict(self):
        return asdict(self)

    def fingerprint(self):
        return hashlib.sha256(json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def applies_to(self, close_ts_ms):
        if close_ts_ms is None:
            return False
        return ((self.effective_from_ms is None or close_ts_ms >= self.effective_from_ms)
                and (self.effective_to_ms is None or close_ts_ms < self.effective_to_ms))


def _rule(series, asset, op, dp, status, note=""):
    return SettlementRule(
        rule_id=f"{series.lower()}_rules_v1", version=1, series=series, asset=asset, comparison_operator=op,
        settlement_decimal_places=dp, rounding_method="NEAREST" if dp is not None else None, tie_behavior="UNSPECIFIED",
        window_policy_id="cf_rti_60s_start_incl_asof_v1", reconstruction_policy_id="strict_v1",
        rule_source=f"{CRYPTO_TERMS}; {OWNER_REVIEW}", rule_url=f"https://kalshi.com/markets/{series.lower()}",
        observed_date="2026-09-29", observed_ts_ms=OBSERVED_2026_09_29_MS, effective_from_ms=None, effective_to_ms=None,
        status=status, note=note)


RULES = (
    _rule("KXBTC15M", "BTC", GTE, 2, "DOCUMENTED", "Resolves Yes if the 60-s CF BRTI average is at least the target; "
          "official value rounded to the nearest 2 decimal places"),
    _rule("KXETH15M", "ETH", GTE, 2, "DOCUMENTED", "at least; nearest 2 decimal places"),
    _rule("KXXRP15M", "XRP", GTE, 4, "DOCUMENTED", "at least; nearest 4 decimal places"),
    _rule("KXSOL15M", "SOL", None, None, "UNVERIFIED",
          "current SOL market-page rule text not verified: comparator and precision UNKNOWN (never inferred from "
          "another asset) -> reconstructed outcomes fail closed; official results unaffected"),
)


def rule_set_fingerprint(rules=RULES):
    body = {"rule_set_version": RULE_SET_VERSION, "rules": sorted((r.to_dict() for r in rules), key=lambda d: (d["series"], d["version"]))}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def rule_for(series, close_ts_ms, rules=RULES):
    """The rule version covering this market's close, or None (unknown series / no covering version)."""
    s = (series or "").upper()
    hits = [r for r in rules if r.series == s and r.applies_to(close_ts_ms)]
    if len(hits) > 1:
        raise ValueError(f"overlapping rule versions for {s}: {[r.rule_id for r in hits]}")
    return hits[0] if hits else None


# ───────────────────────── exact arithmetic ─────────────────────────
def exact(v):
    """A float / str / Decimal as an exact Fraction of its DECIMAL representation (repr of a float)."""
    if isinstance(v, Fraction):
        return v
    if isinstance(v, float):
        return Fraction(Decimal(repr(v)))
    return Fraction(Decimal(str(v)))


def exact_mean(values):
    vals = [exact(v) for v in values]
    return sum(vals, Fraction(0)) / len(vals) if vals else None


def settle_value(mean, rule):
    """-> (rounded Fraction | None, tie_candidates tuple | None). Exact decimal rounding to the rule precision.
    An exact .5 tie with tie_behavior UNSPECIFIED returns (None, (low, high)): no convention is invented."""
    if mean is None or rule is None or rule.settlement_decimal_places is None:
        return None, None
    scale = Fraction(10) ** rule.settlement_decimal_places
    q = mean * scale
    lo = q.numerator // q.denominator
    frac = q - lo
    if frac > Fraction(1, 2):
        return Fraction(lo + 1) / scale, None
    if frac < Fraction(1, 2):
        return Fraction(lo) / scale, None
    low, high = Fraction(lo) / scale, Fraction(lo + 1) / scale
    if rule.tie_behavior == "HALF_UP":
        return high, None
    if rule.tie_behavior == "HALF_EVEN":
        return (low if lo % 2 == 0 else high), None
    return None, (low, high)


def compare(value, strike, operator):
    """The payout criterion: 'yes' when value <operator> strike holds, else 'no'. Exact comparison."""
    v, k = exact(value), exact(strike)
    if operator == GTE:
        ok = v >= k
    elif operator == GT:
        ok = v > k
    elif operator == LTE:
        ok = v <= k
    elif operator == LT:
        ok = v < k
    else:
        raise ValueError(f"unknown comparison operator {operator!r}")
    return "yes" if ok else "no"


def official_outcome(unrounded_values, strike, rule):
    """unrounded sample values (or one mean in a list) -> dict with the rounded settlement value, the outcome and the
    reason. Fails closed on unknown rules, unknown precision, unknown comparator and outcome-changing ties."""
    out = {"rule_id": rule.rule_id if rule else None, "rule_fingerprint": rule.fingerprint() if rule else None,
           "settlement_value": None, "outcome": None, "tie_candidates": None, "status": None, "at_strike": False}
    mean = exact_mean(unrounded_values)
    if rule is None:
        out["status"] = "RULE_UNKNOWN"
        return out
    if not rule.known:
        out["status"] = "RULE_UNVERIFIED"
        return out
    if mean is None:
        out["status"] = "NO_VALUE"
        return out
    sv, tie = settle_value(mean, rule)
    if tie is not None:
        out["tie_candidates"] = tuple(float(t) for t in tie)
        if strike is None:
            out["status"] = "ROUNDING_TIE_UNRESOLVED"
            return out
        outs = {compare(t, strike, rule.comparison_operator) for t in tie}
        if len(outs) == 1:
            out["outcome"] = outs.pop()
            out["status"] = "ROUNDING_TIE_OUTCOME_INVARIANT"
        else:
            out["status"] = "ROUNDING_TIE_UNRESOLVED"
        return out
    out["settlement_value"] = float(sv)
    if strike is None:
        out["status"] = "NO_STRIKE"
        return out
    out["outcome"] = compare(sv, strike, rule.comparison_operator)
    out["at_strike"] = sv == exact(strike)
    out["status"] = "OK"
    return out
