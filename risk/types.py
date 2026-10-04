"""
Risk inputs (Step 6.6): RiskCandidate (a trade PROPOSED by the future signal engine) and RiskSnapshot (the exact
portfolio / system state a decision is made against). Both are frozen dataclasses with exact Decimal financial values.

Representation rules (enforced at construction, raising RiskInputError):
    * financial / probability / quantity fields are Decimal (from str / int / Decimal) or the explicit UNKNOWN
      sentinel; binary floats, bools and None are refused - a missing value must be stated as UNKNOWN, it is never
      coerced to 0 or to "safe";
    * timestamps / ages / counts are int or UNKNOWN; text fields are str (fingerprints may be UNKNOWN).
SEMANTIC validity (ranges, enumerations, consistency) is NOT a construction error: risk.evaluate reports it as an
INVALID_CANDIDATE / INVALID_SNAPSHOT hard veto, so a bad input still yields an auditable decision.
"""
import hashlib
import json
from dataclasses import dataclass, fields

from execution.money import UNKNOWN, canon, dec

RISK_CANDIDATE_SCHEMA_VERSION = 1
RISK_SNAPSHOT_SCHEMA_VERSION = 1
SNAPSHOT_HASH_VERSION = "risk_snapshot_v1"

HEALTH = ("PASS", "DEGRADED", "FAIL", "UNKNOWN", "UNAVAILABLE")
HEALTH_FIELDS = ("feed_health", "model_health", "calibration_health", "settlement_health", "execution_health")
SIGNAL_STATUSES = ("CALL", "NO_CALL")
DIRECTIONS = ("UP", "DOWN")
# execution facts supplied by the execution layer (risk never repairs or infers them)
EXEC_STATES = ("CLEAR", "ACTIVE_ENTRY", "OPEN_POSITION", "EXECUTION_UNKNOWN", "HALTED_FOR_RECONCILIATION",
               "POSITION_MISMATCH", "FILL_MISMATCH", "ACCOUNTING_INCOMPLETE_UNSAFE", "UNKNOWN")
EXEC_UNRESOLVED = ("EXECUTION_UNKNOWN", "HALTED_FOR_RECONCILIATION", "ACCOUNTING_INCOMPLETE_UNSAFE", "UNKNOWN")
EXEC_MISMATCH = ("POSITION_MISMATCH", "FILL_MISMATCH")
EXEC_EXPOSURE = ("ACTIVE_ENTRY", "OPEN_POSITION")


class RiskInputError(ValueError):
    """A risk input that cannot even be represented safely (e.g. a binary float)."""


def _num(v, name):
    if v is UNKNOWN:
        return UNKNOWN
    try:
        return dec(v, name)
    except ValueError as e:
        raise RiskInputError(str(e))


def _int(v, name):
    if v is UNKNOWN:
        return UNKNOWN
    if isinstance(v, bool) or not isinstance(v, int):
        raise RiskInputError(f"{name}: {v!r} must be an int (or UNKNOWN)")
    return v


def _text(v, name, unknown_ok=False):
    if v is UNKNOWN and unknown_ok:
        return UNKNOWN
    if not isinstance(v, str):
        raise RiskInputError(f"{name}: {v!r} must be a str" + (" (or UNKNOWN)" if unknown_ok else ""))
    return v


def _jsonable(v):
    if v is UNKNOWN:
        return "UNKNOWN"
    if hasattr(v, "as_tuple"):
        return canon(v)
    return v


def canonical_json(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


class _Frozen:
    _NUM = ()
    _INT = ()
    _TEXT_UNKNOWN_OK = ()

    def __post_init__(self):
        for f in fields(self):
            v = getattr(self, f.name)
            if f.name in self._NUM:
                v = _num(v, f.name)
            elif f.name in self._INT:
                v = _int(v, f.name)
            else:
                v = _text(v, f.name, f.name in self._TEXT_UNKNOWN_OK)
            object.__setattr__(self, f.name, v)

    def to_dict(self):
        return {f.name: _jsonable(getattr(self, f.name)) for f in fields(self)}

    @classmethod
    def from_dict(cls, d):
        kw = {}
        for f in fields(cls):
            v = d[f.name] if f.name in d else UNKNOWN
            if v == "UNKNOWN":
                v = UNKNOWN
            kw[f.name] = v
        return cls(**kw)

    def content_hash(self):
        return hashlib.sha256(canonical_json(self.to_dict()).encode("ascii")).hexdigest()


@dataclass(frozen=True)
class RiskCandidate(_Frozen):
    candidate_id: str
    created_at: int
    market_ticker: str
    asset: str
    side: str
    signal_status: str
    predicted_direction: object
    requested_contracts: object
    requested_max_limit_price: object
    decision_ts: int
    expires_at: int
    raw_probability: object
    calibrated_probability: object
    market_bid: object
    market_ask: object
    current_executable_price: object
    available_depth: object
    estimated_fee: object                # TOTAL estimated fee for requested_contracts (Decimal | UNKNOWN)
    estimated_slippage: object           # TOTAL estimated slippage for requested_contracts (Decimal | UNKNOWN)
    estimated_net_ev: object             # the candidate's stated net EV (provenance; never changed by risk)
    model_id: object
    model_fingerprint: object
    calibration_fingerprint: object
    signal_fingerprint: object
    checkpoint: str
    market_close_ts: object
    schema_version: int = RISK_CANDIDATE_SCHEMA_VERSION

    _NUM = ("requested_contracts", "requested_max_limit_price", "raw_probability", "calibrated_probability",
            "market_bid", "market_ask", "current_executable_price", "available_depth", "estimated_fee",
            "estimated_slippage", "estimated_net_ev")
    _INT = ("created_at", "decision_ts", "expires_at", "market_close_ts", "schema_version")
    _TEXT_UNKNOWN_OK = ("predicted_direction", "model_id", "model_fingerprint", "calibration_fingerprint",
                        "signal_fingerprint")


@dataclass(frozen=True)
class RiskSnapshot(_Frozen):
    snapshot_id: str
    captured_at: int
    account_equity: object
    available_cash: object
    day_start_equity: object
    day_peak_equity: object
    daily_realized_pnl: object
    daily_unrealized_pnl: object
    open_positions_count: object
    total_open_risk: object
    total_gross_notional: object
    asset_open_risk: object
    asset_gross_notional: object
    same_direction_crypto_risk: object   # open risk of the candidate's exposure group in the candidate's direction
    opposite_direction_crypto_risk: object  # ... in the opposite direction (counts in the group's gross risk)
    consecutive_losses: object
    market_ticker: str
    asset: str
    side: str
    current_position_size: object
    current_market_open_risk: object
    feed_health: str
    model_health: str
    calibration_health: str
    settlement_health: str
    execution_health: str
    quote_age_ms: object
    feature_age_ms: object
    model_decision_age_ms: object
    spread: object
    available_depth: object
    market_minutes_remaining: object
    exposure_group: str                  # the group the provider aggregated (must match policy.asset_group_map)
    market_execution_state: str          # EXEC_STATES for this market (from the execution layer)
    asset_execution_state: str           # worst EXEC_STATES across the asset's other markets
    schema_version: int = RISK_SNAPSHOT_SCHEMA_VERSION

    _NUM = ("account_equity", "available_cash", "day_start_equity", "day_peak_equity", "daily_realized_pnl",
            "daily_unrealized_pnl", "total_open_risk", "total_gross_notional", "asset_open_risk",
            "asset_gross_notional", "same_direction_crypto_risk", "opposite_direction_crypto_risk",
            "current_position_size", "current_market_open_risk", "spread", "available_depth",
            "market_minutes_remaining")
    _INT = ("captured_at", "open_positions_count", "consecutive_losses", "quote_age_ms", "feature_age_ms",
            "model_decision_age_ms", "schema_version")


def risk_snapshot_hash(snapshot):
    """Deterministic, versioned hash of EVERY snapshot field (stable across processes / machines)."""
    body = {"v": SNAPSHOT_HASH_VERSION, "snapshot": snapshot.to_dict()}
    return hashlib.sha256(canonical_json(body).encode("ascii")).hexdigest()
