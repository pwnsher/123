"""
RiskPolicy (Step 6.6): a versioned, immutable set of CONFIGURABLE limits. Nothing here is a research-validated or
optimised threshold: values come from configuration (tests construct their own; config/risk_policy_shadow_v1.json is a
TEST / SHADOW default, NOT research-validated, NOT production-approved).

Boundary semantics (documented and tested):
    * a value <= a configured MAXIMUM is allowed; a value > the maximum is capped or vetoed;
    * a value >= a configured MINIMUM is allowed; a value < the minimum is vetoed;
    * exception, by definition: max_consecutive_losses = N latches the breaker when the N-th consecutive loss is
      recorded (count >= N).

risk_policy_fingerprint covers every policy value plus the risk-rules version and the evaluation order, so any
semantic change (a limit, health behaviour, staleness threshold, TTL, group mapping, rule version) changes it. It
never includes runtime state.
"""
import hashlib
from dataclasses import dataclass, fields
from decimal import Decimal

from execution.money import canon, dec
from risk.reasons import EVALUATION_ORDER, RISK_RULES_VERSION
from risk.types import RiskInputError, canonical_json

RISK_POLICY_SCHEMA_VERSION = 1
POLICY_FINGERPRINT_VERSION = "risk_policy_fp_v1"
SHADOW_LABEL = "TEST / SHADOW DEFAULT - NOT RESEARCH-VALIDATED - NOT PRODUCTION-APPROVED"

_DEC = ("max_contracts_per_trade", "max_notional_per_trade", "max_loss_per_trade", "max_fraction_equity_per_trade",
        "max_asset_open_risk", "max_asset_gross_notional", "max_portfolio_open_risk", "max_portfolio_gross_notional",
        "max_crypto_group_open_risk", "max_same_direction_crypto_risk", "max_daily_realized_loss",
        "max_daily_total_loss", "max_rolling_drawdown", "max_spread", "min_executable_depth",
        "minimum_account_equity", "extra_reserve_per_contract")
_DEC_OPTIONAL = ("unknown_fee_reserve_per_contract", "unknown_slippage_reserve_per_contract", "max_entry_price")
_INT = ("policy_version", "max_consecutive_losses", "max_quote_age_ms", "max_feature_age_ms", "max_decision_age_ms",
        "max_snapshot_age_ms", "approval_ttl_ms", "schema_version")
_BOOL = ("allow_degraded_feed", "allow_degraded_model", "allow_degraded_calibration", "allow_degraded_settlement",
         "allow_degraded_execution", "require_known_fee", "require_known_slippage", "require_known_ev",
         "require_known_depth", "require_known_spread")
HEALTH_ALLOW = {"feed_health": "allow_degraded_feed", "model_health": "allow_degraded_model",
                "calibration_health": "allow_degraded_calibration", "settlement_health": "allow_degraded_settlement",
                "execution_health": "allow_degraded_execution"}


@dataclass(frozen=True)
class RiskPolicy:
    policy_id: str
    policy_version: int
    label: str
    max_contracts_per_trade: object
    max_notional_per_trade: object
    max_loss_per_trade: object
    max_fraction_equity_per_trade: object
    max_asset_open_risk: object
    max_asset_gross_notional: object
    max_portfolio_open_risk: object
    max_portfolio_gross_notional: object
    max_crypto_group_open_risk: object
    max_same_direction_crypto_risk: object
    max_daily_realized_loss: object
    max_daily_total_loss: object
    max_rolling_drawdown: object
    max_consecutive_losses: int
    max_spread: object
    min_executable_depth: object
    max_quote_age_ms: int
    max_feature_age_ms: int
    max_decision_age_ms: int
    max_snapshot_age_ms: int
    minimum_account_equity: object
    allow_degraded_feed: bool
    allow_degraded_model: bool
    allow_degraded_calibration: bool
    allow_degraded_settlement: bool
    allow_degraded_execution: bool
    require_known_fee: bool
    require_known_slippage: bool
    require_known_ev: bool
    approval_ttl_ms: int
    asset_group_map: object              # {asset: group} (stored as a sorted tuple of pairs)
    require_known_depth: bool = True
    require_known_spread: bool = True
    unknown_fee_reserve_per_contract: object = None     # used only when a fee is UNKNOWN and not required
    unknown_slippage_reserve_per_contract: object = None
    extra_reserve_per_contract: object = "0"            # an explicitly modelled per-contract execution reserve
    max_entry_price: object = None                      # optional price TIGHTENING (never widening)
    schema_version: int = RISK_POLICY_SCHEMA_VERSION

    def __post_init__(self):
        for f in fields(self):
            v = getattr(self, f.name)
            if f.name in _DEC or (f.name in _DEC_OPTIONAL and v is not None):
                try:
                    v = dec(v, f.name)
                except ValueError as e:
                    raise RiskInputError(str(e))
            elif f.name in _INT:
                if isinstance(v, bool) or not isinstance(v, int):
                    raise RiskInputError(f"{f.name}: {v!r} must be an int")
            elif f.name in _BOOL:
                if not isinstance(v, bool):
                    raise RiskInputError(f"{f.name}: {v!r} must be a bool")
            elif f.name == "asset_group_map":
                items = v.items() if isinstance(v, dict) else v
                v = tuple(sorted((str(a), str(g)) for a, g in items))
            elif f.name in ("policy_id", "label") and not isinstance(v, str):
                raise RiskInputError(f"{f.name} must be a str")
            object.__setattr__(self, f.name, v)

    def group_of(self, asset):
        return dict(self.asset_group_map).get(asset)

    def problems(self):
        """Semantic problems (-> INVALID_POLICY hard veto)."""
        out = []
        for n in _DEC:
            if getattr(self, n) < 0:
                out.append(f"{n} < 0")
        for n in _DEC_OPTIONAL:
            v = getattr(self, n)
            if v is not None and v < 0:
                out.append(f"{n} < 0")
        if not (Decimal(0) < self.max_fraction_equity_per_trade <= Decimal(1)):
            out.append("max_fraction_equity_per_trade outside (0, 1]")
        if self.max_entry_price is not None and not (Decimal(0) < self.max_entry_price <= Decimal(1)):
            out.append("max_entry_price outside (0, 1]")
        if self.max_consecutive_losses < 1:
            out.append("max_consecutive_losses < 1")
        for n in ("max_quote_age_ms", "max_feature_age_ms", "max_decision_age_ms", "max_snapshot_age_ms"):
            if getattr(self, n) < 0:
                out.append(f"{n} < 0")
        if self.approval_ttl_ms <= 0:
            out.append("approval_ttl_ms must be > 0 (approvals are finite)")
        if not self.asset_group_map:
            out.append("asset_group_map is empty")
        if not self.policy_id or self.policy_version < 1:
            out.append("policy_id / policy_version missing")
        return out

    def to_dict(self):
        out = {}
        for f in fields(self):
            v = getattr(self, f.name)
            if f.name == "asset_group_map":
                out[f.name] = dict(v)
            elif isinstance(v, Decimal):
                out[f.name] = canon(v)
            else:
                out[f.name] = v
        return out

    @classmethod
    def from_dict(cls, d):
        names = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in names})


def risk_policy_fingerprint(policy):
    body = {"v": POLICY_FINGERPRINT_VERSION, "rules": RISK_RULES_VERSION, "evaluation_order": list(EVALUATION_ORDER),
            "policy": policy.to_dict()}
    return hashlib.sha256(canonical_json(body).encode("ascii")).hexdigest()


def load_policy(path):
    import json
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    return RiskPolicy.from_dict(d["policy"])
