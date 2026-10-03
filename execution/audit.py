"""
Prediction / trade audit record (AUDIT_SCHEMA_VERSION 1). Keeps three questions APART for later analysis:
    1. prediction quality   raw / calibrated probability, predicted direction, settlement outcome
    2. decision quality     CALL / NO_CALL, market price / bid / ask / depth, estimated fee / slippage / net EV,
                            risk approval or veto and its reason
    3. execution quality    requested / submitted / filled size, average fill, actual fees, trade PnL
A correct prediction can still be a bad trade, and a losing trade does not prove the probability was wrong. Nothing
here computes accuracy or any aggregate; fields not supplied stay None (unknown), never zero.
"""
from dataclasses import asdict, dataclass
from typing import Optional

from execution.money import jsonable

AUDIT_SCHEMA_VERSION = 1
DIRECTIONS = ("UP", "DOWN")
DECISIONS = ("CALL", "NO_CALL")


@dataclass(frozen=True)
class AuditRecord:
    market: str
    asset: str
    checkpoint: Optional[str] = None
    raw_probability: Optional[str] = None
    calibrated_probability: Optional[str] = None
    predicted_direction: Optional[str] = None
    market_price: Optional[str] = None
    bid: Optional[str] = None
    ask: Optional[str] = None
    available_depth: Optional[str] = None
    estimated_fee: Optional[str] = None
    estimated_slippage: Optional[str] = None
    estimated_net_ev: Optional[str] = None
    decision: Optional[str] = None
    risk_approved: Optional[bool] = None
    risk_reason: Optional[str] = None

    def __post_init__(self):
        if self.predicted_direction is not None and self.predicted_direction not in DIRECTIONS:
            raise ValueError(f"predicted_direction {self.predicted_direction!r} not in {DIRECTIONS}")
        if self.decision is not None and self.decision not in DECISIONS:
            raise ValueError(f"decision {self.decision!r} not in {DECISIONS}")

    def to_dict(self):
        return {k: jsonable(v) for k, v in asdict(self).items()}


def prediction_correct(predicted_direction, settlement_result):
    """True / False only when BOTH are known; otherwise None (never guessed)."""
    if predicted_direction not in DIRECTIONS or settlement_result not in ("yes", "no"):
        return None
    return (predicted_direction == "UP") == (settlement_result == "yes")
