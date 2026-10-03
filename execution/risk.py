"""
The RISK BOUNDARY (Step 6.5). The full risk manager is Step 6.6; execution only ENFORCES that a valid approval
reference exists and is respected:
    * the intent must carry risk_decision_id + risk_snapshot_hash, and the approval book must hold that decision with
      the same snapshot hash;
    * a veto (approved=False) is never overridden; a missing / expired / mismatched approval rejects the intent;
    * execution never increases the approved size, never worsens the approved price, never changes the approved side
      / market. Execution never MANUFACTURES an approval: RiskApprovalBook is only a lookup.
"""
import re
from dataclasses import dataclass

from execution.money import price, qty

_HEX64 = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class RiskApproval:
    decision_id: str
    snapshot_hash: str
    approved: bool
    market_ticker: str
    asset: str
    side: str
    max_contracts: object
    max_limit_price: object
    expires_at: int
    reason: str = ""

    def __post_init__(self):
        if not isinstance(self.approved, bool):
            raise ValueError("approved must be a bool")
        if not _HEX64.match(self.snapshot_hash or ""):
            raise ValueError("snapshot_hash must be 64 hex characters")
        object.__setattr__(self, "max_contracts", qty(self.max_contracts, "max_contracts", positive=True))
        object.__setattr__(self, "max_limit_price", price(self.max_limit_price, "max_limit_price"))


class RiskApprovalBook:
    """A read-only lookup of risk decisions made elsewhere (Step 6.6 will provide the real source)."""

    def __init__(self, approvals=()):
        self._by_id = {}
        for a in approvals:
            self._by_id[a.decision_id] = a

    def get(self, decision_id):
        return self._by_id.get(decision_id)


def check_intent_against_approval(intent, approval, now_ms):
    """-> (ok, reason). Every way an intent could exceed its approval fails."""
    if not intent.risk_decision_id or not intent.risk_snapshot_hash:
        return False, "RISK_APPROVAL_MISSING"
    if approval is None:
        return False, "RISK_APPROVAL_MISSING"
    if approval.snapshot_hash != intent.risk_snapshot_hash:
        return False, "RISK_SNAPSHOT_MISMATCH"
    if not approval.approved:
        return False, "RISK_VETO" + (f": {approval.reason}" if approval.reason else "")
    if now_ms >= approval.expires_at:
        return False, "RISK_APPROVAL_EXPIRED"
    if (approval.market_ticker, approval.asset, approval.side) != (intent.market_ticker, intent.asset, intent.side):
        return False, "RISK_APPROVAL_SCOPE_MISMATCH"
    if intent.requested_contracts > approval.max_contracts:
        return False, "SIZE_EXCEEDS_APPROVAL"
    if intent.max_limit_price > approval.max_limit_price:
        return False, "PRICE_EXCEEDS_APPROVAL"
    return True, "RISK_APPROVED"
