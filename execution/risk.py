"""
The RISK BOUNDARY (Step 6.5; approval binding + single use added in Step 6.6). The risk manager lives in risk/
(Step 6.6); execution only ENFORCES that a valid approval reference exists and is respected:
    * the intent must carry risk_decision_id + risk_snapshot_hash, and the approval book must hold that decision with
      the same snapshot hash;
    * a veto (approved=False) is never overridden; a missing / expired / mismatched approval rejects the intent;
    * execution never increases the approved size, never worsens the approved price, never changes the approved side
      / market. Execution never MANUFACTURES an approval: RiskApprovalBook is only a lookup.
Step 6.6 (approval validation only - no other execution semantics changed):
    * an approval may carry its binding (candidate_id, risk policy fingerprint, signal / model / calibration
      fingerprints, issued_at) sealed by binding_hash (approval_binding_hash): a tampered approval, an approval for
      another candidate, or one whose provenance differs from the intent is rejected;
    * a book may require the CURRENT risk policy fingerprint (an approval issued under another policy is rejected);
    * single logical use: verify() checks, consume() binds the approval to ONE intent_id (a replay of the same intent
      is idempotent; any other intent is rejected). This in-memory book is the manual / test book; risk.manager
      provides the durable, restart-safe book backed by the risk store.
"""
import hashlib
import json
import re
from dataclasses import dataclass

from execution.money import canon, price, qty

APPROVAL_BINDING_VERSION = "approval_binding_v1"
BINDING_FIELDS = ("decision_id", "snapshot_hash", "approved", "market_ticker", "asset", "side", "max_contracts",
                  "max_limit_price", "expires_at", "candidate_id", "policy_fingerprint", "signal_fingerprint",
                  "model_fingerprint", "calibration_fingerprint", "issued_at")
PROVENANCE_FIELDS = ("signal_fingerprint", "model_fingerprint", "calibration_fingerprint")

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
    candidate_id: object = None
    policy_fingerprint: object = None
    signal_fingerprint: object = None
    model_fingerprint: object = None
    calibration_fingerprint: object = None
    issued_at: object = None
    binding_hash: object = None

    def __post_init__(self):
        if not isinstance(self.approved, bool):
            raise ValueError("approved must be a bool")
        if not _HEX64.match(self.snapshot_hash or ""):
            raise ValueError("snapshot_hash must be 64 hex characters")
        object.__setattr__(self, "max_contracts", qty(self.max_contracts, "max_contracts", positive=True))
        object.__setattr__(self, "max_limit_price", price(self.max_limit_price, "max_limit_price"))


def approval_binding_hash(approval):
    """Deterministic seal over every field that binds an approval (versioned canonical JSON, Decimal canonical text)."""
    body = {"v": APPROVAL_BINDING_VERSION}
    for f in BINDING_FIELDS:
        v = getattr(approval, f)
        body[f] = canon(v) if f in ("max_contracts", "max_limit_price") else v
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=True).encode("ascii")).hexdigest()


class RiskApprovalBook:
    """A lookup of risk decisions made elsewhere (manual / test book; risk.manager provides the durable one).
    `required_policy_fingerprint`, when set, rejects approvals issued under another risk policy."""

    def __init__(self, approvals=(), required_policy_fingerprint=None):
        self._by_id = {}
        self._consumed = {}                       # decision_id -> intent_id (single logical use, in memory)
        self.required_policy_fingerprint = required_policy_fingerprint
        for a in approvals:
            self._by_id[a.decision_id] = a

    def get(self, decision_id):
        return self._by_id.get(decision_id)

    def verify(self, intent, now_ms):
        return check_intent_against_approval(intent, self.get(intent.risk_decision_id), now_ms,
                                             self.required_policy_fingerprint)

    def consume(self, intent, execution_key):
        """Bind the approval to this intent. -> (ok, reason). The same intent again is an idempotent replay."""
        prior = self._consumed.get(intent.risk_decision_id)
        if prior is not None and prior != intent.intent_id:
            return False, f"RISK_APPROVAL_ALREADY_CONSUMED by {prior}"
        self._consumed[intent.risk_decision_id] = intent.intent_id
        return True, "RISK_APPROVAL_CONSUMED"


def check_intent_against_approval(intent, approval, now_ms, required_policy_fingerprint=None):
    """-> (ok, reason). Every way an intent could exceed its approval fails."""
    if not intent.risk_decision_id or not intent.risk_snapshot_hash:
        return False, "RISK_APPROVAL_MISSING"
    if approval is None:
        return False, "RISK_APPROVAL_MISSING"
    if approval.binding_hash is not None and approval.binding_hash != approval_binding_hash(approval):
        return False, "RISK_APPROVAL_TAMPERED"
    if approval.candidate_id is not None and approval.candidate_id != intent.candidate_id:
        return False, "RISK_APPROVAL_CANDIDATE_MISMATCH"
    if required_policy_fingerprint is not None and approval.policy_fingerprint != required_policy_fingerprint:
        return False, "RISK_POLICY_CHANGED"
    for f in PROVENANCE_FIELDS:
        if getattr(approval, f) is not None and getattr(approval, f) != getattr(intent, f):
            return False, "RISK_APPROVAL_PROVENANCE_MISMATCH"
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
