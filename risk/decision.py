"""
RiskDecision and RiskApproval (Step 6.6).

RiskDecision: immutable; decision APPROVE / REDUCE / VETO. Its constructor re-checks the core invariants and raises
RiskInvariantViolation if any is broken (a second line of defence behind risk.evaluate):
    approved_contracts >= 0;  approved_contracts <= requested_contracts;  approved_max_limit_price <= requested;
    APPROVE -> approved == requested;  REDUCE -> 0 < approved < requested;  VETO -> approved == 0;
    NO_CALL can never be approved (a NO_CALL candidate's decision is VETO).

risk_decision_id is DETERMINISTIC (versioned canonical JSON, never a random UUID) over: candidate_id, the candidate's
content hash, risk_snapshot_hash, risk_policy_fingerprint, decision, approved contracts, approved price and the reason
codes. The same candidate + snapshot + policy -> the same id; a changed snapshot or policy -> a different id.

RiskApproval: the authorisation artifact execution consumes. Only APPROVE / REDUCE decisions produce one. It is bound
(binding_hash, verified by execution.risk.check_intent_against_approval) to candidate_id, asset, market, side, the
approved size and price, the snapshot hash, the policy fingerprint, issued_at / expires_at (a finite TTL) and the
candidate's signal / model / calibration fingerprints (copied, never manufactured).
"""
import hashlib
from dataclasses import dataclass
from decimal import Decimal

from execution.money import canon
from execution.risk import RiskApproval as ExecutionApproval, approval_binding_hash
from risk.types import canonical_json

RISK_DECISION_SCHEMA_VERSION = 1
RISK_APPROVAL_SCHEMA_VERSION = 1
DECISION_ID_VERSION = "risk_decision_v1"
DECISIONS = ("APPROVE", "REDUCE", "VETO")


class RiskInvariantViolation(RuntimeError):
    pass


def decision_id(candidate_id, candidate_hash, snapshot_hash, policy_fingerprint, decision, approved_contracts,
                approved_max_limit_price, reason_codes):
    body = {"v": DECISION_ID_VERSION, "candidate_id": candidate_id, "candidate_hash": candidate_hash,
            "risk_snapshot_hash": snapshot_hash, "risk_policy_fingerprint": policy_fingerprint, "decision": decision,
            "approved_contracts": canon(approved_contracts),
            "approved_max_limit_price": None if approved_max_limit_price is None else canon(approved_max_limit_price),
            "reason_codes": list(reason_codes)}
    return "rd-" + hashlib.sha256(canonical_json(body).encode("ascii")).hexdigest()[:40]


@dataclass(frozen=True)
class RiskDecision:
    risk_decision_id: str
    candidate_id: str
    candidate_hash: str
    decision: str
    decision_ts: int
    expires_at: int
    market_ticker: str
    asset: str
    side: str
    signal_status: str
    requested_contracts: object
    approved_contracts: object
    requested_max_limit_price: object
    approved_max_limit_price: object
    calculated_worst_case_loss: object
    risk_snapshot_hash: str
    risk_policy_fingerprint: str
    reason_codes: tuple
    human_readable_reasons: tuple
    schema_version: int = RISK_DECISION_SCHEMA_VERSION

    def __post_init__(self):
        check_decision_invariants(self)

    @property
    def approved(self):
        return self.decision in ("APPROVE", "REDUCE")

    def to_dict(self):
        out = {}
        for k in self.__dataclass_fields__:
            v = getattr(self, k)
            out[k] = canon(v) if isinstance(v, Decimal) else (list(v) if isinstance(v, tuple) else v)
        return out


def check_decision_invariants(d):
    if d.decision not in DECISIONS:
        raise RiskInvariantViolation(f"unknown decision {d.decision!r}")
    a = d.approved_contracts
    if not isinstance(a, Decimal) or a < 0:
        raise RiskInvariantViolation("approved_contracts must be a Decimal >= 0")
    if d.decision == "VETO":
        if a != 0:
            raise RiskInvariantViolation("VETO must approve 0 contracts")
        return True
    r, rp, ap = d.requested_contracts, d.requested_max_limit_price, d.approved_max_limit_price
    if d.signal_status != "CALL":
        raise RiskInvariantViolation("only a CALL candidate can ever be approved (NO_CALL -> VETO)")
    if not isinstance(r, Decimal) or a > r:
        raise RiskInvariantViolation(f"approved {a} > requested {r}")
    if not isinstance(ap, Decimal) or not isinstance(rp, Decimal) or ap > rp:
        raise RiskInvariantViolation(f"approved price {ap} > requested {rp}")
    if d.decision == "APPROVE" and a != r:
        raise RiskInvariantViolation("APPROVE must approve exactly the requested size")
    if d.decision == "REDUCE" and not (0 < a < r):
        raise RiskInvariantViolation("REDUCE must approve 0 < size < requested")
    return True


@dataclass(frozen=True)
class RiskApproval:
    risk_decision_id: str
    candidate_id: str
    approved_contracts: object
    approved_max_limit_price: object
    approved_side: str
    market_ticker: str
    asset: str
    issued_at: int
    expires_at: int
    risk_snapshot_hash: str
    risk_policy_fingerprint: str
    signal_fingerprint: str
    model_fingerprint: str
    calibration_fingerprint: str
    schema_version: int = RISK_APPROVAL_SCHEMA_VERSION

    def __post_init__(self):
        if not (self.expires_at > self.issued_at):
            raise RiskInvariantViolation("an approval must have a finite, positive TTL")
        for f in ("signal_fingerprint", "model_fingerprint", "calibration_fingerprint"):
            if not isinstance(getattr(self, f), str) or not getattr(self, f):
                raise RiskInvariantViolation(f"an approval cannot exist without {f} provenance")

    def to_execution(self):
        """The execution-side artifact, sealed with its binding hash."""
        base = dict(decision_id=self.risk_decision_id, snapshot_hash=self.risk_snapshot_hash, approved=True,
                    market_ticker=self.market_ticker, asset=self.asset, side=self.approved_side,
                    max_contracts=self.approved_contracts, max_limit_price=self.approved_max_limit_price,
                    expires_at=self.expires_at, candidate_id=self.candidate_id,
                    policy_fingerprint=self.risk_policy_fingerprint, signal_fingerprint=self.signal_fingerprint,
                    model_fingerprint=self.model_fingerprint, calibration_fingerprint=self.calibration_fingerprint,
                    issued_at=self.issued_at)
        unsealed = ExecutionApproval(**base)
        return ExecutionApproval(**base, binding_hash=approval_binding_hash(unsealed))

    def to_dict(self):
        out = {}
        for k in self.__dataclass_fields__:
            v = getattr(self, k)
            out[k] = canon(v) if isinstance(v, Decimal) else v
        return out

    @classmethod
    def from_dict(cls, d):
        from execution.money import dec
        kw = dict(d)
        kw["approved_contracts"] = dec(kw["approved_contracts"])
        kw["approved_max_limit_price"] = dec(kw["approved_max_limit_price"])
        return cls(**kw)


def approval_for(decision, candidate):
    """APPROVE / REDUCE -> RiskApproval; VETO -> None. Provenance is COPIED from the candidate, never manufactured."""
    if not decision.approved:
        return None
    return RiskApproval(risk_decision_id=decision.risk_decision_id, candidate_id=decision.candidate_id,
                        approved_contracts=decision.approved_contracts,
                        approved_max_limit_price=decision.approved_max_limit_price, approved_side=decision.side,
                        market_ticker=decision.market_ticker, asset=decision.asset, issued_at=decision.decision_ts,
                        expires_at=decision.expires_at, risk_snapshot_hash=decision.risk_snapshot_hash,
                        risk_policy_fingerprint=decision.risk_policy_fingerprint,
                        signal_fingerprint=candidate.signal_fingerprint, model_fingerprint=candidate.model_fingerprint,
                        calibration_fingerprint=candidate.calibration_fingerprint)
