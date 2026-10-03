"""
"Execution may only REDUCE risk" - the invariants checked before anything reaches an adapter and on every observed
fill. A violation raises InvariantViolation (fail closed); nothing is adjusted silently.
"""
from execution.money import dec


class InvariantViolation(RuntimeError):
    pass


INVARIANTS = (
    "order side == intent side (never flipped: UP stays UP, DOWN stays DOWN)",
    "order market_ticker == intent market_ticker",
    "0 < order count <= intent requested_contracts (never increased)",
    "0 < order limit_price <= intent max_limit_price (never widened)",
    "order time_in_force == intent time_in_force; order expiry <= intent expires_at",
    "order client_order_id == the deterministic id of the intent's execution key",
    "filled quantity <= requested_contracts; every fill price <= max_limit_price",
    "an intent is never submitted at or after its expires_at",
    "provenance (model / calibration / signal fingerprints, risk decision id / snapshot) is never rewritten",
)


def assert_order_within_intent(intent, request, client_order_id):
    if request.side != intent.side:
        raise InvariantViolation(f"order side {request.side} != intent side {intent.side}")
    if request.market_ticker != intent.market_ticker:
        raise InvariantViolation("order market_ticker differs from the intent")
    if not (0 < dec(request.count) <= intent.requested_contracts):
        raise InvariantViolation(f"order count {request.count} exceeds requested {intent.requested_contracts}")
    if not (0 < dec(request.limit_price) <= intent.max_limit_price):
        raise InvariantViolation(f"order limit {request.limit_price} exceeds max {intent.max_limit_price}")
    if request.time_in_force != intent.time_in_force or request.expires_at > intent.expires_at:
        raise InvariantViolation("order time-in-force / expiry exceeds the intent")
    if request.client_order_id != client_order_id:
        raise InvariantViolation("order client_order_id is not the deterministic id of this execution")
    return True


def assert_fill_within_intent(intent, fill_price, total_filled):
    if dec(fill_price) > intent.max_limit_price:
        raise InvariantViolation(f"fill price {fill_price} above max_limit_price {intent.max_limit_price}")
    if dec(total_filled) > intent.requested_contracts:
        raise InvariantViolation(f"filled {total_filled} above requested {intent.requested_contracts}")
    return True


def assert_not_expired(intent, now_ms):
    if now_ms >= intent.expires_at:
        raise InvariantViolation(f"intent {intent.intent_id} expired at {intent.expires_at} (now {now_ms})")
    return True


def assert_provenance_unchanged(intent, stored_intent):
    for k in ("model_id", "model_fingerprint", "calibration_fingerprint", "signal_fingerprint", "risk_decision_id",
              "risk_snapshot_hash", "side", "market_ticker", "asset"):
        if str(getattr(intent, k)) != str(stored_intent[k]):
            raise InvariantViolation(f"{k} differs from the recorded intent")
    return True
