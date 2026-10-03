"""
Deterministic execution identity (never random, never wall-clock, never a process counter).

execution_key (EXECUTION_KEY_VERSION "exec_key_v1")
    sha256 of the canonical JSON of {"v": "exec_key_v1", <EXECUTION_KEY_FIELDS>}:
        * keys sorted, separators (",", ":"), ASCII only  -> field ORDER can never change the hash
        * Decimals in canonical text (money.canon: no exponent, no trailing zeros; "5.00" == "5")
        * integers as JSON integers, strings verbatim
    Identical logical intent -> identical key (also after a restart); a different intent_id -> a different key.

client_order_id (CLIENT_ORDER_ID_VERSION "coid_v1")
    "x1" + the first 30 hex characters of sha256("coid_v1|" + execution_key)  -> 32 characters, [a-z0-9] only,
    a compact, future-safe form for an exchange's client-order-id length / character limits. The same logical order
    always maps to the same client_order_id; Step 6.5 uses it only with the paper adapter.
"""
import hashlib
import json

from execution.money import canon

EXECUTION_KEY_VERSION = "exec_key_v1"
EXECUTION_KEY_FIELDS = ("intent_id", "market_ticker", "asset", "side", "requested_contracts", "max_limit_price",
                        "time_in_force", "expires_at", "risk_decision_id")
CLIENT_ORDER_ID_VERSION = "coid_v1"
CLIENT_ORDER_ID_PREFIX = "x1"
CLIENT_ORDER_ID_HEX = 30


def _norm(v):
    if isinstance(v, bool):
        raise ValueError("booleans are not identity values")
    if isinstance(v, int) or isinstance(v, str):
        return v
    return canon(v)                                           # Decimal -> canonical text


def canonical_identity(intent):
    body = {"v": EXECUTION_KEY_VERSION}
    for f in EXECUTION_KEY_FIELDS:
        body[f] = _norm(getattr(intent, f))
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def execution_key(intent):
    return hashlib.sha256(canonical_identity(intent).encode("ascii")).hexdigest()


def client_order_id(exec_key):
    if not (isinstance(exec_key, str) and len(exec_key) == 64 and all(c in "0123456789abcdef" for c in exec_key)):
        raise ValueError("client_order_id needs a 64-hex execution key")
    h = hashlib.sha256(f"{CLIENT_ORDER_ID_VERSION}|{exec_key}".encode("ascii")).hexdigest()
    return CLIENT_ORDER_ID_PREFIX + h[:CLIENT_ORDER_ID_HEX]
