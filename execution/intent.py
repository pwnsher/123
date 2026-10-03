"""
OrderIntent: the strict, immutable request the execution layer receives (from the future calibrated signal + risk
system). Every identity / provenance field is validated; money, prices and quantities are exact Decimals; an unknown
estimate must be stated as UNKNOWN (None is refused, and UNKNOWN is never turned into zero).

The execution layer may only REDUCE what an intent asks for (reject, fill less, fill at an equal or better price,
cancel, expire, halt). It can never flip the side, raise the size, widen the price cap, invent a risk approval or
rewrite provenance - see invariants.py.
"""
import hashlib
import json
import re
from dataclasses import dataclass, fields

from execution.money import UNKNOWN, canon, dec, jsonable, money_or_unknown, price, prob, qty

ORDER_INTENT_SCHEMA_VERSION = 1
SIDES = ("YES", "NO")                               # buy YES contracts / buy NO contracts
DIRECTION_OF_SIDE = {"YES": "UP", "NO": "DOWN"}     # crypto 15-minute markets: YES = settles at / above the strike
TIME_IN_FORCE = ("IMMEDIATE_OR_CANCEL", "FILL_OR_KILL", "GOOD_TIL_EXPIRY")
ASSETS = ("BTC", "ETH", "SOL", "XRP")

_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$")
_TICKER = re.compile(r"^[A-Z0-9][A-Z0-9._-]{2,63}$")
_HEX = re.compile(r"^[0-9a-f]{16,128}$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
EPOCH_MS_MIN, EPOCH_MS_MAX = 1_600_000_000_000, 4_102_444_800_000


class IntentError(ValueError):
    """An OrderIntent failed validation (fail closed: it never reaches the state machine)."""


def _ms(v, name):
    if isinstance(v, bool) or not isinstance(v, int) or not (EPOCH_MS_MIN <= v < EPOCH_MS_MAX):
        raise IntentError(f"{name}: {v!r} is not an integer epoch-millisecond timestamp")
    return v


def _match(rx, v, name):
    if not isinstance(v, str) or not rx.match(v):
        raise IntentError(f"{name}: {v!r} is not a valid identifier")
    return v


@dataclass(frozen=True)
class OrderIntent:
    intent_id: str
    candidate_id: str
    created_at: int
    market_ticker: str
    asset: str
    side: str
    requested_contracts: object
    max_limit_price: object
    time_in_force: str
    decision_ts: int
    expires_at: int
    raw_probability: object
    calibrated_probability: object
    market_price: object
    estimated_fee: object
    estimated_slippage: object
    estimated_net_ev: object
    model_id: str
    model_fingerprint: str
    calibration_fingerprint: str
    signal_fingerprint: str
    risk_decision_id: str
    risk_snapshot_hash: str
    schema_version: int = ORDER_INTENT_SCHEMA_VERSION

    def __post_init__(self):
        s = object.__setattr__
        try:
            if self.schema_version != ORDER_INTENT_SCHEMA_VERSION:
                raise IntentError(f"unsupported OrderIntent schema {self.schema_version!r}")
            for f in ("intent_id", "candidate_id", "model_id", "risk_decision_id"):
                _match(_ID, getattr(self, f), f)
            _match(_TICKER, self.market_ticker, "market_ticker")
            if self.asset not in ASSETS:
                raise IntentError(f"asset {self.asset!r} not in {ASSETS}")
            if self.side not in SIDES:
                raise IntentError(f"side {self.side!r} not in {SIDES}")
            if self.time_in_force not in TIME_IN_FORCE:
                raise IntentError(f"time_in_force {self.time_in_force!r} not in {TIME_IN_FORCE}")
            for f in ("model_fingerprint", "calibration_fingerprint", "signal_fingerprint"):
                _match(_HEX, getattr(self, f), f)
            _match(_HEX64, self.risk_snapshot_hash, "risk_snapshot_hash")
            for f in ("created_at", "decision_ts", "expires_at"):
                _ms(getattr(self, f), f)
            if not (self.decision_ts <= self.created_at < self.expires_at):
                raise IntentError("timestamps must satisfy decision_ts <= created_at < expires_at")
            s(self, "requested_contracts", qty(self.requested_contracts, "requested_contracts", positive=True))
            s(self, "max_limit_price", price(self.max_limit_price, "max_limit_price"))
            s(self, "raw_probability", prob(self.raw_probability, "raw_probability"))
            s(self, "calibrated_probability", prob(self.calibrated_probability, "calibrated_probability"))
            s(self, "market_price", UNKNOWN if self.market_price is UNKNOWN else price(self.market_price, "market_price"))
            s(self, "estimated_fee", money_or_unknown(self.estimated_fee, "estimated_fee", allow_negative=False))
            s(self, "estimated_slippage", money_or_unknown(self.estimated_slippage, "estimated_slippage",
                                                           allow_negative=False))
            s(self, "estimated_net_ev", money_or_unknown(self.estimated_net_ev, "estimated_net_ev"))
        except IntentError:
            raise
        except (ValueError, TypeError) as e:
            raise IntentError(str(e)) from e

    @property
    def predicted_direction(self):
        return DIRECTION_OF_SIDE[self.side]

    def to_dict(self):
        return {f.name: jsonable(getattr(self, f.name)) for f in fields(self)}

    @classmethod
    def from_dict(cls, d):
        d = dict(d)
        for f in ("market_price", "estimated_fee", "estimated_slippage", "estimated_net_ev"):
            if d.get(f) == "UNKNOWN":
                d[f] = UNKNOWN
        return cls(**d)

    def content_hash(self):
        """sha256 of the COMPLETE canonical intent (every field), to detect an intent_id reused with other content."""
        return hashlib.sha256(json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def provenance(intent):
    """The provenance the execution layer must carry unchanged (never overwritten)."""
    return {k: getattr(intent, k) for k in ("model_id", "model_fingerprint", "calibration_fingerprint",
                                            "signal_fingerprint", "risk_decision_id", "risk_snapshot_hash")}


__all__ = ["OrderIntent", "IntentError", "SIDES", "TIME_IN_FORCE", "ASSETS", "DIRECTION_OF_SIDE",
           "ORDER_INTENT_SCHEMA_VERSION", "provenance", "canon", "dec"]
