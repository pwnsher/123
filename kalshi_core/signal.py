"""
SignalDecision — the standard, serialisable record of ONE strategy decision.

It REPRESENTS the legacy output; it never computes a probability, threshold or gate. The
legacy evaluate() remains authoritative (see kalshi_core.adapter). Fields the legacy model
does not produce are Optional and stay None rather than being invented, e.g.:
    calibrated_probability_up   None: the live model applies NO calibration step
    volatility_per_min          None: evaluate() does not return its sigma

Units follow the legacy code: probabilities in [0, 1]; confidence, prices and edges in
CENTS / percent points (0-100); time remaining in minutes.

Serialisation: to_dict()/from_dict() and to_json()/from_json() round-trip exactly
(enums as their string values, sorted keys), so decisions can be journaled as JSON lines
and replayed later.
"""
import json
from dataclasses import dataclass, field, asdict, fields
from enum import Enum
from typing import Optional, List, Dict, Any

from kalshi_core.no_call import NoCallReason
from kalshi_core.data_health import FeedHealth

SIGNAL_SCHEMA_VERSION = 1


class Decision(str, Enum):
    CALL = "CALL"          # legacy signal=True (a callout / paper entry is eligible)
    NO_CALL = "NO_CALL"


class Side(str, Enum):
    UP = "UP"
    DOWN = "DOWN"


@dataclass
class SignalDecision:
    # identity
    asset: str
    decision: Decision
    market_ticker: Optional[str] = None
    series: Optional[str] = None
    schema_version: int = SIGNAL_SCHEMA_VERSION
    # time
    timestamp_epoch_ms: Optional[int] = None        # when the spot price was observed (legacy spot_observed_ts)
    market_close_time: Optional[str] = None         # ISO-8601, as returned by Kalshi
    time_remaining_min: Optional[float] = None      # legacy "remain" (rounded to 0.1 min)
    # prices
    strike: Optional[float] = None
    underlying_price: Optional[float] = None        # unrounded spot (legacy spot_raw)
    yes_bid: Optional[float] = None                 # cents; legacy up_bid
    yes_ask: Optional[float] = None                 # cents; legacy up_ask
    no_bid: Optional[float] = None                  # cents; legacy dn_bid
    no_ask: Optional[float] = None                  # cents; legacy dn_ask
    side_ask: Optional[float] = None                # ask of the favoured side
    # model
    side: Optional[Side] = None                     # favoured side (legacy fav)
    raw_probability_up: Optional[float] = None      # legacy p_up / 100
    raw_probability_down: Optional[float] = None    # 1 - raw_probability_up
    calibrated_probability_up: Optional[float] = None
    calibration_method: str = "NONE"
    confidence_pct: Optional[float] = None          # legacy conf = max(p_up, 100 - p_up)
    raw_edge_cents: Optional[float] = None
    net_edge_cents: Optional[float] = None
    recommended_stop_cents: Optional[float] = None  # legacy rec_stop
    stop_loss_fraction: Optional[float] = None      # legacy sl_pct / 100
    volatility_per_min: Optional[float] = None
    regime: Dict[str, Any] = field(default_factory=dict)   # display indicators the legacy code already computes
    # provenance
    model_id: Optional[str] = None
    strategy_fingerprint: Optional[str] = None
    # decision detail
    no_call_reasons: List[NoCallReason] = field(default_factory=list)
    legacy_status: Optional[str] = None
    legacy_reason: Optional[str] = None
    legacy_verdict: Optional[str] = None
    data_health: List[FeedHealth] = field(default_factory=list)
    perp_overlay: Optional[Dict[str, Any]] = None   # live veto gate result, if one was consulted

    def __post_init__(self):
        if self.decision == Decision.CALL and self.no_call_reasons:
            raise ValueError("a CALL cannot carry NO_CALL reasons")
        if self.decision == Decision.NO_CALL and not self.no_call_reasons:
            raise ValueError("a NO_CALL must carry at least one reason")

    @property
    def is_call(self):
        return self.decision == Decision.CALL

    # ---------- serialisation ----------
    def to_dict(self):
        d = asdict(self)
        d["decision"] = self.decision.value
        d["side"] = self.side.value if self.side is not None else None
        d["no_call_reasons"] = [r.value for r in self.no_call_reasons]
        d["data_health"] = [h.to_dict() for h in self.data_health]
        return d

    @classmethod
    def from_dict(cls, d):
        known = {f.name for f in fields(cls)}
        unknown = set(d) - known
        if unknown:
            raise ValueError(f"unknown SignalDecision fields: {sorted(unknown)}")
        if d.get("schema_version", SIGNAL_SCHEMA_VERSION) != SIGNAL_SCHEMA_VERSION:
            raise ValueError(f"unsupported SignalDecision schema {d.get('schema_version')!r}")
        kw = dict(d)
        kw["decision"] = Decision(d["decision"])
        kw["side"] = Side(d["side"]) if d.get("side") is not None else None
        kw["no_call_reasons"] = [NoCallReason(r) for r in d.get("no_call_reasons") or []]
        kw["data_health"] = [FeedHealth.from_dict(h) for h in d.get("data_health") or []]
        return cls(**kw)

    def to_json(self):
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False)

    @classmethod
    def from_json(cls, s):
        return cls.from_dict(json.loads(s))


# The name used in the roadmap for the future signal -> risk hand-off. Same object today.
TradeCandidate = SignalDecision
