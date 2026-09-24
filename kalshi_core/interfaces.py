"""
Layer interfaces (typing.Protocol) for the future pipeline — TYPES ONLY.

    Market Data -> Feature Engine -> Prediction Model -> Calibration -> Signal Engine
                -> Risk Manager -> Execution Engine -> Kalshi

Nothing here is implemented or wired into the running bot. Today all of DATA, FEATURES,
MODEL and SIGNAL live inside kalshi_dashboard.evaluate() (see docs/ARCHITECTURE.md), and the
only implementation of the combined contract is kalshi_core.adapter over that function.

Separation rules the protocols encode:
  * a ProbabilityModel returns a probability; it never sees an order or an account;
  * a SignalEngine decides whether an opportunity exists (SignalDecision); it never sizes;
  * a RiskManager decides whether capital may be exposed; it never places an order;
  * only an ExecutionEngine (kalshi_core.execution) may ever talk to an order endpoint, and
    it accepts only a risk-approved OrderIntent, never a raw SignalDecision.
"""
from dataclasses import dataclass, field
from typing import Protocol, Optional, Dict, Any, Mapping, Sequence, runtime_checkable

from kalshi_core.signal import SignalDecision


@dataclass(frozen=True)
class MarketSnapshot:
    """Everything the model may see at ONE instant (causality: nothing observed later)."""
    asset: str
    as_of_epoch_ms: int
    market: Mapping[str, Any]                   # Kalshi market object (strike, book, close time)
    spot: Optional[float] = None
    candles: Optional[Mapping[str, Sequence[float]]] = None   # oldest-first 1-min close/high/low
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProbabilityEstimate:
    asset: str
    as_of_epoch_ms: int
    probability_up: float                      # [0, 1]
    model_id: str
    features: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RiskDecision:
    approved: bool
    reasons: Sequence[str] = ()
    max_contracts: int = 0


@runtime_checkable
class MarketDataProvider(Protocol):
    def snapshot(self, asset: str) -> MarketSnapshot: ...


@runtime_checkable
class FeatureEngine(Protocol):
    def features(self, snap: MarketSnapshot) -> Dict[str, Any]: ...


@runtime_checkable
class ProbabilityModel(Protocol):
    model_id: str

    def predict(self, snap: MarketSnapshot, features: Mapping[str, Any]) -> ProbabilityEstimate: ...


@runtime_checkable
class Calibrator(Protocol):
    method: str

    def calibrate(self, estimate: ProbabilityEstimate) -> float: ...


@runtime_checkable
class SignalEngine(Protocol):
    def decide(self, snap: MarketSnapshot, estimate: ProbabilityEstimate,
               calibrated_probability_up: Optional[float]) -> SignalDecision: ...


@runtime_checkable
class RiskManager(Protocol):
    def review(self, decision: SignalDecision) -> RiskDecision: ...


@runtime_checkable
class DecisionStore(Protocol):
    def append(self, decision: SignalDecision) -> None: ...


@runtime_checkable
class LegacyStrategy(Protocol):
    """What exists today: one call does data + features + model + signal for one coin."""
    def evaluate(self, coin: str, cfg: Mapping[str, Any]) -> Dict[str, Any]: ...
