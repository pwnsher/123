"""
Settlement-window convention and reconstruction policy — every boundary rule in one place.

WHAT KALSHI SAYS (public descriptions; the exact rule text could not be fetched from this build
environment, see docs/SETTLEMENT_ENGINE.md): crypto contracts settle on the simple average of the
CF Benchmarks Real-Time Index sampled once per second over the 60 seconds before the close.

WHAT IS NOT PUBLISHED PRECISELY, and therefore a named, testable assumption here:
    * which 60 instants: [close-60s, close-1s] (start inclusive, close exclusive) or
      [close-59s, close] (start exclusive, close inclusive);
    * how a "sample at second k" is taken from a feed that may publish every 200 ms or 1 s
      (exact instant, latest value as-of k, or last value inside [k, k+1s)).
The default policy is marked verified=False. The resolution verifier evaluates every candidate
convention against Kalshi's expiration_value / result, which is how the convention gets verified
once real data exists.

SettlementWindowPolicy answers (see grid() / membership() / sample semantics in engine.py):
    begin boundary     start_inclusive   (default True:  close-60s IS a sample instant)
    close boundary     end_inclusive     (default False: the close instant is NOT a sample instant)
    grid               window_seconds / sample_interval_ms instants, integer ms, UTC epoch
    sample at instant g
        ASOF        latest observation with g - max_sample_age_ms <= event_ts <= g
        EXACT       an observation with event_ts == g, nothing else
        BUCKET_LAST last observation with g <= event_ts < g + interval (known only once the bucket ends)
    final value        arithmetic mean of the sample values (no rounding; round_decimals=None)
    outcome            "yes" if final > strike, "no" if final < strike, None + AT_STRIKE flag if equal

ReconstructionPolicy answers:
    duplicates         identical (event_ts, value) from any source -> one observation, DUPLICATE_DROPPED
    same time, different values
                       PREFER_AMENDED: the value with the largest amend_ts wins if amend times are known
                       and the amendment itself is available; otherwise the instant is CONFLICT (unusable)
    out of order       ordering is by EVENT time, never by arrival; arrival later than
                       reorder_tolerance_ms behind the newest event already received -> OUT_OF_ORDER quality
    gaps / missing     a missing sample is MISSING; never filled unless allow_interpolation (research)
    late data          received after close + late_tolerance_ms -> LATE_OBSERVATION flag; used for the final
                       (label) value only if include_late_in_final; never visible to earlier checkpoints
    coverage           a FINAL value needs filled/expected >= min_coverage (strict default 1.0);
                       otherwise INSUFFICIENT_COVERAGE and final_value None (fail closed), unless a research
                       policy allows a PARTIAL mean
    availability       receive_ts when known, else event_ts + assumed_publication_lag_ms (flagged)
    sources            only CF RTI sources are trusted; proxies (e.g. the Kalshi perp reference price) can
                       never produce HEALTHY
"""
import hashlib
import json
from dataclasses import dataclass, asdict, field
from typing import FrozenSet

from settlement.types import Membership

SAMPLING_MODES = ("ASOF", "EXACT", "BUCKET_LAST")
CONFLICT_RULES = ("PREFER_AMENDED", "FAIL")

TRUSTED_SOURCES = frozenset({"cfb_ws_via_kalshi", "cfb_ws", "cfb_rest_history", "cfb_rest_via_kalshi"})
PROXY_SOURCES = frozenset({"kalshi_perp_reference_price"})
TEST_SOURCES = frozenset({"synthetic"})


def _fp(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


@dataclass(frozen=True)
class SettlementWindowPolicy:
    policy_id: str
    version: int
    window_seconds: int = 60
    sample_interval_ms: int = 1000
    start_inclusive: bool = True
    end_inclusive: bool = False
    sampling: str = "ASOF"
    max_sample_age_ms: int = 1000
    round_decimals: object = None
    verified: bool = False
    description: str = ""

    def __post_init__(self):
        if self.sampling not in SAMPLING_MODES:
            raise ValueError(f"unknown sampling mode {self.sampling!r}")
        if self.window_seconds <= 0 or self.sample_interval_ms <= 0 or (self.window_seconds * 1000) % self.sample_interval_ms:
            raise ValueError("window must be a positive whole number of sample intervals")
        if self.max_sample_age_ms < 0:
            raise ValueError("max_sample_age_ms must be >= 0")

    @property
    def window_ms(self):
        return self.window_seconds * 1000

    def window_bounds(self, close_ts_ms):
        """(start_ms, end_ms) of the window; inclusivity per start_inclusive / end_inclusive."""
        return close_ts_ms - self.window_ms, close_ts_ms

    def grid(self, close_ts_ms):
        """The sample instants (integer ms, ascending). Always exactly window/interval instants."""
        start, end = self.window_bounds(close_ts_ms)
        pts = list(range(start, end + 1, self.sample_interval_ms))
        if not self.start_inclusive:
            pts = pts[1:]
        if not self.end_inclusive:
            pts = [p for p in pts if p < end]
        return pts

    def expected_samples(self):
        return len(self.grid(0))

    def membership(self, event_ts_ms, close_ts_ms):
        """Where an EVENT time falls. Observations just before the window can still feed an ASOF
        sample at the first instant; they are BEFORE_WINDOW here and are reported as such."""
        start, end = self.window_bounds(close_ts_ms)
        after_start = event_ts_ms >= start if self.start_inclusive else event_ts_ms > start
        before_end = event_ts_ms <= end if self.end_inclusive else event_ts_ms < end
        if not after_start:
            return Membership.BEFORE_WINDOW
        if not before_end:
            return Membership.AFTER_WINDOW
        return Membership.IN_WINDOW

    def lookback_start(self, close_ts_ms):
        """Earliest event time that can influence any sample of this window."""
        g = self.grid(close_ts_ms)
        return g[0] - (self.max_sample_age_ms if self.sampling == "ASOF" else 0)

    def sample_known_at(self, grid_ts_ms):
        """Earliest wall time at which the sample for this instant is fully determined."""
        return grid_ts_ms + (self.sample_interval_ms - 1 if self.sampling == "BUCKET_LAST" else 0)

    def to_dict(self):
        return asdict(self)

    def fingerprint(self):
        return _fp(self.to_dict())


@dataclass(frozen=True)
class ReconstructionPolicy:
    policy_id: str
    version: int
    min_coverage: float = 1.0
    allow_partial_mean: bool = False
    partial_min_coverage: float = 0.9
    allow_interpolation: bool = False
    conflict_rule: str = "PREFER_AMENDED"
    reorder_tolerance_ms: int = 2000
    late_tolerance_ms: int = 5000
    include_late_in_final: bool = True
    assumed_publication_lag_ms: int = 0
    stale_after_ms: int = 5000
    trusted_sources: FrozenSet[str] = field(default_factory=lambda: TRUSTED_SOURCES)
    research_only: bool = False
    description: str = ""

    def __post_init__(self):
        if self.conflict_rule not in CONFLICT_RULES:
            raise ValueError(f"unknown conflict rule {self.conflict_rule!r}")
        if not (0.0 < self.min_coverage <= 1.0) or not (0.0 < self.partial_min_coverage <= 1.0):
            raise ValueError("coverage thresholds must be in (0, 1]")
        if (self.allow_partial_mean or self.allow_interpolation) and not self.research_only:
            raise ValueError("partial means / interpolation are research-only and must be named as such")

    def to_dict(self):
        d = asdict(self)
        d["trusted_sources"] = sorted(self.trusted_sources)
        return d

    def fingerprint(self):
        return _fp(self.to_dict())


# ───────────────────────── named policies ─────────────────────────
WINDOW_POLICIES = {p.policy_id: p for p in (
    SettlementWindowPolicy(
        "cf_rti_60s_start_incl_asof_v1", 1, start_inclusive=True, end_inclusive=False, sampling="ASOF",
        description="DEFAULT (assumed, unverified): instants close-60s .. close-1s; value as of each instant"),
    SettlementWindowPolicy(
        "cf_rti_60s_end_incl_asof_v1", 1, start_inclusive=False, end_inclusive=True, sampling="ASOF",
        description="candidate: instants close-59s .. close; value as of each instant"),
    SettlementWindowPolicy(
        "cf_rti_60s_start_incl_exact_v1", 1, start_inclusive=True, end_inclusive=False, sampling="EXACT",
        max_sample_age_ms=0, description="candidate: instants close-60s .. close-1s; only exact-instant values"),
    SettlementWindowPolicy(
        "cf_rti_60s_start_incl_bucket_v1", 1, start_inclusive=True, end_inclusive=False, sampling="BUCKET_LAST",
        max_sample_age_ms=0, description="candidate: last value inside each 1-s bucket [k, k+1s)"),
)}
DEFAULT_WINDOW_POLICY_ID = "cf_rti_60s_start_incl_asof_v1"

RECONSTRUCTION_POLICIES = {p.policy_id: p for p in (
    ReconstructionPolicy("strict_v1", 1, description="DEFAULT: fail closed; every sample must be observed"),
    ReconstructionPolicy("research_partial_v1", 1, allow_partial_mean=True, partial_min_coverage=0.9, research_only=True,
                         description="RESEARCH: mean over observed samples if >= 90% present; quality PARTIAL"),
    ReconstructionPolicy("research_interpolate_v1", 1, allow_partial_mean=True, allow_interpolation=True,
                         partial_min_coverage=0.9, research_only=True,
                         description="RESEARCH: linear interpolation of interior gaps, marked INTERPOLATED; PARTIAL"),
    ReconstructionPolicy("test_synthetic_v1", 1, trusted_sources=TRUSTED_SOURCES | TEST_SOURCES,
                         description="TESTS ONLY: strict, but also trusts the synthetic source"),
)}
DEFAULT_RECONSTRUCTION_POLICY_ID = "strict_v1"


def window_policy(policy_id=None):
    return WINDOW_POLICIES[policy_id or DEFAULT_WINDOW_POLICY_ID]


def reconstruction_policy(policy_id=None):
    return RECONSTRUCTION_POLICIES[policy_id or DEFAULT_RECONSTRUCTION_POLICY_ID]


def available_ts(obs, rpol):
    """Causal availability of an observation (never earlier than its event time)."""
    if obs.receive_ts_ms is not None:
        return max(obs.receive_ts_ms, obs.event_ts_ms)
    return obs.event_ts_ms + rpol.assumed_publication_lag_ms
