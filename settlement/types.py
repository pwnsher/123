"""
Typed settlement records. All timestamps are integer UTC epoch MILLISECONDS.

Two clocks are never mixed:
    event_ts_ms    when the SOURCE says the value applies (CF "time")
    receive_ts_ms  when THIS system received it (None when unknown, e.g. downloaded history)
Availability for causal purposes is receive_ts_ms when known; otherwise event_ts_ms plus the
policy's explicit assumed publication lag, and the observation carries RECEIVE_TIME_MISSING.

Labels (the official Kalshi result / expiration value) live in OfficialResolution, a separate type
that the feature path never receives.
"""
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional, Tuple, Dict, Any

from settlement import RECORD_SCHEMA_VERSION


class Quality(str, Enum):
    HEALTHY = "HEALTHY"                              # every elapsed sample observed, fresh, trusted source
    PARTIAL = "PARTIAL"                              # some samples missing/interpolated but allowed by a RESEARCH policy
    STALE = "STALE"                                  # newest available observation older than stale_after_ms
    INSUFFICIENT_COVERAGE = "INSUFFICIENT_COVERAGE"  # final value refused: coverage below the policy minimum
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"              # a relevant source payload did not match its expected schema
    MISSING = "MISSING"                              # no usable observation at all for this market/time
    OUT_OF_ORDER = "OUT_OF_ORDER"                    # observations arrived out of event order beyond tolerance
    CONFLICT = "CONFLICT"                            # two different values for one event time, not resolvable
    PROXY_SOURCE = "PROXY_SOURCE"                    # only a proxy (not the CF RTI itself) was available
    INVALID = "INVALID"                              # inputs unusable (bad market spec, malformed values only, ...)
    UNKNOWN = "UNKNOWN"                              # unexpected internal condition


# worst first: the overall quality is the worst condition present
QUALITY_PRECEDENCE = (Quality.SCHEMA_MISMATCH, Quality.INVALID, Quality.UNKNOWN, Quality.CONFLICT, Quality.MISSING,
                      Quality.PROXY_SOURCE, Quality.INSUFFICIENT_COVERAGE, Quality.STALE, Quality.OUT_OF_ORDER,
                      Quality.PARTIAL, Quality.HEALTHY)
TRUSTED_QUALITIES = (Quality.HEALTHY,)          # the only state fit for "trustworthy settlement" research


class Flag(str, Enum):
    DUPLICATE_DROPPED = "DUPLICATE_DROPPED"
    CONFLICTING_VALUES = "CONFLICTING_VALUES"
    AMENDMENT_APPLIED = "AMENDMENT_APPLIED"
    OUT_OF_ORDER_ARRIVAL = "OUT_OF_ORDER_ARRIVAL"
    LATE_OBSERVATION = "LATE_OBSERVATION"
    RECEIVE_TIME_MISSING = "RECEIVE_TIME_MISSING"
    MALFORMED_RECORDS_SKIPPED = "MALFORMED_RECORDS_SKIPPED"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    SCHEMA_EXTRA_FIELDS = "SCHEMA_EXTRA_FIELDS"
    GAP = "GAP"
    STALE_LAST_OBSERVATION = "STALE_LAST_OBSERVATION"
    ASOF_SAMPLES = "ASOF_SAMPLES"
    INTERPOLATED_SAMPLES = "INTERPOLATED_SAMPLES"
    PARTIAL_MEAN = "PARTIAL_MEAN"
    AT_STRIKE = "AT_STRIKE"
    PROXY_SOURCE = "PROXY_SOURCE"
    MIXED_SOURCES = "MIXED_SOURCES"
    WINDOW_NOT_STARTED = "WINDOW_NOT_STARTED"
    WINDOW_OPEN = "WINDOW_OPEN"
    NO_STRIKE = "NO_STRIKE"
    UNVERIFIED_WINDOW_CONVENTION = "UNVERIFIED_WINDOW_CONVENTION"
    EXPIRATION_VALUE_MISMATCH = "EXPIRATION_VALUE_MISMATCH"
    OFFICIAL_RESULT_MISSING = "OFFICIAL_RESULT_MISSING"
    TICKER_CLOSE_MISMATCH = "TICKER_CLOSE_MISMATCH"


class Membership(str, Enum):
    """Where an observation's EVENT time falls relative to a market's settlement window."""
    BEFORE_WINDOW = "BEFORE_WINDOW"
    IN_WINDOW = "IN_WINDOW"
    AFTER_WINDOW = "AFTER_WINDOW"          # at/after the excluded (or past the included) close boundary


class SampleKind(str, Enum):
    OBSERVED_EXACT = "OBSERVED_EXACT"      # an observation exactly at the grid instant
    ASOF = "ASOF"                          # the latest earlier observation, within max_sample_age_ms
    BUCKET_LAST = "BUCKET_LAST"            # last observation inside the grid bucket (bucket policies only)
    INTERPOLATED = "INTERPOLATED"          # research policies only; never counted as observed
    MISSING = "MISSING"
    CONFLICT = "CONFLICT"                  # unresolvable conflicting values -> unusable


OBSERVED_KINDS = (SampleKind.OBSERVED_EXACT, SampleKind.ASOF, SampleKind.BUCKET_LAST)


class Phase(str, Enum):
    PRE_WINDOW = "PRE_WINDOW"
    IN_WINDOW = "IN_WINDOW"
    CLOSED = "CLOSED"


@dataclass(frozen=True)
class SettlementObservation:
    asset: str
    index_id: str
    source: str
    value: float
    event_ts_ms: int
    receive_ts_ms: Optional[int] = None
    amend_ts_ms: Optional[int] = None
    seq: Optional[int] = None                  # arrival order within a capture, if known
    repeat_of_previous: Optional[bool] = None
    schema_id: str = ""
    schema_fingerprint: str = ""
    record_schema_version: int = RECORD_SCHEMA_VERSION

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**d)

    def sort_key(self):
        """Deterministic total order (event time first)."""
        return (self.event_ts_ms, self.index_id, self.source, self.receive_ts_ms if self.receive_ts_ms is not None else -1,
                self.seq if self.seq is not None else -1, self.amend_ts_ms if self.amend_ts_ms is not None else -1,
                repr(self.value))


@dataclass(frozen=True)
class AnnotatedObservation:
    """An observation seen through one market's window (membership + causal availability)."""
    observation: SettlementObservation
    market_ticker: str
    market_close_ts_ms: int
    membership: Membership
    available_ts_ms: int
    late: bool


@dataclass(frozen=True)
class SettlementMarket:
    """Everything a FEATURE may know about a market. Deliberately no result / expiration value."""
    ticker: str
    asset: str
    close_ts_ms: int
    index_id: str
    strike: Optional[float] = None
    strike_source: str = ""
    open_ts_ms: Optional[int] = None
    series: str = ""
    metadata_source: str = ""
    metadata_schema_fingerprint: str = ""

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


@dataclass(frozen=True)
class OfficialResolution:
    """LABEL ONLY. The official Kalshi outcome of a settled market."""
    ticker: str
    result: Optional[str]                 # "yes" / "no" / None (not settled or unknown)
    expiration_value: Optional[float] = None
    source: str = ""
    schema_fingerprint: str = ""

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


@dataclass(frozen=True)
class ParseIssue:
    source: str
    kind: str                             # MALFORMED_VALUE, INVALID_TIMESTAMP, SCHEMA_MISMATCH, CORRUPT_RECORD, ...
    detail: str
    event_ts_ms: Optional[int] = None
    index_id: Optional[str] = None
    location: str = ""
    receive_ts_ms: Optional[int] = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


@dataclass(frozen=True)
class Sample:
    grid_ts_ms: int
    kind: SampleKind
    value: Optional[float] = None
    source_event_ts_ms: Optional[int] = None
    age_ms: Optional[int] = None


@dataclass(frozen=True)
class SettlementState:
    """Read-only, CAUSAL settlement state at as_of_ts_ms. Contains no label. Intended for Step 3+,
    NOT allowed to influence production signals in Step 2."""
    asset: str
    market_ticker: str
    index_id: str
    strike: Optional[float]
    close_ts_ms: int
    window_start_ts_ms: int
    as_of_ts_ms: int
    phase: Phase
    seconds_remaining: float
    current_index: Optional[float]                 # newest available observation value
    current_index_event_ts_ms: Optional[int]
    last_observation_age_s: Optional[float]
    observations_seen: int                         # available, deduplicated observations in the lookback
    samples_expected: int
    samples_elapsed: int                           # grid instants <= as_of
    samples_filled: int                            # observed (never interpolated) elapsed samples
    samples_interpolated: int
    samples_missing: int                           # elapsed but unfilled (missing or conflict)
    samples_remaining: int
    coverage_elapsed: Optional[float]              # samples_filled / samples_elapsed
    accumulated_sum: Optional[float]
    accumulated_mean: Optional[float]              # mean of usable elapsed samples
    first_included_ts_ms: Optional[int]
    last_included_ts_ms: Optional[int]
    max_gap_s: Optional[float]                     # largest run of unfilled elapsed samples, in seconds
    quality: Quality
    flags: Tuple[str, ...]
    window_policy_id: str
    reconstruction_policy_id: str
    engine_version: str
    sources: Tuple[str, ...]

    def to_dict(self):
        d = asdict(self)
        d["phase"] = self.phase.value
        d["quality"] = self.quality.value
        d["flags"] = list(self.flags)
        d["sources"] = list(self.sources)
        return d


@dataclass(frozen=True)
class SettlementResult:
    """A reconstructed settlement (final when as_of >= close) with full provenance."""
    state: SettlementState
    final_value: Optional[float]                   # None unless the policy accepts the window (fail closed)
    reconstructed_outcome: Optional[str]           # "yes" / "no" / None (no final value or exactly at strike)
    samples: Tuple[Sample, ...]
    provenance: Dict[str, Any] = field(default_factory=dict)

    @property
    def quality(self):
        return self.state.quality

    def to_dict(self, include_samples=False):
        d = {"state": self.state.to_dict(), "final_value": self.final_value,
             "reconstructed_outcome": self.reconstructed_outcome, "provenance": self.provenance}
        if include_samples:
            d["samples"] = [dict(asdict(s), kind=s.kind.value) for s in self.samples]
        return d
