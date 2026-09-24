"""
Shared causal sampling core. Both the live accumulator and historical reconstruction feed an
ObservationBook in ARRIVAL order and summarise it with summarize(); there is one implementation
of every rule, so live and batch results are identical by construction (tested).

Cost: add() is O(log n) plus O(affected instants) (at most max_sample_age / interval + 1 = 2 for the
default policy); sample() is O(log n); summarize() is O(window instants) = O(60). Nothing ever
recomputes the full observation history.
"""
import bisect
import hashlib
import json
import math

from settlement import ENGINE_VERSION
from settlement.policy import PROXY_SOURCES, available_ts
from settlement.quality import classify
from settlement.types import (Flag, Membership, Phase, Quality, Sample, SampleKind, SettlementState,
                              OBSERVED_KINDS)

CONFLICT = object()          # sentinel: unresolvable value at one event time


class ObservationBook:
    """Event-time index of the TRUSTED observations relevant to one market window.

    Only observations passed to add() exist for the book; callers enforce causality by adding only
    what is available at the time of interest (reconstruction filters by as_of; the live
    accumulator adds as data arrives)."""

    def __init__(self, market, wpol, rpol):
        self.market, self.wpol, self.rpol = market, wpol, rpol
        self.grid = wpol.grid(market.close_ts_ms) if market.close_ts_ms is not None else []
        self.lookback_start = wpol.lookback_start(market.close_ts_ms) if self.grid else None
        self.times = []                  # sorted distinct event times inside [lookback_start, close]
        self.cands = {}                  # event_ts -> list of distinct (value, amend_ts, source)
        self.resolved = {}               # event_ts -> float | CONFLICT
        self.received = 0
        self.index_mismatch = 0
        self.untrusted_ignored = 0
        self.proxy_seen = 0
        self.duplicates = 0
        self.conflict_times = set()
        self.amended_times = set()
        self.late = 0
        self.late_excluded = 0
        self.receive_missing = 0
        self.out_of_order = 0
        self.max_event_seen = None
        self.sources = set()
        self.schema_fps = set()
        self.latest = None               # newest trusted observation by event time (any time <= as_of)
        self.in_lookback = 0             # distinct (event_ts, value) trusted observations in the lookback
        self.used_event_min = None
        self.used_event_max = None
        self._digest_items = []

    # ---------- ingestion ----------
    def add(self, obs, available_ms):
        """Add one available observation. Returns the event time it affects inside the window, or None."""
        self.received += 1
        if obs.index_id != self.market.index_id:
            self.index_mismatch += 1
            return None
        if obs.source in PROXY_SOURCES:
            self.proxy_seen += 1
            return None
        if obs.source not in self.rpol.trusted_sources:
            self.untrusted_ignored += 1
            return None
        if obs.receive_ts_ms is None:
            self.receive_missing += 1
        known_order = obs.receive_ts_ms is not None or obs.seq is not None
        if known_order and self.max_event_seen is not None \
                and obs.event_ts_ms < self.max_event_seen - self.rpol.reorder_tolerance_ms:
            self.out_of_order += 1
        if self.max_event_seen is None or obs.event_ts_ms > self.max_event_seen:
            self.max_event_seen = obs.event_ts_ms
        self.sources.add(obs.source)
        if obs.schema_fingerprint:
            self.schema_fps.add(obs.schema_fingerprint)
        if self.latest is None or (obs.event_ts_ms, repr(obs.value)) > (self.latest.event_ts_ms, repr(self.latest.value)):
            self.latest = obs
        if not self.grid:
            return None
        t = obs.event_ts_ms
        if t < self.lookback_start or t > self.market.close_ts_ms:
            return None
        if available_ms > self.market.close_ts_ms + self.rpol.late_tolerance_ms:
            self.late += 1
            if not self.rpol.include_late_in_final:
                self.late_excluded += 1
                return None
        lst = self.cands.setdefault(t, [])
        if any((v, a) == (obs.value, obs.amend_ts_ms) for v, a, _s in lst):
            self.duplicates += 1                                 # exact repeat (any source): dropped
            return None
        if any(v == obs.value for v, _a, _s in lst):
            self.duplicates += 1                                 # same value, new amendment info: kept
        else:
            self.in_lookback += 1                                # a new distinct (event_ts, value)
        if not lst:
            bisect.insort(self.times, t)
        lst.append((obs.value, obs.amend_ts_ms, obs.source))
        self.used_event_min = t if self.used_event_min is None else min(self.used_event_min, t)
        self.used_event_max = t if self.used_event_max is None else max(self.used_event_max, t)
        self._digest_items.append((t, repr(obs.value), obs.amend_ts_ms, obs.source))
        self.resolved[t] = self._resolve(t, lst)
        return t

    def _resolve(self, t, lst):
        values = {v for v, _a, _s in lst}
        if len(values) == 1:
            self.conflict_times.discard(t)
            return lst[0][0]
        if self.rpol.conflict_rule == "PREFER_AMENDED":
            amended = [(a, v) for v, a, _s in lst if a is not None]
            if amended:
                top = max(a for a, _v in amended)
                winners = {v for a, v in amended if a == top}
                if len(winners) == 1:
                    self.amended_times.add(t)
                    self.conflict_times.discard(t)
                    return winners.pop()
        self.conflict_times.add(t)
        return CONFLICT

    # ---------- sampling ----------
    def sample(self, g):
        w = self.wpol
        times = self.times
        if w.sampling == "EXACT":
            i = bisect.bisect_left(times, g)
            if i < len(times) and times[i] == g:
                return self._mk(g, g, SampleKind.OBSERVED_EXACT)
            return Sample(g, SampleKind.MISSING)
        if w.sampling == "BUCKET_LAST":
            i = bisect.bisect_left(times, g + w.sample_interval_ms) - 1
            if i >= 0 and times[i] >= g:
                return self._mk(g, times[i], SampleKind.BUCKET_LAST)
            return Sample(g, SampleKind.MISSING)
        i = bisect.bisect_right(times, g) - 1                     # ASOF
        if i < 0 or g - times[i] > w.max_sample_age_ms:
            return Sample(g, SampleKind.MISSING)
        return self._mk(g, times[i], SampleKind.OBSERVED_EXACT if times[i] == g else SampleKind.ASOF)

    def _mk(self, g, t, kind):
        v = self.resolved[t]
        if v is CONFLICT:
            return Sample(g, SampleKind.CONFLICT, None, t, g - t)
        return Sample(g, kind, v, t, g - t)

    def affected_instants(self, t):
        """Grid instants whose sample can change when event time t changes."""
        w = self.wpol
        if w.sampling == "EXACT":
            lo, hi = t, t
        elif w.sampling == "BUCKET_LAST":
            lo, hi = t - w.sample_interval_ms + 1, t
        else:
            lo, hi = t, t + w.max_sample_age_ms
        a = bisect.bisect_left(self.grid, lo)
        b = bisect.bisect_right(self.grid, hi)
        return self.grid[a:b]

    def input_digest(self):
        return hashlib.sha256(json.dumps(sorted(self._digest_items, key=lambda x: (x[0], x[1], str(x[2]), x[3])),
                                         separators=(",", ":")).encode()).hexdigest()


def elapsed_instants(book, as_of_ms):
    if as_of_ms is None:
        return list(book.grid)
    return [g for g in book.grid if book.wpol.sample_known_at(g) <= as_of_ms]


def _interpolate(samples, allow):
    """Research only: linear interpolation of MISSING samples strictly between two observed ones."""
    if not allow:
        return samples
    out = list(samples)
    known = [i for i, s in enumerate(out) if s.kind in OBSERVED_KINDS]
    for a, b in zip(known, known[1:]):
        if b - a <= 1:
            continue
        va, vb = out[a].value, out[b].value
        ga, gb = out[a].grid_ts_ms, out[b].grid_ts_ms
        for i in range(a + 1, b):
            if out[i].kind == SampleKind.MISSING:
                g = out[i].grid_ts_ms
                out[i] = Sample(g, SampleKind.INTERPOLATED, va + (vb - va) * (g - ga) / (gb - ga), None, None)
    return out


def summarize(book, samples, as_of_ms, schema_mismatch=False, invalid=False, label_mode=False):
    """Build (SettlementState, final_value, outcome, flags) from a book and its elapsed samples."""
    m, w, r = book.market, book.wpol, book.rpol
    close = m.close_ts_ms
    expected = len(book.grid)
    as_of = close if label_mode else as_of_ms
    samples = _interpolate(samples, r.allow_interpolation)
    elapsed = len(samples)
    filled = sum(1 for s in samples if s.kind in OBSERVED_KINDS)
    interpolated = sum(1 for s in samples if s.kind == SampleKind.INTERPOLATED)
    conflict_samples = sum(1 for s in samples if s.kind == SampleKind.CONFLICT)
    missing = elapsed - filled - interpolated
    usable = [s.value for s in samples if s.kind in OBSERVED_KINDS or s.kind == SampleKind.INTERPOLATED]
    acc_sum = math.fsum(usable) if usable else None
    acc_mean = acc_sum / len(usable) if usable else None
    run = best = 0
    for s in samples:
        if s.kind in OBSERVED_KINDS or s.kind == SampleKind.INTERPOLATED:
            run = 0
        else:
            run += 1
            best = max(best, run)
    filled_ts = [s.grid_ts_ms for s in samples if s.kind in OBSERVED_KINDS]
    if expected and elapsed == expected:
        phase = Phase.CLOSED
    elif expected and (elapsed > 0 or (as_of is not None and as_of >= book.grid[0])):
        phase = Phase.IN_WINDOW
    else:
        phase = Phase.PRE_WINDOW
    latest = book.latest
    last_age = (as_of - latest.event_ts_ms) / 1000.0 if (latest is not None and as_of is not None) else None
    stale = last_age is not None and last_age * 1000.0 > r.stale_after_ms
    has_trusted = latest is not None
    has_proxy_only = not has_trusted and book.proxy_seen > 0
    closed = phase == Phase.CLOSED
    coverage_final = filled / expected if expected else 0.0
    partial_ok = r.allow_partial_mean and (filled + interpolated) / expected >= r.partial_min_coverage if expected else False
    reachable = min(r.min_coverage, r.partial_min_coverage) if r.allow_partial_mean else r.min_coverage
    quality = classify(schema_mismatch=schema_mismatch, invalid=invalid or not expected, conflict_samples=conflict_samples,
                       has_trusted=has_trusted, has_proxy_only=has_proxy_only, phase_closed=closed,
                       phase_pre=phase == Phase.PRE_WINDOW, expected=expected, filled=filled,
                       missing_elapsed=missing, interpolated=interpolated, min_coverage=r.min_coverage,
                       partial_allowed_and_met=partial_ok, reachable_coverage=reachable, stale=stale,
                       out_of_order=book.out_of_order > 0)
    final_value = None
    flags = set()
    if closed and quality not in (Quality.SCHEMA_MISMATCH, Quality.INVALID, Quality.CONFLICT, Quality.MISSING,
                                  Quality.PROXY_SOURCE, Quality.INSUFFICIENT_COVERAGE, Quality.UNKNOWN):
        if coverage_final >= r.min_coverage and not interpolated:
            final_value = acc_mean
        elif partial_ok:
            final_value = acc_mean
            flags.add(Flag.PARTIAL_MEAN)
        if final_value is not None and w.round_decimals is not None:
            final_value = round(final_value, int(w.round_decimals))
    outcome = None
    if final_value is not None and m.strike is not None:
        if final_value > m.strike:
            outcome = "yes"
        elif final_value < m.strike:
            outcome = "no"
        else:
            flags.add(Flag.AT_STRIKE)
    if m.strike is None:
        flags.add(Flag.NO_STRIKE)
    if not w.verified:
        flags.add(Flag.UNVERIFIED_WINDOW_CONVENTION)
    for cond, f in ((book.duplicates, Flag.DUPLICATE_DROPPED), (book.conflict_times, Flag.CONFLICTING_VALUES),
                    (book.amended_times, Flag.AMENDMENT_APPLIED), (book.out_of_order, Flag.OUT_OF_ORDER_ARRIVAL),
                    (book.late, Flag.LATE_OBSERVATION), (book.receive_missing, Flag.RECEIVE_TIME_MISSING),
                    (book.proxy_seen, Flag.PROXY_SOURCE), (missing, Flag.GAP), (interpolated, Flag.INTERPOLATED_SAMPLES),
                    (stale and not closed, Flag.STALE_LAST_OBSERVATION),
                    (any(s.kind == SampleKind.ASOF for s in samples), Flag.ASOF_SAMPLES),
                    (len(book.sources) > 1, Flag.MIXED_SOURCES), (schema_mismatch, Flag.SCHEMA_MISMATCH)):
        if cond:
            flags.add(f)
    if phase == Phase.PRE_WINDOW:
        flags.add(Flag.WINDOW_NOT_STARTED)
    elif phase == Phase.IN_WINDOW:
        flags.add(Flag.WINDOW_OPEN)
    state = SettlementState(
        asset=m.asset, market_ticker=m.ticker, index_id=m.index_id, strike=m.strike, close_ts_ms=close,
        window_start_ts_ms=w.window_bounds(close)[0], as_of_ts_ms=as_of, phase=phase,
        seconds_remaining=max(close - as_of, 0) / 1000.0 if as_of is not None else 0.0,
        current_index=latest.value if latest is not None else None,
        current_index_event_ts_ms=latest.event_ts_ms if latest is not None else None,
        last_observation_age_s=last_age, observations_seen=book.in_lookback,
        samples_expected=expected, samples_elapsed=elapsed, samples_filled=filled,
        samples_interpolated=interpolated, samples_missing=missing, samples_remaining=expected - elapsed,
        coverage_elapsed=(filled / elapsed) if elapsed else None, accumulated_sum=acc_sum, accumulated_mean=acc_mean,
        first_included_ts_ms=filled_ts[0] if filled_ts else None, last_included_ts_ms=filled_ts[-1] if filled_ts else None,
        max_gap_s=best * w.sample_interval_ms / 1000.0 if elapsed else None, quality=quality,
        flags=tuple(sorted(f.value for f in flags)), window_policy_id=w.policy_id, reconstruction_policy_id=r.policy_id,
        engine_version=ENGINE_VERSION, sources=tuple(sorted(book.sources)))
    return state, final_value, outcome, tuple(samples)


def arrival_order(observations, rpol):
    """Deterministic arrival order: availability, then capture sequence, then the total sort key."""
    return sorted(observations, key=lambda o: (available_ts(o, rpol), o.seq if o.seq is not None else -1, o.sort_key()))


def membership(obs, market, wpol):
    return wpol.membership(obs.event_ts_ms, market.close_ts_ms) if market.close_ts_ms is not None else Membership.AFTER_WINDOW
