"""
Deterministic, causal historical reconstruction of one market's settlement window.

    reconstruct(market, observations, as_of_ms=T)   -> state using ONLY observations available by T
    reconstruct(market, observations, as_of_ms=None) -> LABEL mode: the final settlement from all data

Causality: an observation is visible at T only if its availability (receive time, or event time +
the policy's assumed publication lag when no receive time exists) is <= T AND its event time is
<= T. Nothing after T can influence the result: the book is built only from visible observations.

Determinism: inputs are processed in a total arrival order (availability, capture sequence, full
sort key) independent of the caller's list order, and every rule lives in engine.py.

Provenance: every result records its sources, schema fingerprints, source event-time range, policy
ids/versions/fingerprints, engine and reconstruction versions, counts, coverage, quality, flags and
a digest of exactly the observations that entered the window.
"""
from settlement import ENGINE_VERSION, RECONSTRUCTION_VERSION
from settlement.engine import ObservationBook, arrival_order, elapsed_instants, summarize
from settlement.policy import available_ts, reconstruction_policy, window_policy
from settlement.types import SettlementResult


def visible(observations, rpol, as_of_ms):
    """The observations a system could have known at as_of_ms (all of them in label mode)."""
    if as_of_ms is None:
        return list(observations)
    return [o for o in observations if available_ts(o, rpol) <= as_of_ms and o.event_ts_ms <= as_of_ms]


def issue_visible(issue, as_of_ms):
    if as_of_ms is None:
        return True
    known = [t for t in (issue.receive_ts_ms, issue.event_ts_ms) if t is not None]
    return (min(known) <= as_of_ms) if known else True        # undated store-level problems: conservative


def relevant_schema_mismatch(issues, market, wpol, rpol, as_of_ms):
    """True if a SCHEMA_MISMATCH from a trusted source could affect this market's window by as_of."""
    if market.close_ts_ms is None:
        return False
    lo = wpol.lookback_start(market.close_ts_ms)
    hi = market.close_ts_ms
    for i in issues or ():
        if i.kind != "SCHEMA_MISMATCH" or i.source not in rpol.trusted_sources:
            continue
        if i.index_id is not None and i.index_id != market.index_id:
            continue
        if not issue_visible(i, as_of_ms):
            continue
        if i.event_ts_ms is not None:
            if lo <= i.event_ts_ms <= hi:
                return True
        elif i.receive_ts_ms is not None:
            if lo <= i.receive_ts_ms <= hi + rpol.late_tolerance_ms:
                return True
        else:
            return True
    return False


def provenance(book, state, wpol, rpol, as_of_ms, issues_used):
    b = book
    return {
        "engine_version": ENGINE_VERSION, "reconstruction_version": RECONSTRUCTION_VERSION,
        "window_policy": {"id": wpol.policy_id, "version": wpol.version, "fingerprint": wpol.fingerprint(),
                          "verified": wpol.verified},
        "reconstruction_policy": {"id": rpol.policy_id, "version": rpol.version, "fingerprint": rpol.fingerprint(),
                                  "research_only": rpol.research_only},
        "as_of_ts_ms": as_of_ms, "label_mode": as_of_ms is None,
        "market": {"ticker": b.market.ticker, "strike": b.market.strike, "strike_source": b.market.strike_source,
                   "metadata_source": b.market.metadata_source,
                   "metadata_schema_fingerprint": b.market.metadata_schema_fingerprint},
        "sources": sorted(b.sources), "schema_fingerprints": sorted(b.schema_fps),
        "source_event_ts_min_ms": b.used_event_min, "source_event_ts_max_ms": b.used_event_max,
        "input_digest": b.input_digest(),
        "counts": {"received": b.received, "index_mismatch": b.index_mismatch, "untrusted_ignored": b.untrusted_ignored,
                   "proxy_seen": b.proxy_seen, "duplicates": b.duplicates, "conflict_instants": len(b.conflict_times),
                   "amended_instants": len(b.amended_times), "late": b.late, "late_excluded": b.late_excluded,
                   "receive_time_missing": b.receive_missing, "out_of_order": b.out_of_order,
                   "observations_in_lookback": b.in_lookback},
        "coverage": (state.samples_filled / state.samples_expected) if state.samples_expected else None,
        "quality": state.quality.value, "flags": list(state.flags),
        "issues_considered": sorted({i.kind for i in issues_used}),
    }


def reconstruct(market, observations, wpol=None, rpol=None, as_of_ms=None, issues=()):
    wpol = wpol or window_policy()
    rpol = rpol or reconstruction_policy()
    invalid = market.close_ts_ms is None or not market.index_id
    book = ObservationBook(market, wpol, rpol)
    obs = visible(observations, rpol, as_of_ms)
    if as_of_ms is None and market.close_ts_ms is not None:
        obs = [o for o in obs if o.event_ts_ms <= market.close_ts_ms]
    for o in arrival_order(obs, rpol):
        book.add(o, available_ts(o, rpol))
    samples = [book.sample(g) for g in elapsed_instants(book, as_of_ms)] if not invalid else []
    used_issues = [i for i in (issues or ()) if issue_visible(i, as_of_ms)]
    sm = relevant_schema_mismatch(used_issues, market, wpol, rpol, as_of_ms)
    state, final, outcome, samples = summarize(book, samples, as_of_ms, schema_mismatch=sm, invalid=invalid,
                                               label_mode=as_of_ms is None)
    return SettlementResult(state=state, final_value=final, reconstructed_outcome=outcome, samples=samples,
                            provenance=provenance(book, state, wpol, rpol, as_of_ms, used_issues))
