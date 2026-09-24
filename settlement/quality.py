"""
Measurable settlement-quality classification.

Quality is derived only from counted conditions (below); a value merely existing never makes a
state HEALTHY. The overall quality is the worst condition present (types.QUALITY_PRECEDENCE).

    SCHEMA_MISMATCH        a relevant payload from a trusted source failed schema validation
    INVALID                the market spec is unusable (no close time / index id) or
                           the only relevant records were malformed
    CONFLICT               an elapsed sample instant has unresolvable conflicting values
    MISSING                no trusted observation available at or before as_of (within the lookback
                           for in-window/closed phases; ever, for the pre-window phase)
    PROXY_SOURCE           only proxy observations (not the CF RTI) were available
    INSUFFICIENT_COVERAGE  closed window: filled/expected < min_coverage and no research allowance;
                           in-window: so many samples already missing that min_coverage is unreachable
    STALE                  newest available observation older than stale_after_ms (pre-window / in-window)
    OUT_OF_ORDER           at least one arrival more than reorder_tolerance_ms behind the newest event seen
    PARTIAL                some elapsed samples missing but coverage still reachable, or a research
                           policy produced a partial / interpolated value
    HEALTHY                none of the above
"""
from settlement.types import Quality, QUALITY_PRECEDENCE


def worst(conditions):
    present = set(conditions) or {Quality.HEALTHY}
    for q in QUALITY_PRECEDENCE:
        if q in present:
            return q
    return Quality.UNKNOWN


def classify(*, schema_mismatch, invalid, conflict_samples, has_trusted, has_proxy_only, phase_closed, phase_pre,
             expected, filled, missing_elapsed, interpolated, min_coverage, partial_allowed_and_met,
             reachable_coverage, stale, out_of_order):
    """Return the overall Quality from measured conditions (keyword-only on purpose)."""
    c = []
    if schema_mismatch:
        c.append(Quality.SCHEMA_MISMATCH)
    if invalid:
        c.append(Quality.INVALID)
    if conflict_samples:
        c.append(Quality.CONFLICT)
    if not has_trusted:
        c.append(Quality.PROXY_SOURCE if has_proxy_only else Quality.MISSING)
    if phase_closed:
        coverage = filled / expected if expected else 0.0
        if coverage < min_coverage:
            c.append(Quality.PARTIAL if partial_allowed_and_met else Quality.INSUFFICIENT_COVERAGE)
        elif interpolated:
            c.append(Quality.PARTIAL)
    else:
        if missing_elapsed:
            reachable = (expected - missing_elapsed) / expected >= reachable_coverage if expected else False
            c.append(Quality.PARTIAL if reachable else Quality.INSUFFICIENT_COVERAGE)
        if stale:
            c.append(Quality.STALE)
    if out_of_order:
        c.append(Quality.OUT_OF_ORDER)
    return worst(c)
