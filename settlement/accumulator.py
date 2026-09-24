"""
Incremental LIVE settlement accumulator for one market window. OBSERVATION ONLY.

    acc = SettlementAccumulator(market)
    acc.ingest(obs)                 # as each observation arrives (arrival order)
    acc.state(now_ms)               # causal SettlementState at now_ms (cheap)
    acc.result(now_ms)              # + final value once the window has closed

Per ingest: O(log n) insert + re-sampling of the <= 2 grid instants the observation can affect.
Per state(): new grid instants are sampled once each; the summary walks the 60 instants (constant),
never the observation history.

Causality guard: state()/result() raise if asked about a time earlier than the availability of an
observation already ingested (that would put future data into a past state). Live use feeds data as
it arrives, so the guard never triggers; replays must feed in arrival order (engine.arrival_order).

It produces exactly what reconstruction.reconstruct() produces for the same inputs and as_of
(tested), because both use engine.ObservationBook and engine.summarize.
"""
from settlement.engine import ObservationBook, summarize
from settlement.policy import available_ts, reconstruction_policy, window_policy
from settlement.reconstruction import issue_visible, provenance, relevant_schema_mismatch
from settlement.types import SettlementResult


class CausalityError(ValueError):
    pass


class SettlementAccumulator:
    def __init__(self, market, wpol=None, rpol=None):
        self.market = market
        self.wpol = wpol or window_policy()
        self.rpol = rpol or reconstruction_policy()
        self.book = ObservationBook(market, self.wpol, self.rpol)
        self.invalid = market.close_ts_ms is None or not market.index_id
        self._samples = {}
        self._next = 0
        self._last_available = None
        self._issues = []
        self.sample_revisions = 0            # samples changed AFTER being reported (out-of-order arrivals)

    def ingest(self, obs, available_ms=None):
        a = available_ts(obs, self.rpol) if available_ms is None else max(available_ms, obs.event_ts_ms)
        if self._last_available is not None and a < self._last_available:
            raise CausalityError(f"arrival at {a} precedes an earlier ingest at {self._last_available}; "
                                 "feed observations in arrival order")
        self._last_available = a
        t = self.book.add(obs, a)
        if t is not None:
            for g in self.book.affected_instants(t):
                if g in self._samples:
                    new = self.book.sample(g)
                    if new != self._samples[g]:
                        self.sample_revisions += 1
                        self._samples[g] = new
        return t

    def add_issue(self, issue):
        self._issues.append(issue)

    def _advance(self, now_ms):
        if self._last_available is not None and now_ms < self._last_available:
            raise CausalityError(f"state at {now_ms} requested after ingesting data available at {self._last_available}")
        grid = self.book.grid
        while self._next < len(grid) and self.wpol.sample_known_at(grid[self._next]) <= now_ms:
            g = grid[self._next]
            self._samples[g] = self.book.sample(g)
            self._next += 1
        return [self._samples[g] for g in grid[:self._next]]

    def _summary(self, now_ms):
        samples = self._advance(now_ms) if not self.invalid else []
        issues = [i for i in self._issues if issue_visible(i, now_ms)]
        sm = relevant_schema_mismatch(issues, self.market, self.wpol, self.rpol, now_ms)
        return summarize(self.book, samples, now_ms, schema_mismatch=sm, invalid=self.invalid), issues

    def state(self, now_ms):
        (state, _f, _o, _s), _i = self._summary(now_ms)
        return state

    def result(self, now_ms):
        (state, final, outcome, samples), issues = self._summary(now_ms)
        return SettlementResult(state, final, outcome, samples,
                                provenance(self.book, state, self.wpol, self.rpol, now_ms, issues))
