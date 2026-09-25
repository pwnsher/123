"""
Read-only REST for the book venues (driven by Step 3's PollRunner).

SnapshotPoller   Binance diff-depth books: GET /fapi/v1/depth for every book the collector marked as needing a
                 snapshot (first connect, a pu / U / u sequence gap, a disconnect). At most one request per book per
                 min_interval_s; the response is stored raw first and applied by the same reconstructor.
Kalshi markets   the Kalshi websocket adapter's rest_urls() (open markets per series, every 30 s) is polled by
                 Step 4's generic PerpPoller (GET only), which hands responses to MicroCollector.on_rest.
"""


class SnapshotPoller:
    def __init__(self, adapter, getter, collector, clock, min_interval_s=1.0):
        self.adapter, self.getter, self.collector, self.clock = adapter, getter, collector, clock
        self.min_interval_ns = int(min_interval_s * 1e9)
        self.last_req = {}
        self.requests = 0

    def poll(self, reconnect=False):
        n_ev = n_f = n_g = 0
        now = self.clock.mono_ns()
        due = [a for (src, a) in list(self.collector.snapshot_needed) if src == self.adapter.source and
               now - self.last_req.get(a, -10**18) >= self.min_interval_ns]
        failed = 0
        for asset in sorted(due):
            self.last_req[asset] = now
            url, params = self.adapter.snapshot_url(asset)
            try:
                body = self.getter.get_json(url, params)
                self.requests += 1
            except Exception as err:                        # noqa: BLE001 - recorded; retried on the next cycle
                failed += 1
                self.collector.on_poll_error(f"{self.adapter.source}:snapshot", self.clock.wall_ms(), str(err))
                continue
            e, f, g = self.collector.on_rest(self.adapter, f"snapshot:{asset}", body, self.clock.wall_ms(), self.clock.mono_ns())
            n_ev, n_f, n_g = n_ev + e, n_f + f, n_g + g
        self.collector.tick()
        if due and failed == len(due):
            raise ConnectionError(f"all {failed} {self.adapter.source} snapshot requests failed")
        return n_ev, n_f, n_g
