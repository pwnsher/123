"""
Read-only REST polling for the perp venues (driven by Step 3's PollRunner; one runner per venue).

Each call polls the venue's streams that are due (adapter.rest_urls(): {stream: (url, params, interval_s)})
with GET only, and hands every response to PerpCollector.on_rest (raw first, then normalization). A failing
stream is recorded and skipped; the others continue. Only if EVERY due stream failed does poll() raise, so
the runner backs off and reports the venue as RECONNECTING. Due times use the monotonic clock.

Backfill on reconnect (backfill()): Binance aggTrades fromId = last seen aggregate id + 1 (an exact recovery
when the gap is shorter than one page); Bybit / OKX recent trades (duplicates collapse by trade id).
Backfilled events are BACKFILLED with receive_ts = when the response arrived.
"""


class PerpPoller:
    def __init__(self, adapter, getter, collector, clock):
        self.adapter, self.getter, self.collector, self.clock = adapter, getter, collector, clock
        self.next_due = {}                               # stream -> mono ns
        self.requests = 0

    def poll(self, reconnect=False):
        streams = self.adapter.rest_urls()
        now = self.clock.mono_ns()
        n_ev = n_f = n_g = 0
        due = [s for s in streams if self.next_due.get(s, 0) <= now]
        failed = 0
        for s in due:
            url, params, interval_s = streams[s]
            self.next_due[s] = now + int(interval_s * 1e9)
            try:
                body = self.getter.get_json(url, params)
                self.requests += 1
            except Exception as err:                        # noqa: BLE001 - one stream failing never stops the others
                failed += 1
                self.collector.on_poll_error(f"{self.adapter.source}:{s}", self.clock.wall_ms(), str(err))
                continue
            e, f, g = self.collector.on_rest(self.adapter, s, body, self.clock.wall_ms(), self.clock.mono_ns())
            n_ev, n_f, n_g = n_ev + e, n_f + f, n_g + g
        self.collector.tick()
        if due and failed == len(due):
            raise ConnectionError(f"all {failed} {self.adapter.source} REST streams failed")
        return n_ev, n_f, n_g


def backfill(adapter, getter, collector, clock):
    """Trade backfill after a websocket reconnect (best effort; failures are recorded, never raised)."""
    if not hasattr(adapter, "backfill_url"):
        return 0
    n = 0
    for asset in adapter.assets:
        if adapter.source == "binance_usdm":
            det = collector.trade_ids.get((adapter.source, adapter.spec.symbols[asset]))
            url, params = adapter.backfill_url(asset, from_id=(det.last_id + 1) if det and det.last_id is not None else None)
        else:
            url, params = adapter.backfill_url(asset)
        try:
            body = getter.get_json(url, params)
        except Exception as e:                            # noqa: BLE001
            collector.on_poll_error(f"{adapter.source}:backfill", clock.wall_ms(), str(e))
            continue
        n += collector.on_rest(adapter, f"backfill:{asset}", body, clock.wall_ms(), clock.mono_ns())[0]
    return n
