"""Provider adapter interface. The scoring engine never sees provider formats:
adapters turn responses into normalized Readings with provenance."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional

from ...core.enums import ProviderStatus
from ...core.errors import ProviderError
from ...core.types import ProviderReport
from .. import http
from ..provenance import ReadingBuilder


@dataclass
class FetchContext:
    chain: str
    address: str
    cfg: dict
    now: float = field(default_factory=time.time)
    shared: dict = field(default_factory=dict)  # facts one provider hands others (pool addresses)


class Provider:
    name = "provider"
    tier = "fast"            # "fast" providers run on every scan; "deep" ones on escalation
    chains: Optional[set[str]] = None  # None = every chain
    is_mock = False

    def supports(self, chain: str) -> bool:
        return self.chains is None or chain in self.chains

    def reliability(self, cfg: dict) -> float:
        return float(cfg["provider_reliability"].get(self.name.split(":")[0], 0.5))

    # --- helpers for subclasses -------------------------------------------------
    def get_json(self, ctx: FetchContext, url: str, *, method: str = "GET", payload: Any = None,
                 headers: Optional[dict] = None) -> Any:
        h = ctx.cfg["http"]
        return http.request_json(
            self.name, url, method=method, payload=payload, headers=headers,
            timeout=float(h["timeout_seconds"]), retries=int(h["retries"]),
            backoff=float(h["backoff_seconds"]),
            min_interval=float(ctx.cfg["rate_limit_min_interval_seconds"].get(self.name, 0.0)))

    def builder(self, ctx: FetchContext) -> ReadingBuilder:
        return ReadingBuilder(self.name, time.time(), is_mock=self.is_mock)

    # --- the contract -----------------------------------------------------------
    def fetch(self, ctx: FetchContext) -> ProviderReport:
        raise NotImplementedError

    def run(self, ctx: FetchContext) -> ProviderReport:
        """Never raises: a failing provider becomes a report with its status."""
        if not self.supports(ctx.chain):
            return ProviderReport(self.name, ProviderStatus.UNSUPPORTED,
                                  message=f"{self.name} does not cover {ctx.chain}")
        try:
            return self.fetch(ctx)
        except ProviderError as e:
            return ProviderReport(self.name, e.status, message=e.detail)
        except Exception as e:  # malformed provider data must not crash the scan
            return ProviderReport(self.name, ProviderStatus.PARSE_ERROR,
                                  message=f"{self.name}: unexpected response shape "
                                          f"({type(e).__name__})")
