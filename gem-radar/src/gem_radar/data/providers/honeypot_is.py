"""Honeypot.is buy/sell simulation (EVM: Ethereum, BSC, Base). Optional key:
HONEYPOT_IS_API_KEY (sent as X-API-KEY; never logged).
API: GET https://api.honeypot.is/v2/IsHoneypot?address=..&chainID=..
Taxes are percentages (5 = 5%).
"""
from __future__ import annotations

import os

from ...chains.evm import EVM_CHAINS
from ...core.enums import ProviderStatus
from ...core.types import ProviderReport
from ...scoring import normalization as n
from .base import FetchContext, Provider

URL = "https://api.honeypot.is/v2/IsHoneypot?address={a}&chainID={cid}"


class HoneypotIs(Provider):
    name = "honeypot_is"
    tier = "deep"
    chains = {"ethereum", "bsc", "base"}

    def fetch(self, ctx: FetchContext) -> ProviderReport:
        headers = {}
        key = os.environ.get("HONEYPOT_IS_API_KEY")
        if key:
            headers["X-API-KEY"] = key
        cid = EVM_CHAINS[ctx.chain]["chain_id"]
        d = self.get_json(ctx, URL.format(a=ctx.address, cid=cid), headers=headers)
        if not isinstance(d, dict):
            return ProviderReport(self.name, ProviderStatus.PARSE_ERROR, message="not an object")
        b = self.builder(ctx)
        sim_ok = d.get("simulationSuccess")
        if isinstance(sim_ok, bool):
            b.add("sell_simulation_ok", sim_ok,
                  note="" if sim_ok else str(d.get("simulationError") or "")[:200])
        hp = (d.get("honeypotResult") or {}).get("isHoneypot")
        if isinstance(hp, bool) and sim_ok is True:
            b.add("is_honeypot", hp, note=str((d.get("honeypotResult") or {})
                                             .get("honeypotReason") or "")[:200])
        sim = d.get("simulationResult") or {}
        if sim_ok is True:
            b.add("buy_tax_pct", n.pct_from_percent(sim.get("buyTax")))
            b.add("sell_tax_pct", n.pct_from_percent(sim.get("sellTax")))
            b.add("transfer_tax_pct", n.pct_from_percent(sim.get("transferTax")))
        code = d.get("contractCode") or {}
        if isinstance(code.get("openSource"), bool):
            b.add("is_open_source", code["openSource"])
        if isinstance(code.get("isProxy"), bool):
            b.add("is_proxy", code["isProxy"])
        b.add("holder_count", n.count((d.get("token") or {}).get("totalHolders")))
        return ProviderReport(self.name, ProviderStatus.AVAILABLE, b.fetched_at, "", b.readings,
                              {"risk": (d.get("summary") or {}).get("risk")})
