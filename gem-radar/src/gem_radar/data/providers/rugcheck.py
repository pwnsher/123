"""RugCheck (Solana, public): authorities, holders, LP lock, market liquidity.
API: GET https://api.rugcheck.xyz/v1/tokens/{mint}/report
Holder `pct` and `lpLockedPct` are percentages (12.5 = 12.5%).
"""
from __future__ import annotations

from typing import Any

from ...chains.solana import SOLANA_NON_HOLDERS
from ...core.enums import ProviderStatus
from ...core.types import ProviderReport
from ...scoring import normalization as n
from .base import FetchContext, Provider
from .goplus import concentration

URL = "https://api.rugcheck.xyz/v1/tokens/{a}/report"


class RugCheck(Provider):
    name = "rugcheck"
    tier = "fast"
    chains = {"solana"}

    def fetch(self, ctx: FetchContext) -> ProviderReport:
        d = self.get_json(ctx, URL.format(a=ctx.address))
        if not isinstance(d, dict) or not d:
            return ProviderReport(self.name, ProviderStatus.NOT_FOUND, message="no report")
        b = self.builder(ctx)
        tok: dict = d["token"] if isinstance(d.get("token"), dict) else {}
        if "mintAuthority" in tok:
            b.add("mint_authority_active", tok["mintAuthority"] not in (None, ""),
                  raw=tok["mintAuthority"])
        if "freezeAuthority" in tok:
            b.add("freeze_authority_active", tok["freezeAuthority"] not in (None, ""),
                  raw=tok["freezeAuthority"])
        meta: dict = d["tokenMeta"] if isinstance(d.get("tokenMeta"), dict) else {}
        if isinstance(meta.get("mutable"), bool):
            b.add("metadata_mutable", meta["mutable"])
        if isinstance(d.get("rugged"), bool):
            b.add("rugged", d["rugged"])
        tf = d.get("transferFee")
        if isinstance(tf, dict) and "pct" in tf:
            b.add("transfer_tax_pct", n.pct_from_percent(tf.get("pct")))
        b.add("holder_count", n.count(d.get("totalHolders")))
        b.add("liquidity_usd", n.usd(d.get("totalMarketLiquidity")),
              note="RugCheck total market liquidity")

        markets = [m for m in (d.get("markets") or []) if isinstance(m, dict)]
        pool_accounts: set[str] = set()
        for m in markets:
            for k in ("pubkey", "liquidityAAccount", "liquidityBAccount", "liquidityA", "liquidityB"):
                v = m.get(k)
                if isinstance(v, str):
                    pool_accounts.add(v)
        pool_accounts |= set(ctx.shared.get("pool_addresses", set()))
        exclude = pool_accounts | SOLANA_NON_HOLDERS
        holders, excluded = [], 0
        creator = d.get("creator") if isinstance(d.get("creator"), str) else None
        creator_pct = None
        for h in d.get("topHolders") or []:
            if not isinstance(h, dict):
                continue
            pct = n.pct_from_percent(h.get("pct"))
            addr, owner = h.get("address") or "", h.get("owner") or ""
            if pct is None:
                continue
            if addr in exclude or owner in exclude:
                excluded += 1
                continue
            holders.append((addr, pct))
            if creator and creator in (addr, owner):
                creator_pct = (creator_pct or 0.0) + pct
        for k, v in concentration(holders, set()).items():
            b.add(k, v, note=f"RugCheck top holders, {excluded} pool account(s) excluded")
        if creator_pct is not None:
            b.add("dev_pct", round(creator_pct, 4), note="creator wallet among top holders")

        weighted, weight = 0.0, 0.0
        for m in markets:
            lp: dict = m["lp"] if isinstance(m.get("lp"), dict) else {}
            pct = n.pct_from_percent(lp.get("lpLockedPct"))
            usd_ = (n.usd(lp.get("baseUSD")) or 0.0) + (n.usd(lp.get("quoteUSD")) or 0.0)
            if pct is not None and usd_ > 0:
                weighted += pct * usd_
                weight += usd_
        if weight > 0:
            b.add("lp_locked_pct", round(weighted / weight, 4),
                  note="liquidity-weighted LP locked/burned across RugCheck markets")

        extra: dict[str, Any] = {
            "creator": creator,
            "risks": [{"name": r.get("name"), "level": r.get("level")}
                      for r in (d.get("risks") or []) if isinstance(r, dict)][:20],
        }
        return ProviderReport(self.name, ProviderStatus.AVAILABLE, b.fetched_at, "", b.readings,
                              extra)
