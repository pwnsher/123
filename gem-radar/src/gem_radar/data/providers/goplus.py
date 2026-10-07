"""GoPlus Security (public tier, no key needed for low volume).

EVM:    GET https://api.gopluslabs.io/api/v1/token_security/{chain_id}?contract_addresses=..
Solana: GET https://api.gopluslabs.io/api/v1/solana/token_security?contract_addresses=..
GoPlus flags are "1"/"0" strings; percentages and taxes are fractions (0.05 = 5%).
"""
from __future__ import annotations

from typing import Any, Optional

from ...chains.evm import BURN_ADDRESSES, EVM_CHAINS
from ...core.enums import ProviderStatus
from ...core.types import ProviderReport
from ...scoring import normalization as n
from .base import FetchContext, Provider

EVM_URL = "https://api.gopluslabs.io/api/v1/token_security/{cid}?contract_addresses={a}"
SOL_URL = "https://api.gopluslabs.io/api/v1/solana/token_security?contract_addresses={a}"


def _result(data: Any, address: str) -> Optional[dict]:
    if not isinstance(data, dict) or data.get("code") not in (1, "1"):
        return None
    res = data.get("result") or {}
    if not isinstance(res, dict):
        return None
    for k, v in res.items():
        if k.lower() == address.lower() and isinstance(v, dict) and v:
            return v
    return None


def concentration(holders: list[tuple[str, float]], exclude: set[str]) -> dict:
    """top1/top5/top10 % from (address, pct) after removing pools and burn addresses."""
    kept = sorted((p for a, p in holders if a not in exclude), reverse=True)
    out: dict[str, float] = {}
    if kept:
        out["top1_pct"] = round(kept[0], 4)
        out["top5_pct"] = round(sum(kept[:5]), 4)
        out["top10_pct"] = round(sum(kept[:10]), 4)
    return out


class GoPlusEVM(Provider):
    name = "goplus"
    tier = "fast"
    chains = set(EVM_CHAINS)

    def fetch(self, ctx: FetchContext) -> ProviderReport:
        cid = EVM_CHAINS[ctx.chain]["chain_id"]
        r = _result(self.get_json(ctx, EVM_URL.format(cid=cid, a=ctx.address)), ctx.address)
        if r is None:
            return ProviderReport(self.name, ProviderStatus.NOT_FOUND, message="no security record")
        b = self.builder(ctx)
        f = n.flag01
        b.add("is_honeypot", f(r.get("is_honeypot")))
        b.add("cannot_sell_all", f(r.get("cannot_sell_all")))
        b.add("buy_tax_pct", n.pct_from_fraction(r.get("buy_tax")), raw=r.get("buy_tax"))
        b.add("sell_tax_pct", n.pct_from_fraction(r.get("sell_tax")), raw=r.get("sell_tax"))
        b.add("transfer_tax_pct", n.pct_from_fraction(r.get("transfer_tax")), raw=r.get("transfer_tax"))
        b.add("owner_can_change_balance", f(r.get("owner_change_balance")))
        b.add("hidden_owner", f(r.get("hidden_owner")))
        b.add("can_take_back_ownership", f(r.get("can_take_back_ownership")))
        b.add("transfer_pausable", f(r.get("transfer_pausable")))
        b.add("blacklist_enabled", f(r.get("is_blacklisted")))
        b.add("is_proxy", f(r.get("is_proxy")))
        b.add("is_open_source", f(r.get("is_open_source")))
        b.add("trading_cooldown", f(r.get("trading_cooldown")))
        slip = [f(r.get("slippage_modifiable")), f(r.get("personal_slippage_modifiable"))]
        if any(s is True for s in slip):
            b.add("tax_modifiable", True)
        elif all(s is False for s in slip):
            b.add("tax_modifiable", False)

        extra: dict[str, Any] = {}
        renounced: Optional[bool] = None
        if "owner_address" in r:
            owner = (r.get("owner_address") or "").lower()
            renounced = (owner == "") or owner in BURN_ADDRESSES
            b.add("ownership_renounced", renounced, raw=r.get("owner_address"),
                  note="no owner function reported" if owner == "" else "")
        mintable = f(r.get("is_mintable"))
        if mintable is False:
            b.add("mint_authority_active", False, raw=r.get("is_mintable"))
        elif mintable is True:
            if renounced is None:
                extra["mint_function_present"] = True  # ownership unknown: not a reading
            else:
                b.add("mint_authority_active", not renounced, raw=r.get("is_mintable"),
                      note="mint function; owner renounced" if renounced else
                      "mint function callable by an active owner")

        b.add("holder_count", n.count(r.get("holder_count")))
        pools = {(d.get("pair") or "").lower() for d in (r.get("dex") or []) if isinstance(d, dict)}
        pools |= set(ctx.shared.get("pool_addresses", set()))
        holders = []
        for h in r.get("holders") or []:
            if not isinstance(h, dict):
                continue
            pct = n.pct_from_fraction(h.get("percent"))
            addr = (h.get("address") or "").lower()
            if pct is not None and addr:
                holders.append((addr, pct))
        exclude = pools | BURN_ADDRESSES
        excluded = [a for a, _ in holders if a in exclude]
        for k, v in concentration(holders, exclude).items():
            b.add(k, v, note=f"from GoPlus top-{len(holders)} list, excluding "
                              f"{len(excluded)} pool/burn address(es)")

        dev_parts = []
        cp = n.pct_from_fraction(r.get("creator_percent"))
        if cp is not None:
            dev_parts.append(cp)
        op = n.pct_from_fraction(r.get("owner_percent"))
        creator = (r.get("creator_address") or "").lower()
        owner_addr = (r.get("owner_address") or "").lower()
        if op is not None and renounced is False and owner_addr and owner_addr != creator:
            dev_parts.append(op)
        if dev_parts:
            b.add("dev_pct", round(sum(dev_parts), 4), note="creator % (+ active owner % if different)")

        lp = [h for h in (r.get("lp_holders") or []) if isinstance(h, dict)]
        if lp:
            locked = 0.0
            for h in lp:
                pct = n.pct_from_fraction(h.get("percent")) or 0.0
                if str(h.get("is_locked")) == "1" or (h.get("address") or "").lower() in BURN_ADDRESSES:
                    locked += pct
            b.add("lp_locked_pct", round(min(locked, 100.0), 4),
                  note="LP tokens held by lockers or burn addresses (v2-style pools)")

        dex_liq = [n.usd(d.get("liquidity")) for d in (r.get("dex") or []) if isinstance(d, dict)]
        if any(x is not None for x in dex_liq):
            b.add("liquidity_usd", round(sum(x for x in dex_liq if x is not None), 2),
                  note="sum of GoPlus-listed DEX pools")
        extra["creator"] = creator or None
        return ProviderReport(self.name, ProviderStatus.AVAILABLE, b.fetched_at, "",
                              b.readings, extra)


class GoPlusSolana(Provider):
    """Corroborates Solana authorities (deep tier)."""

    name = "goplus"
    tier = "deep"
    chains = {"solana"}

    def fetch(self, ctx: FetchContext) -> ProviderReport:
        r = _result(self.get_json(ctx, SOL_URL.format(a=ctx.address)), ctx.address)
        if r is None:
            return ProviderReport(self.name, ProviderStatus.NOT_FOUND, message="no security record")
        b = self.builder(ctx)

        def status(key: str) -> Optional[bool]:
            v = r.get(key)
            return n.flag01(v.get("status")) if isinstance(v, dict) else None

        b.add("mint_authority_active", status("mintable"))
        b.add("freeze_authority_active", status("freezable"))
        b.add("metadata_mutable", status("metadata_mutable"))
        b.add("non_transferable", n.flag01(r.get("non_transferable")))
        hook = r.get("transfer_hook")
        if isinstance(hook, list):
            b.add("transfer_hook", bool(hook))
        b.add("holder_count", n.count(r.get("holder_count")))
        return ProviderReport(self.name, ProviderStatus.AVAILABLE, b.fetched_at, "", b.readings)
