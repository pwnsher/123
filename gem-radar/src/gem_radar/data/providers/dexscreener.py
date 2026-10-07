"""DexScreener (public, no key): market, liquidity, volume, txns, pool age, listed links.
API: GET https://api.dexscreener.com/latest/dex/tokens/{address}
"""
from __future__ import annotations

import time
from typing import Any

from ...core.enums import ProviderStatus
from ...core.types import ProviderReport
from ...scoring import normalization as n
from .base import FetchContext, Provider

URL = "https://api.dexscreener.com/latest/dex/tokens/{address}"


def _same(a: str, b: str, chain: str) -> bool:
    return a.lower() == b.lower() if chain != "solana" else a == b


class DexScreener(Provider):
    name = "dexscreener"
    tier = "fast"

    def pairs(self, ctx: FetchContext) -> list[dict]:
        """All pairs for the address across chains (memoized for the scan)."""
        if "dexscreener_pairs" not in ctx.shared:
            data = self.get_json(ctx, URL.format(address=ctx.address))
            pairs = data.get("pairs") if isinstance(data, dict) else None
            ctx.shared["dexscreener_pairs"] = pairs if isinstance(pairs, list) else []
            ctx.shared["dexscreener_fetched_at"] = time.time()
        return ctx.shared["dexscreener_pairs"]

    def chains_seen(self, ctx: FetchContext) -> list[str]:
        out = []
        for p in self.pairs(ctx):
            c = n.chain_id(p.get("chainId", ""))
            if c and c not in out:
                out.append(c)
        return out

    def fetch(self, ctx: FetchContext) -> ProviderReport:
        all_pairs = self.pairs(ctx)
        b = self.builder(ctx)
        b.fetched_at = ctx.shared.get("dexscreener_fetched_at", b.fetched_at)
        mine: list[dict] = []
        for p in all_pairs:
            if n.chain_id(p.get("chainId", "")) != ctx.chain:
                continue
            base = (p.get("baseToken") or {}).get("address", "")
            quote = (p.get("quoteToken") or {}).get("address", "")
            if _same(base, ctx.address, ctx.chain) or _same(quote, ctx.address, ctx.chain):
                mine.append(p)
        if not mine:
            return ProviderReport(self.name, ProviderStatus.NOT_FOUND, b.fetched_at,
                                  f"no {ctx.chain} pairs listed for this token")

        def liq(p: dict) -> float:
            return n.usd((p.get("liquidity") or {}).get("usd")) or 0.0

        liqs = [n.usd((p.get("liquidity") or {}).get("usd")) for p in mine]
        known_liqs = [x for x in liqs if x is not None]
        if known_liqs:
            total = sum(known_liqs)
            b.add("liquidity_usd", round(total, 2), note=f"sum over {len(known_liqs)} pools")
            if total > 0:
                b.add("top_pool_share_pct", round(max(known_liqs) / total * 100, 2))
        b.add("pool_count", len(mine))

        as_base = [p for p in mine
                   if _same((p.get("baseToken") or {}).get("address", ""), ctx.address, ctx.chain)]
        main = max(as_base or mine, key=liq)
        if as_base:
            b.add("price_usd", n.usd(main.get("priceUsd")), note=f"pair {main.get('pairAddress')}")
            b.add("market_cap_usd", n.usd(main.get("marketCap")))
            b.add("fdv_usd", n.usd(main.get("fdv")))
            b.add("price_change_24h_pct", n.signed_pct((main.get("priceChange") or {}).get("h24")))

        vols = [n.usd((p.get("volume") or {}).get("h24")) for p in mine]
        if any(v is not None for v in vols):
            b.add("volume_24h_usd", round(sum(v for v in vols if v is not None), 2))
        buys = [n.count(((p.get("txns") or {}).get("h24") or {}).get("buys")) for p in mine]
        sells = [n.count(((p.get("txns") or {}).get("h24") or {}).get("sells")) for p in mine]
        if any(x is not None for x in buys):
            b.add("buys_24h", sum(x for x in buys if x is not None))
        if any(x is not None for x in sells):
            b.add("sells_24h", sum(x for x in sells if x is not None))

        created = [n.ts_from_ms(p.get("pairCreatedAt")) for p in mine]
        created_known = [c for c in created if c is not None]
        if created_known:
            b.add("pool_created_at", min(created_known), note="earliest listed pool")

        infos = [p["info"] for p in mine if isinstance(p.get("info"), dict)]
        if infos:  # links are only known when DexScreener carries an info block
            websites = {w.get("url") for i in infos for w in (i.get("websites") or [])
                        if isinstance(w, dict) and w.get("url")}
            socials = {(s.get("type"), s.get("url")) for i in infos for s in (i.get("socials") or [])
                       if isinstance(s, dict) and s.get("url")}
            b.add("has_website", bool(websites))
            b.add("social_link_count", len(socials),
                  note="links listed on DexScreener; not follower/engagement counts")

        pair_addrs = [str(p["pairAddress"]) for p in mine if p.get("pairAddress")]
        ctx.shared.setdefault("pool_addresses", set()).update(
            a.lower() if ctx.chain != "solana" else a for a in pair_addrs)
        extra: dict[str, Any] = {
            "pairs": [{"dex": p.get("dexId"), "pair": p.get("pairAddress"),
                       "liquidity_usd": n.usd((p.get("liquidity") or {}).get("usd")),
                       "url": p.get("url")} for p in mine][:10],
            "symbol": (main.get("baseToken") or {}).get("symbol"),
            "name": (main.get("baseToken") or {}).get("name"),
        }
        return ProviderReport(self.name, ProviderStatus.AVAILABLE, b.fetched_at,
                              f"{len(mine)} pools", b.readings, extra)
