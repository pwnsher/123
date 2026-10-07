"""On-chain reads over JSON-RPC: the most authoritative source for mint/freeze
authority (Solana) and bytecode presence (EVM).

Endpoints come from env vars (they may embed API keys and are never logged):
  SOLANA_RPC_URL                  default https://api.mainnet-beta.solana.com
  GEM_RADAR_RPC_<CHAIN>           e.g. GEM_RADAR_RPC_ETHEREUM; default publicnode.com
"""
from __future__ import annotations

import os

from ...chains.evm import EVM_CHAINS
from ...chains.solana import TOKEN_2022_PROGRAM, TOKEN_PROGRAM
from ...core.enums import ProviderStatus
from ...core.errors import ProviderError
from ...core.types import ProviderReport
from .base import FetchContext, Provider

PUBLICNODE = {
    "ethereum": "https://ethereum-rpc.publicnode.com",
    "bsc": "https://bsc-rpc.publicnode.com",
    "base": "https://base-rpc.publicnode.com",
    "arbitrum": "https://arbitrum-one-rpc.publicnode.com",
    "polygon": "https://polygon-bor-rpc.publicnode.com",
    "optimism": "https://optimism-rpc.publicnode.com",
    "avalanche": "https://avalanche-c-chain-rpc.publicnode.com",
}


def _rpc(provider: Provider, ctx: FetchContext, url: str, method: str, params: list):
    d = provider.get_json(ctx, url, method="POST",
                          payload={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    if not isinstance(d, dict):
        raise ProviderError(provider.name, ProviderStatus.PARSE_ERROR, "RPC answer not an object")
    err = d.get("error")
    if err:
        code = err.get("code", "") if isinstance(err, dict) else ""
        raise ProviderError(provider.name, ProviderStatus.HTTP_ERROR, f"RPC error {code}")
    return d.get("result")


class SolanaRPC(Provider):
    name = "solana_rpc"
    tier = "fast"
    chains = {"solana"}

    def fetch(self, ctx: FetchContext) -> ProviderReport:
        url = os.environ.get("SOLANA_RPC_URL") or "https://api.mainnet-beta.solana.com"
        res = _rpc(self, ctx, url, "getAccountInfo", [ctx.address, {"encoding": "jsonParsed"}])
        value = (res or {}).get("value") if isinstance(res, dict) else None
        if not value:
            return ProviderReport(self.name, ProviderStatus.NOT_FOUND,
                                  message="no account at this address on Solana mainnet",
                                  extra={"is_mint": False})
        owner = value.get("owner")
        parsed = (value.get("data") or {}).get("parsed") if isinstance(value.get("data"), dict) else None
        if owner not in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM) or not isinstance(parsed, dict) \
                or parsed.get("type") != "mint":
            return ProviderReport(self.name, ProviderStatus.NOT_FOUND,
                                  message="account exists but is not an SPL token mint",
                                  extra={"is_mint": False})
        info = parsed.get("info") or {}
        b = self.builder(ctx)
        if "mintAuthority" in info:
            b.add("mint_authority_active", info["mintAuthority"] is not None, raw=info["mintAuthority"])
        if "freezeAuthority" in info:
            b.add("freeze_authority_active", info["freezeAuthority"] is not None,
                  raw=info["freezeAuthority"])
        exts = {e.get("extension"): e.get("state") for e in (info.get("extensions") or [])
                if isinstance(e, dict)}
        if owner == TOKEN_PROGRAM:
            for m in ("permanent_delegate", "transfer_hook", "non_transferable"):
                b.add(m, False, note="legacy SPL Token program has no extensions")
        else:
            pd = exts.get("permanentDelegate")
            b.add("permanent_delegate", bool(isinstance(pd, dict) and pd.get("delegate")))
            th = exts.get("transferHook")
            b.add("transfer_hook", bool(isinstance(th, dict) and th.get("programId")))
            b.add("non_transferable", "nonTransferable" in exts)
            fee = exts.get("transferFeeConfig")
            if isinstance(fee, dict):
                bps = (fee.get("newerTransferFee") or {}).get("transferFeeBasisPoints")
                if isinstance(bps, (int, float)):
                    b.add("transfer_tax_pct", bps / 100.0, raw=bps)
            else:
                b.add("transfer_tax_pct", 0.0, note="no transfer-fee extension")
        return ProviderReport(self.name, ProviderStatus.AVAILABLE, b.fetched_at,
                              "Token-2022" if owner == TOKEN_2022_PROGRAM else "SPL Token",
                              b.readings, {"is_mint": True, "supply": info.get("supply"),
                                           "decimals": info.get("decimals"),
                                           "extensions": sorted(k for k in exts if k)})


class EvmRPC(Provider):
    name = "evm_rpc"
    tier = "fast"
    chains = set(EVM_CHAINS)

    @staticmethod
    def url_for(chain: str) -> str:
        return os.environ.get(f"GEM_RADAR_RPC_{chain.upper()}") or PUBLICNODE[chain]

    def has_code(self, ctx: FetchContext) -> bool:
        code = _rpc(self, ctx, self.url_for(ctx.chain), "eth_getCode", [ctx.address, "latest"])
        return isinstance(code, str) and code not in ("0x", "0x0", "")

    def fetch(self, ctx: FetchContext) -> ProviderReport:
        b = self.builder(ctx)
        has = self.has_code(ctx)
        b.add("contract_has_code", has)
        return ProviderReport(self.name, ProviderStatus.AVAILABLE, b.fetched_at,
                              "" if has else "no bytecode at this address", b.readings)
