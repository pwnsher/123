"""Provider registry."""
from __future__ import annotations

from .base import FetchContext, Provider
from .dexscreener import DexScreener
from .goplus import GoPlusEVM, GoPlusSolana
from .honeypot_is import HoneypotIs
from .local_files import LocalFiles, StaticProvider
from .rpc import EvmRPC, SolanaRPC
from .rugcheck import RugCheck

__all__ = ["FetchContext", "Provider", "DexScreener", "GoPlusEVM", "GoPlusSolana", "HoneypotIs",
           "LocalFiles", "StaticProvider", "EvmRPC", "SolanaRPC", "RugCheck", "default_providers"]


def default_providers(cfg: dict) -> list[Provider]:
    disabled = set(cfg.get("providers_disabled") or [])
    all_ = [DexScreener(), GoPlusEVM(), EvmRPC(), RugCheck(), SolanaRPC(),
            HoneypotIs(), GoPlusSolana(), LocalFiles()]
    return [p for p in all_ if p.name not in disabled]
