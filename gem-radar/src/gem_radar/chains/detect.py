"""Address validation and syntactic chain-family detection.

Syntax alone never picks one EVM chain: a 0x address is valid on every EVM
chain, so the EVM chain is resolved later from provider evidence (DexScreener
pairs, deployed bytecode), and reported as AMBIGUOUS when that evidence points
at more than one chain.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from ..core.errors import InvalidAddressError
from .evm import EVM_CHAINS, EVM_RE, checksum_ok
from .solana import is_solana_pubkey, is_tron_address

SUPPORTED_CHAINS = ["solana", *EVM_CHAINS.keys()]

_SAFE_INPUT = re.compile(r"^[0-9A-Za-z:_-]{1,128}$")
_MOVE_RE = re.compile(r"^0x[0-9a-fA-F]{64}(::[A-Za-z0-9_]+::[A-Za-z0-9_]+)?$")
_TON_RE = re.compile(r"^(EQ|UQ|kQ|0Q)[A-Za-z0-9_-]{46}$")


@dataclass
class Detection:
    input: str
    address: str                      # normalized (EVM lower-case, base58 as given)
    family: str                       # evm | solana | tron | move | ton
    candidates: list[str] = field(default_factory=list)  # chains the address could be on
    supported: bool = True
    reason: str = ""
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return self.__dict__.copy()


def sanitize(raw: str) -> str:
    """Trim and reject anything that is not plausibly an address token."""
    if not isinstance(raw, str):
        raise InvalidAddressError("address must be a string")
    s = raw.strip()
    if not s:
        raise InvalidAddressError("empty address")
    if not _SAFE_INPUT.match(s):
        raise InvalidAddressError(
            "address contains characters no supported address format uses")
    return s


def detect(raw: str) -> Detection:
    s = sanitize(raw)
    if s.lower().startswith("0x"):
        if EVM_RE.match(s):
            if not checksum_ok(s):
                raise InvalidAddressError(
                    "mixed-case EVM address fails its EIP-55 checksum (likely a typo)")
            return Detection(s, s.lower(), "evm", list(EVM_CHAINS.keys()),
                             notes=["EVM address: valid on every EVM chain; "
                                    "chain resolved from provider evidence"])
        if _MOVE_RE.match(s):
            return Detection(s, s, "move", ["sui", "aptos"], supported=False,
                             reason="Move-chain (Sui/Aptos) tokens are not supported by "
                                    "Gem Radar's data providers yet")
        raise InvalidAddressError("0x address must be 40 hex characters (EVM)")
    if is_tron_address(s):
        return Detection(s, s, "tron", ["tron"], supported=False,
                         reason="TRON tokens are not supported by Gem Radar's providers yet")
    if _TON_RE.match(s):
        return Detection(s, s, "ton", ["ton"], supported=False,
                         reason="TON jettons are not supported by Gem Radar's providers yet")
    if is_solana_pubkey(s):
        return Detection(s, s, "solana", ["solana"],
                         notes=["base58 32-byte key: Solana format (other SVM chains use the "
                                "same format; confirmed only by Solana RPC/DEX evidence)"])
    raise InvalidAddressError("not a valid EVM (0x + 40 hex) or Solana (base58, 32 bytes) address")
