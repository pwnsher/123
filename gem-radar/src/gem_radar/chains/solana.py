"""Base58 decoding for Solana / TRON address checks."""
from __future__ import annotations

import hashlib

B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_INDEX = {c: i for i, c in enumerate(B58_ALPHABET)}

TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

# Token accounts / owners that are not "holders" for concentration purposes.
SOLANA_NON_HOLDERS = {
    "1nc1nerator11111111111111111111111111111111",  # incinerator (burn)
}


def b58decode(s: str) -> bytes | None:
    """Decode base58; None when a character is outside the alphabet."""
    n = 0
    for ch in s:
        i = _B58_INDEX.get(ch)
        if i is None:
            return None
        n = n * 58 + i
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    pad = len(s) - len(s.lstrip("1"))
    return b"\x00" * pad + raw


def is_solana_pubkey(s: str) -> bool:
    if not 32 <= len(s) <= 44:
        return False
    raw = b58decode(s)
    return raw is not None and len(raw) == 32


def is_tron_address(s: str) -> bool:
    if len(s) != 34 or not s.startswith("T"):
        return False
    raw = b58decode(s)
    if raw is None or len(raw) != 25 or raw[0] != 0x41:
        return False
    check = hashlib.sha256(hashlib.sha256(raw[:21]).digest()).digest()[:4]
    return check == raw[21:]
