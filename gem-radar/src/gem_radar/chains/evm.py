"""EVM address handling: format check and EIP-55 checksum (pure-Python Keccak-256)."""
from __future__ import annotations

import re

EVM_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

# Chains Gem Radar analyses, keyed by our name: (DexScreener chainId, GoPlus/EVM chain id)
EVM_CHAINS: dict[str, dict] = {
    "ethereum": {"dexscreener": "ethereum", "chain_id": 1},
    "bsc": {"dexscreener": "bsc", "chain_id": 56},
    "base": {"dexscreener": "base", "chain_id": 8453},
    "arbitrum": {"dexscreener": "arbitrum", "chain_id": 42161},
    "polygon": {"dexscreener": "polygon", "chain_id": 137},
    "optimism": {"dexscreener": "optimism", "chain_id": 10},
    "avalanche": {"dexscreener": "avalanche", "chain_id": 43114},
}

BURN_ADDRESSES = {
    "0x0000000000000000000000000000000000000000",
    "0x000000000000000000000000000000000000dead",
    "0xdead000000000000000042069420694206942069",
}

_RC = [
    0x0000000000000001, 0x0000000000008082, 0x800000000000808A, 0x8000000080008000,
    0x000000000000808B, 0x0000000080000001, 0x8000000080008081, 0x8000000000008009,
    0x000000000000008A, 0x0000000000000088, 0x0000000080008009, 0x000000008000000A,
    0x000000008000808B, 0x800000000000008B, 0x8000000000008089, 0x8000000000008003,
    0x8000000000008002, 0x8000000000000080, 0x000000000000800A, 0x800000008000000A,
    0x8000000080008081, 0x8000000000008080, 0x0000000080000001, 0x8000000080008008,
]
_ROT = [[0, 36, 3, 41, 18], [1, 44, 10, 45, 2], [62, 6, 43, 15, 61],
        [28, 55, 25, 21, 56], [27, 20, 39, 8, 14]]
_MASK = (1 << 64) - 1


def _rol(x: int, n: int) -> int:
    n %= 64
    return ((x << n) | (x >> (64 - n))) & _MASK if n else x


def _keccak_f(a: list[list[int]]) -> None:
    for rc in _RC:
        c = [a[x][0] ^ a[x][1] ^ a[x][2] ^ a[x][3] ^ a[x][4] for x in range(5)]
        d = [c[(x - 1) % 5] ^ _rol(c[(x + 1) % 5], 1) for x in range(5)]
        for x in range(5):
            for y in range(5):
                a[x][y] ^= d[x]
        b = [[0] * 5 for _ in range(5)]
        for x in range(5):
            for y in range(5):
                b[y][(2 * x + 3 * y) % 5] = _rol(a[x][y], _ROT[x][y])
        for x in range(5):
            for y in range(5):
                a[x][y] = b[x][y] ^ ((~b[(x + 1) % 5][y]) & b[(x + 2) % 5][y])
        a[0][0] ^= rc


def keccak256(data: bytes) -> bytes:
    """Keccak-256 as Ethereum uses it (original padding 0x01, not SHA3's 0x06)."""
    rate = 136
    msg = bytearray(data) + b"\x01"
    while len(msg) % rate:
        msg += b"\x00"
    msg[-1] |= 0x80
    a = [[0] * 5 for _ in range(5)]
    for off in range(0, len(msg), rate):
        block = msg[off:off + rate]
        for i in range(rate // 8):
            a[i % 5][i // 5] ^= int.from_bytes(block[i * 8:i * 8 + 8], "little")
        _keccak_f(a)
    out = b"".join(a[i % 5][i // 5].to_bytes(8, "little") for i in range(4))
    return out


def to_checksum(address: str) -> str:
    lower = address[2:].lower()
    h = keccak256(lower.encode()).hex()
    return "0x" + "".join(ch.upper() if int(h[i], 16) >= 8 else ch for i, ch in enumerate(lower))


def checksum_ok(address: str) -> bool:
    """True for all-lower / all-upper (no checksum) or a valid EIP-55 mixed-case address."""
    body = address[2:]
    if body == body.lower() or body == body.upper():
        return True
    return to_checksum(address) == address
