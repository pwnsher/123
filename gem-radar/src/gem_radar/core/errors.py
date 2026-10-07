"""Structured errors. Messages never carry secrets (URLs with keys are redacted)."""
from __future__ import annotations

import re

from .enums import ProviderStatus

_SECRETISH = re.compile(r"(api[-_]?key|token|secret|auth)=([^&\s]+)", re.IGNORECASE)


def redact(text: str) -> str:
    """Strip query-string credentials and path-embedded keys from a message."""
    text = _SECRETISH.sub(lambda m: f"{m.group(1)}=***", text)
    # Helius / Alchemy style keys embedded in the path: /v2/<key>
    return re.sub(r"/(v\d)/[A-Za-z0-9_-]{20,}", r"/\1/***", text)


class GemRadarError(Exception):
    code = "GEM_RADAR_ERROR"

    def to_dict(self) -> dict:
        return {"code": self.code, "message": redact(str(self))}


class InvalidAddressError(GemRadarError):
    code = "MALFORMED_ADDRESS"


class UnsupportedChainError(GemRadarError):
    code = "UNSUPPORTED_CHAIN"

    def __init__(self, chain: str, reason: str):
        super().__init__(f"{chain}: {reason}")
        self.chain = chain
        self.reason = reason


class AmbiguousChainError(GemRadarError):
    code = "AMBIGUOUS_CHAIN"

    def __init__(self, candidates: list[str], reason: str):
        super().__init__(f"ambiguous chain ({', '.join(candidates)}): {reason}")
        self.candidates = candidates
        self.reason = reason


class ProviderError(GemRadarError):
    code = "PROVIDER_ERROR"

    def __init__(self, provider: str, status: ProviderStatus, message: str):
        super().__init__(redact(f"{provider}: {status.value}: {message}"))
        self.detail = redact(message)
        self.provider = provider
        self.status = status
