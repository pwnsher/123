"""Provenance helpers: build Readings and render source lines."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from ..core.enums import Status
from ..core.types import Reading


def iso(ts: float | None) -> str:
    if ts is None:
        return "—"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class ReadingBuilder:
    """Collects readings for one provider fetch, skipping absent values:
    a value the provider did not give is never turned into a reading."""

    def __init__(self, source: str, fetched_at: float, *, is_mock: bool = False):
        self.source = source
        self.fetched_at = fetched_at
        self.is_mock = is_mock
        self.readings: list[Reading] = []

    def add(self, metric: str, value: Any, raw: Any = None, note: str = "") -> None:
        if value is None:
            return
        self.readings.append(Reading(metric, value, self.source, self.fetched_at, Status.LIVE,
                                     raw if raw is not None else value, note, self.is_mock))

    def extend(self, items: Iterable[tuple[str, Any]]) -> None:
        for metric, value in items:
            self.add(metric, value)
