"""Event sink implementations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class NoOpDurableStackEventSink:
    """Default sink that discards all events."""

    async def publish(self, event: dict[str, Any]) -> None:
        _ = event


@dataclass(slots=True)
class InMemoryEventSink:
    """Testing sink that stores published events in memory."""

    events: list[dict[str, Any]] = field(default_factory=list)

    async def publish(self, event: dict[str, Any]) -> None:
        self.events.append(event)
