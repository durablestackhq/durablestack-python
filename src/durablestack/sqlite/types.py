"""SQLite provider options."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SqliteDurableStackOptions:
    """Options for SQLite-backed DurableStack runtime."""

    database_path: str
    table_prefix: str = "durable_stack"
    busy_timeout_ms: int = 5_000

    def __post_init__(self) -> None:
        if self.database_path.strip() == "":
            raise ValueError("database_path is required")
        if self.busy_timeout_ms <= 0:
            raise ValueError("busy_timeout_ms must be greater than 0")
