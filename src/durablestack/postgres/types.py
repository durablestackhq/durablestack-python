"""PostgreSQL provider options."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PostgresDurableStackOptions:
    """Options for PostgreSQL-backed DurableStack runtime."""

    connection_string: str
    table_prefix: str = "durable_stack"

    def __post_init__(self) -> None:
        if self.connection_string.strip() == "":
            raise ValueError("connection_string is required")
