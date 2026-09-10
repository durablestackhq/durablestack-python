"""MySQL runtime factory (Phase 4 scaffold)."""

from __future__ import annotations

from dataclasses import dataclass

from durablestack.core.abstractions import DurableStackRuntime

from .types import MySqlDurableStackOptions


@dataclass(frozen=True, slots=True)
class MySqlRuntimeHandle:
    """Runtime handle for MySQL-backed runtime."""

    runtime: DurableStackRuntime


async def create_durable_stack_mysql(
    mysql: MySqlDurableStackOptions,
) -> MySqlRuntimeHandle:
    """Placeholder factory for upcoming MySQL provider implementation."""

    _ = mysql
    raise NotImplementedError("MySQL provider is scaffolded in Phase 4 and not yet implemented")
