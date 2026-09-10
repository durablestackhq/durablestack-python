"""SQL Server runtime factory (Phase 4 scaffold)."""

from __future__ import annotations

from dataclasses import dataclass

from durablestack.core.abstractions import DurableStackRuntime

from .types import SqlServerDurableStackOptions


@dataclass(frozen=True, slots=True)
class SqlServerRuntimeHandle:
    """Runtime handle for SQL Server-backed runtime."""

    runtime: DurableStackRuntime


async def create_durable_stack_sqlserver(
    sqlserver: SqlServerDurableStackOptions,
) -> SqlServerRuntimeHandle:
    """Placeholder factory for upcoming SQL Server provider implementation."""

    _ = sqlserver
    raise NotImplementedError("SQL Server provider is scaffolded in Phase 4 and not yet implemented")
