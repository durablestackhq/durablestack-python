"""SQL Server runtime factory."""

from __future__ import annotations

from dataclasses import dataclass

from durablestack.core.abstractions import DurableStackEventSink, DurableStackRuntime
from durablestack.core.options import DurableStackOptions
from durablestack.runtime.factory import create_durable_stack_with_store

from .store import SqlServerDurableJobStore
from .types import SqlServerDurableStackOptions


@dataclass(frozen=True, slots=True)
class SqlServerRuntimeHandle:
    """Runtime handle for SQL Server-backed runtime."""

    runtime: DurableStackRuntime
    store: SqlServerDurableJobStore

    async def close_store(self) -> None:
        await self.store.close()


async def create_durable_stack_sqlserver(
    sqlserver: SqlServerDurableStackOptions,
    options: DurableStackOptions | None = None,
    sinks: list[DurableStackEventSink] | None = None,
) -> SqlServerRuntimeHandle:
    """Create a DurableStack runtime backed by SQL Server and run migrations."""

    store = SqlServerDurableJobStore(sqlserver)
    try:
        await store.open()
        runtime = create_durable_stack_with_store(store=store, options=options, sinks=sinks)
        return SqlServerRuntimeHandle(runtime=runtime, store=store)
    except Exception:
        await store.close()
        raise
