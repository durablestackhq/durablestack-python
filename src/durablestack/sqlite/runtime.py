"""SQLite runtime factory."""

from __future__ import annotations

from dataclasses import dataclass

from durablestack.core.abstractions import DurableStackEventSink, DurableStackRuntime
from durablestack.core.options import DurableStackOptions
from durablestack.runtime.factory import create_durable_stack_with_store

from .store import SqliteDurableJobStore
from .types import SqliteDurableStackOptions


@dataclass(frozen=True, slots=True)
class SqliteRuntimeHandle:
    """Runtime handle for SQLite-backed runtime."""

    runtime: DurableStackRuntime
    store: SqliteDurableJobStore

    async def close_store(self) -> None:
        await self.store.close()


async def create_durable_stack_sqlite(
    sqlite: SqliteDurableStackOptions,
    options: DurableStackOptions | None = None,
    sinks: list[DurableStackEventSink] | None = None,
) -> SqliteRuntimeHandle:
    """Create a DurableStack runtime backed by SQLite and run migrations."""

    store = SqliteDurableJobStore(sqlite)
    try:
        await store.open()
        runtime = create_durable_stack_with_store(store=store, options=options, sinks=sinks)
        return SqliteRuntimeHandle(runtime=runtime, store=store)
    except Exception:
        await store.close()
        raise
