"""PostgreSQL runtime factory."""

from __future__ import annotations

from dataclasses import dataclass

from durablestack.core.abstractions import DurableStackEventSink, DurableStackRuntime
from durablestack.core.options import DurableStackOptions
from durablestack.runtime.factory import create_durable_stack_with_store

from .migrator import migrate_postgres
from .store import PostgresDurableJobStore
from .types import PostgresDurableStackOptions


@dataclass(frozen=True, slots=True)
class PostgresRuntimeHandle:
    """Runtime handle for PostgreSQL-backed runtime."""

    runtime: DurableStackRuntime
    store: PostgresDurableJobStore

    async def close_store(self) -> None:
        await self.store.close()


async def create_durable_stack_postgres(
    postgres: PostgresDurableStackOptions,
    options: DurableStackOptions | None = None,
    sinks: list[DurableStackEventSink] | None = None,
) -> PostgresRuntimeHandle:
    """Create a DurableStack runtime backed by PostgreSQL and run migrations."""

    store = PostgresDurableJobStore(postgres)
    try:
        await store.open()
        await migrate_postgres(store.pool, postgres.table_prefix)
        runtime = create_durable_stack_with_store(store=store, options=options, sinks=sinks)
        return PostgresRuntimeHandle(runtime=runtime, store=store)
    except Exception:
        await store.close()
        raise
