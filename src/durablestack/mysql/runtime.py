"""MySQL runtime factory."""

from __future__ import annotations

from dataclasses import dataclass

from durablestack.core.abstractions import DurableStackEventSink, DurableStackRuntime
from durablestack.core.options import DurableStackOptions
from durablestack.runtime.factory import create_durable_stack_with_store

from .store import MySqlDurableJobStore
from .types import MySqlDurableStackOptions


@dataclass(frozen=True, slots=True)
class MySqlRuntimeHandle:
    """Runtime handle for MySQL-backed runtime."""

    runtime: DurableStackRuntime
    store: MySqlDurableJobStore

    async def close_store(self) -> None:
        await self.store.close()


async def create_durable_stack_mysql(
    mysql: MySqlDurableStackOptions,
    options: DurableStackOptions | None = None,
    sinks: list[DurableStackEventSink] | None = None,
) -> MySqlRuntimeHandle:
    """Create a DurableStack runtime backed by MySQL and run migrations."""

    store = MySqlDurableJobStore(mysql)
    try:
        await store.open()
        runtime = create_durable_stack_with_store(store=store, options=options, sinks=sinks)
        return MySqlRuntimeHandle(runtime=runtime, store=store)
    except Exception:
        await store.close()
        raise
