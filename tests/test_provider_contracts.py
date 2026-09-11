from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from durablestack.core.abstractions import DurableJobStore
from durablestack.core.models import RecurringRegistration
from durablestack.core.utils import generate_id
from durablestack.mysql.store import MySqlDurableJobStore
from durablestack.mysql.types import MySqlDurableStackOptions
from durablestack.postgres.migrator import migrate_postgres
from durablestack.postgres.store import PostgresDurableJobStore
from durablestack.postgres.types import PostgresDurableStackOptions
from durablestack.providers.inmemory import InMemoryDurableJobStore
from durablestack.sqlite.store import SqliteDurableJobStore
from durablestack.sqlite.types import SqliteDurableStackOptions
from durablestack.sqlserver.store import SqlServerDurableJobStore
from durablestack.sqlserver.types import SqlServerDurableStackOptions


@dataclass(slots=True)
class _StoreHandle:
    store: DurableJobStore
    closer: Callable[[], Awaitable[None]]


def _postgres_dsn() -> str | None:
    value = os.getenv("DURABLESTACK_TEST_POSTGRES_DSN")
    if value and value.strip():
        return value.strip()
    return None


def _mysql_dsn() -> str | None:
    value = os.getenv("DURABLESTACK_TEST_MYSQL_DSN")
    if value and value.strip():
        return value.strip()
    return None


def _sqlserver_dsn() -> str | None:
    value = os.getenv("DURABLESTACK_TEST_SQLSERVER_DSN")
    if value and value.strip():
        return value.strip()
    return None


async def _create_store(provider: str, tmp_path: Path) -> _StoreHandle:
    if provider == "inmemory":
        store = InMemoryDurableJobStore()

        async def _close() -> None:
            await store.close()

        return _StoreHandle(store=store, closer=_close)

    if provider == "sqlite":
        db_path = tmp_path / f"provider-contract-{generate_id()}.sqlite3"
        store = SqliteDurableJobStore(SqliteDurableStackOptions(database_path=str(db_path), table_prefix="pc_"))
        await store.open()

        async def _close() -> None:
            await store.close()

        return _StoreHandle(store=store, closer=_close)

    if provider == "postgres":
        dsn = _postgres_dsn()
        if dsn is None:
            pytest.skip("Set DURABLESTACK_TEST_POSTGRES_DSN to run Postgres provider contracts")
        prefix = f"pc_{generate_id()[:8]}_"
        store = PostgresDurableJobStore(PostgresDurableStackOptions(connection_string=dsn, table_prefix=prefix))
        await store.open()
        await migrate_postgres(store.pool, prefix)

        async def _close() -> None:
            await store.close()

        return _StoreHandle(store=store, closer=_close)

    if provider == "mysql":
        dsn = _mysql_dsn()
        if dsn is None:
            pytest.skip("Set DURABLESTACK_TEST_MYSQL_DSN to run MySQL provider contracts")
        prefix = f"my_pc_{generate_id()[:8]}_"
        store = MySqlDurableJobStore(MySqlDurableStackOptions(connection_string=dsn, table_prefix=prefix))
        await store.open()

        async def _close() -> None:
            await store.close()

        return _StoreHandle(store=store, closer=_close)

    if provider == "sqlserver":
        dsn = _sqlserver_dsn()
        if dsn is None:
            pytest.skip("Set DURABLESTACK_TEST_SQLSERVER_DSN to run SQL Server provider contracts")
        prefix = f"ss_pc_{generate_id()[:8]}_"
        store = SqlServerDurableJobStore(
            SqlServerDurableStackOptions(connection_string=dsn, table_prefix=prefix)
        )
        await store.open()

        async def _close() -> None:
            await store.close()

        return _StoreHandle(store=store, closer=_close)

    raise AssertionError(f"Unsupported provider: {provider}")


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["inmemory", "sqlite", "postgres", "mysql", "sqlserver"])
async def test_provider_contract_lease_fence_rejects_stale_completion(
    provider: str,
    tmp_path: Path,
) -> None:
    handle = await _create_store(provider, tmp_path)
    store = handle.store
    try:
        now = datetime.now(tz=UTC)
        run_id = (await store.schedule("fence-job", None, now, max_attempts=2)).run_id
        first_claim = await store.claim_due_runs("worker-a", 1, timedelta(milliseconds=50), now)
        assert len(first_claim) == 1
        second_claim = await store.claim_due_runs("worker-b", 1, timedelta(milliseconds=50), now + timedelta(milliseconds=75))
        assert len(second_claim) == 1

        stale = await store.mark_succeeded(run_id, "worker-a", now + timedelta(milliseconds=80))
        current = await store.mark_succeeded(run_id, "worker-b", now + timedelta(milliseconds=90))
        assert stale is False
        assert current is True
    finally:
        await handle.closer()


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["inmemory", "sqlite", "postgres", "mysql", "sqlserver"])
async def test_provider_contract_retries_and_terminal_failure(provider: str, tmp_path: Path) -> None:
    handle = await _create_store(provider, tmp_path)
    store = handle.store
    try:
        now = datetime.now(tz=UTC)
        run_id = (await store.schedule("retry-job", None, now, max_attempts=2)).run_id

        c1 = await store.claim_due_runs("worker", 1, timedelta(seconds=30), now)
        assert len(c1) == 1
        r1 = await store.mark_failed(
            run_id,
            "worker",
            "first failure",
            now + timedelta(seconds=1),
            True,
            now + timedelta(seconds=2),
        )
        assert r1 is True

        c2 = await store.claim_due_runs("worker", 1, timedelta(seconds=30), now + timedelta(seconds=2))
        assert len(c2) == 1
        r2 = await store.mark_failed(
            run_id,
            "worker",
            "second failure",
            now + timedelta(seconds=3),
            False,
            None,
        )
        assert r2 is True

        final = await store.get_run(run_id)
        assert final is not None
        assert final.status == "failed"
        assert final.attempt == 2
    finally:
        await handle.closer()


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["inmemory", "sqlite", "postgres", "mysql", "sqlserver"])
async def test_provider_contract_recurring_slot_uniqueness_under_contention(
    provider: str,
    tmp_path: Path,
) -> None:
    handle = await _create_store(provider, tmp_path)
    store = handle.store
    try:
        now = datetime.now(tz=UTC)
        registration = RecurringRegistration(
            name="contract-recurring",
            handler=lambda payload: payload,
            cron_expression="*/5 * * * *",
            time_zone="UTC",
            enabled=True,
            allow_concurrent_runs=True,
            max_attempts=3,
            retry_behavior="fixed",
            retry_initial_delay_seconds=5,
        )
        await store.upsert_recurring_job(registration, now)
        recurring = (await store.get_due_recurring_jobs(now, 1))[0]

        async def _attempt() -> bool:
            return await store.try_materialize_recurring_run(
                recurring,
                registration,
                now + timedelta(minutes=5),
            )

        import asyncio

        results = await asyncio.gather(_attempt(), _attempt())
        assert sum(1 for item in results if item) == 1
    finally:
        await handle.closer()


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["inmemory", "sqlite", "postgres", "mysql", "sqlserver"])
async def test_provider_contract_runtime_command_receipt_dedupes_by_command_id(
    provider: str,
    tmp_path: Path,
) -> None:
    handle = await _create_store(provider, tmp_path)
    store = handle.store
    try:
        now = datetime.now(tz=UTC)
        first = await store.try_lease_runtime_command_receipt(
            "cmd-1",
            "worker-a",
            timedelta(seconds=30),
            now,
        )
        second = await store.try_lease_runtime_command_receipt(
            "cmd-1",
            "worker-b",
            timedelta(seconds=30),
            now,
        )
        assert first is True
        assert second is False
    finally:
        await handle.closer()


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["inmemory", "sqlite", "postgres", "mysql", "sqlserver"])
async def test_provider_contract_retention_prunes_only_terminal_runs(provider: str, tmp_path: Path) -> None:
    handle = await _create_store(provider, tmp_path)
    store = handle.store
    try:
        now = datetime.now(tz=UTC)
        old = now - timedelta(days=10)

        succeeded_id = (await store.schedule("ret-job", None, old, max_attempts=2)).run_id
        claimed = await store.claim_due_runs("worker", 1, timedelta(seconds=30), old)
        assert claimed
        marked = await store.mark_succeeded(succeeded_id, "worker", old + timedelta(seconds=1))
        assert marked

        pending_id = (await store.schedule("ret-job", None, now + timedelta(days=1), max_attempts=2)).run_id
        leased_id = (await store.schedule("ret-job", None, old + timedelta(seconds=3), max_attempts=2)).run_id
        leased = await store.claim_due_runs("worker", 1, timedelta(seconds=30), old + timedelta(seconds=4))
        assert any(run.run_id == leased_id for run in leased)

        deleted = await store.prune_historical_runs(now - timedelta(days=1), batch_size=50)
        assert deleted >= 1

        pending_run = await store.get_run(pending_id)
        leased_run = await store.get_run(leased_id)
        assert pending_run is not None
        assert pending_run.status == "pending"
        assert leased_run is not None
        assert leased_run.status == "leased"
    finally:
        await handle.closer()
