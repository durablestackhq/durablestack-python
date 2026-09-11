from __future__ import annotations

from dataclasses import dataclass

import pytest

import durablestack.mysql.runtime as mysql_runtime
import durablestack.postgres.runtime as postgres_runtime
import durablestack.sqlite.runtime as sqlite_runtime
import durablestack.sqlserver.runtime as sqlserver_runtime
from durablestack.mysql.types import MySqlDurableStackOptions
from durablestack.postgres.types import PostgresDurableStackOptions
from durablestack.sqlite.types import SqliteDurableStackOptions
from durablestack.sqlserver.types import SqlServerDurableStackOptions


@dataclass
class _FakeStore:
    opened: bool = False
    closed: bool = False

    async def open(self) -> None:
        self.opened = True

    async def close(self) -> None:
        self.closed = True

    @property
    def pool(self) -> object:
        return object()


@pytest.mark.asyncio
async def test_postgres_factory_closes_store_if_runtime_construction_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    holder: dict[str, _FakeStore] = {}

    def fake_store_ctor(_options: PostgresDurableStackOptions) -> _FakeStore:
        store = _FakeStore()
        holder["store"] = store
        return store

    async def fake_migrate(_pool: object, _prefix: str | None) -> None:
        return

    def fail_runtime_creation(**_kwargs: object) -> object:
        raise RuntimeError("runtime creation failed")

    monkeypatch.setattr(postgres_runtime, "PostgresDurableJobStore", fake_store_ctor)
    monkeypatch.setattr(postgres_runtime, "migrate_postgres", fake_migrate)
    monkeypatch.setattr(postgres_runtime, "create_durable_stack_with_store", fail_runtime_creation)

    with pytest.raises(RuntimeError):
        await postgres_runtime.create_durable_stack_postgres(
            PostgresDurableStackOptions(connection_string="postgresql://user:pass@localhost/db")
        )

    assert holder["store"].opened is True
    assert holder["store"].closed is True


@pytest.mark.asyncio
async def test_sqlite_factory_closes_store_if_runtime_construction_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    holder: dict[str, _FakeStore] = {}

    def fake_store_ctor(_options: SqliteDurableStackOptions) -> _FakeStore:
        store = _FakeStore()
        holder["store"] = store
        return store

    def fail_runtime_creation(**_kwargs: object) -> object:
        raise RuntimeError("runtime creation failed")

    monkeypatch.setattr(sqlite_runtime, "SqliteDurableJobStore", fake_store_ctor)
    monkeypatch.setattr(sqlite_runtime, "create_durable_stack_with_store", fail_runtime_creation)

    with pytest.raises(RuntimeError):
        await sqlite_runtime.create_durable_stack_sqlite(
            SqliteDurableStackOptions(database_path=":memory:")
        )

    assert holder["store"].opened is True
    assert holder["store"].closed is True


@pytest.mark.asyncio
async def test_mysql_factory_closes_store_if_runtime_construction_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    holder: dict[str, _FakeStore] = {}

    def fake_store_ctor(_options: MySqlDurableStackOptions) -> _FakeStore:
        store = _FakeStore()
        holder["store"] = store
        return store

    def fail_runtime_creation(**_kwargs: object) -> object:
        raise RuntimeError("runtime creation failed")

    monkeypatch.setattr(mysql_runtime, "MySqlDurableJobStore", fake_store_ctor)
    monkeypatch.setattr(mysql_runtime, "create_durable_stack_with_store", fail_runtime_creation)

    with pytest.raises(RuntimeError):
        await mysql_runtime.create_durable_stack_mysql(
            MySqlDurableStackOptions(connection_string="mysql://user:pass@localhost:3306/db")
        )

    assert holder["store"].opened is True
    assert holder["store"].closed is True


@pytest.mark.asyncio
async def test_sqlserver_factory_closes_store_if_runtime_construction_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    holder: dict[str, _FakeStore] = {}

    def fake_store_ctor(_options: SqlServerDurableStackOptions) -> _FakeStore:
        store = _FakeStore()
        holder["store"] = store
        return store

    def fail_runtime_creation(**_kwargs: object) -> object:
        raise RuntimeError("runtime creation failed")

    monkeypatch.setattr(sqlserver_runtime, "SqlServerDurableJobStore", fake_store_ctor)
    monkeypatch.setattr(sqlserver_runtime, "create_durable_stack_with_store", fail_runtime_creation)

    with pytest.raises(RuntimeError):
        await sqlserver_runtime.create_durable_stack_sqlserver(
            SqlServerDurableStackOptions(connection_string="Driver={ODBC Driver 18 for SQL Server};Server=localhost")
        )

    assert holder["store"].opened is True
    assert holder["store"].closed is True
