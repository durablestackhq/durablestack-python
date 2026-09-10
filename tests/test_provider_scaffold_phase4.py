import pytest

from durablestack.mysql import (
    MySqlDurableStackOptions,
    create_durable_stack_mysql,
    resolve_mysql_table_names,
)
from durablestack.postgres.table_names import resolve_postgres_table_names
from durablestack.sqlite import SqliteDurableStackOptions, resolve_sqlite_table_names
from durablestack.sqlserver import (
    SqlServerDurableStackOptions,
    create_durable_stack_sqlserver,
    resolve_sqlserver_table_names,
)


def test_sqlite_table_names_resolve_prefix() -> None:
    names = resolve_sqlite_table_names("py")
    assert names.jobs == "py_durable_stack_jobs"
    assert names.runs == "py_durable_stack_job_runs"
    assert names.migrations == "py_durable_stack_schema_migrations"
    assert names.runtime_command_receipts == "py_durable_stack_runtime_command_receipts"


def test_mysql_table_names_resolve_prefix() -> None:
    names = resolve_mysql_table_names("py")
    assert names.jobs == "py_durable_stack_jobs"
    assert names.runs == "py_durable_stack_job_runs"
    assert names.migrations == "py_durable_stack_schema_migrations"
    assert names.runtime_command_receipts == "py_durable_stack_runtime_command_receipts"


def test_sqlserver_table_names_resolve_prefix() -> None:
    names = resolve_sqlserver_table_names("py")
    assert names.jobs == "py_durable_stack_jobs"
    assert names.runs == "py_durable_stack_job_runs"
    assert names.migrations == "py_durable_stack_schema_migrations"
    assert names.runtime_command_receipts == "py_durable_stack_runtime_command_receipts"


def test_postgres_table_names_still_resolve_prefix() -> None:
    names = resolve_postgres_table_names("py")
    assert names.jobs == "py_durable_stack_jobs"


def test_sqlite_options_requires_database_path() -> None:
    with pytest.raises(ValueError):
        SqliteDurableStackOptions(database_path="")


def test_mysql_options_requires_connection_string() -> None:
    with pytest.raises(ValueError):
        MySqlDurableStackOptions(connection_string="")


def test_sqlserver_options_requires_connection_string() -> None:
    with pytest.raises(ValueError):
        SqlServerDurableStackOptions(connection_string="")


@pytest.mark.asyncio
async def test_mysql_runtime_factory_is_scaffolded_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        await create_durable_stack_mysql(MySqlDurableStackOptions(connection_string="mysql://placeholder"))


@pytest.mark.asyncio
async def test_sqlserver_runtime_factory_is_scaffolded_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        await create_durable_stack_sqlserver(
            SqlServerDurableStackOptions(connection_string="sqlserver://placeholder")
        )
