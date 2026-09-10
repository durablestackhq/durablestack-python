from durablestack.postgres.table_names import resolve_postgres_table_names
from durablestack.postgres.types import PostgresDurableStackOptions


def test_postgres_table_names_resolve_prefix() -> None:
    names = resolve_postgres_table_names("py")
    assert names.jobs == "py_durable_stack_jobs"
    assert names.runs == "py_durable_stack_job_runs"
    assert names.migrations == "py_durable_stack_schema_migrations"
    assert names.runtime_command_receipts == "py_durable_stack_runtime_command_receipts"


def test_postgres_options_requires_connection_string() -> None:
    try:
        PostgresDurableStackOptions(connection_string="")
        raise AssertionError("expected ValueError")
    except ValueError:
        pass
