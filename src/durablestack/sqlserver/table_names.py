"""SQL Server table naming helpers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SqlServerTableNames:
    jobs: str
    runs: str
    migrations: str
    runtime_command_receipts: str


def normalize_prefix(prefix: str | None) -> str:
    if prefix is None:
        return ""
    stripped = prefix.strip().lower()
    if stripped == "":
        return ""
    if not stripped.endswith("_"):
        stripped += "_"
    return stripped


def resolve_sqlserver_table_names(prefix: str | None) -> SqlServerTableNames:
    base = normalize_prefix(prefix)
    return SqlServerTableNames(
        jobs=f"{base}durable_stack_jobs",
        runs=f"{base}durable_stack_job_runs",
        migrations=f"{base}durable_stack_schema_migrations",
        runtime_command_receipts=f"{base}durable_stack_runtime_command_receipts",
    )
