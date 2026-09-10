"""MySQL table naming helpers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class MySqlTableNames:
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


def resolve_mysql_table_names(prefix: str | None) -> MySqlTableNames:
    base = normalize_prefix(prefix)
    return MySqlTableNames(
        jobs=f"{base}durable_stack_jobs",
        runs=f"{base}durable_stack_job_runs",
        migrations=f"{base}durable_stack_schema_migrations",
        runtime_command_receipts=f"{base}durable_stack_runtime_command_receipts",
    )
