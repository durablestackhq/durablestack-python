"""SQL Server migrator for DurableStack runtime tables."""

from __future__ import annotations

from typing import Any

from .table_names import SqlServerTableNames, resolve_sqlserver_table_names

SCHEMA_VERSION = 1


def _q(name: str) -> str:
    return "[" + name.replace("]", "]]" ) + "]"


def migrate_sqlserver(connection: Any, table_prefix: str | None) -> SqlServerTableNames:
    tables = resolve_sqlserver_table_names(table_prefix)
    _apply_sqlserver_migrations(connection, table_prefix, tables)
    verify_sqlserver_schema(connection, tables)
    return tables


def verify_sqlserver_schema(connection: Any, tables: SqlServerTableNames) -> None:
    probes = [
        f"select top 1 job_name, cron_expression, time_zone, next_run_at_utc from {_q(tables.jobs)}",
        f"select top 1 id, job_name, status, scheduled_for_utc, attempt, max_attempts from {_q(tables.runs)}",
        f"select top 1 version, applied_at_utc from {_q(tables.migrations)}",
        f"select top 1 command_id, status, recorded_at_utc from {_q(tables.runtime_command_receipts)}",
    ]
    cursor = connection.cursor()
    try:
        for sql in probes:
            cursor.execute(sql)
    finally:
        cursor.close()


def _apply_sqlserver_migrations(
    connection: Any,
    table_prefix: str | None,
    tables: SqlServerTableNames,
) -> None:
    lock_name = f"durablestack_py_migrate_{(table_prefix or 'default').strip() or 'default'}"
    cursor = connection.cursor()
    try:
        cursor.execute("BEGIN TRANSACTION")
        cursor.execute(
            """
            DECLARE @lock_result INT;
            EXEC @lock_result = sp_getapplock
                @Resource = ?,
                @LockMode = 'Exclusive',
                @LockOwner = 'Transaction',
                @LockTimeout = 30000;
            SELECT @lock_result AS lock_result;
            """,
            (lock_name,),
        )
        lock_row = cursor.fetchone()
        lock_result = int(lock_row[0]) if lock_row is not None else -999
        if lock_result < 0:
            raise RuntimeError("Failed to acquire SQL Server migration lock")

        cursor.execute(
            f"""
            if object_id('{tables.migrations}', 'U') is null
            begin
                create table {_q(tables.migrations)} (
                  version int not null primary key,
                  applied_at_utc datetime2(6) not null
                );
            end
            """
        )

        cursor.execute(
            f"select version from {_q(tables.migrations)} where version = ?",
            (SCHEMA_VERSION,),
        )
        if cursor.fetchone() is not None:
            connection.commit()
            return

        cursor.execute(
            f"""
            if object_id('{tables.jobs}', 'U') is null
            begin
                create table {_q(tables.jobs)} (
                  job_name nvarchar(255) not null primary key,
                  cron_expression nvarchar(255) not null,
                  time_zone nvarchar(255) not null,
                  max_attempts int not null,
                  enabled bit not null,
                  allow_concurrent_runs bit not null,
                  retry_behavior nvarchar(64) null,
                  retry_initial_delay_seconds int null,
                  next_run_at_utc datetime2(6) not null,
                  updated_at_utc datetime2(6) not null
                );
            end
            """
        )

        cursor.execute(
            f"""
            if object_id('{tables.runs}', 'U') is null
            begin
                create table {_q(tables.runs)} (
                  id nvarchar(32) not null primary key,
                  job_name nvarchar(255) not null,
                  status nvarchar(32) not null,
                  scheduled_for_utc datetime2(6) not null,
                  schedule_slot_utc datetime2(6) null,
                  started_at_utc datetime2(6) null,
                  completed_at_utc datetime2(6) null,
                  attempt int not null,
                  max_attempts int not null,
                  lease_owner nvarchar(255) null,
                  lease_until_utc datetime2(6) null,
                  payload_json nvarchar(max) null,
                  error_message nvarchar(max) null,
                  created_at_utc datetime2(6) not null
                );
            end
            """
        )

        cursor.execute(
            f"""
            if not exists (
                select 1 from sys.indexes
                where name = 'ix_{tables.runs}_status_scheduled'
                  and object_id = object_id('{tables.runs}')
            )
            begin
                create index {_q(f'ix_{tables.runs}_status_scheduled')}
                on {_q(tables.runs)} (status, scheduled_for_utc asc);
            end
            """
        )
        cursor.execute(
            f"""
            if not exists (
                select 1 from sys.indexes
                where name = 'ix_{tables.runs}_job_name_scheduled'
                  and object_id = object_id('{tables.runs}')
            )
            begin
                create index {_q(f'ix_{tables.runs}_job_name_scheduled')}
                on {_q(tables.runs)} (job_name, scheduled_for_utc desc);
            end
            """
        )
        cursor.execute(
            f"""
            if not exists (
                select 1 from sys.indexes
                where name = 'ix_{tables.runs}_recurring_slot_unique'
                  and object_id = object_id('{tables.runs}')
            )
            begin
                create unique index {_q(f'ix_{tables.runs}_recurring_slot_unique')}
                on {_q(tables.runs)} (job_name, schedule_slot_utc)
                where schedule_slot_utc is not null;
            end
            """
        )

        cursor.execute(
            f"""
            if object_id('{tables.runtime_command_receipts}', 'U') is null
            begin
                create table {_q(tables.runtime_command_receipts)} (
                  command_id nvarchar(255) not null primary key,
                  status nvarchar(32) not null,
                  error_code nvarchar(255) null,
                  error_message nvarchar(max) null,
                  run_id nvarchar(32) null,
                  recorded_at_utc datetime2(6) not null,
                  completed_at_utc datetime2(6) null,
                  uploaded_at_utc datetime2(6) null,
                  lease_owner nvarchar(255) null,
                  lease_until_utc datetime2(6) null
                );
            end
            """
        )

        cursor.execute(
            f"""
            merge {_q(tables.migrations)} as target
            using (select cast(? as int) as version) as source
            on target.version = source.version
            when not matched then
              insert (version, applied_at_utc)
              values (source.version, sysutcdatetime());
            """,
            (SCHEMA_VERSION,),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
