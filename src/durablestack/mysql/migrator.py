"""MySQL migrator for DurableStack runtime tables."""

from __future__ import annotations

from typing import Any

from .table_names import MySqlTableNames, resolve_mysql_table_names

SCHEMA_VERSION = 1


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def migrate_mysql(connection: Any, table_prefix: str | None) -> MySqlTableNames:
    tables = resolve_mysql_table_names(table_prefix)
    _apply_mysql_migrations(connection, table_prefix, tables)
    verify_mysql_schema(connection, tables)
    return tables


def verify_mysql_schema(connection: Any, tables: MySqlTableNames) -> None:
    probes = [
        f"select job_name, cron_expression, time_zone, next_run_at_utc from {_q(tables.jobs)} limit 1",
        f"select id, job_name, status, scheduled_for_utc, attempt, max_attempts from {_q(tables.runs)} limit 1",
        f"select version, applied_at_utc from {_q(tables.migrations)} limit 1",
        f"select command_id, status, recorded_at_utc from {_q(tables.runtime_command_receipts)} limit 1",
    ]
    with connection.cursor() as cursor:
        for sql in probes:
            cursor.execute(sql)


def _apply_mysql_migrations(connection: Any, table_prefix: str | None, tables: MySqlTableNames) -> None:
    lock_name = f"durablestack_py_migrate_{(table_prefix or 'default').strip() or 'default'}"
    with connection.cursor() as cursor:
        cursor.execute("select get_lock(%s, 30)", (lock_name,))
        row = cursor.fetchone()
        got_lock = bool(row and next(iter(row.values())) == 1)
        if not got_lock:
            raise RuntimeError("Failed to acquire MySQL migration lock")

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                create table if not exists {_q(tables.migrations)} (
                  version int not null primary key,
                  applied_at_utc datetime(6) not null
                ) engine=InnoDB
                """
            )

            cursor.execute(
                f"select version from {_q(tables.migrations)} where version = %s",
                (SCHEMA_VERSION,),
            )
            if cursor.fetchone() is not None:
                connection.commit()
                return

            cursor.execute(
                f"""
                create table if not exists {_q(tables.jobs)} (
                  job_name varchar(255) not null primary key,
                  cron_expression varchar(255) not null,
                  time_zone varchar(255) not null,
                  max_attempts int not null,
                  enabled tinyint(1) not null,
                  allow_concurrent_runs tinyint(1) not null,
                  retry_behavior varchar(64) null,
                  retry_initial_delay_seconds int null,
                  next_run_at_utc datetime(6) not null,
                  updated_at_utc datetime(6) not null
                ) engine=InnoDB
                """
            )

            cursor.execute(
                f"""
                create table if not exists {_q(tables.runs)} (
                  id char(32) not null primary key,
                  job_name varchar(255) not null,
                  status varchar(32) not null,
                  scheduled_for_utc datetime(6) not null,
                  schedule_slot_utc datetime(6) null,
                  started_at_utc datetime(6) null,
                  completed_at_utc datetime(6) null,
                  attempt int not null,
                  max_attempts int not null,
                  lease_owner varchar(255) null,
                  lease_until_utc datetime(6) null,
                  payload_json longtext null,
                  error_message longtext null,
                  created_at_utc datetime(6) not null,
                  index {_q(f'ix_{tables.runs}_status_scheduled')} (status, scheduled_for_utc),
                  index {_q(f'ix_{tables.runs}_job_name_scheduled')} (job_name, scheduled_for_utc),
                  unique key {_q(f'ix_{tables.runs}_recurring_slot_unique')} (job_name, schedule_slot_utc)
                ) engine=InnoDB
                """
            )

            cursor.execute(
                f"""
                create table if not exists {_q(tables.runtime_command_receipts)} (
                  command_id varchar(255) not null primary key,
                  status varchar(32) not null,
                  error_code varchar(255) null,
                  error_message longtext null,
                  run_id char(32) null,
                  recorded_at_utc datetime(6) not null,
                  completed_at_utc datetime(6) null,
                  uploaded_at_utc datetime(6) null,
                  lease_owner varchar(255) null,
                  lease_until_utc datetime(6) null
                ) engine=InnoDB
                """
            )

            cursor.execute(
                f"""
                insert into {_q(tables.migrations)} (version, applied_at_utc)
                values (%s, utc_timestamp(6))
                on duplicate key update version = values(version)
                """,
                (SCHEMA_VERSION,),
            )
        connection.commit()
    finally:
        with connection.cursor() as cursor:
            cursor.execute("select release_lock(%s)", (lock_name,))
