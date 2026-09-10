"""SQLite migrator for DurableStack runtime tables."""

from __future__ import annotations

import sqlite3

from .table_names import SqliteTableNames, resolve_sqlite_table_names

SCHEMA_VERSION = 1


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def migrate_sqlite(connection: sqlite3.Connection, table_prefix: str | None) -> SqliteTableNames:
    tables = resolve_sqlite_table_names(table_prefix)
    _apply_sqlite_migrations(connection, tables)
    verify_sqlite_schema(connection, tables)
    return tables


def verify_sqlite_schema(connection: sqlite3.Connection, tables: SqliteTableNames) -> None:
    probes = [
        f"select job_name, cron_expression, time_zone, next_run_at_utc from {_q(tables.jobs)} limit 1",
        f"select id, job_name, status, scheduled_for_utc, attempt, max_attempts from {_q(tables.runs)} limit 1",
        f"select version, applied_at_utc from {_q(tables.migrations)} limit 1",
        f"select command_id, status, recorded_at_utc from {_q(tables.runtime_command_receipts)} limit 1",
    ]
    for sql in probes:
        connection.execute(sql)


def _apply_sqlite_migrations(connection: sqlite3.Connection, tables: SqliteTableNames) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            f"""
            create table if not exists {_q(tables.migrations)} (
              version integer primary key,
              applied_at_utc text not null
            )
            """
        )

        exists = connection.execute(
            f"select version from {_q(tables.migrations)} where version = ?",
            (SCHEMA_VERSION,),
        ).fetchone()
        if exists is not None:
            connection.commit()
            return

        connection.execute(
            f"""
            create table if not exists {_q(tables.jobs)} (
              job_name text primary key,
              cron_expression text not null,
              time_zone text not null,
              max_attempts integer not null,
              enabled integer not null,
              allow_concurrent_runs integer not null,
              retry_behavior text null,
              retry_initial_delay_seconds integer null,
              next_run_at_utc text not null,
              updated_at_utc text not null
            )
            """
        )

        connection.execute(
            f"""
            create table if not exists {_q(tables.runs)} (
              id text primary key,
              job_name text not null,
              status text not null,
              scheduled_for_utc text not null,
              schedule_slot_utc text null,
              started_at_utc text null,
              completed_at_utc text null,
              attempt integer not null,
              max_attempts integer not null,
              lease_owner text null,
              lease_until_utc text null,
              payload_json text null,
              error_message text null,
              created_at_utc text not null
            )
            """
        )
        connection.execute(
            f"""
            create index if not exists {_q(f'ix_{tables.runs}_status_scheduled')}
            on {_q(tables.runs)} (status, scheduled_for_utc asc)
            """
        )
        connection.execute(
            f"""
            create index if not exists {_q(f'ix_{tables.runs}_job_name_scheduled')}
            on {_q(tables.runs)} (job_name, scheduled_for_utc desc)
            """
        )
        connection.execute(
            f"""
            create unique index if not exists {_q(f'ix_{tables.runs}_recurring_slot_unique')}
            on {_q(tables.runs)} (job_name, schedule_slot_utc)
            where schedule_slot_utc is not null
            """
        )

        connection.execute(
            f"""
            create table if not exists {_q(tables.runtime_command_receipts)} (
              command_id text primary key,
              status text not null,
              error_code text null,
              error_message text null,
              run_id text null,
              recorded_at_utc text not null,
              completed_at_utc text null,
              uploaded_at_utc text null,
              lease_owner text null,
              lease_until_utc text null
            )
            """
        )

        connection.execute(
            f"""
            insert into {_q(tables.migrations)} (version, applied_at_utc)
            values (?, strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now'))
            on conflict(version) do nothing
            """,
            (SCHEMA_VERSION,),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
