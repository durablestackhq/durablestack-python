"""PostgreSQL migrator for DurableStack runtime tables."""

from __future__ import annotations

import hashlib

import asyncpg

from .table_names import PostgresTableNames, resolve_postgres_table_names

SCHEMA_VERSION = 1


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _migration_lock_name(table_prefix: str | None) -> str:
    base = (table_prefix or "default").strip() or "default"
    return f"durablestack_py_migrate_{base}"


def _lock_key(name: str) -> int:
    digest = hashlib.sha256(name.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


async def migrate_postgres(pool: asyncpg.Pool, table_prefix: str | None) -> None:
    tables = resolve_postgres_table_names(table_prefix)
    await _apply_postgres_migrations(pool, table_prefix, tables)
    await verify_postgres_schema(pool, tables)


async def verify_postgres_schema(pool: asyncpg.Pool, tables: PostgresTableNames) -> None:
    probes = [
        f"select job_name, cron_expression, time_zone, next_run_at_utc from {_q(tables.jobs)} limit 1",
        f"select id, job_name, status, scheduled_for_utc, attempt, max_attempts from {_q(tables.runs)} limit 1",
        f"select version, applied_at_utc from {_q(tables.migrations)} limit 1",
        f"select command_id, status, recorded_at_utc from {_q(tables.runtime_command_receipts)} limit 1",
    ]
    async with pool.acquire() as conn:
        for sql in probes:
            await conn.execute(sql)


async def _apply_postgres_migrations(
    pool: asyncpg.Pool,
    table_prefix: str | None,
    tables: PostgresTableNames,
) -> None:
    lock_name = _migration_lock_name(table_prefix)
    key = _lock_key(lock_name)

    async with pool.acquire() as conn, conn.transaction():
        await conn.execute("select pg_advisory_xact_lock($1::bigint)", key)

        await conn.execute(
            f"""
            create table if not exists {_q(tables.migrations)} (
              version integer primary key,
              applied_at_utc timestamptz not null
            );
            """
        )

        existing = await conn.fetchrow(
            f"select version from {_q(tables.migrations)} where version = $1",
            SCHEMA_VERSION,
        )
        if existing is not None:
            return

        await conn.execute(
            f"""
            create table if not exists {_q(tables.jobs)} (
              job_name text primary key,
              cron_expression text not null,
              time_zone text not null,
              max_attempts integer not null,
              enabled boolean not null,
              allow_concurrent_runs boolean not null,
              retry_behavior text null,
              retry_initial_delay_seconds integer null,
              next_run_at_utc timestamptz not null,
              updated_at_utc timestamptz not null
            );
            """
        )

        await conn.execute(
            f"""
            create table if not exists {_q(tables.runs)} (
              id uuid primary key,
              job_name text not null,
              status text not null,
              scheduled_for_utc timestamptz not null,
              schedule_slot_utc timestamptz null,
              started_at_utc timestamptz null,
              completed_at_utc timestamptz null,
              attempt integer not null,
              max_attempts integer not null,
              lease_owner text null,
              lease_until_utc timestamptz null,
              payload_json jsonb null,
              error_message text null,
              created_at_utc timestamptz not null
            );
            """
        )

        await conn.execute(
            f"""
            create index if not exists {_q(f'ix_{tables.runs}_status_scheduled')}
            on {_q(tables.runs)} (status, scheduled_for_utc asc);
            """
        )
        await conn.execute(
            f"""
            create index if not exists {_q(f'ix_{tables.runs}_job_name_scheduled')}
            on {_q(tables.runs)} (job_name, scheduled_for_utc desc);
            """
        )
        await conn.execute(
            f"""
            create unique index if not exists {_q(f'ix_{tables.runs}_recurring_slot_unique')}
            on {_q(tables.runs)} (job_name, schedule_slot_utc)
            where schedule_slot_utc is not null;
            """
        )

        await conn.execute(
            f"""
            create table if not exists {_q(tables.runtime_command_receipts)} (
              command_id text primary key,
              status text not null,
              error_code text null,
              error_message text null,
              run_id uuid null,
              recorded_at_utc timestamptz not null,
              completed_at_utc timestamptz null,
              uploaded_at_utc timestamptz null,
              lease_owner text null,
              lease_until_utc timestamptz null
            );
            """
        )

        await conn.execute(
            f"""
            insert into {_q(tables.migrations)} (version, applied_at_utc)
            values ($1, now())
            on conflict (version) do nothing;
            """,
            SCHEMA_VERSION,
        )
