"""PostgreSQL DurableJobStore implementation."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import asyncpg

from durablestack.core.models import EnqueueResult, JobRun, RecurringJobState, RecurringRegistration
from durablestack.core.utils import ensure_utc, generate_id

from .table_names import PostgresTableNames, resolve_postgres_table_names
from .types import PostgresDurableStackOptions


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _require_open_pool(value: asyncpg.Pool | None) -> asyncpg.Pool:
    if value is None:
        raise RuntimeError("Postgres store is not open")
    return value


def _require_tables(value: PostgresTableNames | None) -> PostgresTableNames:
    if value is None:
        raise RuntimeError("Postgres store is not open")
    return value


def _to_utc_or_none(value: Any) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError("expected datetime value")
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _is_row_count_one(result: Any) -> bool:
    text = cast(str, result)
    return text.endswith("1")


def _row_to_run(row: asyncpg.Record) -> JobRun:
    payload = row["payload_json"]
    payload_json = json.dumps(payload) if payload is not None else None
    return JobRun(
        run_id=str(row["id"]),
        job_name=str(row["job_name"]),
        status=str(row["status"]),
        created_by=None,
        payload_json=payload_json,
        attempt=int(row["attempt"]),
        max_attempts=int(row["max_attempts"]),
        created_at_utc=ensure_utc(row["created_at_utc"]),
        due_at_utc=ensure_utc(row["scheduled_for_utc"]),
        leased_by=row["lease_owner"],
        lease_expires_at_utc=_to_utc_or_none(row["lease_until_utc"]),
        started_at_utc=_to_utc_or_none(row["started_at_utc"]),
        completed_at_utc=_to_utc_or_none(row["completed_at_utc"]),
        last_error=row["error_message"],
        schedule_slot_utc=_to_utc_or_none(row["schedule_slot_utc"]),
    )


def _row_to_recurring(row: asyncpg.Record) -> RecurringJobState:
    return RecurringJobState(
        job_name=str(row["job_name"]),
        cron_expression=str(row["cron_expression"]),
        time_zone=str(row["time_zone"]),
        enabled=bool(row["enabled"]),
        allow_concurrent_runs=bool(row["allow_concurrent_runs"]),
        max_attempts=int(row["max_attempts"]),
        next_run_at_utc=ensure_utc(row["next_run_at_utc"]),
    )


@dataclass(slots=True)
class PostgresDurableJobStore:
    """PostgreSQL-backed DurableJobStore implementation."""

    options: PostgresDurableStackOptions
    _pool: asyncpg.Pool | None = None
    _tables: PostgresTableNames | None = None

    async def open(self) -> None:
        self._pool = await asyncpg.create_pool(dsn=self.options.connection_string)
        self._tables = resolve_postgres_table_names(self.options.table_prefix)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            self._tables = None

    @property
    def pool(self) -> asyncpg.Pool:
        return _require_open_pool(self._pool)

    @property
    def tables(self) -> PostgresTableNames:
        return _require_tables(self._tables)

    async def enqueue(self, job_name: str, payload_json: str | None, max_attempts: int) -> EnqueueResult:
        return await self.schedule(job_name, payload_json, datetime.now(tz=UTC), max_attempts)

    async def schedule(
        self,
        job_name: str,
        payload_json: str | None,
        run_at_utc: datetime,
        max_attempts: int,
    ) -> EnqueueResult:
        pool = self.pool
        tables = self.tables
        run_id = uuid.UUID(generate_id())
        row = await pool.fetchrow(
            f"""
            insert into {_q(tables.runs)}
            (id, job_name, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc,
             attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
            values ($1::uuid, $2, 'pending', $3::timestamptz, null, null, null,
                    0, $4, null, null, $5::jsonb, null, now())
            returning id::text;
            """,
            run_id,
            job_name,
            ensure_utc(run_at_utc),
            max(1, int(max_attempts)),
            payload_json,
        )
        if row is None:
            raise RuntimeError("insert failed")
        return EnqueueResult(run_id=str(row["id"]))

    async def claim_due_runs(
        self,
        worker_name: str,
        batch_size: int,
        lease_duration: timedelta,
        now_utc: datetime,
    ) -> list[JobRun]:
        pool = self.pool
        tables = self.tables
        rows = await pool.fetch(
            f"""
            with candidates as (
              select id
              from {_q(tables.runs)}
              where
                (status = 'pending' and scheduled_for_utc <= $3::timestamptz)
                or
                (status = 'leased' and (lease_until_utc is null or lease_until_utc <= $3::timestamptz))
              order by scheduled_for_utc asc
              for update skip locked
              limit $1
            ),
            updated as (
              update {_q(tables.runs)} r
              set
                attempt = r.attempt + 1,
                status = case when (r.attempt + 1) > r.max_attempts then 'failed' else 'leased' end,
                started_at_utc = coalesce(r.started_at_utc, $3::timestamptz),
                completed_at_utc = case when (r.attempt + 1) > r.max_attempts then $3::timestamptz else r.completed_at_utc end,
                error_message = case when (r.attempt + 1) > r.max_attempts then coalesce(r.error_message, 'Run exceeded max attempts before claim') else r.error_message end,
                lease_owner = case when (r.attempt + 1) > r.max_attempts then null else $2 end,
                lease_until_utc = case when (r.attempt + 1) > r.max_attempts then null else $3::timestamptz + $4::interval end
              from candidates c
              where r.id = c.id
              returning r.*
            )
            select * from updated where status = 'leased' order by scheduled_for_utc asc;
            """,
            max(1, int(batch_size)),
            worker_name,
            ensure_utc(now_utc),
            f"{max(1, int(lease_duration.total_seconds()))} seconds",
        )
        return [_row_to_run(row) for row in rows]

    async def extend_lease(
        self,
        run_id: str,
        worker_name: str,
        lease_duration: timedelta,
        now_utc: datetime,
    ) -> bool:
        pool = self.pool
        tables = self.tables
        result = await pool.execute(
            f"""
            update {_q(tables.runs)}
            set lease_until_utc = $3::timestamptz + $4::interval
            where id = $1::uuid
              and status = 'leased'
              and lease_owner = $2;
            """,
            uuid.UUID(run_id),
            worker_name,
            ensure_utc(now_utc),
            f"{max(1, int(lease_duration.total_seconds()))} seconds",
        )
        return _is_row_count_one(result)

    async def mark_succeeded(self, run_id: str, worker_name: str, completed_at_utc: datetime) -> bool:
        pool = self.pool
        tables = self.tables
        result = await pool.execute(
            f"""
            update {_q(tables.runs)}
            set status = 'succeeded', completed_at_utc = $3::timestamptz,
                lease_owner = null, lease_until_utc = null, error_message = null
            where id = $1::uuid and status = 'leased' and lease_owner = $2;
            """,
            uuid.UUID(run_id),
            worker_name,
            ensure_utc(completed_at_utc),
        )
        return _is_row_count_one(result)

    async def mark_failed(
        self,
        run_id: str,
        worker_name: str,
        error_message: str,
        completed_at_utc: datetime,
        should_retry: bool,
        retry_at_utc: datetime | None,
    ) -> bool:
        pool = self.pool
        tables = self.tables
        if should_retry and retry_at_utc is not None:
            result = await pool.execute(
                f"""
                update {_q(tables.runs)}
                set status = 'pending', scheduled_for_utc = $3::timestamptz,
                    lease_owner = null, lease_until_utc = null, error_message = $4
                where id = $1::uuid and status = 'leased' and lease_owner = $2;
                """,
                uuid.UUID(run_id),
                worker_name,
                ensure_utc(retry_at_utc),
                error_message,
            )
            return _is_row_count_one(result)

        result = await pool.execute(
            f"""
            update {_q(tables.runs)}
            set status = 'failed', completed_at_utc = $3::timestamptz,
                lease_owner = null, lease_until_utc = null, error_message = $4
            where id = $1::uuid and status = 'leased' and lease_owner = $2;
            """,
            uuid.UUID(run_id),
            worker_name,
            ensure_utc(completed_at_utc),
            error_message,
        )
        return _is_row_count_one(result)

    async def cancel_run(self, run_id: str, reason: str, completed_at_utc: datetime) -> bool:
        pool = self.pool
        tables = self.tables
        result = await pool.execute(
            f"""
            update {_q(tables.runs)}
            set status = 'failed', completed_at_utc = $2::timestamptz,
                error_message = $3, lease_owner = null, lease_until_utc = null
            where id = $1::uuid and status in ('pending', 'leased');
            """,
            uuid.UUID(run_id),
            ensure_utc(completed_at_utc),
            reason,
        )
        return _is_row_count_one(result)

    async def get_run(self, run_id: str) -> JobRun | None:
        pool = self.pool
        tables = self.tables
        row = await pool.fetchrow(f"select * from {_q(tables.runs)} where id = $1::uuid", uuid.UUID(run_id))
        return _row_to_run(row) if row else None

    async def get_recent_runs(self, take: int) -> list[JobRun]:
        pool = self.pool
        tables = self.tables
        rows = await pool.fetch(
            f"select * from {_q(tables.runs)} order by scheduled_for_utc desc limit $1",
            max(1, int(take)),
        )
        return [_row_to_run(row) for row in rows]

    async def get_runs_by_status(self, status: str, take: int) -> list[JobRun]:
        pool = self.pool
        tables = self.tables
        rows = await pool.fetch(
            f"select * from {_q(tables.runs)} where status = $1 order by scheduled_for_utc desc limit $2",
            status,
            max(1, int(take)),
        )
        return [_row_to_run(row) for row in rows]

    async def get_runs_by_job_name(self, job_name: str, take: int) -> list[JobRun]:
        pool = self.pool
        tables = self.tables
        rows = await pool.fetch(
            f"select * from {_q(tables.runs)} where job_name = $1 order by scheduled_for_utc desc limit $2",
            job_name,
            max(1, int(take)),
        )
        return [_row_to_run(row) for row in rows]

    async def get_enqueued_runs(self, take: int) -> list[JobRun]:
        pool = self.pool
        tables = self.tables
        rows = await pool.fetch(
            f"""
            select * from {_q(tables.runs)}
            where status = 'pending' and schedule_slot_utc is null
            order by scheduled_for_utc desc
            limit $1
            """,
            max(1, int(take)),
        )
        return [_row_to_run(row) for row in rows]

    async def get_due_recurring_jobs(self, now_utc: datetime, batch_size: int) -> list[RecurringJobState]:
        pool = self.pool
        tables = self.tables
        rows = await pool.fetch(
            f"""
            select * from {_q(tables.jobs)}
            where enabled = true and next_run_at_utc <= $1::timestamptz
            order by next_run_at_utc asc
            limit $2
            """,
            ensure_utc(now_utc),
            max(1, int(batch_size)),
        )
        return [_row_to_recurring(row) for row in rows]

    async def upsert_recurring_job(
        self,
        registration: RecurringRegistration,
        next_run_at_utc: datetime,
    ) -> None:
        pool = self.pool
        tables = self.tables
        await pool.execute(
            f"""
            insert into {_q(tables.jobs)}
            (job_name, cron_expression, time_zone, max_attempts, enabled, allow_concurrent_runs,
             retry_behavior, retry_initial_delay_seconds, next_run_at_utc, updated_at_utc)
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9::timestamptz, now())
            on conflict (job_name)
            do update set
              cron_expression = excluded.cron_expression,
              time_zone = excluded.time_zone,
              max_attempts = excluded.max_attempts,
              enabled = excluded.enabled,
              allow_concurrent_runs = excluded.allow_concurrent_runs,
              retry_behavior = excluded.retry_behavior,
              retry_initial_delay_seconds = excluded.retry_initial_delay_seconds,
              next_run_at_utc = excluded.next_run_at_utc,
              updated_at_utc = now();
            """,
            registration.name,
            registration.cron_expression,
            registration.time_zone,
            registration.max_attempts,
            registration.enabled,
            registration.allow_concurrent_runs,
            registration.retry_behavior,
            registration.retry_initial_delay_seconds,
            ensure_utc(next_run_at_utc),
        )

    async def try_materialize_recurring_run(
        self,
        recurring: RecurringJobState,
        registration: RecurringRegistration,
        next_run_at_utc: datetime,
    ) -> bool:
        pool = self.pool
        tables = self.tables
        async with pool.acquire() as conn, conn.transaction():
                if not recurring.allow_concurrent_runs:
                    active = await conn.fetchrow(
                        f"select 1 from {_q(tables.runs)} where job_name = $1 and status in ('pending', 'leased') limit 1",
                        recurring.job_name,
                    )
                    if active is not None:
                        return False

                locked = await conn.fetchrow(
                    f"""
                    select next_run_at_utc
                    from {_q(tables.jobs)}
                    where job_name = $1 and enabled = true
                    for update
                    """,
                    recurring.job_name,
                )
                if locked is None:
                    return False

                current_next = ensure_utc(locked["next_run_at_utc"])
                if abs((current_next - recurring.next_run_at_utc).total_seconds()) > 0.001:
                    return False

                await conn.execute(
                    f"""
                    update {_q(tables.jobs)}
                    set next_run_at_utc = $2::timestamptz,
                        updated_at_utc = now()
                    where job_name = $1
                    """,
                    recurring.job_name,
                    ensure_utc(next_run_at_utc),
                )

                run_id = uuid.UUID(generate_id())
                try:
                    await conn.execute(
                        f"""
                        insert into {_q(tables.runs)}
                        (id, job_name, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc,
                         attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
                        values ($1::uuid, $2, 'pending', $3::timestamptz, $4::timestamptz, null, null,
                                0, $5, null, null, null, null, now())
                        """,
                        run_id,
                        recurring.job_name,
                        ensure_utc(recurring.next_run_at_utc),
                        ensure_utc(recurring.next_run_at_utc),
                        registration.max_attempts,
                    )
                except asyncpg.UniqueViolationError:
                    return False

                return True

    async def get_recurring_jobs(self, include_disabled: bool) -> list[RecurringJobState]:
        pool = self.pool
        tables = self.tables
        if include_disabled:
            rows = await pool.fetch(f"select * from {_q(tables.jobs)} order by job_name asc")
        else:
            rows = await pool.fetch(
                f"select * from {_q(tables.jobs)} where enabled = true order by job_name asc"
            )
        return [_row_to_recurring(row) for row in rows]

    async def set_recurring_job_enabled(
        self,
        job_name: str,
        enabled: bool,
        next_run_at_utc: datetime | None,
    ) -> bool:
        pool = self.pool
        tables = self.tables
        result = await pool.execute(
            f"""
            update {_q(tables.jobs)}
            set enabled = $2,
                next_run_at_utc = coalesce($3::timestamptz, next_run_at_utc),
                updated_at_utc = now()
            where job_name = $1;
            """,
            job_name,
            enabled,
            ensure_utc(next_run_at_utc) if next_run_at_utc is not None else None,
        )
        return _is_row_count_one(result)

    async def update_recurring_job_schedule(
        self,
        job_name: str,
        cron_expression: str,
        time_zone: str,
        next_run_at_utc: datetime,
    ) -> bool:
        pool = self.pool
        tables = self.tables
        result = await pool.execute(
            f"""
            update {_q(tables.jobs)}
            set cron_expression = $2,
                time_zone = $3,
                next_run_at_utc = $4::timestamptz,
                updated_at_utc = now()
            where job_name = $1;
            """,
            job_name,
            cron_expression,
            time_zone,
            ensure_utc(next_run_at_utc),
        )
        return _is_row_count_one(result)

    async def try_enqueue_if_no_active_run(
        self,
        job_name: str,
        payload_json: str | None,
        due_at_utc: datetime,
        max_attempts: int,
    ) -> str | None:
        pool = self.pool
        tables = self.tables
        run_id = uuid.UUID(generate_id())
        row = await pool.fetchrow(
            f"""
            with can_enqueue as (
              select 1
              where not exists (
                select 1 from {_q(tables.runs)}
                where job_name = $1 and status in ('pending', 'leased')
              )
            )
            insert into {_q(tables.runs)}
            (id, job_name, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc,
             attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
            select $2::uuid, $1, 'pending', $3::timestamptz, null, null, null,
                   0, $4, null, null, $5::jsonb, null, now()
            from can_enqueue
            returning id::text;
            """,
            job_name,
            run_id,
            ensure_utc(due_at_utc),
            max(1, int(max_attempts)),
            payload_json,
        )
        return str(row["id"]) if row else None

    async def prune_historical_runs(self, completed_before_utc: datetime, batch_size: int) -> int:
        pool = self.pool
        tables = self.tables
        result = await pool.execute(
            f"""
            with doomed as (
              select id
              from {_q(tables.runs)}
              where status in ('succeeded', 'failed')
                and completed_at_utc is not null
                and completed_at_utc < $1::timestamptz
              order by completed_at_utc asc
              limit $2
            )
            delete from {_q(tables.runs)} r
            using doomed d
            where r.id = d.id;
            """,
            ensure_utc(completed_before_utc),
            max(1, int(batch_size)),
        )
        parts = result.split()
        if len(parts) == 2 and parts[0] == "DELETE":
            return int(parts[1])
        return 0
