"""SQLite DurableJobStore implementation."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from durablestack.core.constants import (
    RUN_STATUS_FAILED,
    RUN_STATUS_LEASED,
    RUN_STATUS_PENDING,
    RUN_STATUS_SUCCEEDED,
)
from durablestack.core.models import (
    EnqueueResult,
    JobRun,
    RecurringJobState,
    RecurringRegistration,
    RuntimeCommandReceipt,
    RuntimeCommandReceiptStatus,
)
from durablestack.core.utils import ensure_utc, generate_id

from .migrator import migrate_sqlite
from .table_names import SqliteTableNames
from .types import SqliteDurableStackOptions


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _is_active(status: str) -> bool:
    return status in {RUN_STATUS_PENDING, RUN_STATUS_LEASED}


def _dt_to_text(value: datetime) -> str:
    return ensure_utc(value).isoformat(timespec="microseconds")


def _text_to_dt(value: Any) -> datetime:
    text = cast(str, value)
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _row_to_run(row: sqlite3.Row) -> JobRun:
    return JobRun(
        run_id=cast(str, row["id"]),
        job_name=cast(str, row["job_name"]),
        status=cast(str, row["status"]),
        created_by=None,
        payload_json=cast(str | None, row["payload_json"]),
        attempt=cast(int, row["attempt"]),
        max_attempts=cast(int, row["max_attempts"]),
        created_at_utc=_text_to_dt(row["created_at_utc"]),
        due_at_utc=_text_to_dt(row["scheduled_for_utc"]),
        leased_by=cast(str | None, row["lease_owner"]),
        lease_expires_at_utc=_text_to_dt(row["lease_until_utc"]) if row["lease_until_utc"] else None,
        started_at_utc=_text_to_dt(row["started_at_utc"]) if row["started_at_utc"] else None,
        completed_at_utc=_text_to_dt(row["completed_at_utc"]) if row["completed_at_utc"] else None,
        last_error=cast(str | None, row["error_message"]),
        schedule_slot_utc=_text_to_dt(row["schedule_slot_utc"]) if row["schedule_slot_utc"] else None,
    )


def _row_to_recurring(row: sqlite3.Row) -> RecurringJobState:
    return RecurringJobState(
        job_name=cast(str, row["job_name"]),
        cron_expression=cast(str, row["cron_expression"]),
        time_zone=cast(str, row["time_zone"]),
        enabled=bool(row["enabled"]),
        allow_concurrent_runs=bool(row["allow_concurrent_runs"]),
        max_attempts=cast(int, row["max_attempts"]),
        next_run_at_utc=_text_to_dt(row["next_run_at_utc"]),
    )


def _row_to_receipt(row: sqlite3.Row) -> RuntimeCommandReceipt:
    status = cast(str, row["status"])
    if status not in {"leased", "acknowledged", "succeeded", "failed"}:
        raise ValueError(f"unsupported receipt status: {status}")
    return RuntimeCommandReceipt(
        command_id=cast(str, row["command_id"]),
        status=cast(RuntimeCommandReceiptStatus, status),
        recorded_at_utc=_text_to_dt(row["recorded_at_utc"]),
        completed_at_utc=_text_to_dt(row["completed_at_utc"]) if row["completed_at_utc"] else None,
        run_id=cast(str | None, row["run_id"]),
        error_code=cast(str | None, row["error_code"]),
        error_message=cast(str | None, row["error_message"]),
        uploaded_at_utc=_text_to_dt(row["uploaded_at_utc"]) if row["uploaded_at_utc"] else None,
        lease_owner=cast(str | None, row["lease_owner"]),
        lease_until_utc=_text_to_dt(row["lease_until_utc"]) if row["lease_until_utc"] else None,
    )


@dataclass(slots=True)
class SqliteDurableJobStore:
    """SQLite-backed DurableJobStore implementation."""

    options: SqliteDurableStackOptions
    _connection: sqlite3.Connection | None = None
    _tables: SqliteTableNames | None = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def open(self) -> None:
        connection = sqlite3.connect(
            self.options.database_path,
            timeout=self.options.busy_timeout_ms / 1000,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        tables = migrate_sqlite(connection, self.options.table_prefix)
        self._connection = connection
        self._tables = tables

    async def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            self._tables = None

    @property
    def connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("SQLite store is not open")
        return self._connection

    @property
    def tables(self) -> SqliteTableNames:
        if self._tables is None:
            raise RuntimeError("SQLite store is not open")
        return self._tables

    async def enqueue(self, job_name: str, payload_json: str | None, max_attempts: int) -> EnqueueResult:
        return await self.schedule(job_name, payload_json, datetime.now(tz=UTC), max_attempts)

    async def schedule(
        self,
        job_name: str,
        payload_json: str | None,
        run_at_utc: datetime,
        max_attempts: int,
    ) -> EnqueueResult:
        async with self._lock:
            run_id = generate_id()
            self.connection.execute(
                f"""
                insert into {_q(self.tables.runs)}
                (id, job_name, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc,
                 attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    job_name,
                    RUN_STATUS_PENDING,
                    _dt_to_text(run_at_utc),
                    None,
                    None,
                    None,
                    0,
                    max(1, int(max_attempts)),
                    None,
                    None,
                    payload_json,
                    None,
                    _dt_to_text(datetime.now(tz=UTC)),
                ),
            )
            return EnqueueResult(run_id=run_id)

    async def claim_due_runs(
        self,
        worker_name: str,
        batch_size: int,
        lease_duration: timedelta,
        now_utc: datetime,
    ) -> list[JobRun]:
        now_text = _dt_to_text(now_utc)
        lease_until_text = _dt_to_text(ensure_utc(now_utc) + lease_duration)
        claimed: list[JobRun] = []
        async with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                rows = self.connection.execute(
                    f"""
                    select id, attempt, max_attempts, started_at_utc
                    from {_q(self.tables.runs)}
                    where
                      (status = ? and scheduled_for_utc <= ?)
                      or
                      (status = ? and (lease_until_utc is null or lease_until_utc <= ?))
                    order by scheduled_for_utc asc
                    limit ?
                    """,
                    (
                        RUN_STATUS_PENDING,
                        now_text,
                        RUN_STATUS_LEASED,
                        now_text,
                        max(1, int(batch_size)),
                    ),
                ).fetchall()
                for row in rows:
                    run_id = cast(str, row["id"])
                    attempt = cast(int, row["attempt"]) + 1
                    max_attempts = cast(int, row["max_attempts"])
                    started_at = cast(str | None, row["started_at_utc"])
                    if attempt > max_attempts:
                        self.connection.execute(
                            f"""
                            update {_q(self.tables.runs)}
                            set status = ?,
                                attempt = ?,
                                completed_at_utc = ?,
                                lease_owner = null,
                                lease_until_utc = null,
                                error_message = coalesce(error_message, 'Run exceeded max attempts before claim')
                            where id = ?
                            """,
                            (RUN_STATUS_FAILED, attempt, now_text, run_id),
                        )
                        continue

                    self.connection.execute(
                        f"""
                        update {_q(self.tables.runs)}
                        set status = ?,
                            attempt = ?,
                            lease_owner = ?,
                            lease_until_utc = ?,
                            started_at_utc = coalesce(started_at_utc, ?),
                            completed_at_utc = null
                        where id = ?
                        """,
                        (
                            RUN_STATUS_LEASED,
                            attempt,
                            worker_name,
                            lease_until_text,
                            started_at or now_text,
                            run_id,
                        ),
                    )
                    mapped = self.connection.execute(
                        f"select * from {_q(self.tables.runs)} where id = ?",
                        (run_id,),
                    ).fetchone()
                    if mapped is not None:
                        claimed.append(_row_to_run(mapped))
                self.connection.commit()
            except Exception:
                self.connection.rollback()
                raise
        return claimed

    async def extend_lease(
        self,
        run_id: str,
        worker_name: str,
        lease_duration: timedelta,
        now_utc: datetime,
    ) -> bool:
        async with self._lock:
            cursor = self.connection.execute(
                f"""
                update {_q(self.tables.runs)}
                set lease_until_utc = ?
                where id = ?
                  and status = ?
                  and lease_owner = ?
                """,
                (_dt_to_text(ensure_utc(now_utc) + lease_duration), run_id, RUN_STATUS_LEASED, worker_name),
            )
            return cursor.rowcount == 1

    async def mark_succeeded(self, run_id: str, worker_name: str, completed_at_utc: datetime) -> bool:
        async with self._lock:
            cursor = self.connection.execute(
                f"""
                update {_q(self.tables.runs)}
                set status = ?,
                    completed_at_utc = ?,
                    lease_owner = null,
                    lease_until_utc = null,
                    error_message = null
                where id = ? and status = ? and lease_owner = ?
                """,
                (
                    RUN_STATUS_SUCCEEDED,
                    _dt_to_text(completed_at_utc),
                    run_id,
                    RUN_STATUS_LEASED,
                    worker_name,
                ),
            )
            return cursor.rowcount == 1

    async def mark_failed(
        self,
        run_id: str,
        worker_name: str,
        error_message: str,
        completed_at_utc: datetime,
        should_retry: bool,
        retry_at_utc: datetime | None,
    ) -> bool:
        async with self._lock:
            if should_retry and retry_at_utc is not None:
                cursor_retry = self.connection.execute(
                    f"""
                    update {_q(self.tables.runs)}
                    set status = ?,
                        scheduled_for_utc = ?,
                        lease_owner = null,
                        lease_until_utc = null,
                        completed_at_utc = null,
                        error_message = ?
                    where id = ? and status = ? and lease_owner = ?
                    """,
                    (
                        RUN_STATUS_PENDING,
                        _dt_to_text(retry_at_utc),
                        error_message,
                        run_id,
                        RUN_STATUS_LEASED,
                        worker_name,
                    ),
                )
                return cursor_retry.rowcount == 1

            cursor = self.connection.execute(
                f"""
                update {_q(self.tables.runs)}
                set status = ?,
                    completed_at_utc = ?,
                    lease_owner = null,
                    lease_until_utc = null,
                    error_message = ?
                where id = ? and status = ? and lease_owner = ?
                """,
                (
                    RUN_STATUS_FAILED,
                    _dt_to_text(completed_at_utc),
                    error_message,
                    run_id,
                    RUN_STATUS_LEASED,
                    worker_name,
                ),
            )
            return cursor.rowcount == 1

    async def cancel_run(self, run_id: str, reason: str, completed_at_utc: datetime) -> bool:
        async with self._lock:
            cursor = self.connection.execute(
                f"""
                update {_q(self.tables.runs)}
                set status = ?,
                    completed_at_utc = ?,
                    error_message = ?,
                    lease_owner = null,
                    lease_until_utc = null
                where id = ? and status in (?, ?)
                """,
                (
                    RUN_STATUS_FAILED,
                    _dt_to_text(completed_at_utc),
                    reason,
                    run_id,
                    RUN_STATUS_PENDING,
                    RUN_STATUS_LEASED,
                ),
            )
            return cursor.rowcount == 1

    async def get_run(self, run_id: str) -> JobRun | None:
        async with self._lock:
            row = self.connection.execute(
                f"select * from {_q(self.tables.runs)} where id = ?",
                (run_id,),
            ).fetchone()
            return _row_to_run(row) if row is not None else None

    async def get_recent_runs(self, take: int) -> list[JobRun]:
        async with self._lock:
            rows = self.connection.execute(
                f"select * from {_q(self.tables.runs)} order by scheduled_for_utc desc limit ?",
                (max(1, int(take)),),
            ).fetchall()
            return [_row_to_run(row) for row in rows]

    async def get_runs_by_status(self, status: str, take: int) -> list[JobRun]:
        async with self._lock:
            rows = self.connection.execute(
                f"select * from {_q(self.tables.runs)} where status = ? order by scheduled_for_utc desc limit ?",
                (status, max(1, int(take))),
            ).fetchall()
            return [_row_to_run(row) for row in rows]

    async def get_runs_by_job_name(self, job_name: str, take: int) -> list[JobRun]:
        async with self._lock:
            rows = self.connection.execute(
                f"select * from {_q(self.tables.runs)} where job_name = ? order by scheduled_for_utc desc limit ?",
                (job_name, max(1, int(take))),
            ).fetchall()
            return [_row_to_run(row) for row in rows]

    async def get_enqueued_runs(self, take: int) -> list[JobRun]:
        async with self._lock:
            rows = self.connection.execute(
                f"""
                select * from {_q(self.tables.runs)}
                where status = ? and schedule_slot_utc is null
                order by scheduled_for_utc desc
                limit ?
                """,
                (RUN_STATUS_PENDING, max(1, int(take))),
            ).fetchall()
            return [_row_to_run(row) for row in rows]

    async def get_due_recurring_jobs(self, now_utc: datetime, batch_size: int) -> list[RecurringJobState]:
        async with self._lock:
            rows = self.connection.execute(
                f"""
                select * from {_q(self.tables.jobs)}
                where enabled = 1 and next_run_at_utc <= ?
                order by next_run_at_utc asc
                limit ?
                """,
                (_dt_to_text(now_utc), max(1, int(batch_size))),
            ).fetchall()
            return [_row_to_recurring(row) for row in rows]

    async def upsert_recurring_job(
        self,
        registration: RecurringRegistration,
        next_run_at_utc: datetime,
    ) -> None:
        async with self._lock:
            self.connection.execute(
                f"""
                insert into {_q(self.tables.jobs)}
                (job_name, cron_expression, time_zone, max_attempts, enabled, allow_concurrent_runs,
                 retry_behavior, retry_initial_delay_seconds, next_run_at_utc, updated_at_utc)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(job_name)
                do update set
                  cron_expression = excluded.cron_expression,
                  time_zone = excluded.time_zone,
                  max_attempts = excluded.max_attempts,
                  enabled = excluded.enabled,
                  allow_concurrent_runs = excluded.allow_concurrent_runs,
                  retry_behavior = excluded.retry_behavior,
                  retry_initial_delay_seconds = excluded.retry_initial_delay_seconds,
                  next_run_at_utc = excluded.next_run_at_utc,
                  updated_at_utc = excluded.updated_at_utc
                """,
                (
                    registration.name,
                    registration.cron_expression,
                    registration.time_zone,
                    registration.max_attempts,
                    1 if registration.enabled else 0,
                    1 if registration.allow_concurrent_runs else 0,
                    registration.retry_behavior,
                    registration.retry_initial_delay_seconds,
                    _dt_to_text(next_run_at_utc),
                    _dt_to_text(datetime.now(tz=UTC)),
                ),
            )

    async def try_materialize_recurring_run(
        self,
        recurring: RecurringJobState,
        registration: RecurringRegistration,
        next_run_at_utc: datetime,
    ) -> bool:
        async with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                if not recurring.allow_concurrent_runs:
                    active = self.connection.execute(
                        f"""
                        select 1
                        from {_q(self.tables.runs)}
                        where job_name = ? and status in (?, ?)
                        limit 1
                        """,
                        (recurring.job_name, RUN_STATUS_PENDING, RUN_STATUS_LEASED),
                    ).fetchone()
                    if active is not None:
                        self.connection.commit()
                        return False

                locked = self.connection.execute(
                    f"""
                    select next_run_at_utc, enabled
                    from {_q(self.tables.jobs)}
                    where job_name = ?
                    """,
                    (recurring.job_name,),
                ).fetchone()
                if locked is None or int(locked["enabled"]) != 1:
                    self.connection.commit()
                    return False

                current_next = cast(str, locked["next_run_at_utc"])
                if current_next != _dt_to_text(recurring.next_run_at_utc):
                    self.connection.commit()
                    return False

                self.connection.execute(
                    f"""
                    insert into {_q(self.tables.runs)}
                    (id, job_name, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc,
                     attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        generate_id(),
                        recurring.job_name,
                        RUN_STATUS_PENDING,
                        _dt_to_text(recurring.next_run_at_utc),
                        _dt_to_text(recurring.next_run_at_utc),
                        None,
                        None,
                        0,
                        registration.max_attempts,
                        None,
                        None,
                        None,
                        None,
                        _dt_to_text(datetime.now(tz=UTC)),
                    ),
                )

                self.connection.execute(
                    f"""
                    update {_q(self.tables.jobs)}
                    set next_run_at_utc = ?, updated_at_utc = ?
                    where job_name = ?
                    """,
                    (
                        _dt_to_text(next_run_at_utc),
                        _dt_to_text(datetime.now(tz=UTC)),
                        recurring.job_name,
                    ),
                )
                self.connection.commit()
                return True
            except sqlite3.IntegrityError:
                self.connection.rollback()
                return False
            except Exception:
                self.connection.rollback()
                raise

    async def get_recurring_jobs(self, include_disabled: bool) -> list[RecurringJobState]:
        async with self._lock:
            if include_disabled:
                rows = self.connection.execute(
                    f"select * from {_q(self.tables.jobs)} order by job_name asc"
                ).fetchall()
            else:
                rows = self.connection.execute(
                    f"select * from {_q(self.tables.jobs)} where enabled = 1 order by job_name asc"
                ).fetchall()
            return [_row_to_recurring(row) for row in rows]

    async def set_recurring_job_enabled(
        self,
        job_name: str,
        enabled: bool,
        next_run_at_utc: datetime | None,
    ) -> bool:
        async with self._lock:
            cursor = self.connection.execute(
                f"""
                update {_q(self.tables.jobs)}
                set enabled = ?,
                    next_run_at_utc = coalesce(?, next_run_at_utc),
                    updated_at_utc = ?
                where job_name = ?
                """,
                (
                    1 if enabled else 0,
                    _dt_to_text(next_run_at_utc) if next_run_at_utc is not None else None,
                    _dt_to_text(datetime.now(tz=UTC)),
                    job_name,
                ),
            )
            return cursor.rowcount == 1

    async def update_recurring_job_schedule(
        self,
        job_name: str,
        cron_expression: str,
        time_zone: str,
        next_run_at_utc: datetime,
    ) -> bool:
        async with self._lock:
            cursor = self.connection.execute(
                f"""
                update {_q(self.tables.jobs)}
                set cron_expression = ?,
                    time_zone = ?,
                    next_run_at_utc = ?,
                    updated_at_utc = ?
                where job_name = ?
                """,
                (
                    cron_expression,
                    time_zone,
                    _dt_to_text(next_run_at_utc),
                    _dt_to_text(datetime.now(tz=UTC)),
                    job_name,
                ),
            )
            return cursor.rowcount == 1

    async def try_enqueue_if_no_active_run(
        self,
        job_name: str,
        payload_json: str | None,
        due_at_utc: datetime,
        max_attempts: int,
    ) -> str | None:
        async with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                active = self.connection.execute(
                    f"""
                    select status
                    from {_q(self.tables.runs)}
                    where job_name = ? and status in (?, ?)
                    limit 1
                    """,
                    (job_name, RUN_STATUS_PENDING, RUN_STATUS_LEASED),
                ).fetchone()
                if active is not None:
                    self.connection.commit()
                    return None

                run_id = generate_id()
                self.connection.execute(
                    f"""
                    insert into {_q(self.tables.runs)}
                    (id, job_name, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc,
                     attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        job_name,
                        RUN_STATUS_PENDING,
                        _dt_to_text(due_at_utc),
                        None,
                        None,
                        None,
                        0,
                        max(1, int(max_attempts)),
                        None,
                        None,
                        payload_json,
                        None,
                        _dt_to_text(datetime.now(tz=UTC)),
                    ),
                )
                self.connection.commit()
                return run_id
            except Exception:
                self.connection.rollback()
                raise

    async def prune_historical_runs(self, completed_before_utc: datetime, batch_size: int) -> int:
        cutoff = _dt_to_text(completed_before_utc)
        async with self._lock:
            rows = self.connection.execute(
                f"""
                select id
                from {_q(self.tables.runs)}
                where status in (?, ?)
                  and completed_at_utc is not null
                  and completed_at_utc < ?
                order by completed_at_utc asc
                limit ?
                """,
                (
                    RUN_STATUS_SUCCEEDED,
                    RUN_STATUS_FAILED,
                    cutoff,
                    max(1, int(batch_size)),
                ),
            ).fetchall()
            if not rows:
                return 0
            ids = [cast(str, row["id"]) for row in rows]
            placeholders = ",".join(["?"] * len(ids))
            cursor = self.connection.execute(
                f"delete from {_q(self.tables.runs)} where id in ({placeholders})",
                ids,
            )
            return max(0, int(cursor.rowcount))

    async def try_lease_runtime_command_receipt(
        self,
        command_id: str,
        worker_name: str,
        lease_duration: timedelta,
        recorded_at_utc: datetime,
    ) -> bool:
        now_text = _dt_to_text(recorded_at_utc)
        lease_until = _dt_to_text(ensure_utc(recorded_at_utc) + lease_duration)
        async with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                row = self.connection.execute(
                    f"select * from {_q(self.tables.runtime_command_receipts)} where command_id = ?",
                    (command_id,),
                ).fetchone()
                if row is None:
                    self.connection.execute(
                        f"""
                        insert into {_q(self.tables.runtime_command_receipts)}
                        (command_id, status, error_code, error_message, run_id, recorded_at_utc, completed_at_utc,
                         uploaded_at_utc, lease_owner, lease_until_utc)
                        values (?, 'leased', null, null, null, ?, null, null, ?, ?)
                        """,
                        (command_id, now_text, worker_name, lease_until),
                    )
                    self.connection.commit()
                    return True

                status = cast(str, row["status"])
                if status in {"succeeded", "failed"}:
                    self.connection.commit()
                    return False

                lease_owner = cast(str | None, row["lease_owner"])
                lease_until_utc = cast(str | None, row["lease_until_utc"])
                lease_expired = lease_until_utc is None or lease_until_utc <= now_text
                if not lease_expired and lease_owner != worker_name:
                    self.connection.commit()
                    return False

                self.connection.execute(
                    f"""
                    update {_q(self.tables.runtime_command_receipts)}
                    set status = 'leased',
                        recorded_at_utc = ?,
                        lease_owner = ?,
                        lease_until_utc = ?
                    where command_id = ?
                    """,
                    (now_text, worker_name, lease_until, command_id),
                )
                self.connection.commit()
                return True
            except Exception:
                self.connection.rollback()
                raise

    async def mark_runtime_command_acknowledged(
        self,
        command_id: str,
        worker_name: str,
        recorded_at_utc: datetime,
    ) -> bool:
        async with self._lock:
            cursor = self.connection.execute(
                f"""
                update {_q(self.tables.runtime_command_receipts)}
                set status = 'acknowledged', recorded_at_utc = ?
                where command_id = ? and lease_owner = ?
                """,
                (_dt_to_text(recorded_at_utc), command_id, worker_name),
            )
            return cursor.rowcount == 1

    async def mark_runtime_command_succeeded(
        self,
        command_id: str,
        worker_name: str,
        recorded_at_utc: datetime,
        completed_at_utc: datetime,
        run_id: str | None,
    ) -> bool:
        async with self._lock:
            cursor = self.connection.execute(
                f"""
                update {_q(self.tables.runtime_command_receipts)}
                set status = 'succeeded',
                    recorded_at_utc = ?,
                    completed_at_utc = ?,
                    run_id = ?,
                    lease_owner = null,
                    lease_until_utc = null
                where command_id = ? and lease_owner = ?
                """,
                (
                    _dt_to_text(recorded_at_utc),
                    _dt_to_text(completed_at_utc),
                    run_id,
                    command_id,
                    worker_name,
                ),
            )
            return cursor.rowcount == 1

    async def mark_runtime_command_failed(
        self,
        command_id: str,
        worker_name: str,
        recorded_at_utc: datetime,
        completed_at_utc: datetime,
        error_code: str | None,
        error_message: str | None,
    ) -> bool:
        async with self._lock:
            cursor = self.connection.execute(
                f"""
                update {_q(self.tables.runtime_command_receipts)}
                set status = 'failed',
                    recorded_at_utc = ?,
                    completed_at_utc = ?,
                    error_code = ?,
                    error_message = ?,
                    lease_owner = null,
                    lease_until_utc = null
                where command_id = ? and lease_owner = ?
                """,
                (
                    _dt_to_text(recorded_at_utc),
                    _dt_to_text(completed_at_utc),
                    error_code,
                    error_message,
                    command_id,
                    worker_name,
                ),
            )
            return cursor.rowcount == 1

    async def get_runtime_command_receipts(self, take: int) -> list[RuntimeCommandReceipt]:
        async with self._lock:
            rows = self.connection.execute(
                f"""
                select *
                from {_q(self.tables.runtime_command_receipts)}
                where uploaded_at_utc is null
                  and status in ('acknowledged', 'succeeded', 'failed')
                order by recorded_at_utc asc
                limit ?
                """,
                (max(1, int(take)),),
            ).fetchall()
            return [_row_to_receipt(row) for row in rows]

    async def mark_runtime_command_receipt_uploaded(
        self,
        command_id: str,
        uploaded_at_utc: datetime,
    ) -> bool:
        async with self._lock:
            cursor = self.connection.execute(
                f"""
                update {_q(self.tables.runtime_command_receipts)}
                set uploaded_at_utc = ?
                where command_id = ?
                """,
                (_dt_to_text(uploaded_at_utc), command_id),
            )
            return cursor.rowcount == 1
