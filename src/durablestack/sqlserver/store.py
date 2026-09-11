"""SQL Server DurableJobStore implementation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from importlib import import_module
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

from .migrator import migrate_sqlserver
from .table_names import SqlServerTableNames
from .types import SqlServerDurableStackOptions


def _q(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def _rowcount_is_one(value: Any) -> bool:
    return int(cast(int, value)) == 1


def _require_pyodbc() -> Any:
    try:
        return import_module("pyodbc")
    except ImportError as exc:
        raise RuntimeError("pyodbc is required for SQL Server provider") from exc


def _validate_connection_string(value: str) -> None:
    text = value.strip()
    if text == "":
        raise ValueError("connection_string is required")
    if "=" not in text:
        raise ValueError("SQL Server connection_string must be an ODBC key/value string")


def _to_db_datetime(value: datetime) -> datetime:
    utc = ensure_utc(value)
    return utc.replace(tzinfo=None)


def _from_db_datetime(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("expected datetime value")
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _row_to_run(row: Any) -> JobRun:
    return JobRun(
        run_id=cast(str, row.id),
        job_name=cast(str, row.job_name),
        status=cast(str, row.status),
        created_by=None,
        payload_json=cast(str | None, row.payload_json),
        attempt=cast(int, row.attempt),
        max_attempts=cast(int, row.max_attempts),
        created_at_utc=_from_db_datetime(row.created_at_utc),
        due_at_utc=_from_db_datetime(row.scheduled_for_utc),
        leased_by=cast(str | None, row.lease_owner),
        lease_expires_at_utc=_from_db_datetime(row.lease_until_utc) if row.lease_until_utc else None,
        started_at_utc=_from_db_datetime(row.started_at_utc) if row.started_at_utc else None,
        completed_at_utc=_from_db_datetime(row.completed_at_utc) if row.completed_at_utc else None,
        last_error=cast(str | None, row.error_message),
        schedule_slot_utc=_from_db_datetime(row.schedule_slot_utc) if row.schedule_slot_utc else None,
    )


def _row_to_recurring(row: Any) -> RecurringJobState:
    return RecurringJobState(
        job_name=cast(str, row.job_name),
        cron_expression=cast(str, row.cron_expression),
        time_zone=cast(str, row.time_zone),
        enabled=bool(row.enabled),
        allow_concurrent_runs=bool(row.allow_concurrent_runs),
        max_attempts=cast(int, row.max_attempts),
        next_run_at_utc=_from_db_datetime(row.next_run_at_utc),
    )


def _row_to_receipt(row: Any) -> RuntimeCommandReceipt:
    status = cast(str, row.status)
    if status not in {"leased", "acknowledged", "succeeded", "failed"}:
        raise ValueError(f"unsupported receipt status: {status}")
    return RuntimeCommandReceipt(
        command_id=cast(str, row.command_id),
        status=cast(RuntimeCommandReceiptStatus, status),
        recorded_at_utc=_from_db_datetime(row.recorded_at_utc),
        completed_at_utc=_from_db_datetime(row.completed_at_utc) if row.completed_at_utc else None,
        run_id=cast(str | None, row.run_id),
        error_code=cast(str | None, row.error_code),
        error_message=cast(str | None, row.error_message),
        uploaded_at_utc=_from_db_datetime(row.uploaded_at_utc) if row.uploaded_at_utc else None,
        lease_owner=cast(str | None, row.lease_owner),
        lease_until_utc=_from_db_datetime(row.lease_until_utc) if row.lease_until_utc else None,
    )


@dataclass(slots=True)
class SqlServerDurableJobStore:
    """SQL Server-backed DurableJobStore implementation."""

    options: SqlServerDurableStackOptions
    _connection: Any | None = None
    _tables: SqlServerTableNames | None = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def open(self) -> None:
        _validate_connection_string(self.options.connection_string)
        pyodbc = _require_pyodbc()
        self._connection = pyodbc.connect(self.options.connection_string, autocommit=False)
        self._tables = migrate_sqlserver(self.connection, self.options.table_prefix)

    async def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            self._tables = None

    @property
    def connection(self) -> Any:
        if self._connection is None:
            raise RuntimeError("SQL Server store is not open")
        return self._connection

    @property
    def tables(self) -> SqlServerTableNames:
        if self._tables is None:
            raise RuntimeError("SQL Server store is not open")
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
        run_id = generate_id()
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
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
                        _to_db_datetime(run_at_utc),
                        None,
                        None,
                        None,
                        0,
                        max(1, int(max_attempts)),
                        None,
                        None,
                        payload_json,
                        None,
                        _to_db_datetime(datetime.now(tz=UTC)),
                    ),
                )
                self.connection.commit()
            finally:
                cursor.close()
        return EnqueueResult(run_id=run_id)

    async def claim_due_runs(
        self,
        worker_name: str,
        batch_size: int,
        lease_duration: timedelta,
        now_utc: datetime,
    ) -> list[JobRun]:
        now = _to_db_datetime(now_utc)
        lease_until = _to_db_datetime(ensure_utc(now_utc) + lease_duration)
        count = max(1, int(batch_size))
        claimed: list[JobRun] = []

        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"""
                    select top ({count}) id, attempt, max_attempts
                    from {_q(self.tables.runs)} with (updlock, readpast, rowlock)
                    where
                      (status = ? and scheduled_for_utc <= ?)
                      or
                      (status = ? and (lease_until_utc is null or lease_until_utc <= ?))
                    order by scheduled_for_utc asc
                    """,
                    (RUN_STATUS_PENDING, now, RUN_STATUS_LEASED, now),
                )
                rows = cursor.fetchall()
                for row in rows:
                    run_id = cast(str, row.id)
                    attempt = cast(int, row.attempt) + 1
                    max_attempts = cast(int, row.max_attempts)
                    if attempt > max_attempts:
                        cursor.execute(
                            f"""
                            update {_q(self.tables.runs)}
                            set attempt = ?,
                                status = ?,
                                completed_at_utc = ?,
                                error_message = coalesce(error_message, ?),
                                lease_owner = null,
                                lease_until_utc = null
                            where id = ?
                            """,
                            (attempt, RUN_STATUS_FAILED, now, "Run exceeded max attempts before claim", run_id),
                        )
                        continue

                    cursor.execute(
                        f"""
                        update {_q(self.tables.runs)}
                        set attempt = ?,
                            status = ?,
                            started_at_utc = coalesce(started_at_utc, ?),
                            completed_at_utc = null,
                            lease_owner = ?,
                            lease_until_utc = ?
                        where id = ?
                        """,
                        (attempt, RUN_STATUS_LEASED, now, worker_name, lease_until, run_id),
                    )
                    cursor.execute(f"select * from {_q(self.tables.runs)} where id = ?", (run_id,))
                    mapped = cursor.fetchone()
                    if mapped is not None:
                        claimed.append(_row_to_run(mapped))
                self.connection.commit()
            except Exception:
                self.connection.rollback()
                raise
            finally:
                cursor.close()
        return claimed

    async def extend_lease(
        self,
        run_id: str,
        worker_name: str,
        lease_duration: timedelta,
        now_utc: datetime,
    ) -> bool:
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"""
                    update {_q(self.tables.runs)}
                    set lease_until_utc = ?
                    where id = ? and status = ? and lease_owner = ?
                    """,
                    (
                        _to_db_datetime(ensure_utc(now_utc) + lease_duration),
                        run_id,
                        RUN_STATUS_LEASED,
                        worker_name,
                    ),
                )
                self.connection.commit()
                return _rowcount_is_one(cursor.rowcount)
            finally:
                cursor.close()

    async def mark_succeeded(self, run_id: str, worker_name: str, completed_at_utc: datetime) -> bool:
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
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
                        _to_db_datetime(completed_at_utc),
                        run_id,
                        RUN_STATUS_LEASED,
                        worker_name,
                    ),
                )
                self.connection.commit()
                return _rowcount_is_one(cursor.rowcount)
            finally:
                cursor.close()

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
            cursor = self.connection.cursor()
            try:
                if should_retry and retry_at_utc is not None:
                    cursor.execute(
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
                            _to_db_datetime(retry_at_utc),
                            error_message,
                            run_id,
                            RUN_STATUS_LEASED,
                            worker_name,
                        ),
                    )
                    self.connection.commit()
                    return _rowcount_is_one(cursor.rowcount)

                cursor.execute(
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
                        _to_db_datetime(completed_at_utc),
                        error_message,
                        run_id,
                        RUN_STATUS_LEASED,
                        worker_name,
                    ),
                )
                self.connection.commit()
                return _rowcount_is_one(cursor.rowcount)
            finally:
                cursor.close()

    async def cancel_run(self, run_id: str, reason: str, completed_at_utc: datetime) -> bool:
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
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
                        _to_db_datetime(completed_at_utc),
                        reason,
                        run_id,
                        RUN_STATUS_PENDING,
                        RUN_STATUS_LEASED,
                    ),
                )
                self.connection.commit()
                return _rowcount_is_one(cursor.rowcount)
            finally:
                cursor.close()

    async def get_run(self, run_id: str) -> JobRun | None:
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(f"select * from {_q(self.tables.runs)} where id = ?", (run_id,))
                row = cursor.fetchone()
                return _row_to_run(row) if row is not None else None
            finally:
                cursor.close()

    async def get_recent_runs(self, take: int) -> list[JobRun]:
        count = max(1, int(take))
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(f"select top ({count}) * from {_q(self.tables.runs)} order by scheduled_for_utc desc")
                return [_row_to_run(row) for row in cursor.fetchall()]
            finally:
                cursor.close()

    async def get_runs_by_status(self, status: str, take: int) -> list[JobRun]:
        count = max(1, int(take))
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"select top ({count}) * from {_q(self.tables.runs)} where status = ? order by scheduled_for_utc desc",
                    (status,),
                )
                return [_row_to_run(row) for row in cursor.fetchall()]
            finally:
                cursor.close()

    async def get_runs_by_job_name(self, job_name: str, take: int) -> list[JobRun]:
        count = max(1, int(take))
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"select top ({count}) * from {_q(self.tables.runs)} where job_name = ? order by scheduled_for_utc desc",
                    (job_name,),
                )
                return [_row_to_run(row) for row in cursor.fetchall()]
            finally:
                cursor.close()

    async def get_enqueued_runs(self, take: int) -> list[JobRun]:
        count = max(1, int(take))
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"""
                    select top ({count}) *
                    from {_q(self.tables.runs)}
                    where status = ? and schedule_slot_utc is null
                    order by scheduled_for_utc desc
                    """,
                    (RUN_STATUS_PENDING,),
                )
                return [_row_to_run(row) for row in cursor.fetchall()]
            finally:
                cursor.close()

    async def get_due_recurring_jobs(self, now_utc: datetime, batch_size: int) -> list[RecurringJobState]:
        count = max(1, int(batch_size))
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"""
                    select top ({count}) *
                    from {_q(self.tables.jobs)}
                    where enabled = 1 and next_run_at_utc <= ?
                    order by next_run_at_utc asc
                    """,
                    (_to_db_datetime(now_utc),),
                )
                return [_row_to_recurring(row) for row in cursor.fetchall()]
            finally:
                cursor.close()

    async def upsert_recurring_job(
        self,
        registration: RecurringRegistration,
        next_run_at_utc: datetime,
    ) -> None:
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"""
                    merge {_q(self.tables.jobs)} as target
                    using (
                      select ? as job_name, ? as cron_expression, ? as time_zone, ? as max_attempts,
                             ? as enabled, ? as allow_concurrent_runs, ? as retry_behavior,
                             ? as retry_initial_delay_seconds, ? as next_run_at_utc, ? as updated_at_utc
                    ) as source
                    on target.job_name = source.job_name
                    when matched then update set
                      cron_expression = source.cron_expression,
                      time_zone = source.time_zone,
                      max_attempts = source.max_attempts,
                      enabled = source.enabled,
                      allow_concurrent_runs = source.allow_concurrent_runs,
                      retry_behavior = source.retry_behavior,
                      retry_initial_delay_seconds = source.retry_initial_delay_seconds,
                      next_run_at_utc = source.next_run_at_utc,
                      updated_at_utc = source.updated_at_utc
                    when not matched then insert
                    (job_name, cron_expression, time_zone, max_attempts, enabled, allow_concurrent_runs,
                     retry_behavior, retry_initial_delay_seconds, next_run_at_utc, updated_at_utc)
                    values
                    (source.job_name, source.cron_expression, source.time_zone, source.max_attempts,
                     source.enabled, source.allow_concurrent_runs, source.retry_behavior,
                     source.retry_initial_delay_seconds, source.next_run_at_utc, source.updated_at_utc);
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
                        _to_db_datetime(next_run_at_utc),
                        _to_db_datetime(datetime.now(tz=UTC)),
                    ),
                )
                self.connection.commit()
            finally:
                cursor.close()

    async def try_materialize_recurring_run(
        self,
        recurring: RecurringJobState,
        registration: RecurringRegistration,
        next_run_at_utc: datetime,
    ) -> bool:
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                if not recurring.allow_concurrent_runs:
                    cursor.execute(
                        f"""
                        select top (1) 1
                        from {_q(self.tables.runs)} with (updlock, holdlock, rowlock)
                        where job_name = ? and status in (?, ?)
                        """,
                        (recurring.job_name, RUN_STATUS_PENDING, RUN_STATUS_LEASED),
                    )
                    if cursor.fetchone() is not None:
                        self.connection.commit()
                        return False

                cursor.execute(
                    f"""
                    select next_run_at_utc, enabled
                    from {_q(self.tables.jobs)} with (updlock, holdlock, rowlock)
                    where job_name = ?
                    """,
                    (recurring.job_name,),
                )
                row = cursor.fetchone()
                if row is None or int(cast(int, row.enabled)) != 1:
                    self.connection.commit()
                    return False
                current_next = _from_db_datetime(row.next_run_at_utc)
                if abs((current_next - recurring.next_run_at_utc).total_seconds()) > 0.001:
                    self.connection.commit()
                    return False

                cursor.execute(
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
                        _to_db_datetime(recurring.next_run_at_utc),
                        _to_db_datetime(recurring.next_run_at_utc),
                        None,
                        None,
                        0,
                        registration.max_attempts,
                        None,
                        None,
                        None,
                        None,
                        _to_db_datetime(datetime.now(tz=UTC)),
                    ),
                )
                cursor.execute(
                    f"""
                    update {_q(self.tables.jobs)}
                    set next_run_at_utc = ?, updated_at_utc = ?
                    where job_name = ?
                    """,
                    (
                        _to_db_datetime(next_run_at_utc),
                        _to_db_datetime(datetime.now(tz=UTC)),
                        recurring.job_name,
                    ),
                )
                self.connection.commit()
                return True
            except Exception as exc:
                pyodbc = _require_pyodbc()
                self.connection.rollback()
                if isinstance(exc, pyodbc.IntegrityError):
                    return False
                raise
            finally:
                cursor.close()

    async def get_recurring_jobs(self, include_disabled: bool) -> list[RecurringJobState]:
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                if include_disabled:
                    cursor.execute(f"select * from {_q(self.tables.jobs)} order by job_name asc")
                else:
                    cursor.execute(
                        f"select * from {_q(self.tables.jobs)} where enabled = 1 order by job_name asc"
                    )
                return [_row_to_recurring(row) for row in cursor.fetchall()]
            finally:
                cursor.close()

    async def set_recurring_job_enabled(
        self,
        job_name: str,
        enabled: bool,
        next_run_at_utc: datetime | None,
    ) -> bool:
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"""
                    update {_q(self.tables.jobs)}
                    set enabled = ?,
                        next_run_at_utc = coalesce(?, next_run_at_utc),
                        updated_at_utc = ?
                    where job_name = ?
                    """,
                    (
                        1 if enabled else 0,
                        _to_db_datetime(next_run_at_utc) if next_run_at_utc is not None else None,
                        _to_db_datetime(datetime.now(tz=UTC)),
                        job_name,
                    ),
                )
                self.connection.commit()
                return _rowcount_is_one(cursor.rowcount)
            finally:
                cursor.close()

    async def update_recurring_job_schedule(
        self,
        job_name: str,
        cron_expression: str,
        time_zone: str,
        next_run_at_utc: datetime,
    ) -> bool:
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
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
                        _to_db_datetime(next_run_at_utc),
                        _to_db_datetime(datetime.now(tz=UTC)),
                        job_name,
                    ),
                )
                self.connection.commit()
                return _rowcount_is_one(cursor.rowcount)
            finally:
                cursor.close()

    async def try_enqueue_if_no_active_run(
        self,
        job_name: str,
        payload_json: str | None,
        due_at_utc: datetime,
        max_attempts: int,
    ) -> str | None:
        run_id = generate_id()
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"""
                    select top (1) 1
                    from {_q(self.tables.runs)} with (updlock, holdlock, rowlock)
                    where job_name = ? and status in (?, ?)
                    """,
                    (job_name, RUN_STATUS_PENDING, RUN_STATUS_LEASED),
                )
                if cursor.fetchone() is not None:
                    self.connection.commit()
                    return None

                cursor.execute(
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
                        _to_db_datetime(due_at_utc),
                        None,
                        None,
                        None,
                        0,
                        max(1, int(max_attempts)),
                        None,
                        None,
                        payload_json,
                        None,
                        _to_db_datetime(datetime.now(tz=UTC)),
                    ),
                )
                self.connection.commit()
                return run_id
            except Exception:
                self.connection.rollback()
                raise
            finally:
                cursor.close()

    async def prune_historical_runs(self, completed_before_utc: datetime, batch_size: int) -> int:
        count = max(1, int(batch_size))
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"""
                    ;with doomed as (
                        select top ({count}) id
                        from {_q(self.tables.runs)} with (updlock, readpast, rowlock)
                        where status in (?, ?)
                          and completed_at_utc is not null
                          and completed_at_utc < ?
                        order by completed_at_utc asc
                    )
                    delete from {_q(self.tables.runs)}
                    where id in (select id from doomed)
                    """,
                    (RUN_STATUS_SUCCEEDED, RUN_STATUS_FAILED, _to_db_datetime(completed_before_utc)),
                )
                deleted = max(0, int(cast(int, cursor.rowcount)))
                self.connection.commit()
                return deleted
            except Exception:
                self.connection.rollback()
                raise
            finally:
                cursor.close()

    async def try_lease_runtime_command_receipt(
        self,
        command_id: str,
        worker_name: str,
        lease_duration: timedelta,
        recorded_at_utc: datetime,
    ) -> bool:
        now = _to_db_datetime(recorded_at_utc)
        lease_until = _to_db_datetime(ensure_utc(recorded_at_utc) + lease_duration)
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"""
                    select status, lease_owner, lease_until_utc
                    from {_q(self.tables.runtime_command_receipts)} with (updlock, holdlock, rowlock)
                    where command_id = ?
                    """,
                    (command_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    cursor.execute(
                        f"""
                        insert into {_q(self.tables.runtime_command_receipts)}
                        (command_id, status, error_code, error_message, run_id, recorded_at_utc,
                         completed_at_utc, uploaded_at_utc, lease_owner, lease_until_utc)
                        values (?, 'leased', null, null, null, ?, null, null, ?, ?)
                        """,
                        (command_id, now, worker_name, lease_until),
                    )
                    self.connection.commit()
                    return True

                status = cast(str, row.status)
                if status in {"succeeded", "failed"}:
                    self.connection.commit()
                    return False

                lease_owner = cast(str | None, row.lease_owner)
                lease_until_utc = cast(datetime | None, row.lease_until_utc)
                lease_expired = lease_until_utc is None or lease_until_utc <= now
                if not lease_expired and lease_owner != worker_name:
                    self.connection.commit()
                    return False

                cursor.execute(
                    f"""
                    update {_q(self.tables.runtime_command_receipts)}
                    set status = 'leased',
                        recorded_at_utc = ?,
                        lease_owner = ?,
                        lease_until_utc = ?
                    where command_id = ?
                    """,
                    (now, worker_name, lease_until, command_id),
                )
                self.connection.commit()
                return True
            except Exception:
                self.connection.rollback()
                raise
            finally:
                cursor.close()

    async def mark_runtime_command_acknowledged(
        self,
        command_id: str,
        worker_name: str,
        recorded_at_utc: datetime,
    ) -> bool:
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"""
                    update {_q(self.tables.runtime_command_receipts)}
                    set status = 'acknowledged', recorded_at_utc = ?
                    where command_id = ? and lease_owner = ?
                    """,
                    (_to_db_datetime(recorded_at_utc), command_id, worker_name),
                )
                self.connection.commit()
                return _rowcount_is_one(cursor.rowcount)
            finally:
                cursor.close()

    async def mark_runtime_command_succeeded(
        self,
        command_id: str,
        worker_name: str,
        recorded_at_utc: datetime,
        completed_at_utc: datetime,
        run_id: str | None,
    ) -> bool:
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
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
                        _to_db_datetime(recorded_at_utc),
                        _to_db_datetime(completed_at_utc),
                        run_id,
                        command_id,
                        worker_name,
                    ),
                )
                self.connection.commit()
                return _rowcount_is_one(cursor.rowcount)
            finally:
                cursor.close()

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
            cursor = self.connection.cursor()
            try:
                cursor.execute(
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
                        _to_db_datetime(recorded_at_utc),
                        _to_db_datetime(completed_at_utc),
                        error_code,
                        error_message,
                        command_id,
                        worker_name,
                    ),
                )
                self.connection.commit()
                return _rowcount_is_one(cursor.rowcount)
            finally:
                cursor.close()

    async def get_runtime_command_receipts(self, take: int) -> list[RuntimeCommandReceipt]:
        count = max(1, int(take))
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"""
                    select top ({count}) *
                    from {_q(self.tables.runtime_command_receipts)}
                    where uploaded_at_utc is null
                      and status in ('acknowledged', 'succeeded', 'failed')
                    order by recorded_at_utc asc
                    """
                )
                return [_row_to_receipt(row) for row in cursor.fetchall()]
            finally:
                cursor.close()

    async def mark_runtime_command_receipt_uploaded(
        self,
        command_id: str,
        uploaded_at_utc: datetime,
    ) -> bool:
        async with self._lock:
            cursor = self.connection.cursor()
            try:
                cursor.execute(
                    f"""
                    update {_q(self.tables.runtime_command_receipts)}
                    set uploaded_at_utc = ?
                    where command_id = ?
                    """,
                    (_to_db_datetime(uploaded_at_utc), command_id),
                )
                self.connection.commit()
                return _rowcount_is_one(cursor.rowcount)
            finally:
                cursor.close()
