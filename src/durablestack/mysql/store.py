"""MySQL DurableJobStore implementation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from importlib import import_module
from typing import Any, cast
from urllib.parse import parse_qs, unquote, urlparse

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

from .migrator import migrate_mysql
from .table_names import MySqlTableNames
from .types import MySqlDurableStackOptions


def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _rowcount_is_one(value: Any) -> bool:
    return int(cast(int, value)) == 1


def _require_pymysql() -> Any:
    try:
        return import_module("pymysql")
    except ImportError as exc:
        raise RuntimeError("pymysql is required for MySQL provider") from exc


def _to_db_datetime(value: datetime) -> datetime:
    utc = ensure_utc(value)
    return utc.replace(tzinfo=None)


def _from_db_datetime(value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("expected datetime value")
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _row_to_run(row: dict[str, Any]) -> JobRun:
    return JobRun(
        run_id=cast(str, row["id"]),
        job_name=cast(str, row["job_name"]),
        status=cast(str, row["status"]),
        created_by=None,
        payload_json=cast(str | None, row["payload_json"]),
        attempt=cast(int, row["attempt"]),
        max_attempts=cast(int, row["max_attempts"]),
        created_at_utc=_from_db_datetime(row["created_at_utc"]),
        due_at_utc=_from_db_datetime(row["scheduled_for_utc"]),
        leased_by=cast(str | None, row["lease_owner"]),
        lease_expires_at_utc=_from_db_datetime(row["lease_until_utc"]) if row["lease_until_utc"] else None,
        started_at_utc=_from_db_datetime(row["started_at_utc"]) if row["started_at_utc"] else None,
        completed_at_utc=_from_db_datetime(row["completed_at_utc"]) if row["completed_at_utc"] else None,
        last_error=cast(str | None, row["error_message"]),
        schedule_slot_utc=_from_db_datetime(row["schedule_slot_utc"]) if row["schedule_slot_utc"] else None,
    )


def _row_to_recurring(row: dict[str, Any]) -> RecurringJobState:
    return RecurringJobState(
        job_name=cast(str, row["job_name"]),
        cron_expression=cast(str, row["cron_expression"]),
        time_zone=cast(str, row["time_zone"]),
        enabled=bool(row["enabled"]),
        allow_concurrent_runs=bool(row["allow_concurrent_runs"]),
        max_attempts=cast(int, row["max_attempts"]),
        next_run_at_utc=_from_db_datetime(row["next_run_at_utc"]),
    )


def _row_to_receipt(row: dict[str, Any]) -> RuntimeCommandReceipt:
    status = cast(str, row["status"])
    if status not in {"leased", "acknowledged", "succeeded", "failed"}:
        raise ValueError(f"unsupported receipt status: {status}")
    return RuntimeCommandReceipt(
        command_id=cast(str, row["command_id"]),
        status=cast(RuntimeCommandReceiptStatus, status),
        recorded_at_utc=_from_db_datetime(row["recorded_at_utc"]),
        completed_at_utc=_from_db_datetime(row["completed_at_utc"]) if row["completed_at_utc"] else None,
        run_id=cast(str | None, row["run_id"]),
        error_code=cast(str | None, row["error_code"]),
        error_message=cast(str | None, row["error_message"]),
        uploaded_at_utc=_from_db_datetime(row["uploaded_at_utc"]) if row["uploaded_at_utc"] else None,
        lease_owner=cast(str | None, row["lease_owner"]),
        lease_until_utc=_from_db_datetime(row["lease_until_utc"]) if row["lease_until_utc"] else None,
    )


def _parse_connection_string(connection_string: str) -> dict[str, Any]:
    parsed = urlparse(connection_string)
    if parsed.scheme not in {"mysql", "mysql+pymysql"}:
        raise ValueError("MySQL connection_string must use mysql:// or mysql+pymysql://")

    database = parsed.path.lstrip("/")
    if database == "":
        raise ValueError("MySQL connection_string must include a database name")

    query = parse_qs(parsed.query)
    charset = query.get("charset", ["utf8mb4"])[0]

    return {
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 3306,
        "user": unquote(parsed.username) if parsed.username else None,
        "password": unquote(parsed.password) if parsed.password else None,
        "database": database,
        "charset": charset,
    }


@dataclass(slots=True)
class MySqlDurableJobStore:
    """MySQL-backed DurableJobStore implementation."""

    options: MySqlDurableStackOptions
    _connection: Any | None = None
    _tables: MySqlTableNames | None = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def open(self) -> None:
        connect_options = _parse_connection_string(self.options.connection_string)
        pymysql = _require_pymysql()
        self._connection = pymysql.connect(
            host=connect_options["host"],
            port=connect_options["port"],
            user=connect_options["user"],
            password=connect_options["password"],
            database=connect_options["database"],
            charset=connect_options["charset"],
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )
        self._tables = migrate_mysql(self.connection, self.options.table_prefix)

    async def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            self._tables = None

    @property
    def connection(self) -> Any:
        if self._connection is None:
            raise RuntimeError("MySQL store is not open")
        return self._connection

    @property
    def tables(self) -> MySqlTableNames:
        if self._tables is None:
            raise RuntimeError("MySQL store is not open")
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
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                insert into {_q(self.tables.runs)}
                (id, job_name, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc,
                 attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        claimed: list[JobRun] = []
        async with self._lock, self.connection.cursor() as cursor:
            try:
                cursor.execute("start transaction")
                cursor.execute(
                    f"""
                    select id, attempt, max_attempts
                    from {_q(self.tables.runs)}
                    where
                      (status = %s and scheduled_for_utc <= %s)
                      or
                      (status = %s and (lease_until_utc is null or lease_until_utc <= %s))
                    order by scheduled_for_utc asc
                    limit %s
                    for update skip locked
                    """,
                    (
                        RUN_STATUS_PENDING,
                        now,
                        RUN_STATUS_LEASED,
                        now,
                        max(1, int(batch_size)),
                    ),
                )
                rows = cast(list[dict[str, Any]], cursor.fetchall())
                for row in rows:
                    run_id = cast(str, row["id"])
                    attempt = cast(int, row["attempt"]) + 1
                    max_attempts = cast(int, row["max_attempts"])
                    if attempt > max_attempts:
                        cursor.execute(
                            f"""
                            update {_q(self.tables.runs)}
                            set attempt = %s,
                                status = %s,
                                completed_at_utc = %s,
                                error_message = coalesce(error_message, %s),
                                lease_owner = null,
                                lease_until_utc = null
                            where id = %s
                            """,
                            (attempt, RUN_STATUS_FAILED, now, "Run exceeded max attempts before claim", run_id),
                        )
                        continue

                    cursor.execute(
                        f"""
                        update {_q(self.tables.runs)}
                        set attempt = %s,
                            status = %s,
                            started_at_utc = coalesce(started_at_utc, %s),
                            completed_at_utc = null,
                            lease_owner = %s,
                            lease_until_utc = %s
                        where id = %s
                        """,
                        (attempt, RUN_STATUS_LEASED, now, worker_name, lease_until, run_id),
                    )
                    cursor.execute(
                        f"select * from {_q(self.tables.runs)} where id = %s",
                        (run_id,),
                    )
                    mapped = cast(dict[str, Any] | None, cursor.fetchone())
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
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                update {_q(self.tables.runs)}
                set lease_until_utc = %s
                where id = %s
                  and status = %s
                  and lease_owner = %s
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

    async def mark_succeeded(self, run_id: str, worker_name: str, completed_at_utc: datetime) -> bool:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                update {_q(self.tables.runs)}
                set status = %s,
                    completed_at_utc = %s,
                    lease_owner = null,
                    lease_until_utc = null,
                    error_message = null
                where id = %s and status = %s and lease_owner = %s
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

    async def mark_failed(
        self,
        run_id: str,
        worker_name: str,
        error_message: str,
        completed_at_utc: datetime,
        should_retry: bool,
        retry_at_utc: datetime | None,
    ) -> bool:
        async with self._lock, self.connection.cursor() as cursor:
            if should_retry and retry_at_utc is not None:
                cursor.execute(
                    f"""
                    update {_q(self.tables.runs)}
                    set status = %s,
                        scheduled_for_utc = %s,
                        lease_owner = null,
                        lease_until_utc = null,
                        completed_at_utc = null,
                        error_message = %s
                    where id = %s and status = %s and lease_owner = %s
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
                set status = %s,
                    completed_at_utc = %s,
                    lease_owner = null,
                    lease_until_utc = null,
                    error_message = %s
                where id = %s and status = %s and lease_owner = %s
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

    async def cancel_run(self, run_id: str, reason: str, completed_at_utc: datetime) -> bool:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                update {_q(self.tables.runs)}
                set status = %s,
                    completed_at_utc = %s,
                    error_message = %s,
                    lease_owner = null,
                    lease_until_utc = null
                where id = %s and status in (%s, %s)
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

    async def get_run(self, run_id: str) -> JobRun | None:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(f"select * from {_q(self.tables.runs)} where id = %s", (run_id,))
            row = cast(dict[str, Any] | None, cursor.fetchone())
            return _row_to_run(row) if row is not None else None

    async def get_recent_runs(self, take: int) -> list[JobRun]:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"select * from {_q(self.tables.runs)} order by scheduled_for_utc desc limit %s",
                (max(1, int(take)),),
            )
            rows = cast(list[dict[str, Any]], cursor.fetchall())
            return [_row_to_run(row) for row in rows]

    async def get_runs_by_status(self, status: str, take: int) -> list[JobRun]:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"select * from {_q(self.tables.runs)} where status = %s order by scheduled_for_utc desc limit %s",
                (status, max(1, int(take))),
            )
            rows = cast(list[dict[str, Any]], cursor.fetchall())
            return [_row_to_run(row) for row in rows]

    async def get_runs_by_job_name(self, job_name: str, take: int) -> list[JobRun]:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"select * from {_q(self.tables.runs)} where job_name = %s order by scheduled_for_utc desc limit %s",
                (job_name, max(1, int(take))),
            )
            rows = cast(list[dict[str, Any]], cursor.fetchall())
            return [_row_to_run(row) for row in rows]

    async def get_enqueued_runs(self, take: int) -> list[JobRun]:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                select * from {_q(self.tables.runs)}
                where status = %s and schedule_slot_utc is null
                order by scheduled_for_utc desc
                limit %s
                """,
                (RUN_STATUS_PENDING, max(1, int(take))),
            )
            rows = cast(list[dict[str, Any]], cursor.fetchall())
            return [_row_to_run(row) for row in rows]

    async def get_due_recurring_jobs(self, now_utc: datetime, batch_size: int) -> list[RecurringJobState]:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                select * from {_q(self.tables.jobs)}
                where enabled = 1 and next_run_at_utc <= %s
                order by next_run_at_utc asc
                limit %s
                """,
                (_to_db_datetime(now_utc), max(1, int(batch_size))),
            )
            rows = cast(list[dict[str, Any]], cursor.fetchall())
            return [_row_to_recurring(row) for row in rows]

    async def upsert_recurring_job(
        self,
        registration: RecurringRegistration,
        next_run_at_utc: datetime,
    ) -> None:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                insert into {_q(self.tables.jobs)}
                (job_name, cron_expression, time_zone, max_attempts, enabled, allow_concurrent_runs,
                 retry_behavior, retry_initial_delay_seconds, next_run_at_utc, updated_at_utc)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                on duplicate key update
                  cron_expression = values(cron_expression),
                  time_zone = values(time_zone),
                  max_attempts = values(max_attempts),
                  enabled = values(enabled),
                  allow_concurrent_runs = values(allow_concurrent_runs),
                  retry_behavior = values(retry_behavior),
                  retry_initial_delay_seconds = values(retry_initial_delay_seconds),
                  next_run_at_utc = values(next_run_at_utc),
                  updated_at_utc = values(updated_at_utc)
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

    async def try_materialize_recurring_run(
        self,
        recurring: RecurringJobState,
        registration: RecurringRegistration,
        next_run_at_utc: datetime,
    ) -> bool:
        async with self._lock, self.connection.cursor() as cursor:
            try:
                cursor.execute("start transaction")
                if not recurring.allow_concurrent_runs:
                    cursor.execute(
                        f"""
                        select 1
                        from {_q(self.tables.runs)}
                        where job_name = %s and status in (%s, %s)
                        limit 1
                        for update
                        """,
                        (recurring.job_name, RUN_STATUS_PENDING, RUN_STATUS_LEASED),
                    )
                    if cursor.fetchone() is not None:
                        self.connection.commit()
                        return False

                cursor.execute(
                    f"""
                    select next_run_at_utc, enabled
                    from {_q(self.tables.jobs)}
                    where job_name = %s
                    for update
                    """,
                    (recurring.job_name,),
                )
                locked = cast(dict[str, Any] | None, cursor.fetchone())
                if locked is None or int(cast(int, locked["enabled"])) != 1:
                    self.connection.commit()
                    return False

                current_next = _from_db_datetime(locked["next_run_at_utc"])
                if abs((current_next - recurring.next_run_at_utc).total_seconds()) > 0.001:
                    self.connection.commit()
                    return False

                cursor.execute(
                    f"""
                    insert into {_q(self.tables.runs)}
                    (id, job_name, status, scheduled_for_utc, schedule_slot_utc, started_at_utc, completed_at_utc,
                     attempt, max_attempts, lease_owner, lease_until_utc, payload_json, error_message, created_at_utc)
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    set next_run_at_utc = %s,
                        updated_at_utc = %s
                    where job_name = %s
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
                pymysql = _require_pymysql()
                self.connection.rollback()
                if isinstance(exc, pymysql.err.IntegrityError):
                    return False
                raise

    async def get_recurring_jobs(self, include_disabled: bool) -> list[RecurringJobState]:
        async with self._lock, self.connection.cursor() as cursor:
            if include_disabled:
                cursor.execute(f"select * from {_q(self.tables.jobs)} order by job_name asc")
            else:
                cursor.execute(
                    f"select * from {_q(self.tables.jobs)} where enabled = 1 order by job_name asc"
                )
            rows = cast(list[dict[str, Any]], cursor.fetchall())
            return [_row_to_recurring(row) for row in rows]

    async def set_recurring_job_enabled(
        self,
        job_name: str,
        enabled: bool,
        next_run_at_utc: datetime | None,
    ) -> bool:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                update {_q(self.tables.jobs)}
                set enabled = %s,
                    next_run_at_utc = coalesce(%s, next_run_at_utc),
                    updated_at_utc = %s
                where job_name = %s
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

    async def update_recurring_job_schedule(
        self,
        job_name: str,
        cron_expression: str,
        time_zone: str,
        next_run_at_utc: datetime,
    ) -> bool:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                update {_q(self.tables.jobs)}
                set cron_expression = %s,
                    time_zone = %s,
                    next_run_at_utc = %s,
                    updated_at_utc = %s
                where job_name = %s
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

    async def try_enqueue_if_no_active_run(
        self,
        job_name: str,
        payload_json: str | None,
        due_at_utc: datetime,
        max_attempts: int,
    ) -> str | None:
        run_id = generate_id()
        async with self._lock, self.connection.cursor() as cursor:
            try:
                cursor.execute("start transaction")
                cursor.execute(
                    f"""
                    select 1
                    from {_q(self.tables.runs)}
                    where job_name = %s and status in (%s, %s)
                    limit 1
                    for update
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
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

    async def prune_historical_runs(self, completed_before_utc: datetime, batch_size: int) -> int:
        deleted = 0
        async with self._lock, self.connection.cursor() as cursor:
            try:
                cursor.execute("start transaction")
                cursor.execute(
                    f"""
                    select id
                    from {_q(self.tables.runs)}
                    where status in (%s, %s)
                      and completed_at_utc is not null
                      and completed_at_utc < %s
                    order by completed_at_utc asc
                    limit %s
                    for update
                    """,
                    (
                        RUN_STATUS_SUCCEEDED,
                        RUN_STATUS_FAILED,
                        _to_db_datetime(completed_before_utc),
                        max(1, int(batch_size)),
                    ),
                )
                rows = cast(list[dict[str, Any]], cursor.fetchall())
                if not rows:
                    self.connection.commit()
                    return 0
                ids = [cast(str, row["id"]) for row in rows]
                placeholders = ",".join(["%s"] * len(ids))
                cursor.execute(
                    f"delete from {_q(self.tables.runs)} where id in ({placeholders})",
                    ids,
                )
                deleted = max(0, int(cursor.rowcount))
                self.connection.commit()
            except Exception:
                self.connection.rollback()
                raise
        return deleted

    async def try_lease_runtime_command_receipt(
        self,
        command_id: str,
        worker_name: str,
        lease_duration: timedelta,
        recorded_at_utc: datetime,
    ) -> bool:
        now = _to_db_datetime(recorded_at_utc)
        lease_until = _to_db_datetime(ensure_utc(recorded_at_utc) + lease_duration)
        async with self._lock, self.connection.cursor() as cursor:
            try:
                cursor.execute("start transaction")
                cursor.execute(
                    f"""
                    select status, lease_owner, lease_until_utc
                    from {_q(self.tables.runtime_command_receipts)}
                    where command_id = %s
                    for update
                    """,
                    (command_id,),
                )
                row = cast(dict[str, Any] | None, cursor.fetchone())
                if row is None:
                    cursor.execute(
                        f"""
                        insert into {_q(self.tables.runtime_command_receipts)}
                        (command_id, status, error_code, error_message, run_id, recorded_at_utc,
                         completed_at_utc, uploaded_at_utc, lease_owner, lease_until_utc)
                        values (%s, 'leased', null, null, null, %s, null, null, %s, %s)
                        """,
                        (command_id, now, worker_name, lease_until),
                    )
                    self.connection.commit()
                    return True

                status = cast(str, row["status"])
                if status in {"succeeded", "failed"}:
                    self.connection.commit()
                    return False

                lease_owner = cast(str | None, row["lease_owner"])
                lease_until_utc = cast(datetime | None, row["lease_until_utc"])
                lease_expired = lease_until_utc is None or lease_until_utc <= now
                if not lease_expired and lease_owner != worker_name:
                    self.connection.commit()
                    return False

                cursor.execute(
                    f"""
                    update {_q(self.tables.runtime_command_receipts)}
                    set status = 'leased', recorded_at_utc = %s, lease_owner = %s, lease_until_utc = %s
                    where command_id = %s
                    """,
                    (now, worker_name, lease_until, command_id),
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
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                update {_q(self.tables.runtime_command_receipts)}
                set status = 'acknowledged', recorded_at_utc = %s
                where command_id = %s and lease_owner = %s
                """,
                (_to_db_datetime(recorded_at_utc), command_id, worker_name),
            )
            self.connection.commit()
            return _rowcount_is_one(cursor.rowcount)

    async def mark_runtime_command_succeeded(
        self,
        command_id: str,
        worker_name: str,
        recorded_at_utc: datetime,
        completed_at_utc: datetime,
        run_id: str | None,
    ) -> bool:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                update {_q(self.tables.runtime_command_receipts)}
                set status = 'succeeded',
                    recorded_at_utc = %s,
                    completed_at_utc = %s,
                    run_id = %s,
                    lease_owner = null,
                    lease_until_utc = null
                where command_id = %s and lease_owner = %s
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

    async def mark_runtime_command_failed(
        self,
        command_id: str,
        worker_name: str,
        recorded_at_utc: datetime,
        completed_at_utc: datetime,
        error_code: str | None,
        error_message: str | None,
    ) -> bool:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                update {_q(self.tables.runtime_command_receipts)}
                set status = 'failed',
                    recorded_at_utc = %s,
                    completed_at_utc = %s,
                    error_code = %s,
                    error_message = %s,
                    lease_owner = null,
                    lease_until_utc = null
                where command_id = %s and lease_owner = %s
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

    async def get_runtime_command_receipts(self, take: int) -> list[RuntimeCommandReceipt]:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                select *
                from {_q(self.tables.runtime_command_receipts)}
                where uploaded_at_utc is null
                  and status in ('acknowledged', 'succeeded', 'failed')
                order by recorded_at_utc asc
                limit %s
                """,
                (max(1, int(take)),),
            )
            rows = cast(list[dict[str, Any]], cursor.fetchall())
            return [_row_to_receipt(row) for row in rows]

    async def mark_runtime_command_receipt_uploaded(
        self,
        command_id: str,
        uploaded_at_utc: datetime,
    ) -> bool:
        async with self._lock, self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                update {_q(self.tables.runtime_command_receipts)}
                set uploaded_at_utc = %s
                where command_id = %s
                """,
                (_to_db_datetime(uploaded_at_utc), command_id),
            )
            self.connection.commit()
            return _rowcount_is_one(cursor.rowcount)
