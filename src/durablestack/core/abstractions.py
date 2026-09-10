"""Core protocols and abstraction contracts for DurableStack Python."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import Any, Protocol

from .models import (
    EnqueueResult,
    JobRegistration,
    JobRun,
    RecurringJobState,
    RecurringRegistration,
    RuntimeCommandReceipt,
)

JobHandler = Callable[..., Any | Awaitable[Any]]


class DurableJobStore(Protocol):
    """Storage contract implemented by in-memory and durable providers."""

    async def enqueue(
        self,
        job_name: str,
        payload_json: str | None,
        max_attempts: int,
    ) -> EnqueueResult: ...

    async def schedule(
        self,
        job_name: str,
        payload_json: str | None,
        run_at_utc: datetime,
        max_attempts: int,
    ) -> EnqueueResult: ...

    async def get_run(self, run_id: str) -> JobRun | None: ...

    async def get_recent_runs(self, take: int) -> list[JobRun]: ...

    async def get_runs_by_status(self, status: str, take: int) -> list[JobRun]: ...

    async def get_runs_by_job_name(self, job_name: str, take: int) -> list[JobRun]: ...

    async def get_enqueued_runs(self, take: int) -> list[JobRun]: ...

    async def claim_due_runs(
        self,
        worker_name: str,
        batch_size: int,
        lease_duration: timedelta,
        now_utc: datetime,
    ) -> list[JobRun]: ...

    async def extend_lease(
        self,
        run_id: str,
        worker_name: str,
        lease_duration: timedelta,
        now_utc: datetime,
    ) -> bool: ...

    async def mark_succeeded(self, run_id: str, worker_name: str, completed_at_utc: datetime) -> bool: ...

    async def mark_failed(
        self,
        run_id: str,
        worker_name: str,
        error_message: str,
        completed_at_utc: datetime,
        should_retry: bool,
        retry_at_utc: datetime | None,
    ) -> bool: ...

    async def cancel_run(self, run_id: str, reason: str, completed_at_utc: datetime) -> bool: ...

    async def get_due_recurring_jobs(
        self,
        now_utc: datetime,
        batch_size: int,
    ) -> list[RecurringJobState]: ...

    async def upsert_recurring_job(
        self,
        registration: RecurringRegistration,
        next_run_at_utc: datetime,
    ) -> None: ...

    async def try_materialize_recurring_run(
        self,
        recurring: RecurringJobState,
        registration: RecurringRegistration,
        next_run_at_utc: datetime,
    ) -> bool: ...

    async def get_recurring_jobs(self, include_disabled: bool) -> list[RecurringJobState]: ...

    async def set_recurring_job_enabled(
        self,
        job_name: str,
        enabled: bool,
        next_run_at_utc: datetime | None,
    ) -> bool: ...

    async def update_recurring_job_schedule(
        self,
        job_name: str,
        cron_expression: str,
        time_zone: str,
        next_run_at_utc: datetime,
    ) -> bool: ...

    async def try_enqueue_if_no_active_run(
        self,
        job_name: str,
        payload_json: str | None,
        due_at_utc: datetime,
        max_attempts: int,
    ) -> str | None: ...

    async def prune_historical_runs(self, completed_before_utc: datetime, batch_size: int) -> int: ...

    async def try_lease_runtime_command_receipt(
        self,
        command_id: str,
        worker_name: str,
        lease_duration: timedelta,
        recorded_at_utc: datetime,
    ) -> bool: ...

    async def mark_runtime_command_acknowledged(
        self,
        command_id: str,
        worker_name: str,
        recorded_at_utc: datetime,
    ) -> bool: ...

    async def mark_runtime_command_succeeded(
        self,
        command_id: str,
        worker_name: str,
        recorded_at_utc: datetime,
        completed_at_utc: datetime,
        run_id: str | None,
    ) -> bool: ...

    async def mark_runtime_command_failed(
        self,
        command_id: str,
        worker_name: str,
        recorded_at_utc: datetime,
        completed_at_utc: datetime,
        error_code: str | None,
        error_message: str | None,
    ) -> bool: ...

    async def get_runtime_command_receipts(self, take: int) -> list[RuntimeCommandReceipt]: ...

    async def mark_runtime_command_receipt_uploaded(
        self,
        command_id: str,
        uploaded_at_utc: datetime,
    ) -> bool: ...

    async def close(self) -> None: ...


class RuntimeControlAdmin(Protocol):
    """Runtime control admin capabilities used by control sync service."""

    async def list_scheduled_jobs(self, include_disabled: bool = True) -> list[RecurringJobState]: ...

    async def set_scheduled_job_enabled(self, job_name: str, enabled: bool) -> bool: ...

    async def update_scheduled_job_cron(
        self,
        job_name: str,
        cron_expression: str,
        time_zone: str,
    ) -> bool: ...

    async def run_scheduled_job_now(self, job_name: str) -> str | None: ...


class DurableStackEventSink(Protocol):
    """Sink contract for runtime events."""

    async def publish(self, event: dict[str, Any]) -> None: ...


class DurableStackRuntime(Protocol):
    """Public runtime lifecycle and operation surface."""

    async def start(self) -> None: ...

    async def stop(self, *, drain_timeout: timedelta | None = None) -> None: ...

    def register_job(self, name: str, handler: JobHandler, options: Any | None = None) -> None: ...

    def register_recurring(
        self,
        name: str,
        cron_expression: str,
        time_zone: str,
        handler: JobHandler,
        options: Any | None = None,
    ) -> None: ...

    @property
    def store(self) -> DurableJobStore: ...

    async def enqueue(self, job_name: str, payload: Any = None) -> str: ...

    async def schedule(self, job_name: str, payload: Any, run_at_utc: datetime) -> str: ...

    async def cancel_run(self, run_id: str) -> bool: ...

    async def get_run(self, run_id: str) -> JobRun | None: ...

    async def get_recent_runs(self, take: int = 100) -> list[JobRun]: ...

    async def get_runs_by_status(self, status: str, take: int = 100) -> list[JobRun]: ...

    async def get_runs_by_job_name(self, job_name: str, take: int = 100) -> list[JobRun]: ...

    async def get_enqueued_runs(self, take: int = 100) -> list[JobRun]: ...

    async def list_scheduled_jobs(self, include_disabled: bool = True) -> list[RecurringJobState]: ...

    async def set_scheduled_job_enabled(self, job_name: str, enabled: bool) -> bool: ...

    async def update_scheduled_job_cron(
        self,
        job_name: str,
        cron_expression: str,
        time_zone: str,
    ) -> bool: ...

    async def run_scheduled_job_now(self, job_name: str) -> str | None: ...


class DurableJobRegistry(Protocol):
    """Registry contract for job and recurring registrations."""

    def register_job(self, registration: JobRegistration) -> None: ...

    def register_recurring(self, registration: RecurringRegistration) -> None: ...

    def get_job(self, name: str) -> JobRegistration | None: ...

    def get_recurring(self, name: str) -> RecurringRegistration | None: ...

    def get_all_recurring(self) -> list[RecurringRegistration]: ...
