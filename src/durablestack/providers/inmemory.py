"""In-memory DurableJobStore implementation for Phase 1."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta

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
)
from durablestack.core.utils import ensure_utc, generate_id, utc_now


def _is_active(status: str) -> bool:
    return status in {RUN_STATUS_PENDING, RUN_STATUS_LEASED}


@dataclass(slots=True)
class InMemoryDurableJobStore:
    """In-memory storage implementing Phase 1 behavior semantics."""

    _runs: dict[str, JobRun] = field(default_factory=dict)
    _recurring: dict[str, RecurringJobState] = field(default_factory=dict)
    _runtime_receipts: dict[str, RuntimeCommandReceipt] = field(default_factory=dict)

    async def enqueue(self, job_name: str, payload_json: str | None, max_attempts: int) -> EnqueueResult:
        return await self.schedule(job_name, payload_json, utc_now(), max_attempts)

    async def schedule(
        self,
        job_name: str,
        payload_json: str | None,
        run_at_utc: datetime,
        max_attempts: int,
    ) -> EnqueueResult:
        due_at_utc = ensure_utc(run_at_utc)
        created_at_utc = utc_now()
        run_id = generate_id()
        run = JobRun(
            run_id=run_id,
            job_name=job_name,
            status=RUN_STATUS_PENDING,
            created_by=None,
            payload_json=payload_json,
            attempt=0,
            max_attempts=max(1, int(max_attempts)),
            created_at_utc=created_at_utc,
            due_at_utc=due_at_utc,
            leased_by=None,
            lease_expires_at_utc=None,
            started_at_utc=None,
            completed_at_utc=None,
            last_error=None,
            schedule_slot_utc=None,
        )
        self._runs[run_id] = run
        return EnqueueResult(run_id=run_id)

    async def get_run(self, run_id: str) -> JobRun | None:
        return self._runs.get(run_id)

    async def get_recent_runs(self, take: int) -> list[JobRun]:
        return sorted(self._runs.values(), key=lambda x: x.due_at_utc, reverse=True)[: max(1, take)]

    async def get_runs_by_status(self, status: str, take: int) -> list[JobRun]:
        runs = [run for run in self._runs.values() if run.status == status]
        return sorted(runs, key=lambda x: x.due_at_utc, reverse=True)[: max(1, take)]

    async def get_runs_by_job_name(self, job_name: str, take: int) -> list[JobRun]:
        runs = [run for run in self._runs.values() if run.job_name == job_name]
        return sorted(runs, key=lambda x: x.due_at_utc, reverse=True)[: max(1, take)]

    async def get_enqueued_runs(self, take: int) -> list[JobRun]:
        runs = [
            run
            for run in self._runs.values()
            if run.status == RUN_STATUS_PENDING and run.schedule_slot_utc is None
        ]
        return sorted(runs, key=lambda x: x.due_at_utc, reverse=True)[: max(1, take)]

    async def claim_due_runs(
        self,
        worker_name: str,
        batch_size: int,
        lease_duration: timedelta,
        now_utc: datetime,
    ) -> list[JobRun]:
        now = ensure_utc(now_utc)
        due = [
            run
            for run in self._runs.values()
            if (
                run.status == RUN_STATUS_PENDING
                and run.due_at_utc <= now
            )
            or (
                run.status == RUN_STATUS_LEASED
                and run.lease_expires_at_utc is not None
                and run.lease_expires_at_utc <= now
            )
        ]
        due = sorted(due, key=lambda x: x.due_at_utc)
        claimed: list[JobRun] = []
        for run in due:
            if len(claimed) >= max(1, batch_size):
                break
            if run.attempt >= run.max_attempts:
                self._runs[run.run_id] = replace(
                    run,
                    status=RUN_STATUS_FAILED,
                    leased_by=None,
                    lease_expires_at_utc=None,
                    completed_at_utc=now,
                )
                continue

            updated = replace(
                run,
                status=RUN_STATUS_LEASED,
                attempt=run.attempt + 1,
                leased_by=worker_name,
                lease_expires_at_utc=now + lease_duration,
                started_at_utc=run.started_at_utc or now,
                completed_at_utc=None,
            )
            self._runs[run.run_id] = updated
            claimed.append(updated)

        return claimed

    async def extend_lease(
        self,
        run_id: str,
        worker_name: str,
        lease_duration: timedelta,
        now_utc: datetime,
    ) -> bool:
        now = ensure_utc(now_utc)
        run = self._runs.get(run_id)
        if run is None:
            return False
        if run.status != RUN_STATUS_LEASED or run.leased_by != worker_name:
            return False
        self._runs[run_id] = replace(run, lease_expires_at_utc=now + lease_duration)
        return True

    async def mark_succeeded(self, run_id: str, worker_name: str, completed_at_utc: datetime) -> bool:
        completed_at = ensure_utc(completed_at_utc)
        run = self._runs.get(run_id)
        if run is None:
            return False
        if run.status != RUN_STATUS_LEASED or run.leased_by != worker_name:
            return False
        self._runs[run_id] = replace(
            run,
            status=RUN_STATUS_SUCCEEDED,
            leased_by=None,
            lease_expires_at_utc=None,
            completed_at_utc=completed_at,
            last_error=None,
        )
        return True

    async def mark_failed(
        self,
        run_id: str,
        worker_name: str,
        error_message: str,
        completed_at_utc: datetime,
        should_retry: bool,
        retry_at_utc: datetime | None,
    ) -> bool:
        completed_at = ensure_utc(completed_at_utc)
        run = self._runs.get(run_id)
        if run is None:
            return False
        if run.status != RUN_STATUS_LEASED or run.leased_by != worker_name:
            return False

        retry_at = ensure_utc(retry_at_utc) if retry_at_utc is not None else None
        if should_retry and retry_at is not None and run.attempt < run.max_attempts:
            self._runs[run_id] = replace(
                run,
                status=RUN_STATUS_PENDING,
                due_at_utc=retry_at,
                leased_by=None,
                lease_expires_at_utc=None,
                completed_at_utc=None,
                last_error=error_message,
            )
            return True

        self._runs[run_id] = replace(
            run,
            status=RUN_STATUS_FAILED,
            leased_by=None,
            lease_expires_at_utc=None,
            completed_at_utc=completed_at,
            last_error=error_message,
        )
        return True

    async def cancel_run(self, run_id: str, reason: str, completed_at_utc: datetime) -> bool:
        completed_at = ensure_utc(completed_at_utc)
        run = self._runs.get(run_id)
        if run is None:
            return False
        if run.status in {RUN_STATUS_SUCCEEDED, RUN_STATUS_FAILED}:
            return False

        self._runs[run_id] = replace(
            run,
            status=RUN_STATUS_FAILED,
            leased_by=None,
            lease_expires_at_utc=None,
            completed_at_utc=completed_at,
            last_error=reason,
        )
        return True

    async def get_due_recurring_jobs(
        self,
        now_utc: datetime,
        batch_size: int,
    ) -> list[RecurringJobState]:
        now = ensure_utc(now_utc)
        due = [
            recurring
            for recurring in self._recurring.values()
            if recurring.enabled and recurring.next_run_at_utc <= now
        ]
        return sorted(due, key=lambda x: x.next_run_at_utc)[: max(1, batch_size)]

    async def upsert_recurring_job(
        self,
        registration: RecurringRegistration,
        next_run_at_utc: datetime,
    ) -> None:
        next_run = ensure_utc(next_run_at_utc)
        self._recurring[registration.name] = RecurringJobState(
            job_name=registration.name,
            cron_expression=registration.cron_expression,
            time_zone=registration.time_zone,
            enabled=registration.enabled,
            allow_concurrent_runs=registration.allow_concurrent_runs,
            max_attempts=registration.max_attempts,
            next_run_at_utc=next_run,
        )

    async def try_materialize_recurring_run(
        self,
        recurring: RecurringJobState,
        registration: RecurringRegistration,
        next_run_at_utc: datetime,
    ) -> bool:
        next_run = ensure_utc(next_run_at_utc)
        stored = self._recurring.get(recurring.job_name)
        if stored is None:
            return False
        if stored.next_run_at_utc != recurring.next_run_at_utc or not stored.enabled:
            return False

        slot_exists = any(
            run.job_name == recurring.job_name and run.schedule_slot_utc == recurring.next_run_at_utc
            for run in self._runs.values()
        )
        if slot_exists:
            return False

        if not stored.allow_concurrent_runs:
            has_active = any(
                run.job_name == recurring.job_name and _is_active(run.status)
                for run in self._runs.values()
            )
            if has_active:
                return False

        run_id = generate_id()
        run = JobRun(
            run_id=run_id,
            job_name=recurring.job_name,
            status=RUN_STATUS_PENDING,
            created_by=None,
            payload_json=None,
            attempt=0,
            max_attempts=registration.max_attempts,
            created_at_utc=recurring.next_run_at_utc,
            due_at_utc=recurring.next_run_at_utc,
            leased_by=None,
            lease_expires_at_utc=None,
            started_at_utc=None,
            completed_at_utc=None,
            last_error=None,
            schedule_slot_utc=recurring.next_run_at_utc,
        )
        self._runs[run_id] = run
        self._recurring[recurring.job_name] = RecurringJobState(
            job_name=stored.job_name,
            cron_expression=stored.cron_expression,
            time_zone=stored.time_zone,
            enabled=stored.enabled,
            allow_concurrent_runs=stored.allow_concurrent_runs,
            max_attempts=stored.max_attempts,
            next_run_at_utc=next_run,
        )
        return True

    async def get_recurring_jobs(self, include_disabled: bool) -> list[RecurringJobState]:
        rows = [
            row
            for row in self._recurring.values()
            if include_disabled or row.enabled
        ]
        return sorted(rows, key=lambda x: x.job_name)

    async def set_recurring_job_enabled(
        self,
        job_name: str,
        enabled: bool,
        next_run_at_utc: datetime | None,
    ) -> bool:
        next_run = ensure_utc(next_run_at_utc) if next_run_at_utc is not None else None
        row = self._recurring.get(job_name)
        if row is None:
            return False
        self._recurring[job_name] = RecurringJobState(
            job_name=row.job_name,
            cron_expression=row.cron_expression,
            time_zone=row.time_zone,
            enabled=enabled,
            allow_concurrent_runs=row.allow_concurrent_runs,
            max_attempts=row.max_attempts,
            next_run_at_utc=next_run or row.next_run_at_utc,
        )
        return True

    async def update_recurring_job_schedule(
        self,
        job_name: str,
        cron_expression: str,
        time_zone: str,
        next_run_at_utc: datetime,
    ) -> bool:
        next_run = ensure_utc(next_run_at_utc)
        row = self._recurring.get(job_name)
        if row is None:
            return False
        self._recurring[job_name] = RecurringJobState(
            job_name=row.job_name,
            cron_expression=cron_expression,
            time_zone=time_zone,
            enabled=row.enabled,
            allow_concurrent_runs=row.allow_concurrent_runs,
            max_attempts=row.max_attempts,
            next_run_at_utc=next_run,
        )
        return True

    async def try_enqueue_if_no_active_run(
        self,
        job_name: str,
        payload_json: str | None,
        due_at_utc: datetime,
        max_attempts: int,
    ) -> str | None:
        due_at = ensure_utc(due_at_utc)
        has_active = any(
            run.job_name == job_name and _is_active(run.status)
            for run in self._runs.values()
        )
        if has_active:
            return None
        result = await self.schedule(job_name, payload_json, due_at, max_attempts)
        return result.run_id

    async def prune_historical_runs(self, completed_before_utc: datetime, batch_size: int) -> int:
        completed_before = ensure_utc(completed_before_utc)
        terminal = [
            run
            for run in self._runs.values()
            if run.status in {RUN_STATUS_SUCCEEDED, RUN_STATUS_FAILED}
            and run.completed_at_utc is not None
            and run.completed_at_utc < completed_before
        ]
        terminal = sorted(terminal, key=lambda x: x.completed_at_utc or x.due_at_utc)
        deleted = 0
        for run in terminal:
            if deleted >= max(1, batch_size):
                break
            del self._runs[run.run_id]
            deleted += 1
        return deleted

    async def close(self) -> None:
        return

    async def try_lease_runtime_command_receipt(
        self,
        command_id: str,
        worker_name: str,
        lease_duration: timedelta,
        recorded_at_utc: datetime,
    ) -> bool:
        now = ensure_utc(recorded_at_utc)
        existing = self._runtime_receipts.get(command_id)
        if existing is None:
            self._runtime_receipts[command_id] = RuntimeCommandReceipt(
                command_id=command_id,
                status="leased",
                recorded_at_utc=now,
                completed_at_utc=None,
                run_id=None,
                error_code=None,
                error_message=None,
                uploaded_at_utc=None,
                lease_owner=worker_name,
                lease_until_utc=now + lease_duration,
            )
            return True

        if existing.status in {"succeeded", "failed"}:
            return False

        lease_expired = existing.lease_until_utc is None or existing.lease_until_utc <= now
        if lease_expired or existing.lease_owner == worker_name:
            self._runtime_receipts[command_id] = replace(
                existing,
                status="leased",
                recorded_at_utc=now,
                lease_owner=worker_name,
                lease_until_utc=now + lease_duration,
            )
            return True

        return False

    async def mark_runtime_command_acknowledged(
        self,
        command_id: str,
        worker_name: str,
        recorded_at_utc: datetime,
    ) -> bool:
        existing = self._runtime_receipts.get(command_id)
        if existing is None or existing.lease_owner != worker_name:
            return False
        self._runtime_receipts[command_id] = replace(
            existing,
            status="acknowledged",
            recorded_at_utc=ensure_utc(recorded_at_utc),
        )
        return True

    async def mark_runtime_command_succeeded(
        self,
        command_id: str,
        worker_name: str,
        recorded_at_utc: datetime,
        completed_at_utc: datetime,
        run_id: str | None,
    ) -> bool:
        existing = self._runtime_receipts.get(command_id)
        if existing is None or existing.lease_owner != worker_name:
            return False
        self._runtime_receipts[command_id] = replace(
            existing,
            status="succeeded",
            recorded_at_utc=ensure_utc(recorded_at_utc),
            completed_at_utc=ensure_utc(completed_at_utc),
            run_id=run_id,
            lease_owner=None,
            lease_until_utc=None,
        )
        return True

    async def mark_runtime_command_failed(
        self,
        command_id: str,
        worker_name: str,
        recorded_at_utc: datetime,
        completed_at_utc: datetime,
        error_code: str | None,
        error_message: str | None,
    ) -> bool:
        existing = self._runtime_receipts.get(command_id)
        if existing is None or existing.lease_owner != worker_name:
            return False
        self._runtime_receipts[command_id] = replace(
            existing,
            status="failed",
            recorded_at_utc=ensure_utc(recorded_at_utc),
            completed_at_utc=ensure_utc(completed_at_utc),
            error_code=error_code,
            error_message=error_message,
            lease_owner=None,
            lease_until_utc=None,
        )
        return True

    async def get_runtime_command_receipts(self, take: int) -> list[RuntimeCommandReceipt]:
        rows = [
            receipt
            for receipt in self._runtime_receipts.values()
            if receipt.uploaded_at_utc is None and receipt.status in {"acknowledged", "succeeded", "failed"}
        ]
        rows = sorted(rows, key=lambda x: x.recorded_at_utc)
        return rows[: max(1, int(take))]

    async def mark_runtime_command_receipt_uploaded(
        self,
        command_id: str,
        uploaded_at_utc: datetime,
    ) -> bool:
        existing = self._runtime_receipts.get(command_id)
        if existing is None:
            return False
        self._runtime_receipts[command_id] = replace(existing, uploaded_at_utc=ensure_utc(uploaded_at_utc))
        return True
