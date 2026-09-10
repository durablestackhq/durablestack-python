"""Core processor loop for claim/execute/retry/materialization behavior."""

from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from .abstractions import DurableJobStore, DurableStackEventSink
from .constants import (
    EVENT_TYPE_JOB_CLAIMED,
    EVENT_TYPE_JOB_FAILED,
    EVENT_TYPE_JOB_RETRIED,
    EVENT_TYPE_JOB_STARTED,
    EVENT_TYPE_JOB_SUCCEEDED,
    EVENT_TYPE_RETRY_SCHEDULED,
    EVENT_VERSION,
)
from .cron import get_next_occurrence_utc
from .models import JobContext, JobRegistration, RecurringRegistration
from .options import DurableStackOptions
from .registry import InMemoryDurableJobRegistry
from .utils import (
    deserialize_payload,
    generate_id,
    to_seconds,
    utc_now,
    with_jitter,
)

_logger = logging.getLogger(__name__)


def _sanitize_error_message(error: Exception, include_error_detail: bool) -> str | None:
    if include_error_detail:
        return str(error)
    return None


@dataclass(slots=True)
class DurableStackProcessor:
    """Executes one polling cycle and manages in-flight job tasks."""

    store: DurableJobStore
    registry: InMemoryDurableJobRegistry
    options: DurableStackOptions
    sinks: list[DurableStackEventSink]
    _in_flight: set[asyncio.Task[None]] = field(default_factory=set)
    _next_retention_sweep_at_utc: datetime = field(default_factory=utc_now)
    _handler_arity_cache: dict[int, int] = field(default_factory=dict)

    async def initialize_recurring_jobs(self) -> None:
        now = utc_now()
        existing = await self.store.get_recurring_jobs(include_disabled=True)
        existing_names = {row.job_name for row in existing}

        for registration in self.registry.get_all_recurring():
            if registration.name in existing_names:
                continue
            next_run = get_next_occurrence_utc(
                registration.cron_expression,
                registration.time_zone,
                now,
            )
            await self.store.upsert_recurring_job(registration, next_run)

    async def process_once(self, stop_event: asyncio.Event) -> int:
        if stop_event.is_set():
            return 0

        await self._prune_historical_runs_if_due()
        await self._materialize_due_recurring_runs()

        available = max(0, self.options.max_concurrent_runs - len(self._in_flight))
        if available <= 0:
            return 0

        now = utc_now()
        claim_count = max(1, min(available, self.options.claim_batch_size))
        claimed = await self.store.claim_due_runs(
            worker_name=self.options.worker_name,
            batch_size=claim_count,
            lease_duration=self.options.lease_duration,
            now_utc=now,
        )

        for run in claimed:
            await self._publish_event(
                {
                    "eventId": generate_id(),
                    "eventType": EVENT_TYPE_JOB_CLAIMED,
                    "eventVersion": EVENT_VERSION,
                    "occurredAtUtc": utc_now().isoformat(),
                    "runId": run.run_id,
                    "jobName": run.job_name,
                    "attempt": run.attempt,
                    "maxAttempts": run.max_attempts,
                    "workerName": self.options.worker_name,
                }
            )
            task = asyncio.create_task(self._execute_run_safely(run.run_id, stop_event))
            self._in_flight.add(task)
            task.add_done_callback(self._in_flight.discard)

        return len(claimed)

    async def drain_in_flight_runs(self, timeout: timedelta) -> None:
        if not self._in_flight:
            return

        done, pending = await asyncio.wait(
            self._in_flight,
            timeout=max(0.0, to_seconds(timeout)),
        )
        _ = done
        for task in pending:
            task.cancel()

    @property
    def in_flight_count(self) -> int:
        return len(self._in_flight)

    async def _execute_run_safely(self, run_id: str, stop_event: asyncio.Event) -> None:
        try:
            await self._execute_run(run_id, stop_event)
        except Exception:  # noqa: BLE001
            return

    async def _execute_run(self, run_id: str, stop_event: asyncio.Event) -> None:
        run = await self.store.get_run(run_id)
        if run is None:
            return

        registration = self.registry.get_job(run.job_name)
        recurring_registration = self.registry.get_recurring(run.job_name)
        selected: JobRegistration | RecurringRegistration | None = registration or recurring_registration
        started = utc_now()

        await self._publish_event(
            {
                "eventId": generate_id(),
                "eventType": EVENT_TYPE_JOB_STARTED,
                "eventVersion": EVENT_VERSION,
                "occurredAtUtc": started.isoformat(),
                "runId": run.run_id,
                "jobName": run.job_name,
                "attempt": run.attempt,
                "maxAttempts": run.max_attempts,
                "workerName": self.options.worker_name,
            }
        )

        heartbeat_task = asyncio.create_task(self._lease_heartbeat_loop(run.run_id, stop_event))
        lease_lost = False
        try:
            if selected is None:
                raise RuntimeError(f"No registered job named '{run.job_name}'.")

            payload = deserialize_payload(run.payload_json)
            context = JobContext(
                run_id=run.run_id,
                job_name=run.job_name,
                attempt=run.attempt,
                max_attempts=run.max_attempts,
                payload=payload,
            )
            arity = self._resolve_handler_arity(selected.handler)
            await _invoke_handler(selected.handler, arity, payload, context, stop_event)

            if heartbeat_task.done() and heartbeat_task.result() is False:
                lease_lost = True
                return

            marked = await self.store.mark_succeeded(run.run_id, self.options.worker_name, utc_now())
            if marked:
                await self._publish_event(
                    {
                        "eventId": generate_id(),
                        "eventType": EVENT_TYPE_JOB_SUCCEEDED,
                        "eventVersion": EVENT_VERSION,
                        "occurredAtUtc": utc_now().isoformat(),
                        "runId": run.run_id,
                        "jobName": run.job_name,
                        "attempt": run.attempt,
                        "maxAttempts": run.max_attempts,
                        "workerName": self.options.worker_name,
                        "durationMs": int((utc_now() - started).total_seconds() * 1000),
                    }
                )
        except Exception as exc:  # noqa: BLE001
            if stop_event.is_set() and not lease_lost:
                return

            should_retry = run.attempt < run.max_attempts
            retry_seconds = self._calculate_retry_seconds(run.attempt, selected)
            retry_at = utc_now() + timedelta(seconds=retry_seconds) if should_retry else None
            marked = await self.store.mark_failed(
                run_id=run.run_id,
                worker_name=self.options.worker_name,
                error_message=str(exc),
                completed_at_utc=utc_now(),
                should_retry=should_retry,
                retry_at_utc=retry_at,
            )
            if marked:
                await self._publish_event(
                    {
                        "eventId": generate_id(),
                        "eventType": EVENT_TYPE_JOB_FAILED,
                        "eventVersion": EVENT_VERSION,
                        "occurredAtUtc": utc_now().isoformat(),
                        "runId": run.run_id,
                        "jobName": run.job_name,
                        "attempt": run.attempt,
                        "maxAttempts": run.max_attempts,
                        "workerName": self.options.worker_name,
                        "errorType": type(exc).__name__,
                        "errorMessage": _sanitize_error_message(
                            exc,
                            self.options.include_error_detail_in_events,
                        ),
                        "durationMs": int((utc_now() - started).total_seconds() * 1000),
                        "retryAtUtc": retry_at.isoformat() if retry_at is not None else None,
                    }
                )
                if should_retry and retry_at is not None:
                    await self._publish_event(
                        {
                            "eventId": generate_id(),
                            "eventType": EVENT_TYPE_JOB_RETRIED,
                            "eventVersion": EVENT_VERSION,
                            "occurredAtUtc": utc_now().isoformat(),
                            "runId": run.run_id,
                            "jobName": run.job_name,
                            "attempt": run.attempt,
                            "maxAttempts": run.max_attempts,
                            "workerName": self.options.worker_name,
                            "retryAtUtc": retry_at.isoformat(),
                        }
                    )
                    await self._publish_event(
                        {
                            "eventId": generate_id(),
                            "eventType": EVENT_TYPE_RETRY_SCHEDULED,
                            "eventVersion": EVENT_VERSION,
                            "occurredAtUtc": utc_now().isoformat(),
                            "runId": run.run_id,
                            "jobName": run.job_name,
                            "attempt": run.attempt,
                            "maxAttempts": run.max_attempts,
                            "workerName": self.options.worker_name,
                            "retryAtUtc": retry_at.isoformat(),
                        }
                    )
        finally:
            heartbeat_task.cancel()

    async def _lease_heartbeat_loop(self, run_id: str, stop_event: asyncio.Event) -> bool:
        interval = max(0.25, to_seconds(self.options.lease_duration) / 2.0)
        while not stop_event.is_set():
            await asyncio.sleep(interval)
            try:
                extended = await self.store.extend_lease(
                    run_id=run_id,
                    worker_name=self.options.worker_name,
                    lease_duration=self.options.lease_duration,
                    now_utc=utc_now(),
                )
                if not extended:
                    return False
            except Exception as exc:  # noqa: BLE001
                _logger.warning("Lease heartbeat extend failed for run '%s': %s", run_id, str(exc))
                continue
        return True

    async def _materialize_due_recurring_runs(self) -> None:
        now = utc_now()
        due = await self.store.get_due_recurring_jobs(now_utc=now, batch_size=100)
        for recurring in due:
            registration = self.registry.get_recurring(recurring.job_name)
            if registration is None:
                continue
            next_run = get_next_occurrence_utc(
                recurring.cron_expression,
                recurring.time_zone,
                recurring.next_run_at_utc,
            )
            if self.options.recurring.catch_up_policy == "skip_missed":
                while next_run <= now:
                    next_run = get_next_occurrence_utc(
                        recurring.cron_expression,
                        recurring.time_zone,
                        next_run,
                    )
            await self.store.try_materialize_recurring_run(recurring, registration, next_run)

    async def _prune_historical_runs_if_due(self) -> None:
        if not self.options.retention_enabled:
            return
        now = utc_now()
        if now < self._next_retention_sweep_at_utc:
            return

        self._next_retention_sweep_at_utc = now + self.options.retention_sweep_interval
        await self.store.prune_historical_runs(
            completed_before_utc=now - self.options.retention_max_age,
            batch_size=self.options.retention_delete_batch_size,
        )

    def _calculate_retry_seconds(
        self,
        attempt: int,
        registration: JobRegistration | RecurringRegistration | None,
    ) -> float:
        behavior = registration.retry_behavior if registration is not None else self.options.retry.behavior
        initial_delay = (
            registration.retry_initial_delay_seconds
            if registration is not None
            else int(self.options.retry.delay.total_seconds())
        )
        base = max(1, initial_delay)
        if behavior == "fixed":
            retry_seconds = float(base)
        else:
            exp = float(base * (2 ** max(0, attempt - 1)))
            retry_seconds = min(exp, to_seconds(self.options.retry.max_delay))
        retry_seconds = max(0.01, retry_seconds)
        return with_jitter(
            retry_seconds,
            self.options.retry.jitter_enabled,
            self.options.retry.jitter_ratio,
        )

    async def _publish_event(self, event: dict[str, Any]) -> None:
        for sink in self.sinks:
            try:
                await sink.publish(event)
            except Exception as exc:  # noqa: BLE001
                _logger.warning("Event sink publish failed for event '%s': %s", event.get("eventType"), str(exc))
                continue

    def _resolve_handler_arity(self, handler: Any) -> int:
        cache_key = id(handler)
        cached = self._handler_arity_cache.get(cache_key)
        if cached is not None:
            return cached

        signature = inspect.signature(handler)
        positional = [
            p
            for p in signature.parameters.values()
            if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        has_varargs = any(
            p.kind == inspect.Parameter.VAR_POSITIONAL for p in signature.parameters.values()
        )

        max_positional = 999 if has_varargs else len(positional)
        if max_positional >= 3:
            arity = 3
        elif max_positional == 2:
            arity = 2
        elif max_positional == 1:
            arity = 1
        else:
            raise TypeError("Job handler must accept at least one positional argument (payload)")

        self._handler_arity_cache[cache_key] = arity
        return arity


async def _invoke_handler(
    handler: Any,
    arity: int,
    payload: Any,
    context: JobContext,
    stop_event: asyncio.Event,
) -> Any:
    if asyncio.iscoroutinefunction(handler):
        return await _invoke_async_handler(handler, arity, payload, context, stop_event)

    return await asyncio.to_thread(
        _invoke_sync_handler,
        handler,
        arity,
        payload,
        context,
        stop_event,
    )


async def _invoke_async_handler(
    handler: Any,
    arity: int,
    payload: Any,
    context: JobContext,
    stop_event: asyncio.Event,
) -> Any:
    if arity >= 3:
        return await handler(payload, context, stop_event)
    if arity == 2:
        return await handler(payload, context)
    return await handler(payload)


def _invoke_sync_handler(
    handler: Any,
    arity: int,
    payload: Any,
    context: JobContext,
    stop_event: asyncio.Event,
) -> Any:
    if arity >= 3:
        return handler(payload, context, stop_event)
    if arity == 2:
        return handler(payload, context)
    return handler(payload)
