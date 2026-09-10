"""Runtime factory and in-memory runtime host implementation."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, cast

from durablestack.core.abstractions import DurableStackEventSink, DurableStackRuntime, JobHandler
from durablestack.core.constants import EVENT_TYPE_WORKER_HEARTBEAT, EVENT_VERSION, RUN_STATUSES
from durablestack.core.cron import get_next_occurrence_utc, validate_time_zone
from durablestack.core.event_sink import NoOpDurableStackEventSink
from durablestack.core.models import (
    JobRegistration,
    JobRun,
    RecurringJobState,
    RecurringRegistration,
    RetryBehavior,
)
from durablestack.core.options import DurableStackOptions, normalize_options
from durablestack.core.processor import DurableStackProcessor
from durablestack.core.registry import InMemoryDurableJobRegistry
from durablestack.core.utils import generate_id, serialize_payload, to_seconds, utc_now, with_jitter
from durablestack.providers.inmemory import InMemoryDurableJobStore

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RegisterJobOptions:
    max_attempts: int | None = None
    retry_behavior: str | None = None
    retry_initial_delay_seconds: int | None = None


@dataclass(frozen=True, slots=True)
class RegisterRecurringOptions:
    max_attempts: int | None = None
    enabled: bool = True
    allow_concurrent_runs: bool = False
    retry_behavior: str | None = None
    retry_initial_delay_seconds: int | None = None


@dataclass(slots=True)
class DurableStackRuntimeImpl:
    options: DurableStackOptions
    sinks: list[DurableStackEventSink] = field(default_factory=list)
    store: InMemoryDurableJobStore = field(default_factory=InMemoryDurableJobStore)
    _registry: InMemoryDurableJobRegistry = field(default_factory=InMemoryDurableJobRegistry)
    _running: bool = False
    _loop_task: asyncio.Task[None] | None = None
    _stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    _processor: DurableStackProcessor | None = None

    def __post_init__(self) -> None:
        configured_sinks: list[DurableStackEventSink]
        if self.sinks:
            configured_sinks = self.sinks
        else:
            configured_sinks = [NoOpDurableStackEventSink()]
        self.sinks = configured_sinks
        self._processor = DurableStackProcessor(self.store, self._registry, self.options, self.sinks)

    async def start(self) -> None:
        if self._running:
            return
        if self._processor is None:
            raise RuntimeError("processor not configured")

        self._running = True
        self._stop_event = asyncio.Event()
        await self._processor.initialize_recurring_jobs()
        self._loop_task = asyncio.create_task(self._run_loop())

    async def stop(self, *, drain_timeout: timedelta | None = None) -> None:
        if not self._running:
            return
        if self._processor is None:
            raise RuntimeError("processor not configured")

        self._running = False
        if self._loop_task is not None:
            await self._loop_task

        await self._processor.drain_in_flight_runs(drain_timeout or self.options.shutdown_drain_timeout)
        self._stop_event.set()

    def register_job(self, name: str, handler: JobHandler, options: RegisterJobOptions | None = None) -> None:
        resolved_options = options or RegisterJobOptions()
        retry_behavior = _coerce_retry_behavior(
            resolved_options.retry_behavior or self.options.retry.behavior
        )
        registration = JobRegistration(
            name=name,
            handler=handler,
            max_attempts=max(1, int(resolved_options.max_attempts or self.options.retry.max_attempts)),
            retry_behavior=retry_behavior,
            retry_initial_delay_seconds=max(
                1,
                int(resolved_options.retry_initial_delay_seconds or self.options.retry.delay.total_seconds()),
            ),
        )
        self._registry.register_job(registration)

    def register_recurring(
        self,
        name: str,
        cron_expression: str,
        time_zone: str,
        handler: JobHandler,
        options: RegisterRecurringOptions | None = None,
    ) -> None:
        validate_time_zone(time_zone)
        resolved_options = options or RegisterRecurringOptions()
        retry_behavior = _coerce_retry_behavior(
            resolved_options.retry_behavior or self.options.retry.behavior
        )
        registration = RecurringRegistration(
            name=name,
            handler=handler,
            cron_expression=cron_expression,
            time_zone=time_zone,
            enabled=resolved_options.enabled,
            allow_concurrent_runs=resolved_options.allow_concurrent_runs,
            max_attempts=max(1, int(resolved_options.max_attempts or self.options.retry.max_attempts)),
            retry_behavior=retry_behavior,
            retry_initial_delay_seconds=max(
                1,
                int(resolved_options.retry_initial_delay_seconds or self.options.retry.delay.total_seconds()),
            ),
        )
        self._registry.register_recurring(registration)

    async def enqueue(self, job_name: str, payload: Any = None) -> str:
        registration = self._registry.get_job(job_name) or self._registry.get_recurring(job_name)
        if registration is None:
            raise ValueError(f"No registered job named '{job_name}'.")
        result = await self.store.enqueue(
            job_name=job_name,
            payload_json=serialize_payload(payload),
            max_attempts=registration.max_attempts,
        )
        return result.run_id

    async def schedule(self, job_name: str, payload: Any, run_at_utc: datetime) -> str:
        registration = self._registry.get_job(job_name) or self._registry.get_recurring(job_name)
        if registration is None:
            raise ValueError(f"No registered job named '{job_name}'.")

        result = await self.store.schedule(
            job_name=job_name,
            payload_json=serialize_payload(payload),
            run_at_utc=run_at_utc,
            max_attempts=registration.max_attempts,
        )
        return result.run_id

    async def cancel_run(self, run_id: str) -> bool:
        return await self.store.cancel_run(run_id, "Run cancelled", utc_now())

    async def get_run(self, run_id: str) -> JobRun | None:
        return await self.store.get_run(run_id)

    async def get_recent_runs(self, take: int = 100) -> list[JobRun]:
        return await self.store.get_recent_runs(max(1, take))

    async def get_runs_by_status(self, status: str, take: int = 100) -> list[JobRun]:
        normalized = status.lower()
        if normalized not in RUN_STATUSES:
            raise ValueError(f"Invalid status '{status}'.")
        return await self.store.get_runs_by_status(normalized, max(1, take))

    async def get_runs_by_job_name(self, job_name: str, take: int = 100) -> list[JobRun]:
        return await self.store.get_runs_by_job_name(job_name, max(1, take))

    async def get_enqueued_runs(self, take: int = 100) -> list[JobRun]:
        return await self.store.get_enqueued_runs(max(1, take))

    async def list_scheduled_jobs(self, include_disabled: bool = True) -> list[RecurringJobState]:
        return await self.store.get_recurring_jobs(include_disabled)

    async def set_scheduled_job_enabled(self, job_name: str, enabled: bool) -> bool:
        next_run = None
        if enabled:
            recurring = self._registry.get_recurring(job_name)
            if recurring is None:
                return False
            next_run = get_next_occurrence_utc(recurring.cron_expression, recurring.time_zone, utc_now())
        return await self.store.set_recurring_job_enabled(job_name, enabled, next_run)

    async def update_scheduled_job_cron(
        self,
        job_name: str,
        cron_expression: str,
        time_zone: str,
    ) -> bool:
        validate_time_zone(time_zone)
        next_run = get_next_occurrence_utc(cron_expression, time_zone, utc_now())
        return await self.store.update_recurring_job_schedule(
            job_name,
            cron_expression,
            time_zone,
            next_run,
        )

    async def run_scheduled_job_now(self, job_name: str) -> str | None:
        recurring = self._registry.get_recurring(job_name)
        if recurring is None:
            return None
        return await self.store.try_enqueue_if_no_active_run(
            job_name,
            None,
            utc_now(),
            recurring.max_attempts,
        )

    async def _run_loop(self) -> None:
        if self._processor is None:
            return

        delay_seconds = with_jitter(
            to_seconds(self.options.poll_interval),
            self.options.poll_jitter_enabled,
            self.options.poll_jitter_ratio,
        )
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)

        while self._running:
            await self._processor.process_once(self._stop_event)
            await self._emit_heartbeat()
            delay_seconds = with_jitter(
                to_seconds(self.options.poll_interval),
                self.options.poll_jitter_enabled,
                self.options.poll_jitter_ratio,
            )
            await asyncio.sleep(max(0.0, delay_seconds))

    async def _emit_heartbeat(self) -> None:
        event = {
            "eventId": f"hb-{generate_id()}",
            "eventType": EVENT_TYPE_WORKER_HEARTBEAT,
            "eventVersion": EVENT_VERSION,
            "occurredAtUtc": utc_now().isoformat(),
            "workerName": self.options.worker_name,
        }
        for sink in self.sinks:
            try:
                await sink.publish(event)
            except Exception as exc:  # noqa: BLE001
                _logger.warning("Heartbeat publish failed: %s", str(exc))
                continue


def create_durable_stack(
    options: DurableStackOptions | None = None,
    sinks: list[DurableStackEventSink] | None = None,
) -> DurableStackRuntime:
    """Create a DurableStack runtime instance."""

    return DurableStackRuntimeImpl(options=normalize_options(options), sinks=sinks or [])


def _coerce_retry_behavior(value: str) -> RetryBehavior:
    if value not in {"fixed", "exponential"}:
        raise ValueError("retry_behavior must be 'fixed' or 'exponential'")
    return cast(RetryBehavior, value)
