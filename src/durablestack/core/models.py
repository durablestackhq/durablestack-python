"""Core data models for DurableStack Python runtime contracts."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, TypeAlias

RetryBehavior: TypeAlias = Literal["fixed", "exponential"]

JsonPrimitive: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonPrimitive | dict[str, "JsonValue"] | list["JsonValue"]
HandlerCallable: TypeAlias = Callable[..., object]


@dataclass(frozen=True, slots=True)
class JobRun:
    """Represents one durable job run record."""

    run_id: str
    job_name: str
    status: str
    created_by: str | None
    payload_json: str | None
    attempt: int
    max_attempts: int
    created_at_utc: datetime
    due_at_utc: datetime
    leased_by: str | None
    lease_expires_at_utc: datetime | None
    started_at_utc: datetime | None
    completed_at_utc: datetime | None
    last_error: str | None
    schedule_slot_utc: datetime | None


@dataclass(frozen=True, slots=True)
class ScheduledJobDefinition:
    """Recurring schedule definition."""

    job_name: str
    cron_expression: str
    time_zone: str
    enabled: bool
    next_run_at_utc: datetime | None
    allow_concurrent_runs: bool = False


@dataclass(frozen=True, slots=True)
class EnqueueResult:
    """Result returned by enqueue/schedule operations."""

    run_id: str


@dataclass(frozen=True, slots=True)
class JobContext:
    """Execution context passed to a job handler."""

    run_id: str
    job_name: str
    attempt: int
    max_attempts: int
    payload: JsonValue


@dataclass(frozen=True, slots=True)
class RecurringJobState:
    """Persisted recurring schedule state used for materialization."""

    job_name: str
    cron_expression: str
    time_zone: str
    enabled: bool
    allow_concurrent_runs: bool
    max_attempts: int
    next_run_at_utc: datetime


@dataclass(frozen=True, slots=True)
class JobRegistration:
    """Registered one-off job metadata and handler."""

    name: str
    handler: HandlerCallable
    max_attempts: int
    retry_behavior: RetryBehavior
    retry_initial_delay_seconds: int


@dataclass(frozen=True, slots=True)
class RecurringRegistration:
    """Registered recurring metadata and handler."""

    name: str
    handler: HandlerCallable
    cron_expression: str
    time_zone: str
    enabled: bool
    allow_concurrent_runs: bool
    max_attempts: int
    retry_behavior: RetryBehavior
    retry_initial_delay_seconds: int
