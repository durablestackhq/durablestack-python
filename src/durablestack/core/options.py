"""Runtime options models and normalization for DurableStack Python."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta

from .models import RetryBehavior


@dataclass(frozen=True, slots=True)
class RetryOptions:
    """Retry behavior options for failed runs."""

    max_attempts: int = 3
    behavior: RetryBehavior = "fixed"
    delay: timedelta = timedelta(seconds=30)
    max_delay: timedelta = timedelta(minutes=15)
    jitter_enabled: bool = True
    jitter_ratio: float = 0.2


@dataclass(frozen=True, slots=True)
class RecurringOptions:
    """Recurring scheduling options."""

    catch_up_policy: str = "skip_missed"


@dataclass(frozen=True, slots=True)
class DurableStackOptions:
    """Top-level runtime options."""

    worker_name: str = "durablestack-python"
    poll_interval: timedelta = timedelta(seconds=1)
    poll_jitter_enabled: bool = True
    poll_jitter_ratio: float = 0.2
    lease_duration: timedelta = timedelta(seconds=30)
    max_concurrent_runs: int = 4
    claim_batch_size: int = 8
    shutdown_drain_timeout: timedelta = timedelta(seconds=30)
    retention_enabled: bool = True
    retention_max_age: timedelta = timedelta(days=30)
    retention_sweep_interval: timedelta = timedelta(minutes=5)
    retention_delete_batch_size: int = 100
    retry: RetryOptions = RetryOptions()
    recurring: RecurringOptions = RecurringOptions()
    include_error_detail_in_events: bool = False


def normalize_options(options: DurableStackOptions | None) -> DurableStackOptions:
    """Normalize and validate runtime options with parity-safe defaults."""

    value = options or DurableStackOptions()

    if value.max_concurrent_runs <= 0:
        raise ValueError("max_concurrent_runs must be greater than 0")
    if value.claim_batch_size <= 0:
        raise ValueError("claim_batch_size must be greater than 0")
    if value.retry.max_attempts <= 0:
        raise ValueError("retry.max_attempts must be greater than 0")
    if value.poll_interval <= timedelta(0):
        raise ValueError("poll_interval must be greater than 0")
    if value.poll_jitter_ratio < 0:
        raise ValueError("poll_jitter_ratio cannot be negative")
    if value.lease_duration <= timedelta(0):
        raise ValueError("lease_duration must be greater than 0")
    if value.shutdown_drain_timeout < timedelta(0):
        raise ValueError("shutdown_drain_timeout cannot be negative")
    if value.retention_max_age <= timedelta(0):
        raise ValueError("retention_max_age must be greater than 0")
    if value.retention_sweep_interval <= timedelta(0):
        raise ValueError("retention_sweep_interval must be greater than 0")
    if value.retention_delete_batch_size <= 0:
        raise ValueError("retention_delete_batch_size must be greater than 0")
    if value.retry.delay < timedelta(0):
        raise ValueError("retry.delay cannot be negative")
    if value.retry.max_delay <= timedelta(0):
        raise ValueError("retry.max_delay must be greater than 0")
    if value.retry.jitter_ratio < 0:
        raise ValueError("retry.jitter_ratio cannot be negative")
    if value.retry.behavior not in {"fixed", "exponential"}:
        raise ValueError("retry.behavior must be 'fixed' or 'exponential'")
    if value.recurring.catch_up_policy not in {"skip_missed", "catch_up"}:
        raise ValueError("recurring.catch_up_policy must be 'skip_missed' or 'catch_up'")

    return replace(value)
