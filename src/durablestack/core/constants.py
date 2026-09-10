"""Canonical constants for cross-runtime parity contracts."""

from __future__ import annotations

from typing import Final

RUN_STATUS_PENDING: Final[str] = "pending"
RUN_STATUS_LEASED: Final[str] = "leased"
RUN_STATUS_SUCCEEDED: Final[str] = "succeeded"
RUN_STATUS_FAILED: Final[str] = "failed"

RUN_STATUSES: Final[tuple[str, str, str, str]] = (
    RUN_STATUS_PENDING,
    RUN_STATUS_LEASED,
    RUN_STATUS_SUCCEEDED,
    RUN_STATUS_FAILED,
)

EVENT_TYPE_JOB_CLAIMED: Final[str] = "job_claimed"
EVENT_TYPE_JOB_STARTED: Final[str] = "job_started"
EVENT_TYPE_JOB_SUCCEEDED: Final[str] = "job_succeeded"
EVENT_TYPE_JOB_FAILED: Final[str] = "job_failed"
EVENT_TYPE_JOB_RETRIED: Final[str] = "job_retried"
EVENT_TYPE_RETRY_SCHEDULED: Final[str] = "retry_scheduled"
EVENT_TYPE_WORKER_HEARTBEAT: Final[str] = "worker_heartbeat"

EVENT_TYPES: Final[tuple[str, ...]] = (
    EVENT_TYPE_JOB_CLAIMED,
    EVENT_TYPE_JOB_STARTED,
    EVENT_TYPE_JOB_SUCCEEDED,
    EVENT_TYPE_JOB_FAILED,
    EVENT_TYPE_JOB_RETRIED,
    EVENT_TYPE_RETRY_SCHEDULED,
    EVENT_TYPE_WORKER_HEARTBEAT,
)

EVENT_VERSION: Final[int] = 2

RUNTIME_CONTROL_COMMAND_SET_SCHEDULE_ENABLED: Final[str] = "set_schedule_enabled"
RUNTIME_CONTROL_COMMAND_RUN_SCHEDULE_NOW: Final[str] = "run_schedule_now"
RUNTIME_CONTROL_COMMAND_UPDATE_SCHEDULE_CRON: Final[str] = "update_schedule_cron"

RUNTIME_CONTROL_COMMAND_TYPES: Final[tuple[str, str, str]] = (
    RUNTIME_CONTROL_COMMAND_SET_SCHEDULE_ENABLED,
    RUNTIME_CONTROL_COMMAND_RUN_SCHEDULE_NOW,
    RUNTIME_CONTROL_COMMAND_UPDATE_SCHEDULE_CRON,
)

RUNTIME_CONTROL_RECEIPT_LEASED: Final[str] = "leased"
RUNTIME_CONTROL_RECEIPT_ACKNOWLEDGED: Final[str] = "acknowledged"
RUNTIME_CONTROL_RECEIPT_SUCCEEDED: Final[str] = "succeeded"
RUNTIME_CONTROL_RECEIPT_FAILED: Final[str] = "failed"

RUNTIME_CONTROL_RECEIPT_STATUSES: Final[tuple[str, str, str, str]] = (
    RUNTIME_CONTROL_RECEIPT_LEASED,
    RUNTIME_CONTROL_RECEIPT_ACKNOWLEDGED,
    RUNTIME_CONTROL_RECEIPT_SUCCEEDED,
    RUNTIME_CONTROL_RECEIPT_FAILED,
)
