"""External contract validators (Phase 0 baseline)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from durablestack.core.constants import (
    EVENT_TYPES,
    EVENT_VERSION,
    RUNTIME_CONTROL_COMMAND_TYPES,
    RUNTIME_CONTROL_RECEIPT_STATUSES,
)


def validate_event_payload(payload: dict[str, Any]) -> None:
    """Validate eventing payload envelope shape."""

    required = {
        "eventType",
        "eventVersion",
        "occurredAtUtc",
        "runId",
        "jobName",
        "attempt",
        "maxAttempts",
        "workerName",
        "runtime",
        "runtimeVersion",
    }
    _ensure_required_fields(payload, required)

    event_type = _require_str(payload, "eventType")
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unsupported eventType: {event_type}")

    version = payload["eventVersion"]
    if not isinstance(version, int) or version != EVENT_VERSION:
        raise ValueError(f"eventVersion must be {EVENT_VERSION}")

    _require_iso_datetime(payload, "occurredAtUtc")
    _require_str(payload, "runId")
    _require_str(payload, "jobName")
    _require_int(payload, "attempt", minimum=1)
    _require_int(payload, "maxAttempts", minimum=1)
    _require_str(payload, "workerName")
    _require_str(payload, "runtime")
    _require_str(payload, "runtimeVersion")


def validate_runtime_control_command(payload: dict[str, Any]) -> None:
    """Validate runtime-control command envelope baseline fields."""

    required = {"commandId", "commandType", "issuedAtUtc"}
    _ensure_required_fields(payload, required)

    _require_str(payload, "commandId")
    command_type = _require_str(payload, "commandType")
    if command_type not in RUNTIME_CONTROL_COMMAND_TYPES:
        raise ValueError(f"unsupported commandType: {command_type}")

    _require_iso_datetime(payload, "issuedAtUtc")


def validate_runtime_control_receipt(payload: dict[str, Any]) -> None:
    """Validate runtime-control command receipt baseline fields."""

    required = {"commandId", "status", "updatedAtUtc"}
    _ensure_required_fields(payload, required)

    _require_str(payload, "commandId")
    status = _require_str(payload, "status")
    if status not in RUNTIME_CONTROL_RECEIPT_STATUSES:
        raise ValueError(f"unsupported receipt status: {status}")

    _require_iso_datetime(payload, "updatedAtUtc")


def _ensure_required_fields(payload: dict[str, Any], required: set[str]) -> None:
    missing = [name for name in sorted(required) if name not in payload]
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")


def _require_str(payload: dict[str, Any], field_name: str) -> str:
    value = payload[field_name]
    if not isinstance(value, str) or value == "":
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _require_int(payload: dict[str, Any], field_name: str, minimum: int) -> int:
    value = payload[field_name]
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    if value < minimum:
        raise ValueError(f"{field_name} must be >= {minimum}")
    return value


def _require_iso_datetime(payload: dict[str, Any], field_name: str) -> datetime:
    value = payload[field_name]
    if not isinstance(value, str) or value == "":
        raise ValueError(f"{field_name} must be a non-empty ISO-8601 string")
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO-8601 datetime") from exc
