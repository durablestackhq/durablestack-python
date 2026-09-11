"""External contract validators (Phase 0 baseline)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

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


def validate_telemetry_event_dto(payload: dict[str, Any]) -> None:
    """Validate one telemetry event DTO for ingestion."""

    required = {
        "eventType",
        "eventVersion",
        "occurredAtUtc",
        "workerName",
        "runtime",
        "runtimeVersion",
    }
    _ensure_required_fields(payload, required)

    event_type = _require_str(payload, "eventType")
    if event_type != "worker_heartbeat_batch":
        run_required = {"runId", "jobName", "attempt", "maxAttempts"}
        _ensure_required_fields(payload, run_required)
        _require_str(payload, "runId")
        _require_str(payload, "jobName")
        _require_int(payload, "attempt", minimum=1)
        _require_int(payload, "maxAttempts", minimum=1)

    version = payload["eventVersion"]
    if not isinstance(version, int) or version != EVENT_VERSION:
        raise ValueError(f"eventVersion must be {EVENT_VERSION}")
    _require_iso_datetime(payload, "occurredAtUtc")
    _require_str(payload, "workerName")
    _require_str(payload, "runtime")
    _require_str(payload, "runtimeVersion")

    if "durationMs" in payload:
        _require_int(payload, "durationMs", minimum=0)
    if "errorType" in payload:
        _require_str(payload, "errorType")
    if "errorMessage" in payload and payload["errorMessage"] is not None:
        _require_str(payload, "errorMessage")
    if "payloadJson" in payload and payload["payloadJson"] is not None:
        _require_str(payload, "payloadJson")


def validate_telemetry_batch_request(payload: dict[str, Any]) -> None:
    """Validate telemetry ingestion batch request DTO."""

    required = {"tenantId", "idempotencyKey", "events"}
    _ensure_required_fields(payload, required)
    _require_str(payload, "tenantId")
    _require_str(payload, "idempotencyKey")
    events = payload["events"]
    if not isinstance(events, list) or len(events) == 0:
        raise ValueError("events must be a non-empty array")

    for event in events:
        if not isinstance(event, dict):
            raise TypeError("events items must be objects")
        validate_telemetry_event_dto(event)

    if "serviceName" in payload and payload["serviceName"] is not None:
        _require_str(payload, "serviceName")


def validate_runtime_control_sync_request(payload: dict[str, Any]) -> None:
    """Validate runtime-control sync request DTO."""

    required = {
        "tenantId",
        "workerName",
        "runtime",
        "runtimeVersion",
        "sentAtUtc",
        "snapshotItems",
        "receipts",
    }
    _ensure_required_fields(payload, required)

    _require_str(payload, "tenantId")
    _require_str(payload, "workerName")
    _require_str(payload, "runtime")
    _require_str(payload, "runtimeVersion")
    _require_iso_datetime(payload, "sentAtUtc")

    snapshot_items = payload["snapshotItems"]
    receipts = payload["receipts"]
    if not isinstance(snapshot_items, list):
        raise TypeError("snapshotItems must be an array")
    if not isinstance(receipts, list):
        raise TypeError("receipts must be an array")

    for item in snapshot_items:
        if not isinstance(item, dict):
            raise TypeError("snapshotItems items must be objects")
        validate_runtime_job_snapshot_item(item)

    for item in receipts:
        if not isinstance(item, dict):
            raise TypeError("receipts items must be objects")
        validate_runtime_control_receipt_dto(item)


def validate_runtime_job_snapshot_item(payload: dict[str, Any]) -> None:
    required = {
        "jobName",
        "cronExpression",
        "timeZone",
        "enabled",
        "nextRunAtUtc",
        "maxAttempts",
        "allowConcurrentRuns",
        "lastSeenAtUtc",
    }
    _ensure_required_fields(payload, required)
    _require_str(payload, "jobName")
    _require_str(payload, "cronExpression")
    _require_str(payload, "timeZone")
    _require_bool(payload, "enabled")
    _require_iso_datetime(payload, "nextRunAtUtc")
    _require_int(payload, "maxAttempts", minimum=1)
    _require_bool(payload, "allowConcurrentRuns")
    _require_iso_datetime(payload, "lastSeenAtUtc")


def validate_runtime_control_receipt_dto(payload: dict[str, Any]) -> None:
    required = {"commandId", "status", "recordedAtUtc"}
    _ensure_required_fields(payload, required)
    _require_str(payload, "commandId")
    status = _require_str(payload, "status")
    if status not in RUNTIME_CONTROL_RECEIPT_STATUSES:
        raise ValueError(f"unsupported receipt status: {status}")
    _require_iso_datetime(payload, "recordedAtUtc")

    if "completedAtUtc" in payload and payload["completedAtUtc"] is not None:
        _require_iso_datetime(payload, "completedAtUtc")
    if "runId" in payload and payload["runId"] is not None:
        _require_str(payload, "runId")
    if "errorCode" in payload and payload["errorCode"] is not None:
        _require_str(payload, "errorCode")
    if "errorMessage" in payload and payload["errorMessage"] is not None:
        _require_str(payload, "errorMessage")


def validate_runtime_control_sync_response(payload: dict[str, Any]) -> None:
    """Validate runtime-control sync response DTO (commands or Commands)."""

    commands = payload.get("commands")
    if commands is None:
        commands = payload.get("Commands")

    if commands is None:
        return
    if not isinstance(commands, list):
        raise TypeError("commands must be an array")

    for item in commands:
        if not isinstance(item, dict):
            raise TypeError("command envelopes must be objects")
        validate_runtime_command_envelope(item)


def validate_runtime_command_envelope(payload: dict[str, Any]) -> None:
    command_id = _require_any_string(payload, "commandId", "CommandId")
    command_type = _require_any_string(payload, "commandType", "CommandType")
    issued_at = _require_any_string(payload, "issuedAtUtc", "IssuedAtUtc")
    _ = command_id
    if command_type not in RUNTIME_CONTROL_COMMAND_TYPES:
        raise ValueError(f"unsupported commandType: {command_type}")
    _validate_iso_datetime_text(issued_at, "issuedAtUtc")

    payload_json = _optional_any_string(payload, "payloadJson", "PayloadJson")
    if payload_json is None:
        raise ValueError("payloadJson is required")

    expires_at = _optional_any_string(payload, "expiresAtUtc", "ExpiresAtUtc")
    if expires_at is not None:
        _validate_iso_datetime_text(expires_at, "expiresAtUtc")


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


def _require_bool(payload: dict[str, Any], field_name: str) -> bool:
    value = payload[field_name]
    if not isinstance(value, bool):
        raise TypeError(f"{field_name} must be a boolean")
    return value


def _require_any_string(payload: dict[str, Any], first: str, second: str) -> str:
    if first in payload and isinstance(payload[first], str) and payload[first] != "":
        return cast(str, payload[first])
    if second in payload and isinstance(payload[second], str) and payload[second] != "":
        return cast(str, payload[second])
    raise ValueError(f"{first}/{second} must be a non-empty string")


def _optional_any_string(payload: dict[str, Any], first: str, second: str) -> str | None:
    if first in payload:
        value = payload[first]
        if value is None:
            return None
        if isinstance(value, str) and value != "":
            return value
        raise ValueError(f"{first} must be a non-empty string when provided")
    if second in payload:
        value = payload[second]
        if value is None:
            return None
        if isinstance(value, str) and value != "":
            return value
        raise ValueError(f"{second} must be a non-empty string when provided")
    return None


def _validate_iso_datetime_text(value: str, field_name: str) -> None:
    try:
        _ = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO-8601 datetime") from exc
