import json
from pathlib import Path
from typing import Any, cast

import pytest

from durablestack.validators.contracts import (
    validate_event_payload,
    validate_runtime_command_envelope,
    validate_runtime_control_command,
    validate_runtime_control_receipt,
    validate_runtime_control_sync_request,
    validate_runtime_control_sync_response,
    validate_telemetry_batch_request,
    validate_telemetry_event_dto,
)

_FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads((_FIXTURES_DIR / name).read_text(encoding="utf-8")))


def test_validate_event_payload_accepts_minimal_valid_shape() -> None:
    payload = {
        "eventType": "job_started",
        "eventVersion": 2,
        "occurredAtUtc": "2026-01-01T00:00:00Z",
        "runId": "run-1",
        "jobName": "job-a",
        "attempt": 1,
        "maxAttempts": 3,
        "workerName": "worker-1",
        "runtime": "python",
        "runtimeVersion": "0.1.0",
    }
    validate_event_payload(payload)


def test_validate_runtime_control_command_rejects_unknown_type() -> None:
    payload = {
        "commandId": "cmd-1",
        "commandType": "unsupported",
        "issuedAtUtc": "2026-01-01T00:00:00Z",
    }
    with pytest.raises(ValueError):
        validate_runtime_control_command(payload)


def test_validate_runtime_control_receipt_accepts_known_status() -> None:
    payload = {
        "commandId": "cmd-1",
        "status": "acknowledged",
        "updatedAtUtc": "2026-01-01T00:00:00+00:00",
    }
    validate_runtime_control_receipt(payload)


def test_validate_telemetry_batch_request_accepts_valid_payload() -> None:
    payload = {
        "tenantId": "tenant-1",
        "idempotencyKey": "k-1",
        "events": [
            {
                "eventType": "job_started",
                "eventVersion": 2,
                "occurredAtUtc": "2026-01-01T00:00:00Z",
                "runId": "run-1",
                "jobName": "job-a",
                "attempt": 1,
                "maxAttempts": 3,
                "workerName": "worker-1",
                "runtime": "python",
                "runtimeVersion": "0.1.0",
            }
        ],
    }
    validate_telemetry_batch_request(payload)


def test_validate_telemetry_event_dto_rejects_missing_run_fields() -> None:
    payload = {
        "eventType": "job_started",
        "eventVersion": 2,
        "occurredAtUtc": "2026-01-01T00:00:00Z",
        "workerName": "worker-1",
        "runtime": "python",
        "runtimeVersion": "0.1.0",
    }
    with pytest.raises(ValueError):
        validate_telemetry_event_dto(payload)


def test_validate_runtime_control_sync_request_accepts_valid_shape() -> None:
    payload = {
        "tenantId": "tenant-1",
        "workerName": "worker-1",
        "runtime": "Python",
        "runtimeVersion": "3.13",
        "sentAtUtc": "2026-01-01T00:00:00Z",
        "snapshotItems": [
            {
                "jobName": "job-a",
                "cronExpression": "* * * * *",
                "timeZone": "UTC",
                "enabled": True,
                "nextRunAtUtc": "2026-01-01T00:01:00Z",
                "maxAttempts": 3,
                "allowConcurrentRuns": False,
                "lastSeenAtUtc": "2026-01-01T00:00:00Z",
            }
        ],
        "receipts": [
            {
                "commandId": "cmd-1",
                "status": "acknowledged",
                "recordedAtUtc": "2026-01-01T00:00:00Z",
            }
        ],
    }
    validate_runtime_control_sync_request(payload)


def test_validate_runtime_control_sync_response_accepts_pascal_case_commands() -> None:
    payload = {
        "Commands": [
            {
                "CommandId": "cmd-1",
                "CommandType": "run_schedule_now",
                "PayloadJson": "{}",
                "IssuedAtUtc": "2026-01-01T00:00:00Z",
            }
        ]
    }
    validate_runtime_control_sync_response(payload)


def test_validate_runtime_command_envelope_rejects_unsupported_type() -> None:
    payload = {
        "commandId": "cmd-1",
        "commandType": "unknown",
        "payloadJson": "{}",
        "issuedAtUtc": "2026-01-01T00:00:00Z",
    }
    with pytest.raises(ValueError):
        validate_runtime_command_envelope(payload)


def test_telemetry_batch_request_golden_fixture_is_valid() -> None:
    payload = _load_fixture("telemetry-batch-request.golden.json")
    validate_telemetry_batch_request(payload)


def test_runtime_control_sync_request_golden_fixture_is_valid() -> None:
    payload = _load_fixture("runtime-control-sync-request.golden.json")
    validate_runtime_control_sync_request(payload)


def test_runtime_control_sync_response_golden_fixture_is_valid() -> None:
    payload = _load_fixture("runtime-control-sync-response.golden.json")
    validate_runtime_control_sync_response(payload)
