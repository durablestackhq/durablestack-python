import pytest

from durablestack.validators.contracts import (
    validate_event_payload,
    validate_runtime_control_command,
    validate_runtime_control_receipt,
)


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
