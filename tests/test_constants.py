from durablestack.core.constants import (
    EVENT_TYPES,
    EVENT_VERSION,
    RUN_STATUSES,
    RUNTIME_CONTROL_COMMAND_TYPES,
    RUNTIME_CONTROL_RECEIPT_STATUSES,
)


def test_canonical_contract_constants() -> None:
    assert RUN_STATUSES == ("pending", "leased", "succeeded", "failed")
    assert EVENT_VERSION == 2
    assert EVENT_TYPES == (
        "job_claimed",
        "job_started",
        "job_succeeded",
        "job_failed",
        "job_retried",
        "retry_scheduled",
        "worker_heartbeat",
    )
    assert RUNTIME_CONTROL_COMMAND_TYPES == (
        "set_schedule_enabled",
        "run_schedule_now",
        "update_schedule_cron",
    )
    assert RUNTIME_CONTROL_RECEIPT_STATUSES == (
        "leased",
        "acknowledged",
        "succeeded",
        "failed",
    )
