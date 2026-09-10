from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from durablestack.core.event_sink import InMemoryEventSink
from durablestack.core.options import DurableStackOptions, RetryOptions
from durablestack.runtime.factory import create_durable_stack


@pytest.mark.asyncio
async def test_runtime_enqueue_and_process_success() -> None:
    sink = InMemoryEventSink()
    runtime = create_durable_stack(
        options=DurableStackOptions(
            poll_interval=timedelta(milliseconds=50),
            poll_jitter_enabled=False,
        ),
        sinks=[sink],
    )

    completed: list[str] = []

    async def handler(payload: object) -> None:
        completed.append(str(payload))

    runtime.register_job("job-a", handler)
    await runtime.start()
    run_id = await runtime.enqueue("job-a", {"id": "a1"})
    await asyncio_sleep_until(lambda: len(completed) == 1, timeout_seconds=2)
    await runtime.stop()

    run = await runtime.get_run(run_id)
    assert run is not None
    assert run.status == "succeeded"
    event_types = [evt["eventType"] for evt in sink.events]
    assert "job_claimed" in event_types
    assert "job_started" in event_types
    assert "job_succeeded" in event_types


@pytest.mark.asyncio
async def test_runtime_retries_then_fails_at_max_attempts() -> None:
    sink = InMemoryEventSink()
    runtime = create_durable_stack(
        options=DurableStackOptions(
            poll_interval=timedelta(milliseconds=25),
            poll_jitter_enabled=False,
            retry=RetryOptions(
                max_attempts=3,
                behavior="fixed",
                delay=timedelta(seconds=0),
                jitter_enabled=False,
            ),
        ),
        sinks=[sink],
    )
    attempts = {"count": 0}

    async def failing_handler(payload: object) -> None:
        _ = payload
        attempts["count"] += 1
        raise RuntimeError("boom")

    runtime.register_job("job-fail", failing_handler)
    await runtime.start()
    run_id = await runtime.enqueue("job-fail", {"id": "f1"})
    await asyncio_sleep_until(lambda: attempts["count"] >= 3, timeout_seconds=6)
    await asyncio_sleep_until(
        lambda: any(evt["eventType"] == "job_failed" for evt in sink.events), timeout_seconds=5
    )
    await runtime.stop()

    run = await runtime.get_run(run_id)
    assert run is not None
    assert run.status == "failed"
    failed_events = [evt for evt in sink.events if evt["eventType"] == "job_failed"]
    assert len(failed_events) >= 1


@pytest.mark.asyncio
async def test_runtime_recurring_job_materializes_and_runs() -> None:
    sink = InMemoryEventSink()
    runtime = create_durable_stack(
        options=DurableStackOptions(
            poll_interval=timedelta(milliseconds=50),
            poll_jitter_enabled=False,
        ),
        sinks=[sink],
    )
    calls = {"count": 0}

    async def recurring_handler(payload: object) -> None:
        _ = payload
        calls["count"] += 1

    runtime.register_recurring(
        "job-r",
        "* * * * * *",
        "UTC",
        recurring_handler,
    )
    await runtime.start()
    await asyncio_sleep_until(lambda: calls["count"] >= 1, timeout_seconds=3)
    await runtime.stop()

    started_events = [evt for evt in sink.events if evt["eventType"] == "job_started"]
    assert len(started_events) >= 1


async def asyncio_sleep_until(predicate: Callable[[], bool], timeout_seconds: int) -> None:
    import asyncio

    deadline = datetime.now(tz=UTC) + timedelta(seconds=timeout_seconds)
    while datetime.now(tz=UTC) < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met before timeout")
