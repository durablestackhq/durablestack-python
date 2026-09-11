from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

import pytest

from durablestack.core.event_sink import InMemoryEventSink
from durablestack.core.options import DurableStackOptions, RecurringOptions, RetryOptions
from durablestack.providers.inmemory import InMemoryDurableJobStore
from durablestack.runtime.factory import create_durable_stack, create_durable_stack_with_store


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
    await wait_until(lambda: len(completed) == 1, timeout_seconds=2)
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
    await wait_until(lambda: attempts["count"] >= 3, timeout_seconds=6)
    await wait_until(lambda: any(evt["eventType"] == "job_failed" for evt in sink.events), timeout_seconds=5)
    await runtime.stop()

    run = await runtime.get_run(run_id)
    assert run is not None
    assert run.status == "failed"


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

    runtime.register_recurring("job-r", "* * * * * *", "UTC", recurring_handler)
    await runtime.start()
    await wait_until(lambda: calls["count"] >= 1, timeout_seconds=3)
    await runtime.stop()

    started_events = [evt for evt in sink.events if evt["eventType"] == "job_started"]
    assert len(started_events) >= 1


@pytest.mark.asyncio
async def test_runtime_skip_missed_recurring_policy_skips_backlog() -> None:
    sink = InMemoryEventSink()
    store = InMemoryDurableJobStore()
    runtime = create_durable_stack_with_store(
        store=store,
        options=DurableStackOptions(
            poll_interval=timedelta(milliseconds=20),
            poll_jitter_enabled=False,
            recurring=RecurringOptions(catch_up_policy="skip_missed"),
        ),
        sinks=[sink],
    )
    calls = {"count": 0}

    async def handler(payload: object) -> None:
        _ = payload
        calls["count"] += 1

    runtime.register_recurring("skip-job", "* * * * * *", "UTC", handler)
    await runtime.start()

    recurring = (await store.get_recurring_jobs(True))[0]
    await store.set_recurring_job_enabled(
        recurring.job_name,
        True,
        datetime.now(tz=UTC) - timedelta(seconds=3),
    )

    await wait_until(lambda: calls["count"] >= 1, timeout_seconds=3)
    await runtime.stop()
    assert calls["count"] == 1


@pytest.mark.asyncio
async def test_register_job_rejects_handler_without_payload_parameter() -> None:
    runtime = create_durable_stack()

    async def invalid_handler() -> None:
        return None

    with pytest.raises(TypeError):
        runtime.register_job("invalid", invalid_handler)


@pytest.mark.asyncio
async def test_two_workers_do_not_double_claim_same_run() -> None:
    store = InMemoryDurableJobStore()
    sink_one = InMemoryEventSink()
    sink_two = InMemoryEventSink()

    worker_one = create_durable_stack_with_store(
        store=store,
        options=DurableStackOptions(
            worker_name="worker-one",
            poll_interval=timedelta(milliseconds=20),
            poll_jitter_enabled=False,
            lease_duration=timedelta(milliseconds=250),
            max_concurrent_runs=1,
        ),
        sinks=[sink_one],
    )
    worker_two = create_durable_stack_with_store(
        store=store,
        options=DurableStackOptions(
            worker_name="worker-two",
            poll_interval=timedelta(milliseconds=20),
            poll_jitter_enabled=False,
            lease_duration=timedelta(milliseconds=250),
            max_concurrent_runs=1,
        ),
        sinks=[sink_two],
    )

    async def slow_handler(payload: object) -> None:
        _ = payload
        import asyncio

        await asyncio.sleep(0.2)

    worker_one.register_job("shared-job", slow_handler)
    worker_two.register_job("shared-job", slow_handler)

    await worker_one.start()
    await worker_two.start()
    run_id = await worker_one.enqueue("shared-job", {"v": 1})

    await wait_until(
        lambda: (len(events_of(sink_one, "job_claimed")) + len(events_of(sink_two, "job_claimed"))) >= 1,
        timeout_seconds=2,
    )
    await wait_until(
        lambda: False,
        timeout_seconds=4,
        async_predicate=lambda: run_status_is(store, run_id, "succeeded"),
    )

    await worker_one.stop()
    await worker_two.stop()

    total_claimed = len(events_of(sink_one, "job_claimed")) + len(events_of(sink_two, "job_claimed"))
    assert total_claimed == 1


@pytest.mark.asyncio
async def test_stale_worker_cannot_mark_completion_after_lease_expiry() -> None:
    store = InMemoryDurableJobStore()
    now = datetime.now(tz=UTC)
    run_id = (await store.schedule("fence-job", None, now, max_attempts=2)).run_id

    claimed_by_one = await store.claim_due_runs(
        worker_name="worker-one",
        batch_size=1,
        lease_duration=timedelta(milliseconds=50),
        now_utc=now,
    )
    assert len(claimed_by_one) == 1

    claimed_by_two = await store.claim_due_runs(
        worker_name="worker-two",
        batch_size=1,
        lease_duration=timedelta(milliseconds=50),
        now_utc=now + timedelta(milliseconds=75),
    )
    assert len(claimed_by_two) == 1

    stale_write = await store.mark_succeeded(run_id, "worker-one", completed_at_utc=now + timedelta(milliseconds=80))
    assert stale_write is False

    current_write = await store.mark_succeeded(run_id, "worker-two", completed_at_utc=now + timedelta(milliseconds=90))
    assert current_write is True


@pytest.mark.asyncio
async def test_recurring_catch_up_materializes_multiple_missed_slots() -> None:
    sink = InMemoryEventSink()
    store = InMemoryDurableJobStore()
    runtime = create_durable_stack_with_store(
        store=store,
        options=DurableStackOptions(
            poll_interval=timedelta(milliseconds=20),
            poll_jitter_enabled=False,
            recurring=RecurringOptions(catch_up_policy="catch_up"),
        ),
        sinks=[sink],
    )
    calls: list[str] = []

    async def handler(payload: object) -> None:
        calls.append("x")
        _ = payload

    runtime.register_recurring("catchup-job", "* * * * * *", "UTC", handler)
    await runtime.start()

    recurring = (await store.get_recurring_jobs(True))[0]
    await store.set_recurring_job_enabled(
        recurring.job_name,
        True,
        datetime.now(tz=UTC) - timedelta(seconds=3),
    )

    await wait_until(lambda: len(calls) >= 2, timeout_seconds=3)
    await runtime.stop()
    assert len(calls) >= 2


@pytest.mark.asyncio
async def test_retention_batch_limit_prunes_only_configured_count() -> None:
    store = InMemoryDurableJobStore()
    base = datetime.now(tz=UTC) - timedelta(days=10)
    for index in range(5):
        run_id = (await store.schedule("retention-job", None, base + timedelta(seconds=index), max_attempts=1)).run_id
        claimed = await store.claim_due_runs(
            worker_name="retention-worker",
            batch_size=1,
            lease_duration=timedelta(seconds=5),
            now_utc=base + timedelta(seconds=index + 1),
        )
        assert claimed
        marked = await store.mark_succeeded(
            run_id,
            "retention-worker",
            completed_at_utc=base + timedelta(seconds=index + 2),
        )
        assert marked

    deleted = await store.prune_historical_runs(
        completed_before_utc=datetime.now(tz=UTC) - timedelta(days=1),
        batch_size=2,
    )
    assert deleted == 2

    remaining = await store.get_recent_runs(10)
    assert len(remaining) == 3


async def wait_until(
    predicate: Callable[[], bool],
    timeout_seconds: int,
    async_predicate: Callable[[], Awaitable[bool]] | None = None,
) -> None:
    import asyncio

    deadline = datetime.now(tz=UTC) + timedelta(seconds=timeout_seconds)
    while datetime.now(tz=UTC) < deadline:
        if predicate():
            return
        if async_predicate is not None and await async_predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met before timeout")


def events_of(sink: InMemoryEventSink, event_type: str) -> list[dict[str, object]]:
    return [evt for evt in sink.events if evt.get("eventType") == event_type]


async def run_status_is(store: InMemoryDurableJobStore, run_id: str, expected: str) -> bool:
    run = await store.get_run(run_id)
    return run is not None and run.status == expected
