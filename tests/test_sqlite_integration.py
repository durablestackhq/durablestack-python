from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

import pytest

from durablestack.core.models import RecurringRegistration
from durablestack.core.options import DurableStackOptions
from durablestack.sqlite import (
    SqliteDurableJobStore,
    SqliteDurableStackOptions,
    create_durable_stack_sqlite,
)

pytestmark = pytest.mark.asyncio


async def test_sqlite_enqueue_claim_and_succeed(tmp_path: pytest.TempPathFactory) -> None:
    db = tmp_path / "durablestack.sqlite3"
    store = SqliteDurableJobStore(SqliteDurableStackOptions(database_path=str(db), table_prefix="py_it_"))
    await store.open()
    try:
        run_id = (await store.enqueue("job-a", None, max_attempts=3)).run_id
        claimed = await store.claim_due_runs(
            worker_name="worker-1",
            batch_size=1,
            lease_duration=timedelta(seconds=30),
            now_utc=datetime.now(tz=UTC),
        )
        assert len(claimed) == 1
        assert claimed[0].run_id == run_id

        stale = await store.mark_succeeded(run_id, "worker-2", completed_at_utc=datetime.now(tz=UTC))
        assert stale is False

        current = await store.mark_succeeded(run_id, "worker-1", completed_at_utc=datetime.now(tz=UTC))
        assert current is True
    finally:
        await store.close()


async def test_sqlite_runtime_smoke_start_enqueue_stop(tmp_path: pytest.TempPathFactory) -> None:
    db = tmp_path / "durablestack-runtime.sqlite3"
    handle = await create_durable_stack_sqlite(
        SqliteDurableStackOptions(database_path=str(db), table_prefix="py_rt_"),
        options=DurableStackOptions(
            worker_name="sqlite-runtime-smoke",
            poll_interval=timedelta(milliseconds=25),
            poll_jitter_enabled=False,
        ),
    )
    runtime = handle.runtime
    calls = {"count": 0}

    async def handler(payload: object) -> None:
        _ = payload
        calls["count"] += 1

    runtime.register_job("smoke-job", handler)
    await runtime.start()
    run_id = await runtime.enqueue("smoke-job", {"smoke": True})

    deadline = datetime.now(tz=UTC) + timedelta(seconds=6)
    while datetime.now(tz=UTC) < deadline and calls["count"] < 1:
        await asyncio_sleep(0.05)

    await runtime.stop()
    run = await runtime.get_run(run_id)
    assert run is not None
    assert run.status == "succeeded"

    await handle.close_store()


async def test_sqlite_recurring_slot_unique_under_contention(tmp_path: pytest.TempPathFactory) -> None:
    db = tmp_path / "durablestack-recurring.sqlite3"
    store = SqliteDurableJobStore(SqliteDurableStackOptions(database_path=str(db), table_prefix="py_rec_"))
    await store.open()
    try:
        now = datetime.now(tz=UTC)
        registration = RecurringRegistration(
            name="sqlite-race",
            handler=lambda payload: payload,
            cron_expression="*/5 * * * *",
            time_zone="UTC",
            enabled=True,
            allow_concurrent_runs=True,
            max_attempts=3,
            retry_behavior="fixed",
            retry_initial_delay_seconds=5,
        )
        await store.upsert_recurring_job(registration, now)
        recurring = (await store.get_due_recurring_jobs(now, 1))[0]

        async def attempt_once() -> bool:
            return await store.try_materialize_recurring_run(
                recurring,
                registration,
                now + timedelta(minutes=5),
            )

        results = await gather_two(attempt_once, attempt_once)
        assert sum(1 for item in results if item) == 1
    finally:
        await store.close()


async def asyncio_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


async def gather_two(
    first: Callable[[], Awaitable[bool]],
    second: Callable[[], Awaitable[bool]],
) -> list[bool]:
    import asyncio

    return await asyncio.gather(first(), second())
