from datetime import UTC, datetime, timedelta

import pytest

from durablestack.core.models import RecurringRegistration
from durablestack.providers.inmemory import InMemoryDurableJobStore


def _now() -> datetime:
    return datetime.now(tz=UTC)


@pytest.mark.asyncio
async def test_lease_fencing_rejects_stale_worker_completion() -> None:
    store = InMemoryDurableJobStore()
    run_id = (await store.enqueue("job-a", None, max_attempts=3)).run_id
    claimed = await store.claim_due_runs("worker-1", batch_size=1, lease_duration=timedelta(seconds=30), now_utc=_now())
    assert len(claimed) == 1

    stale = await store.mark_succeeded(run_id, "worker-2", completed_at_utc=_now())
    assert stale is False

    current = await store.mark_succeeded(run_id, "worker-1", completed_at_utc=_now())
    assert current is True


@pytest.mark.asyncio
async def test_recurring_slot_uniqueness_blocks_duplicate_materialization() -> None:
    store = InMemoryDurableJobStore()
    now = _now()
    registration = RecurringRegistration(
        name="job-r",
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
    recurring = (await store.get_due_recurring_jobs(now, 10))[0]

    first = await store.try_materialize_recurring_run(recurring, registration, now + timedelta(minutes=5))
    second = await store.try_materialize_recurring_run(recurring, registration, now + timedelta(minutes=10))

    assert first is True
    assert second is False
