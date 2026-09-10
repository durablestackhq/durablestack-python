from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest

from durablestack.postgres import PostgresDurableStackOptions, create_durable_stack_postgres

pytestmark = pytest.mark.asyncio


def _postgres_dsn() -> str | None:
    value = os.getenv("DURABLESTACK_TEST_POSTGRES_DSN")
    if value and value.strip():
        return value.strip()
    return None


@pytest.mark.skipif(_postgres_dsn() is None, reason="Set DURABLESTACK_TEST_POSTGRES_DSN to run Postgres tests")
async def test_postgres_enqueue_claim_and_succeed() -> None:
    dsn = _postgres_dsn()
    assert dsn is not None

    handle = await create_durable_stack_postgres(
        PostgresDurableStackOptions(connection_string=dsn, table_prefix="py_it_"),
    )
    store = handle.store
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
        await handle.close_store()
