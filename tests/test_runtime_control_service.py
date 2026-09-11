from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from durablestack.core.options import DurableStackOptions, EventingOptions
from durablestack.observability.http import HttpPostRequest, HttpResponseData
from durablestack.observability.runtime_control import RuntimeControlSyncService
from durablestack.providers.inmemory import InMemoryDurableJobStore
from durablestack.runtime.factory import create_durable_stack

_FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads((_FIXTURES_DIR / name).read_text(encoding="utf-8")))


@dataclass(slots=True)
class FakeHttpPost:
    statuses: list[int]
    body_text: str = "{}"
    calls: list[HttpPostRequest] = field(default_factory=list)

    async def __call__(self, request: HttpPostRequest, timeout_seconds: float) -> HttpResponseData:
        _ = timeout_seconds
        self.calls.append(request)
        status = self.statuses.pop(0) if self.statuses else 200
        return HttpResponseData(status=status, body_text=self.body_text)


@pytest.mark.asyncio
async def test_runtime_control_retries_transient_and_stops_on_success() -> None:
    store = InMemoryDurableJobStore()
    runtime = create_durable_stack(
        options=DurableStackOptions(
            worker_name="w-ctrl",
            eventing=EventingOptions(
                tenant_id="tenant-1",
                client_secret="secret-1",
                ingestion_api_base_url="https://example.com",
                runtime_control_sync_jitter_enabled=False,
                runtime_control_enabled=False,
            ),
        )
    )
    fake = FakeHttpPost(statuses=[500, 429, 200], body_text='{"commands": []}')
    service = RuntimeControlSyncService(store=store, admin=runtime, options=runtime.options, http_post=fake)

    await service.sync_once()

    assert len(fake.calls) == 3
    assert fake.calls[-1].headers["X-DurableStack-TenantId"] == "tenant-1"
    assert fake.calls[-1].headers["X-DurableStack-ClientSecret"] == "secret-1"


@pytest.mark.asyncio
async def test_runtime_control_does_not_retry_on_auth_failure() -> None:
    store = InMemoryDurableJobStore()
    runtime = create_durable_stack(
        options=DurableStackOptions(
            worker_name="w-ctrl",
            eventing=EventingOptions(
                tenant_id="tenant-1",
                client_secret="secret-1",
                ingestion_api_base_url="https://example.com",
                runtime_control_enabled=False,
            ),
        )
    )
    fake = FakeHttpPost(statuses=[401, 200], body_text='{"commands": []}')
    service = RuntimeControlSyncService(store=store, admin=runtime, options=runtime.options, http_post=fake)

    await service.sync_once()

    assert len(fake.calls) == 1


@pytest.mark.asyncio
async def test_runtime_control_processes_command_and_persists_receipt() -> None:
    runtime = create_durable_stack(
        options=DurableStackOptions(
            worker_name="w-ctrl",
            eventing=EventingOptions(
                tenant_id="tenant-1",
                client_secret="secret-1",
                ingestion_api_base_url="https://example.com",
                runtime_control_enabled=False,
            ),
        )
    )

    async def recurring_handler(payload: object) -> None:
        _ = payload

    runtime.register_recurring("job-r", "* * * * *", "UTC", recurring_handler)

    commands = RuntimeControlSyncService.parse_response(
        json.dumps(
            {
                "Commands": [
                    {
                        "CommandId": "cmd-1",
                        "CommandType": "run_schedule_now",
                        "PayloadJson": json.dumps({"JobName": "job-r"}),
                        "IssuedAtUtc": "2026-01-01T00:00:00+00:00",
                    }
                ]
            }
        )
    )
    assert commands is not None
    assert len(commands) == 1

    service = RuntimeControlSyncService(
        store=runtime.store,
        admin=runtime,
        options=runtime.options,
        http_post=FakeHttpPost(statuses=[200]),
    )
    await service.process_commands(commands)

    receipts = await runtime.store.get_runtime_command_receipts(10)
    assert len(receipts) == 1
    assert receipts[0].command_id == "cmd-1"
    assert receipts[0].status == "succeeded"


@pytest.mark.asyncio
async def test_runtime_control_upload_marks_receipts_as_uploaded() -> None:
    store = InMemoryDurableJobStore()
    runtime = create_durable_stack(
        options=DurableStackOptions(
            worker_name="w-ctrl",
            eventing=EventingOptions(
                tenant_id="tenant-1",
                client_secret="secret-1",
                ingestion_api_base_url="https://example.com",
                runtime_control_enabled=False,
            ),
        )
    )
    now = datetime.now(tz=UTC)
    leased = await store.try_lease_runtime_command_receipt(
        "cmd-upload",
        "w-ctrl",
        timedelta(seconds=30),
        now,
    )
    assert leased
    ack = await store.mark_runtime_command_acknowledged("cmd-upload", "w-ctrl", now)
    assert ack

    fake = FakeHttpPost(statuses=[200], body_text='{"commands": []}')
    service = RuntimeControlSyncService(store=store, admin=runtime, options=runtime.options, http_post=fake)
    await service.sync_once()

    receipts = await store.get_runtime_command_receipts(10)
    assert len(receipts) == 0


def test_parse_response_accepts_runtime_control_golden_fixture() -> None:
    payload = _load_fixture("runtime-control-sync-response.golden.json")
    commands = RuntimeControlSyncService.parse_response(json.dumps(payload))

    assert commands is not None
    assert len(commands) == 3
    assert commands[0].command_type == "set_schedule_enabled"
    assert commands[1].command_type == "run_schedule_now"
    assert commands[2].command_type == "update_schedule_cron"
