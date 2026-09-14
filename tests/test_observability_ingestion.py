from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

import pytest

from durablestack.core.options import DurableStackOptions, EventingOptions
from durablestack.observability.http import HttpPostRequest, HttpResponseData
from durablestack.observability.ingestion import create_ingestion_eventing

_FIXTURES_DIR = Path(__file__).parent / "fixtures"


@dataclass(slots=True)
class FakeHttpPost:
    statuses: list[int]
    calls: list[HttpPostRequest] = field(default_factory=list)

    async def __call__(self, request: HttpPostRequest, timeout_seconds: float) -> HttpResponseData:
        _ = timeout_seconds
        self.calls.append(request)
        status = self.statuses.pop(0) if self.statuses else 200
        return HttpResponseData(status=status, body_text="{}")


@pytest.mark.asyncio
async def test_ingestion_uses_auth_headers_and_retries_transient_failures() -> None:
    options = DurableStackOptions(
        worker_name="w1",
        eventing=EventingOptions(
            tenant_id="tenant-1",
            client_secret="secret-1",
            ingestion_api_base_url="https://example.com",
            ingestion_path="/v1/events/batch",
            ingestion_flush_interval=timedelta(milliseconds=10),
            ingestion_max_retry_attempts=3,
            ingestion_sync_jitter_enabled=False,
        ),
    )
    fake = FakeHttpPost(statuses=[500, 429, 202])
    sink, service = create_ingestion_eventing(options, http_post=fake)
    await sink.publish(
        {
            "eventId": "e1",
            "eventType": "job_started",
            "eventVersion": 2,
            "occurredAtUtc": "2026-01-01T00:00:00Z",
            "runId": "r1",
            "jobName": "j1",
            "attempt": 1,
            "maxAttempts": 3,
            "workerName": "w1",
        }
    )

    await service.flush_once()

    assert len(fake.calls) == 3
    last = fake.calls[-1]
    assert last.headers["X-DurableStack-TenantId"] == "tenant-1"
    assert last.headers["X-DurableStack-ClientSecret"] == "secret-1"
    assert "X-Correlation-Id" in last.headers


@pytest.mark.asyncio
async def test_ingestion_does_not_retry_auth_failures() -> None:
    options = DurableStackOptions(
        worker_name="w1",
        eventing=EventingOptions(
            tenant_id="tenant-1",
            client_secret="secret-1",
            ingestion_api_base_url="https://example.com",
            ingestion_path="/v1/events/batch",
            ingestion_max_retry_attempts=5,
            ingestion_sync_jitter_enabled=False,
        ),
    )
    fake = FakeHttpPost(statuses=[401, 202, 202])
    sink, service = create_ingestion_eventing(options, http_post=fake)
    await sink.publish(
        {
            "eventId": "e1",
            "eventType": "job_started",
            "eventVersion": 2,
            "occurredAtUtc": "2026-01-01T00:00:00Z",
            "runId": "r1",
            "jobName": "j1",
            "attempt": 1,
            "maxAttempts": 3,
            "workerName": "w1",
        }
    )

    await service.flush_once()

    assert len(fake.calls) == 1


def test_ingestion_endpoint_rejects_insecure_non_loopback_http() -> None:
    options = DurableStackOptions(
        eventing=EventingOptions(
            tenant_id="tenant-1",
            client_secret="secret-1",
            ingestion_api_base_url="http://example.com",
        )
    )
    _, service = create_ingestion_eventing(options)
    with pytest.raises(ValueError):
        service.start()


@pytest.mark.asyncio
async def test_ingestion_payload_matches_telemetry_batch_golden_keys() -> None:
    fixture = json.loads((_FIXTURES_DIR / "telemetry-batch-request.golden.json").read_text(encoding="utf-8"))
    options = DurableStackOptions(
        worker_name="worker-1",
        eventing=EventingOptions(
            tenant_id="tenant-001",
            client_secret="secret-1",
            service_name="sample-service",
            ingestion_api_base_url="https://example.com",
            ingestion_path="/v1/events/batch",
            ingestion_sync_jitter_enabled=False,
        ),
    )
    fake = FakeHttpPost(statuses=[202])
    sink, service = create_ingestion_eventing(options, http_post=fake, runtime_name="Python 3.13", runtime_version="3.13")
    await sink.publish(
        {
            "eventType": "job_started",
            "eventVersion": 2,
            "occurredAtUtc": "2026-01-01T00:00:00+00:00",
            "runId": "run-001",
            "jobName": "invoice.sync",
            "attempt": 1,
            "maxAttempts": 3,
            "workerName": "worker-1",
            "durationMs": 12,
            "payloadJson": "{\"message\":\"starting\"}",
        }
    )
    await sink.publish(
        {
            "eventType": "worker_heartbeat",
            "eventVersion": 2,
            "occurredAtUtc": "2026-01-01T00:00:01+00:00",
            "workerName": "worker-1",
        }
    )
    await sink.publish(
        {
            "eventType": "worker_heartbeat",
            "eventVersion": 2,
            "occurredAtUtc": "2026-01-01T00:00:05+00:00",
            "workerName": "worker-1",
        }
    )

    await service.flush_once()

    assert len(fake.calls) == 1
    payload = json.loads(fake.calls[0].body)
    assert set(payload.keys()) == set(fixture.keys())
    assert payload["tenantId"] == fixture["tenantId"]
    assert payload["serviceName"] == fixture["serviceName"]
    assert isinstance(payload["idempotencyKey"], str)
    assert len(payload["events"]) == len(fixture["events"])
    assert payload["events"][0]["eventType"] == fixture["events"][0]["eventType"]
    assert payload["events"][1]["eventType"] == fixture["events"][1]["eventType"]
