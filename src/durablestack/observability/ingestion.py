"""Hosted ingestion sync service and sink implementation."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin

from durablestack.core.abstractions import DurableStackEventSink
from durablestack.core.constants import EVENT_TYPE_WORKER_HEARTBEAT
from durablestack.core.options import DurableStackOptions
from durablestack.core.utils import generate_id, with_jitter
from durablestack.validators.contracts import validate_telemetry_batch_request

from .http import HttpPost, HttpPostRequest, HttpResponseData, is_transient_status
from .url_validation import assert_secure_endpoint

_logger = logging.getLogger(__name__)


def _response_snippet(body_text: str, max_len: int = 300) -> str:
    text = body_text.strip()
    if text == "":
        return ""
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


def default_http_post() -> HttpPost:
    import urllib.error
    import urllib.request

    async def _post(request: HttpPostRequest, timeout_seconds: float) -> HttpResponseData:
        def _sync_post() -> HttpResponseData:
            req = urllib.request.Request(
                request.url,
                data=request.body.encode("utf-8"),
                headers=dict(request.headers),
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
                    body = response.read().decode("utf-8")
                    return HttpResponseData(status=response.status, body_text=body)
            except urllib.error.HTTPError as exc:
                body = exc.read().decode("utf-8") if exc.fp is not None else ""
                return HttpResponseData(status=int(exc.code), body_text=body)

        return await asyncio.to_thread(_sync_post)

    return _post


def _normalize_runtime_version(value: str) -> str:
    trimmed = value.strip()
    if not trimmed:
        return "unknown"
    return trimmed.removeprefix("v")


def _default_runtime_name(runtime_version: str) -> str:
    return f"Python {_normalize_runtime_version(runtime_version)}"


def _build_idempotency_key(worker_name: str, sequence: int) -> str:
    return f"{worker_name}:{int(datetime.now(tz=UTC).timestamp() * 1000)}:{sequence}"


@dataclass(slots=True)
class IngestionDurableStackEventSink(DurableStackEventSink):
    """Queue-backed event sink with bounded memory."""

    max_queue_size: int = 10_000
    _queue: deque[dict[str, Any]] = field(default_factory=deque)

    async def publish(self, event: dict[str, Any]) -> None:
        if len(self._queue) >= self.max_queue_size:
            self._queue.popleft()
        self._queue.append(event)

    def drain(self, max_items: int) -> list[dict[str, Any]]:
        count = max(1, int(max_items))
        items: list[dict[str, Any]] = []
        while self._queue and len(items) < count:
            items.append(self._queue.popleft())
        return items

    @property
    def size(self) -> int:
        return len(self._queue)


@dataclass(slots=True)
class IngestionEventSyncService:
    """Batched eventing sync service with auth + retry + idempotency guardrails."""

    sink: IngestionDurableStackEventSink
    options: DurableStackOptions
    http_post: HttpPost
    runtime_name: str
    runtime_version: str
    _running: bool = False
    _loop_task: asyncio.Task[None] | None = None
    _sequence: int = 0

    def start(self) -> None:
        if self._running:
            return
        if not self._has_credentials():
            return
        self._resolve_endpoint()
        self._running = True
        self._loop_task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._loop_task is not None:
            await self._loop_task

        for _ in range(10):
            if self.sink.size == 0:
                break
            await self.flush_once()

    async def flush_once(self) -> None:
        if not self._has_credentials():
            return

        batch = self.sink.drain(self.options.eventing.ingestion_max_batch_size)
        if not batch:
            return

        payload = {
            "tenantId": self.options.eventing.tenant_id,
            "idempotencyKey": _build_idempotency_key(self.options.worker_name, self._next_sequence()),
            "serviceName": self.options.eventing.service_name,
            "events": _build_events(batch, self.runtime_name, self.runtime_version),
        }
        validate_telemetry_batch_request(payload)
        body = json.dumps(payload)
        if len(body.encode("utf-8")) > self.options.eventing.ingestion_max_request_body_bytes:
            return

        endpoint = self._resolve_endpoint()
        max_attempts = max(1, min(10, self.options.eventing.ingestion_max_retry_attempts))

        for attempt in range(1, max_attempts + 1):
            try:
                response = await self.http_post(
                    HttpPostRequest(
                        url=endpoint,
                        headers={
                            "Content-Type": "application/json",
                            "Accept": "application/json",
                            "X-DurableStack-TenantId": self.options.eventing.tenant_id or "",
                            "X-DurableStack-ClientSecret": self.options.eventing.client_secret or "",
                            "X-Correlation-Id": generate_id().replace("-", ""),
                        },
                        body=body,
                    ),
                    timeout_seconds=30,
                )
            except Exception as exc:  # noqa: BLE001
                if attempt >= max_attempts:
                    _logger.warning("Ingestion sync failed after retries due to request error: %s", str(exc))
                    return
                await asyncio.sleep(min(10, attempt) * 0.2)
                continue

            if 200 <= response.status < 300:
                return
            if response.status in {401, 403}:
                snippet = _response_snippet(response.body_text)
                if snippet:
                    _logger.warning(
                        "Ingestion authorization failed with status %s: %s",
                        response.status,
                        snippet,
                    )
                else:
                    _logger.warning("Ingestion authorization failed with status %s", response.status)
                return
            if not is_transient_status(response.status) or attempt >= max_attempts:
                _logger.warning("Ingestion sync failed with status %s", response.status)
                return

            await asyncio.sleep(min(10, attempt) * 0.2)

    async def _run_loop(self) -> None:
        delay = with_jitter(
            self.options.eventing.ingestion_flush_interval.total_seconds(),
            self.options.eventing.ingestion_sync_jitter_enabled,
            self.options.eventing.ingestion_sync_jitter_ratio,
        )
        if delay > 0:
            await asyncio.sleep(delay)

        while self._running:
            try:
                await self.flush_once()
            except Exception as exc:  # noqa: BLE001
                _logger.warning("Ingestion sync cycle failed: %s", str(exc))
                await asyncio.sleep(2)

            delay = with_jitter(
                self.options.eventing.ingestion_flush_interval.total_seconds(),
                self.options.eventing.ingestion_sync_jitter_enabled,
                self.options.eventing.ingestion_sync_jitter_ratio,
            )
            await asyncio.sleep(max(0.0, delay))

    def _resolve_endpoint(self) -> str:
        endpoint = urljoin(self.options.eventing.ingestion_api_base_url, self.options.eventing.ingestion_path)
        assert_secure_endpoint(endpoint, "eventing.ingestion_api_base_url")
        return endpoint

    def _has_credentials(self) -> bool:
        return bool(self.options.eventing.tenant_id and self.options.eventing.client_secret)

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence


def _build_events(events: list[dict[str, Any]], runtime_name: str, runtime_version: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    heartbeat: list[dict[str, Any]] = []
    for event in events:
        if event.get("eventType") == EVENT_TYPE_WORKER_HEARTBEAT:
            heartbeat.append(event)
            continue
        mapped = dict(event)
        mapped["runtime"] = runtime_name
        mapped["runtimeVersion"] = runtime_version
        out.append(mapped)

    if heartbeat:
        latest = max(heartbeat, key=lambda x: str(x.get("occurredAtUtc", "")))
        earliest = min(heartbeat, key=lambda x: str(x.get("occurredAtUtc", "")))
        out.append(
            {
                "eventType": "worker_heartbeat_batch",
                "eventVersion": latest.get("eventVersion", 2),
                "occurredAtUtc": latest.get("occurredAtUtc"),
                "workerName": latest.get("workerName"),
                "runtime": runtime_name,
                "runtimeVersion": runtime_version,
                "payloadJson": json.dumps(
                    {
                        "heartbeatCount": len(heartbeat),
                        "firstHeartbeatAtUtc": earliest.get("occurredAtUtc"),
                        "lastHeartbeatAtUtc": latest.get("occurredAtUtc"),
                    }
                ),
            }
        )

    return out


def create_ingestion_eventing(
    options: DurableStackOptions,
    http_post: HttpPost | None = None,
    runtime_name: str | None = None,
    runtime_version: str = "unknown",
) -> tuple[IngestionDurableStackEventSink, IngestionEventSyncService]:
    """Create sink + service pair used by runtime host."""

    sink = IngestionDurableStackEventSink()
    effective_runtime_name = runtime_name or _default_runtime_name(runtime_version)
    service = IngestionEventSyncService(
        sink=sink,
        options=options,
        http_post=http_post or default_http_post(),
        runtime_name=effective_runtime_name,
        runtime_version=_normalize_runtime_version(runtime_version),
    )
    return sink, service
