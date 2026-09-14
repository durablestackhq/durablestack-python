"""Runtime-control sync service."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from urllib.parse import urljoin

from durablestack.core.abstractions import DurableJobStore, RuntimeControlAdmin
from durablestack.core.models import RuntimeCommandEnvelope, RuntimeCommandType
from durablestack.core.options import DurableStackOptions
from durablestack.core.utils import generate_id, with_jitter
from durablestack.validators.contracts import validate_runtime_control_sync_request

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


@dataclass(slots=True)
class RuntimeControlSyncService:
    """Synchronizes schedule snapshots/receipts and executes inbound commands."""

    store: DurableJobStore
    admin: RuntimeControlAdmin
    options: DurableStackOptions
    http_post: HttpPost
    runtime_name: str = "Python"
    runtime_version: str = "unknown"
    _running: bool = False
    _loop_task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._running:
            return
        if not self.options.eventing.runtime_control_enabled:
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

    async def sync_once(self) -> None:
        if not self._has_credentials():
            return

        sent_at = datetime.now(tz=UTC)
        schedules = await self.admin.list_scheduled_jobs(True)
        receipts = await self.store.get_runtime_command_receipts(
            self.options.eventing.runtime_control_max_receipt_upload
        )

        payload = {
            "tenantId": self.options.eventing.tenant_id,
            "workerName": self.options.worker_name,
            "runtime": self.runtime_name,
            "runtimeVersion": self.runtime_version,
            "sentAtUtc": sent_at.isoformat(),
            "snapshotItems": [
                {
                    "jobName": row.job_name,
                    "cronExpression": row.cron_expression,
                    "timeZone": row.time_zone,
                    "enabled": row.enabled,
                    "nextRunAtUtc": row.next_run_at_utc.isoformat(),
                    "maxAttempts": row.max_attempts,
                    "allowConcurrentRuns": row.allow_concurrent_runs,
                    "lastSeenAtUtc": sent_at.isoformat(),
                }
                for row in schedules
            ],
            "receipts": [
                {
                    "commandId": row.command_id,
                    "status": row.status,
                    "recordedAtUtc": row.recorded_at_utc.isoformat(),
                    "completedAtUtc": row.completed_at_utc.isoformat() if row.completed_at_utc else None,
                    "runId": row.run_id,
                    "errorCode": row.error_code,
                    "errorMessage": row.error_message,
                }
                for row in receipts
            ],
        }
        validate_runtime_control_sync_request(payload)

        response = await self._post_with_retry(json.dumps(payload))
        if response is None:
            return

        for receipt in receipts:
            await self.store.mark_runtime_command_receipt_uploaded(receipt.command_id, datetime.now(tz=UTC))

        commands = self.parse_response(response.body_text)
        if commands is None:
            return
        await self.process_commands(commands)

    async def process_commands(self, commands: list[RuntimeCommandEnvelope]) -> None:
        for command in commands:
            if command.expires_at_utc and command.expires_at_utc <= datetime.now(tz=UTC):
                continue

            leased = await self.store.try_lease_runtime_command_receipt(
                command.command_id,
                self.options.worker_name,
                self.options.eventing.runtime_control_command_lease_duration,
                datetime.now(tz=UTC),
            )
            if not leased:
                continue

            acknowledged = await self.store.mark_runtime_command_acknowledged(
                command.command_id,
                self.options.worker_name,
                datetime.now(tz=UTC),
            )
            if not acknowledged:
                continue

            try:
                result = await self._execute_command(command)
                if result.get("success"):
                    await self.store.mark_runtime_command_succeeded(
                        command.command_id,
                        self.options.worker_name,
                        datetime.now(tz=UTC),
                        datetime.now(tz=UTC),
                        cast(str | None, result.get("runId")),
                    )
                else:
                    await self.store.mark_runtime_command_failed(
                        command.command_id,
                        self.options.worker_name,
                        datetime.now(tz=UTC),
                        datetime.now(tz=UTC),
                        str(result.get("errorCode") or "command_failed"),
                        str(result.get("errorMessage") or "Command failed"),
                    )
            except Exception as exc:  # noqa: BLE001
                await self.store.mark_runtime_command_failed(
                    command.command_id,
                    self.options.worker_name,
                    datetime.now(tz=UTC),
                    datetime.now(tz=UTC),
                    "runtime_exception",
                    str(exc),
                )

    async def _execute_command(self, command: RuntimeCommandEnvelope) -> dict[str, Any]:
        command_type = command.command_type.strip()
        if command_type == "":
            return {
                "success": False,
                "errorCode": "invalid_command_type",
                "errorMessage": "CommandType is required.",
            }

        payload = _safe_json_object(command.payload_json)

        def payload_string(camel_key: str, pascal_key: str) -> str | None:
            value = payload.get(camel_key)
            if isinstance(value, str):
                return value
            value = payload.get(pascal_key)
            if isinstance(value, str):
                return value
            return None

        def payload_bool(camel_key: str, pascal_key: str) -> bool | None:
            value = payload.get(camel_key)
            if isinstance(value, bool):
                return value
            value = payload.get(pascal_key)
            if isinstance(value, bool):
                return value
            return None

        if command_type == "set_schedule_enabled":
            job_name = payload_string("jobName", "JobName")
            enabled = payload_bool("enabled", "Enabled")
            if not job_name or enabled is None:
                return {
                    "success": False,
                    "errorCode": "invalid_payload",
                    "errorMessage": "jobName/enabled required",
                }
            updated = await self.admin.set_scheduled_job_enabled(job_name.strip(), enabled)
            if updated:
                return {"success": True}
            return {
                "success": False,
                "errorCode": "schedule_not_found",
                "errorMessage": "Scheduled job not found",
            }

        if command_type == "run_schedule_now":
            job_name = payload_string("jobName", "JobName")
            if not job_name:
                return {
                    "success": False,
                    "errorCode": "invalid_payload",
                    "errorMessage": "jobName required",
                }
            run_id = await self.admin.run_scheduled_job_now(job_name.strip())
            if run_id:
                return {"success": True, "runId": run_id}
            return {
                "success": False,
                "errorCode": "schedule_not_found",
                "errorMessage": "Scheduled job not found",
            }

        if command_type == "update_schedule_cron":
            job_name = payload_string("jobName", "JobName")
            cron_expression = payload_string("cronExpression", "CronExpression")
            time_zone = payload_string("timeZone", "TimeZone")
            if not job_name or not cron_expression or not time_zone:
                return {
                    "success": False,
                    "errorCode": "invalid_payload",
                    "errorMessage": "jobName/cronExpression/timeZone required",
                }
            updated = await self.admin.update_scheduled_job_cron(
                job_name.strip(), cron_expression.strip(), time_zone.strip()
            )
            if updated:
                return {"success": True}
            return {
                "success": False,
                "errorCode": "schedule_not_found",
                "errorMessage": "Scheduled job not found",
            }

        return {
            "success": False,
            "errorCode": "unsupported_command_type",
            "errorMessage": f"Unsupported command type '{command_type}'",
        }

    async def _post_with_retry(self, payload_json: str) -> HttpResponseData | None:
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
                        body=payload_json,
                    ),
                    timeout_seconds=30,
                )
            except Exception as exc:  # noqa: BLE001
                if attempt >= max_attempts:
                    _logger.warning("Runtime-control sync failed after request retries: %s", str(exc))
                    return None
                await asyncio.sleep(_compute_backoff_delay(attempt))
                continue

            if 200 <= response.status < 300:
                return response
            if response.status in {401, 403}:
                snippet = _response_snippet(response.body_text)
                if snippet:
                    _logger.warning(
                        "Runtime-control authorization failed with status %s: %s",
                        response.status,
                        snippet,
                    )
                else:
                    _logger.warning("Runtime-control authorization failed with status %s", response.status)
                return None
            if not is_transient_status(response.status) or attempt >= max_attempts:
                _logger.warning("Runtime-control sync rejected with status %s", response.status)
                return None

            await asyncio.sleep(_compute_backoff_delay(attempt))

        return None

    async def _run_loop(self) -> None:
        interval = with_jitter(
            self.options.eventing.runtime_control_sync_interval.total_seconds(),
            self.options.eventing.runtime_control_sync_jitter_enabled,
            self.options.eventing.runtime_control_sync_jitter_ratio,
        )
        if interval > 0:
            await asyncio.sleep(interval)

        while self._running:
            try:
                await self.sync_once()
            except Exception as exc:  # noqa: BLE001
                _logger.warning("Runtime-control sync cycle failed: %s", str(exc))
                await asyncio.sleep(2)

            interval = with_jitter(
                self.options.eventing.runtime_control_sync_interval.total_seconds(),
                self.options.eventing.runtime_control_sync_jitter_enabled,
                self.options.eventing.runtime_control_sync_jitter_ratio,
            )
            await asyncio.sleep(max(0.0, interval))

    def _resolve_endpoint(self) -> str:
        endpoint = urljoin(self.options.eventing.ingestion_api_base_url, self.options.eventing.runtime_control_sync_path)
        assert_secure_endpoint(endpoint, "eventing.ingestion_api_base_url")
        return endpoint

    def _has_credentials(self) -> bool:
        return bool(self.options.eventing.tenant_id and self.options.eventing.client_secret)

    @staticmethod
    def parse_response(body: str) -> list[RuntimeCommandEnvelope] | None:
        parsed = _safe_json_object(body)
        commands_raw = parsed.get("commands")
        if commands_raw is None:
            commands_raw = parsed.get("Commands")
        if not isinstance(commands_raw, list):
            return []

        commands: list[RuntimeCommandEnvelope] = []
        for item in commands_raw:
            if not isinstance(item, dict):
                continue

            command_id = _pick_string(item, "commandId", "CommandId")
            command_type = _pick_string(item, "commandType", "CommandType")
            payload_json = _pick_string(item, "payloadJson", "PayloadJson") or "{}"
            issued_at = _pick_datetime(item, "issuedAtUtc", "IssuedAtUtc") or datetime.now(tz=UTC)
            expires_at = _pick_datetime(item, "expiresAtUtc", "ExpiresAtUtc", required=False)

            if not command_id or command_type is None:
                continue

            commands.append(
                RuntimeCommandEnvelope(
                    command_id=command_id,
                    command_type=cast(RuntimeCommandType, command_type),
                    payload_json=payload_json,
                    issued_at_utc=issued_at,
                    expires_at_utc=expires_at,
                )
            )
        return commands


def _safe_json_object(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    if isinstance(parsed, dict):
        return parsed
    return {}


def _pick_string(obj: dict[str, Any], camel: str, pascal: str) -> str | None:
    value = obj.get(camel)
    if isinstance(value, str):
        return value
    value = obj.get(pascal)
    if isinstance(value, str):
        return value
    return None


def _pick_datetime(
    obj: dict[str, Any],
    camel: str,
    pascal: str,
    required: bool = True,
) -> datetime | None:
    text = _pick_string(obj, camel, pascal)
    if text is None:
        return datetime.now(tz=UTC) if required else None
    try:
        return datetime.fromisoformat(text).astimezone(UTC)
    except ValueError:
        return datetime.now(tz=UTC) if required else None


def _compute_backoff_delay(attempt: int) -> float:
    bounded = max(1, attempt)
    base_ms = min(30_000, 500 * (2 ** max(0, bounded - 1)))
    jitter_ms = int((attempt * 7) % 251)
    return float(base_ms + jitter_ms) / 1000.0
