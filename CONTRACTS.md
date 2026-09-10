# DurableStack Python Contracts (Phase 3)

## Run Status Vocabulary

- `pending`
- `leased`
- `succeeded`
- `failed`

## Event Types

- `job_claimed`
- `job_started`
- `job_succeeded`
- `job_failed`
- `job_retried`
- `retry_scheduled`
- `worker_heartbeat`

## Event Version

- `2`

## Runtime-Control Command Types

- `set_schedule_enabled`
- `run_schedule_now`
- `update_schedule_cron`

## Runtime-Control Receipt Status Values

- `leased`
- `acknowledged`
- `succeeded`
- `failed`

## External Payload Contract Policy

- Field names and value semantics align with current platform-facing .NET payloads.
- Validators are strict for required fields and permissive for nullable optional fields.
- Breaking payload changes require coordinated runtime + platform versioning.

## Telemetry Ingestion DTOs

### Telemetry Event DTO

Required fields:

- `eventType` (string, one of contract event types)
- `eventVersion` (int, current value `2`)
- `occurredAtUtc` (ISO-8601 UTC timestamp)
- `workerName` (non-empty string)
- `runtime` (non-empty string)
- `runtimeVersion` (non-empty string)

Conditionally required for run-scoped events:

- `runId` (non-empty string)
- `jobName` (non-empty string)
- `attempt` (int, >= 1)
- `maxAttempts` (int, >= 1)

Optional fields:

- `durationMs` (int >= 0)
- `errorType` (string)
- `errorMessage` (string)
- `payloadJson` (string)

### Telemetry Batch Request DTO

Required fields:

- `tenantId` (non-empty string)
- `idempotencyKey` (non-empty string)
- `events` (non-empty array of telemetry event DTO)

Optional fields:

- `serviceName` (string)

## Runtime-Control DTOs

### Runtime-Control Sync Request DTO

Required fields:

- `tenantId` (non-empty string)
- `workerName` (non-empty string)
- `runtime` (non-empty string)
- `runtimeVersion` (non-empty string)
- `sentAtUtc` (ISO-8601 UTC timestamp)
- `snapshotItems` (array)
- `receipts` (array)

`snapshotItems` item fields:

- `jobName` (non-empty string)
- `cronExpression` (non-empty string)
- `timeZone` (non-empty string)
- `enabled` (boolean)
- `nextRunAtUtc` (ISO-8601 UTC timestamp)
- `maxAttempts` (int >= 1)
- `allowConcurrentRuns` (boolean)
- `lastSeenAtUtc` (ISO-8601 UTC timestamp)

`receipts` item fields:

- `commandId` (non-empty string)
- `status` (one of: `leased`, `acknowledged`, `succeeded`, `failed`)
- `recordedAtUtc` (ISO-8601 UTC timestamp)

Optional receipt fields:

- `completedAtUtc` (ISO-8601 UTC timestamp)
- `runId` (string)
- `errorCode` (string)
- `errorMessage` (string)

### Runtime-Control Sync Response DTO

Top-level fields:

- `commands` or `Commands` (array of command envelope)

Command envelope fields (camelCase or PascalCase accepted by parser):

- `commandId`/`CommandId` (non-empty string)
- `commandType`/`CommandType` (one of runtime-control command types)
- `payloadJson`/`PayloadJson` (string JSON object)
- `issuedAtUtc`/`IssuedAtUtc` (ISO-8601 UTC timestamp)
- `expiresAtUtc`/`ExpiresAtUtc` (optional ISO-8601 UTC timestamp)

### Runtime-Control Command Payload Shapes

- `set_schedule_enabled`
  - required: `jobName` (or `JobName`), `enabled` (or `Enabled`)
- `run_schedule_now`
  - required: `jobName` (or `JobName`)
- `update_schedule_cron`
  - required: `jobName`/`JobName`, `cronExpression`/`CronExpression`, `timeZone`/`TimeZone`

## Error/Retry Policy Contracts

- 2xx responses are treated as success.
- 401/403 are treated as terminal auth failures without retry.
- Transient statuses (`408`, `409`, `425`, `429`, `5xx`) are retried with bounded attempts and backoff.
- Non-transient non-2xx statuses are terminal for that sync attempt.
- Sync services include correlation ID headers and must include tenant/client-secret headers when credentials are configured.

## Schema and Runtime Boundaries

- Runtime schemas are runtime-specific by design.
- Python and .NET runtimes may share a DB instance but must use separate tables/prefixes.
- Startup must verify that existing tables at the configured prefix match Python runtime schema expectations.
