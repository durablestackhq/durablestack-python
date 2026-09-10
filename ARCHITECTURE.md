# DurableStack Python Architecture

## Goals

- Match DurableStack runtime semantics defined by the .NET runtime for run lifecycle, leasing, retries, recurring scheduling, and retention.
- Preserve platform-facing contract compatibility for telemetry/event ingestion and runtime-control sync.
- Provide a Python-idiomatic runtime that is async-first and production-safe.

## Source of Truth and Compatibility Rules

- .NET behavior is canonical when there is any ambiguity.
- Node.js behavior is a secondary parity reference where it intentionally mirrors .NET.
- Cross-runtime worker execution is not supported: Python workers execute Python handlers only.
- Cross-runtime external contracts are required: payload fields, status values, and command semantics must match hosted platform expectations.

## Runtime Shape (Phase 0 Contract)

- Runtime API surface is explicit and lifecycle-driven (`start`, `stop`, registration, enqueue/schedule/query/admin operations).
- Processor contract separates orchestration from storage provider specifics.
- Storage operations are abstracted via a provider interface that all durable backends must implement.
- Event sink contract is pluggable and independent of run-state persistence.
- Runtime-control sync contract is pluggable and uses durable command receipts.

## Phase 0 Deliverables

- Canonical constants for:
  - run statuses,
  - event types and event version,
  - runtime-control command and receipt status values.
- Options model and normalization entrypoint.
- Core abstraction layer (runtime, processor, client/admin/query services, job store, event sink).
- External DTO models and validators for telemetry and runtime-control payloads.
- Package scaffold and test scaffold.

## Planned Runtime Semantics (Parity Targets)

1. Run lifecycle states: `pending` -> `leased` -> `succeeded|failed`.
2. Lease-based distributed claiming and heartbeat extension while executing.
3. Lease-fenced completion writes.
4. Retry scheduling with max-attempt terminal behavior.
5. Recurring materialization with IANA timezone handling and slot uniqueness semantics.
6. Retention that prunes only terminal runs.
7. Runtime-control sync and command receipt lifecycle semantics.
8. Event payload compatibility with hosted ingestion APIs.

## Design Notes

- Runtime implementation will be `asyncio`-first.
- Handlers will support both async and sync callables (sync handlers executed via an executor).
- Startup must include schema migration and schema verification when durable providers are used.
- Runtime-specific schema separation is intentional. Shared DB instances are allowed; shared runtime tables are not.

## Non-goals for Phase 0

- Durable provider implementations.
- Full runtime host loop and execution engine.
- Framework-specific integrations.
- Job autodiscovery.
