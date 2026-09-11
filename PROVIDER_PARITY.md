# Provider Parity (Phase 4)

This document captures provider parity status and migration/locking expectations for the Python runtime.

## Current status

- PostgreSQL: implemented with migrations, schema probes, and lease-fenced semantics.
- SQLite: implemented with migrations, schema probes, and single-writer transaction locking (`BEGIN IMMEDIATE`).
- MySQL: implemented with migrations, schema probes, and lease-fenced semantics.
- SQL Server: implemented with migrations, schema probes, and lease-fenced semantics.

## Shared provider contract expectations

All providers must preserve these behaviors:

- Run lifecycle: `pending -> leased -> succeeded|failed`.
- Lease-fenced completion: stale worker completion writes are rejected.
- Retry semantics: failed runs can requeue to `pending`; terminal failure when max attempts reached.
- Recurring slot uniqueness: one run per (`job_name`, `schedule_slot_utc`) slot.
- Runtime-control receipts: receipt lease/ack/success/failure lifecycle with command-id dedupe.
- Retention cleanup: prune terminal runs only.

## Locking and migration notes

- PostgreSQL:
  - Migration lock: `pg_advisory_xact_lock` using stable key from table prefix.
  - Claim path: `FOR UPDATE SKIP LOCKED` candidate selection + fenced update.
- SQLite:
  - Migration lock: transactional lock via `BEGIN IMMEDIATE`.
  - Claim/materialization paths: `BEGIN IMMEDIATE` guarded read-update sequences.
  - Concurrency scope: safe for multi-worker processes sharing one DB file with SQLite single-writer model.
- MySQL:
  - Migration lock: named lock via `GET_LOCK` / `RELEASE_LOCK` keyed by table prefix.
  - Claim path: `FOR UPDATE SKIP LOCKED` candidate selection + fenced updates in transactions.
- SQL Server:
  - Migration lock: `sp_getapplock` with transaction-scoped exclusive lock keyed by table prefix.
  - Claim path: `UPDLOCK` + `READPAST` row selection and fenced updates.

## Test coverage in repo

- Postgres integration: `tests/test_postgres_integration.py` (env-gated).
- SQLite integration: `tests/test_sqlite_integration.py`.
- MySQL integration: `tests/test_mysql_integration.py` (env-gated).
- SQL Server integration: `tests/test_sqlserver_integration.py` (env-gated).
- Phase 4 provider scaffolds: `tests/test_provider_scaffold_phase4.py`.
