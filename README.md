# durablestack-python

DurableStack Python runtime (in active development): durable background jobs and recurring scheduling with platform-compatible contracts.

## Status

This package is currently in Phase 4 (provider parity).

Implemented so far:

- Phase 0: scaffold, contracts/constants, validators baseline, docs
- Phase 1: in-memory runtime/store with lease fencing, retries, recurring materialization, retention, parity tests
- Phase 2: PostgreSQL provider, migrations, schema verification, Postgres store/runtime factory, integration tests
- Phase 3: hosted ingestion + runtime-control sync services with contract validators and golden payload fixtures
- Phase 4 (in progress): SQLite provider implemented; MySQL and SQL Server provider scaffolds added

Observability note:

- We are explicitly applying lessons from the Node.js rollout around hosted API sync (tenant/client-secret auth, endpoint safety, retry policy, idempotency, shutdown flush). The Python ingestion sync foundation now includes these guardrails and dedicated tests.

## Key documents

- `ARCHITECTURE.md`
- `CONTRACTS.md`
- `PROVIDER_PARITY.md`

## Local development

Python 3.11+ is required.

Install dev dependencies:

```bash
pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```

Run lint + typing:

```bash
ruff check .
mypy
```

Run Postgres integration tests (optional):

```bash
set DURABLESTACK_TEST_POSTGRES_DSN=postgresql://user:pass@localhost:5432/dbname
pytest -q tests/test_postgres_integration.py
```

Run SQLite integration tests:

```bash
pytest -q tests/test_sqlite_integration.py
```
