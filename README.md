# durablestack-python

DurableStack Python runtime (in active development): durable background jobs and recurring scheduling with platform-compatible contracts.

## Status

This package has completed Phase 4 (provider parity).

Implemented so far:

- Phase 0: scaffold, contracts/constants, validators baseline, docs
- Phase 1: in-memory runtime/store with lease fencing, retries, recurring materialization, retention, parity tests
- Phase 2: PostgreSQL provider, migrations, schema verification, Postgres store/runtime factory, integration tests
- Phase 3: hosted ingestion + runtime-control sync services with contract validators and golden payload fixtures
- Phase 4: SQLite, MySQL, and SQL Server providers implemented with shared contract coverage

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

Run MySQL integration tests (optional):

```bash
set DURABLESTACK_TEST_MYSQL_DSN=mysql://user:pass@localhost:3306/dbname
pytest -q tests/test_mysql_integration.py
```

Run SQL Server integration tests (optional):

```bash
set DURABLESTACK_TEST_SQLSERVER_DSN=Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=durablestack;Uid=sa;Pwd=Your_password123;Encrypt=no;TrustServerCertificate=yes;
pytest -q tests/test_sqlserver_integration.py
```

## Provider matrix

| Provider | Runtime factory | Driver dependency | Integration env var | Integration test |
| --- | --- | --- | --- | --- |
| InMemory | `create_durable_stack(...)` | none | none | `tests/test_runtime_phase1.py` |
| PostgreSQL | `create_durable_stack_postgres(...)` | `asyncpg` | `DURABLESTACK_TEST_POSTGRES_DSN` | `tests/test_postgres_integration.py` |
| SQLite | `create_durable_stack_sqlite(...)` | stdlib `sqlite3` | none | `tests/test_sqlite_integration.py` |
| MySQL | `create_durable_stack_mysql(...)` | `pymysql` | `DURABLESTACK_TEST_MYSQL_DSN` | `tests/test_mysql_integration.py` |
| SQL Server | `create_durable_stack_sqlserver(...)` | `pyodbc` | `DURABLESTACK_TEST_SQLSERVER_DSN` | `tests/test_sqlserver_integration.py` |
