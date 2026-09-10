# durablestack-python

DurableStack Python runtime (in active development): durable background jobs and recurring scheduling with platform-compatible contracts.

## Status

This package is currently in Phase 0 (foundations and contract freeze).

Implemented in this phase:

- project/package scaffold,
- canonical constants for run/event/runtime-control contracts,
- core options/models/abstractions baseline,
- external contract validator baseline,
- architecture and contracts documentation.

## Key documents

- `ARCHITECTURE.md`
- `CONTRACTS.md`

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
