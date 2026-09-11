# Releasing DurableStack Python

This guide covers local pre-publish validation for beta and stable releases.

## Versioning

- Use PEP 440 versioning in `pyproject.toml`.
- Beta example: `0.1.0b1`.
- Next beta: increment beta number (`0.1.0b2`, `0.1.0b3`, ...).

## Pre-publish quality gates

Run from `durablestack-python/`:

```bash
python -m ruff check .
python -m mypy
python -m pytest -q
```

## Build and package validation

Run from `durablestack-python/`:

```bash
python -m build
python -m twine check dist/*
```

Expected artifacts:

- `dist/durablestack-<version>-py3-none-any.whl`
- `dist/durablestack-<version>.tar.gz`

If `twine check` passes and tests are green, the package is ready for CI-driven publish.

## CI-driven publish on `main`

This repo includes:

- `.github/workflows/ci.yml`: PR/push quality gates
- `.github/workflows/publish-pypi.yml`: publish pipeline for `main`

Required GitHub secret:

- `PYPI_API_TOKEN`: API token from PyPI account settings

Publish behavior:

1. Push to `main` with an updated `project.version` in `pyproject.toml`.
2. Workflow runs quality gates, build, and `twine check`.
3. Workflow checks whether that exact version already exists on PyPI.
4. If version is new, it publishes `dist/*` to PyPI.
5. If version already exists, it exits without publishing.
