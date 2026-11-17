# Changelog

## 1.2.0 - 2026-11-17

First stable release of the DurableStack Python runtime, versioned 1.2.0 to keep major.minor parity with the .NET and Node.js libraries.

### Fixed

- PostgreSQL migrations failed for some table prefixes, including the default `durable_stack`, because the advisory lock key could exceed PostgreSQL's signed `bigint` range.
- PostgreSQL job leases shorter than one second were rounded up to one second, so lease expiry did not match the other providers or the provider contract tests.

### Packaging

- Declared Production/Stable on PyPI.
- Added an MIT `LICENSE` file.
- Corrected GitHub repository URLs to the `durablestackhq` organization.
