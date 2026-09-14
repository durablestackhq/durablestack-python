# DurableStack for Python

![DurableStack Logo](https://docs.durablestack.com/images/branding/logo-light-28.png)

DurableStack provides reliable background jobs and recurring scheduling for Python applications.

## Install

```bash
pip install durablestack
```

## Quick Start

```python
from datetime import datetime, UTC

from durablestack import create_durable_stack


async def send_email(payload: object) -> None:
    print("sending", payload)


async def cleanup_cache(payload: object) -> None:
    print("cleanup", payload)


runtime = create_durable_stack()
runtime.register_job("send-email", send_email)
runtime.register_recurring("cleanup-cache", "*/5 * * * *", "UTC", cleanup_cache)

await runtime.start()
run_id = await runtime.enqueue("send-email", {"to": "hello@example.com"})
print("enqueued", run_id)

recurring_run_id = await runtime.run_scheduled_job_now("cleanup-cache")
print("recurring run", recurring_run_id)

await runtime.stop()
```

## Providers

- InMemory
- PostgreSQL
- MySQL
- SQL Server
- SQLite

## Runtime Features

- Durable lifecycle with lease-based execution
- Retries with configurable backoff behavior
- Recurring schedules with IANA time zones
- Runtime-control command sync and receipt tracking
- Hosted telemetry ingestion support

## Optional Hosted Eventing / Runtime-Control

When tenant credentials are configured, the runtime can publish telemetry and receive runtime-control commands.

```python
from durablestack.core.options import DurableStackOptions, EventingOptions
from durablestack import create_durable_stack

runtime = create_durable_stack(
    DurableStackOptions(
        eventing=EventingOptions(
            tenant_id="<tenant-id>",
            client_secret="<client-secret>",
            service_name="my-python-worker",
        ),
        include_error_detail_in_events=True,
    )
)
```

## Project Links

- Homepage: https://durablestack.com
- Documentation: https://docs.durablestack.com
- Source: https://github.com/durablestack/durablestack-python
