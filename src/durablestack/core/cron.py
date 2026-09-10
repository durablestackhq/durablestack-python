"""Cron/time zone helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast
from zoneinfo import ZoneInfo

from croniter import croniter


def validate_time_zone(time_zone: str) -> None:
    ZoneInfo(time_zone)


def get_next_occurrence_utc(cron_expression: str, time_zone: str, base_utc: datetime) -> datetime:
    if base_utc.tzinfo is None:
        raise ValueError("base_utc must be timezone-aware")

    zone = ZoneInfo(time_zone)
    base_local = base_utc.astimezone(zone)
    next_local = cast(datetime, croniter(cron_expression, base_local).get_next(datetime))
    if next_local.tzinfo is None:
        next_local = next_local.replace(tzinfo=zone)
    return next_local.astimezone(UTC)
