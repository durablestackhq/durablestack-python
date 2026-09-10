"""Shared runtime utility functions."""

from __future__ import annotations

import json
import random
import uuid
from datetime import UTC, datetime, timedelta
from typing import cast

from .models import JsonValue


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware and UTC")
    return value.astimezone(UTC)


def generate_id() -> str:
    return uuid.uuid4().hex


def serialize_payload(payload: JsonValue | None) -> str | None:
    if payload is None:
        return None
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def deserialize_payload(payload_json: str | None) -> JsonValue | None:
    if payload_json is None:
        return None
    value = json.loads(payload_json)
    return cast(JsonValue, value)


def with_jitter(base_seconds: float, enabled: bool, jitter_ratio: float) -> float:
    if not enabled or base_seconds <= 0:
        return max(0.0, base_seconds)

    spread = base_seconds * max(0.0, jitter_ratio)
    low = max(0.0, base_seconds - spread)
    high = base_seconds + spread
    return random.uniform(low, high)


def to_seconds(value: timedelta) -> float:
    return value.total_seconds()
