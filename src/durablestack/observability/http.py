"""HTTP primitives for observability sync services."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class HttpResponseData:
    status: int
    body_text: str


@dataclass(frozen=True, slots=True)
class HttpPostRequest:
    url: str
    headers: Mapping[str, str]
    body: str


class HttpPost(Protocol):
    async def __call__(self, request: HttpPostRequest, timeout_seconds: float) -> HttpResponseData: ...


def is_transient_status(status: int) -> bool:
    return status in {408, 409, 425, 429} or status >= 500
