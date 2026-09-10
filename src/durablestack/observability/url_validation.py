"""Endpoint URL validation for hosted services."""

from __future__ import annotations

from urllib.parse import urlparse


def assert_secure_endpoint(url: str, option_name: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme == "https":
        return

    if parsed.scheme != "http":
        raise ValueError(f"{option_name} must use https (or loopback http for local dev)")

    host = (parsed.hostname or "").lower()
    if host == "localhost" or host == "127.0.0.1" or host == "::1":
        return
    if host.startswith("127."):
        return

    raise ValueError(f"{option_name} must use https for non-loopback hosts")
