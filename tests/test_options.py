from datetime import timedelta
from typing import Any, cast

import pytest

from durablestack.core.options import DurableStackOptions, RetryOptions, normalize_options


def test_normalize_options_accepts_defaults() -> None:
    options = normalize_options(None)
    assert options.max_concurrent_runs > 0
    assert options.claim_batch_size > 0


def test_normalize_options_rejects_invalid_concurrency() -> None:
    with pytest.raises(ValueError):
        normalize_options(DurableStackOptions(max_concurrent_runs=0))


def test_normalize_options_rejects_negative_retry_delay() -> None:
    with pytest.raises(ValueError):
        normalize_options(
            DurableStackOptions(retry=RetryOptions(max_attempts=2, delay=timedelta(seconds=-1)))
        )


def test_normalize_options_rejects_invalid_retry_behavior() -> None:
    with pytest.raises(ValueError):
        normalize_options(
            DurableStackOptions(
                retry=RetryOptions(
                    max_attempts=2,
                    behavior=cast(Any, "invalid"),
                    delay=timedelta(seconds=1),
                )
            )
        )
