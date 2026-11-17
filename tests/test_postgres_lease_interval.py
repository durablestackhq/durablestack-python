from datetime import timedelta

from durablestack.postgres.store import _positive_lease_interval


def test_positive_lease_interval_preserves_subsecond_durations() -> None:
    assert _positive_lease_interval(timedelta(milliseconds=50)) == timedelta(milliseconds=50)
    assert _positive_lease_interval(timedelta(seconds=30)) == timedelta(seconds=30)


def test_positive_lease_interval_replaces_non_positive_with_one_second() -> None:
    assert _positive_lease_interval(timedelta(0)) == timedelta(seconds=1)
    assert _positive_lease_interval(timedelta(seconds=-1)) == timedelta(seconds=1)
