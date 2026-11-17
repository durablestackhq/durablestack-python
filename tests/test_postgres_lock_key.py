from __future__ import annotations

import hashlib

from durablestack.postgres.migrator import _lock_key, _migration_lock_name

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


def test_lock_key_fits_postgres_bigint_for_default_and_test_prefixes() -> None:
    prefixes = [None, "", "durable_stack", "py_it_", "py_rt_", "py_rec_"]
    for prefix in prefixes:
        key = _lock_key(_migration_lock_name(prefix))
        assert INT64_MIN <= key <= INT64_MAX


def test_lock_key_is_signed_int64_interpretation_of_sha256_prefix() -> None:
    name = _migration_lock_name("durable_stack")
    expected = int.from_bytes(hashlib.sha256(name.encode("utf-8")).digest()[:8], "big", signed=True)
    assert _lock_key(name) == expected
    unsigned = int.from_bytes(hashlib.sha256(name.encode("utf-8")).digest()[:8], "big", signed=False)
    assert unsigned > INT64_MAX
