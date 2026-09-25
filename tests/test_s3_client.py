"""Tests for the S3 client's connection-pool sizing.

No network: `make_client` is a pure config builder, so only the returned client's
config is asserted. The pool must grow with `--workers`, because a tar listing
issues one ranged GET per member and botocore's default pool of 10 makes urllib3
discard and re-establish connections ("Connection pool is full").
"""

from typing import Any

from botocore import UNSIGNED

from dataset_manager.s3 import make_client


def client_config(*, sign: bool = False, workers: int) -> Any:
    """The `Config` botocore stores on the client `make_client` builds.

    `_client_config` is the only way to read the built client's settings back;
    botocore exposes it at runtime but does not declare it for type checkers.
    """
    client: Any = make_client(sign=sign, workers=workers)
    return client._client_config


def test_the_pool_scales_with_the_worker_count() -> None:
    for workers in (1, 8, 64):
        assert client_config(workers=workers).max_pool_connections == max(10, workers * 2)


def test_the_retry_policy_and_unsigned_reads_survive_pool_sizing() -> None:
    unsigned = client_config(workers=8)
    assert unsigned.retries == {"mode": "standard", "total_max_attempts": 10}
    assert unsigned.signature_version is UNSIGNED

    assert client_config(sign=True, workers=8).signature_version is not UNSIGNED
