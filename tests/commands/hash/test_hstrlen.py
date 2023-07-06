import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)


def test_hstrlen(redis: Redis):
    hash_name = "myhash"
    redis.hmset(hash_name, {"first": "123456", "second": "123"})

    assert redis.hstrlen(hash_name, "first") == 6
    assert redis.hstrlen(hash_name, "second") == 3
