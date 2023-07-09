import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)


def test_hsetnx(redis: Redis):
    hash_name = "myhash"

    result = redis.hsetnx(hash_name, "field1", "value1")
    assert result is True
    assert redis.hget(hash_name, "field1") == "value1"

    result = redis.hsetnx(hash_name, "field1", "new_value")
    assert result is False
    assert redis.hget(hash_name, "field1") == "value1"
