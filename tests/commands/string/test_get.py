import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)


def test_get(redis: Redis):
    key = "mykey"
    value = "Hello, Redis!"

    redis.set(key, value)

    result = redis.get(key)
    assert result == value


def test_get_none(redis: Redis):
    key = "non_existing_mykey"

    assert redis.get(key) is None
