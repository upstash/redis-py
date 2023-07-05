import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_getrange(redis: Redis):
    key = "mykey"
    value = "Hello, Redis!"

    redis.set(key, value)

    result = redis.getrange(key, 7, 11)
    assert result == "Redis"

    result = redis.getrange(key, 7, -1)
    assert result == "Redis!"

    result = redis.getrange(key, 0, -1)
    assert result == value
