import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_strlen(redis: Redis):
    key = "mykey"
    value = "Hello, World!"

    redis.set(key, value)

    result = redis.strlen(key)

    assert result == 13  # Length of the string
