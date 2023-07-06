import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)


def test_setrange(redis: Redis):
    key = "mykey"
    value = "Hello, World!"

    redis.set(key, value)

    result = redis.setrange(key, 7, "Redis")

    assert result == 13

    expected_value = "Hello, Redis!"
    assert redis.get(key) == expected_value
