import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_incr(redis: Redis):
    key = "mykey"
    initial_value = 5

    redis.set(key, initial_value)

    result = redis.incr(key)
    assert result == initial_value + 1

    updated_value = redis.get(key)
    assert updated_value == str(initial_value + 1)
