import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)


def test_incrby(redis: Redis):
    key = "mykey"
    initial_value = 5
    increment = 2

    redis.set(key, initial_value)

    result = redis.incrby(key, increment)
    assert result == initial_value + increment

    updated_value = redis.get(key)
    assert updated_value == str(initial_value + increment)
