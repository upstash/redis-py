import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_getset(redis: Redis):
    key = "mykey"
    initial_value = "Hello"
    new_value = "Goodbye"

    redis.set(key, initial_value)

    result = redis.getset(key, new_value)
    assert result == initial_value

    updated_value = redis.get(key)
    assert updated_value == new_value
