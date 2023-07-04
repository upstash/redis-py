import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_incr():
    key = "mykey"
    initial_value = 5

    redis.set(key, initial_value)

    result = redis.incr(key)
    assert result == initial_value + 1

    updated_value = redis.get(key)
    assert updated_value == str(initial_value + 1)
