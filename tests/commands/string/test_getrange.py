import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_getrange():
    key = "mykey"
    value = "Hello, Redis!"

    redis.set(key, value)

    result = redis.getrange(key, 7, 11)
    assert result == "Redis"

    result = redis.getrange(key, 7, -1)
    assert result == "Redis!"

    result = redis.getrange(key, 0, -1)
    assert result == value
