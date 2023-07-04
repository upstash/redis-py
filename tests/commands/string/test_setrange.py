import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)


def test_setrange():
    key = "mykey"
    value = "Hello, World!"

    redis.set(key, value)

    result = redis.setrange(key, 7, "Redis")

    assert result == 13

    expected_value = "Hello, Redis!"
    assert redis.get(key) == expected_value
