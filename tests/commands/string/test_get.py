import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)


def test_get():
    key = "mykey"
    value = "Hello, Redis!"

    redis.set(key, value)

    result = redis.get(key)
    assert result == value


def test_get_none():
    key = "non_existing_mykey"

    assert redis.get(key) is None
