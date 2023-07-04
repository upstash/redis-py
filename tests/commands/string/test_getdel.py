import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)


def test_getdel():
    key = "mykey"
    value = "Hello, Redis!"

    redis.set(key, value)

    result = redis.get(key)
    assert result == value

    result = redis.getdel(key)
    assert result == value

    assert redis.get(key) is None


def test_getdel_none():
    key = "non_existing_mykey"

    assert redis.getdel(key) is None
