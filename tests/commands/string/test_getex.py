import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)


def test_getex():
    key = "mykey"
    value = "Hello, Redis!"

    redis.set(key, value)

    assert redis.getex(key) == value
    assert redis.ttl(key) == -1

    redis.getex(key, ex=10)
    assert redis.ttl(key) == 10

    redis.getex(key, persist=True)
    assert redis.ttl(key) == -1
