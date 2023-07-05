import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)


def test_getdel(redis: Redis):
    key = "mykey"
    value = "Hello, Redis!"

    redis.set(key, value)

    result = redis.get(key)
    assert result == value

    result = redis.getdel(key)
    assert result == value

    assert redis.get(key) is None


def test_getdel_none(redis: Redis):
    key = "non_existing_mykey"

    assert redis.getdel(key) is None
