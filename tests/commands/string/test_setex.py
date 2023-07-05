import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_setex(redis: Redis):
    key = "mykey"
    value = "myvalue"
    ex_seconds = 10

    result = redis.setex(key, ex_seconds, value)

    assert result is True
    assert redis.get(key) == value
    assert redis.ttl(key) == ex_seconds


def test_setex_without_formatting(redis: Redis):
    redis._format_return = False
    key = "mykey"
    value = "myvalue"
    ex_seconds = 10

    result = redis.setex(key, ex_seconds, value)

    assert result == "OK"
