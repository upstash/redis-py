import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_set(redis: Redis):
    key = "mykey"
    value = "myvalue"
    ex_seconds = 10

    result = redis.set(key, value, ex=ex_seconds)

    assert result is True
    assert redis.get(key) == value
    assert redis.ttl(key) == ex_seconds


def test_set_without_formatting(redis: Redis):
    redis._format_return = False
    key = "mykey"
    value = "myvalue"
    ex_seconds = 10

    result = redis.set(key, value, ex=ex_seconds)

    assert result == "OK"


def test_set_invalid_parameters(redis: Redis):
    key = "mykey"
    value = "myvalue"
    ex_seconds = 10

    with pytest.raises(Exception):
        redis.set(key, value, ex=ex_seconds, pxat=ex_seconds)
