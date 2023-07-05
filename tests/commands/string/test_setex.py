import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_setex():
    key = "mykey"
    value = "myvalue"
    ex_seconds = 10

    result = redis.setex(key, ex_seconds, value)

    assert result is True
    assert redis.get(key) == value
    assert redis.ttl(key) == ex_seconds


def test_setex_without_formatting():
    redis._format_return = False
    key = "mykey"
    value = "myvalue"
    ex_seconds = 10

    result = redis.setex(key, ex_seconds, value)

    assert result == "OK"

    redis._format_return = True
