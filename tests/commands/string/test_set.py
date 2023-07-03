import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_set():
    key = "mykey"
    value = "myvalue"
    ex_seconds = 10

    result = redis.set(key, value, ex=ex_seconds)

    assert result is True
    assert redis.get(key) == value
    assert redis.ttl(key) == ex_seconds


def test_set_without_formatting():
    redis.format_return = False
    key = "mykey"
    value = "myvalue"
    ex_seconds = 10

    result = redis.set(key, value, ex=ex_seconds)

    assert result == "OK"

    redis.format_return = True


def test_set_invalid_parameters():
    key = "mykey"
    value = "myvalue"
    ex_seconds = 10

    with pytest.raises(Exception):
        redis.set(key, value, ex=ex_seconds, pxat=ex_seconds)
