import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_setnx():
    key = "mykey"
    value = "myvalue"

    result = redis.setnx(key, value)

    assert result is True
    assert redis.get(key) == value

    # Try setting the key again
    result = redis.setnx(key, "newvalue")
    assert result is False
    assert redis.get(key) == value


def test_setnx_without_formatting():
    redis._format_return = False

    key = "mykey"
    value = "myvalue"

    result = redis.setnx(key, value)

    assert result is 1

    result = redis.setnx(key, "newvalue")
    assert result is 0

    redis._format_return = True
