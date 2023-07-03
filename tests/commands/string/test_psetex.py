import time
import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_psetex():
    key = "mykey"
    value = "myvalue"
    expiration_ms = 1000

    result = redis.psetex(key, expiration_ms, value)

    assert result is True
    assert redis.get(key) == value
    assert redis.pttl(key) > 0

    # Wait for the key to expire
    time.sleep(2)

    assert redis.get(key) is None


def test_psetex_without_formatting():
    redis.format_return = False
    key = "mykey"
    value = "myvalue"
    expiration_ms = 1000

    result = redis.psetex(key, expiration_ms, value)

    assert result == "OK"

    redis.format_return = True
