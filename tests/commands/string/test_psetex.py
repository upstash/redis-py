import time

import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_psetex(redis: Redis):
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


def test_psetex_without_formatting(redis: Redis):
    redis._format_return = False
    key = "mykey"
    value = "myvalue"
    expiration_ms = 1000

    result = redis.psetex(key, expiration_ms, value)

    assert result == "OK"
