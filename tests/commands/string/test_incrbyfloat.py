import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_incrbyfloat(redis: Redis):
    key = "mykey"
    initial_value = 3.14
    increment = 1.23

    redis.set(key, initial_value)

    result = redis.incrbyfloat(key, increment)
    assert isinstance(result, float)

    assert result == pytest.approx(initial_value + increment)


def test_incrbyfloat_without_formatting(redis: Redis):
    redis._format_return = False

    key = "mykey"
    initial_value = 3.14
    increment = 1.23

    redis.set(key, initial_value)

    result = redis.incrbyfloat(key, increment)
    assert isinstance(result, str)
