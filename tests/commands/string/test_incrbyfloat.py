import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)


def test_incrbyfloat():
    key = "mykey"
    initial_value = 3.14
    increment = 1.23

    redis.set(key, initial_value)

    result = redis.incrbyfloat(key, increment)
    assert isinstance(result, float)

    assert result == pytest.approx(initial_value + increment)


def test_incrbyfloat_without_formatting():
    redis.format_return = False

    key = "mykey"
    initial_value = 3.14
    increment = 1.23

    redis.set(key, initial_value)

    result = redis.incrbyfloat(key, increment)
    assert isinstance(result, str)

    redis.format_return = True
