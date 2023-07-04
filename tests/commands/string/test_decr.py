import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)


def test_decr():
    key = "mykey"
    start_value = 10

    redis.set(key, start_value)

    result = redis.decr(key)
    assert result == start_value - 1

    final_value = redis.get(key)
    assert final_value == str(start_value - 1)
