import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)


def test_append():
    key = "mykey"
    value = "Hello, "
    append_value = "world!"

    result = redis.append(key, value)
    assert result == len(value)

    result = redis.append(key, append_value)
    assert result == len(value) + len(append_value)

    final_value = redis.get(key)
    assert final_value == value + append_value
