import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)


def test_append(redis: Redis):
    key = "mykey"
    value = "Hello, "
    append_value = "world!"

    result = redis.append(key, value)
    assert result == len(value)

    result = redis.append(key, append_value)
    assert result == len(value) + len(append_value)

    final_value = redis.get(key)
    assert final_value == value + append_value
