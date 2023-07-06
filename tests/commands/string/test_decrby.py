import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)


def test_decrby(redis: Redis):
    key = "mykey"
    start_value = 10
    decrement_value = 3

    redis.set(key, start_value)

    result = redis.decrby(key, decrement_value)
    assert result == start_value - decrement_value

    final_value = redis.get(key)
    assert final_value == str(start_value - decrement_value)
