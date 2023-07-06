import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_keys(redis: Redis):
    keys = ["key1", "key2", "key3"]
    redis.delete(*keys)


def test_mset(redis: Redis):
    key_value_pairs = {"key1": "value1", "key2": "value2", "key3": "value3"}

    redis.mset(key_value_pairs)

    for key, value in key_value_pairs.items():
        assert redis.get(key) == value
