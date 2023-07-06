import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_keys(redis: Redis):
    keys = ["key1", "key2", "key3"]
    redis.delete(*keys)
    yield
    redis.delete(*keys)


def test_msetnx_all_keys_do_not_exist(redis: Redis):
    key_value_pairs = {"key1": "value1", "key2": "value2", "key3": "value3"}

    result = redis.msetnx(key_value_pairs)

    assert result is True

    for key, value in key_value_pairs.items():
        assert redis.get(key) == value


def test_msetnx_some_keys_exist(redis: Redis):
    key_value_pairs = {"key1": "value1", "key2": "value2", "key3": "value3"}

    redis.set("key2", "existing_value")

    result = redis.msetnx(key_value_pairs)

    assert result is False
    assert redis.get("key1") is None
    assert redis.get("key2") == "existing_value"
    assert redis.get("key3") is None
