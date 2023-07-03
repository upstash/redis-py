import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_keys():
    keys = ["key1", "key2", "key3"]
    redis.delete(*keys)
    yield
    redis.delete(*keys)


def test_mget():
    keys = ["key1", "key2", "key3"]
    values = ["value1", "value2", "value3"]

    for key, value in zip(keys, values):
        redis.set(key, value)

    result = redis.mget(*keys)
    assert result == values


def test_mget_non_existing():
    keys = ["non_existing_key", "non_existing_key2"]

    result = redis.mget(*keys)
    assert result == [None, None]
