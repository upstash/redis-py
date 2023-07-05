import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_data(redis: Redis):
    redis.flushdb()


def test_dbsize_empty(redis: Redis):
    result = redis.dbsize()
    assert result == 0


def test_dbsize_nonempty(redis: Redis):
    redis.set("key1", "value1")
    redis.set("key2", "value2")
    redis.set("key3", "value3")

    result = redis.dbsize()
    assert result == 3


def test_dbsize_after_deletion(redis: Redis):
    redis.set("key1", "value1")
    redis.set("key2", "value2")
    redis.set("key3", "value3")

    redis.delete("key2")

    result = redis.dbsize()
    assert result == 2
