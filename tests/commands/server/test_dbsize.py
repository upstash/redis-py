import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_data():
    redis.flushdb()


def test_dbsize_empty():
    result = redis.dbsize()
    assert result == 0


def test_dbsize_nonempty():
    redis.set("key1", "value1")
    redis.set("key2", "value2")
    redis.set("key3", "value3")

    result = redis.dbsize()
    assert result == 3


def test_dbsize_after_deletion():
    redis.set("key1", "value1")
    redis.set("key2", "value2")
    redis.set("key3", "value3")

    redis.delete("key2")

    result = redis.dbsize()
    assert result == 2
