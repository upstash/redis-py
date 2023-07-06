import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_db(redis: Redis):
    redis.flushdb()


def test_flushdb(redis: Redis):
    redis.set("key1", "value1")
    redis.set("key2", "value2")
    redis.set("key3", "value3")

    result = redis.flushdb()
    assert result is True

    assert redis.get("key1") is None
    assert redis.get("key2") is None
    assert redis.get("key3") is None


def test_flushdb_without_formatting(redis: Redis):
    redis._format_return = False

    result = redis.flushdb()
    assert result is "OK"
