import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_all(redis: Redis):
    redis.flushall()


def test_flushall(redis: Redis):
    redis.set("key1", "value1")
    redis.set("key2", "value2")
    redis.set("key3", "value3")

    result = redis.flushall()
    assert result is True

    assert redis.get("key1") is None
    assert redis.get("key2") is None
    assert redis.get("key3") is None


def test_flushall_without_formatting(redis: Redis):
    redis._format_return = False

    result = redis.flushall()
    assert result is "OK"
