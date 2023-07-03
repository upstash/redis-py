import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_all():
    redis.flushall()


def test_flushall():
    redis.set("key1", "value1")
    redis.set("key2", "value2")
    redis.set("key3", "value3")

    result = redis.flushall()
    assert result is True

    assert redis.get("key1") is None
    assert redis.get("key2") is None
    assert redis.get("key3") is None


def test_flushall_without_formatting():
    redis.format_return = False

    result = redis.flushall()
    assert result is "OK"

    redis.format_return = True
