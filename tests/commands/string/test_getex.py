import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_key(redis: Redis):
    key = "mykey"
    redis.delete(key)


def test_getex(redis: Redis):
    key = "mykey"
    value = "Hello, Redis!"

    redis.set(key, value)

    assert redis.getex(key) == value
    assert redis.ttl(key) == -1

    redis.getex(key, ex=10)
    assert redis.ttl(key) == 10

    redis.getex(key, persist=True)
    assert redis.ttl(key) == -1
