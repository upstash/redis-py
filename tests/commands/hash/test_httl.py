import pytest
from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)


def test_httl_returns_correct_ttl(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    redis.hexpire(hash_name, field, 5)
    ttl = redis.httl(hash_name, [field])[0]

    assert ttl > 0
    assert ttl <= 5


def test_httl_returns_minus1_if_no_expiry(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    assert redis.httl(hash_name, [field])[0] == -1


def test_httl_returns_minus2_if_field_does_not_exist(redis: Redis):
    hash_name = "myhash"
    field = "field1"

    assert redis.httl(hash_name, [field])[0] == -2
