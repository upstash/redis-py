import pytest
from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)


def test_hpersist_removes_expiry(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    redis.hexpire(hash_name, field, 500)
    ttl_before = redis.httl(hash_name, field)
    assert ttl_before[0] > 0

    redis.hpersist(hash_name, field)
    ttl_after = redis.httl(hash_name, field)
    assert ttl_after == [-1]


def test_hpersist_does_nothing_if_no_expiry(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1500"

    redis.hset(hash_name, field, value)
    assert redis.hpersist(hash_name, field) == [-1]
