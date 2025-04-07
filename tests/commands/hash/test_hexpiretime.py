import pytest
import time
from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)


def test_hexpiretime_returns_correct_expiry(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    redis.hexpire(hash_name, field, 5)
    expiry_time = redis.hexpiretime(hash_name, field)[0]

    assert expiry_time > int(time.time())
    assert expiry_time <= int(time.time()) + 5


def test_hexpiretime_returns_minus1_if_no_expiry(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    assert redis.hexpiretime(hash_name, field)[0] == -1


def test_hexpiretime_returns_minus2_if_field_does_not_exist(redis: Redis):
    hash_name = "myhash"
    field = "field1"

    assert redis.hexpiretime(hash_name, field)[0] == -2