import pytest
import time
from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)


def test_hpexpire_sets_expiry_in_milliseconds(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    assert redis.hpexpire(hash_name, field, 500) == [1]

    time.sleep(1)
    assert redis.hget(hash_name, field) is None


def test_hpexpire_does_not_set_expiry_if_field_does_not_exist(redis: Redis):
    hash_name = "myhash"
    field = "field1"

    assert redis.hpexpire(hash_name, field, 500) == [-2]


def test_hpexpire_overwrites_existing_expiry(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    redis.hpexpire(hash_name, field, 1000)
    assert redis.hpexpire(hash_name, field, 2000) == [1]

    time.sleep(2.5)
    assert redis.hget(hash_name, field) is None