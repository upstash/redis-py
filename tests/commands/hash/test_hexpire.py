import pytest
import time
from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)


def test_hexpire_expires_hash_key(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    assert redis.hexpire(hash_name, field, 1) == [1]

    time.sleep(2)
    assert redis.hget(hash_name, field) is None


def test_hexpire_nx_sets_expiry_if_no_expiry(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    assert redis.hexpire(hash_name, field, 1, "NX") == [1]

    time.sleep(2)
    assert redis.hget(hash_name, field) is None


def test_hexpire_nx_does_not_set_expiry_if_already_exists(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    redis.hexpire(hash_name, field, 1000)
    assert redis.hexpire(hash_name, field, 1, "NX") == [0]


def test_hexpire_xx_sets_expiry_if_exists(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    redis.hexpire(hash_name, field, 1)
    assert redis.hexpire(hash_name, [field], 5, "XX") == [1]

    time.sleep(6)
    assert redis.hget(hash_name, field) is None


def test_hexpire_xx_does_not_set_expiry_if_not_exists(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    assert redis.hexpire(hash_name, field, 5, "XX") == [0]


def test_hexpire_gt_sets_expiry_if_new_greater(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    redis.hexpire(hash_name, field, 1)
    assert redis.hexpire(hash_name, field, 5, "GT") == [1]

    time.sleep(6)
    assert redis.hget(hash_name, field) is None


def test_hexpire_gt_does_not_set_if_new_not_greater(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    redis.hexpire(hash_name, field, 10)
    assert redis.hexpire(hash_name, field, 5, "GT") == [0]


def test_hexpire_lt_sets_expiry_if_new_less(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    redis.hexpire(hash_name, [field], 5)
    assert redis.hexpire(hash_name, field, 3, "LT") == [1]

    time.sleep(4)
    assert redis.hget(hash_name, field) is None


def test_hexpire_lt_does_not_set_if_new_not_less(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    redis.hexpire(hash_name, field, 10)
    assert redis.hexpire(hash_name, field, 20, "LT") == [0]


def test_hexpire_returns_minus2_if_field_does_not_exist(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    assert redis.hexpire(hash_name, field, 1) == [-2]


def test_hexpire_returns_minus2_if_hash_does_not_exist(redis: Redis):
    assert redis.hexpire("nonexistent_hash", "field1", 1) == [-2]


def test_hexpire_returns_2_when_called_with_zero_seconds(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    assert redis.hexpire(hash_name, field, 0) == [2]
    assert redis.hget(hash_name, field) is None
