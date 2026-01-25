import time

import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)
    yield
    redis.delete(hash_name)


def test_hsetex_single_field_with_ex(redis: Redis):
    hash_name = "myhash"
    field1 = "field1"
    value1 = "value1"

    # Set field with expiration in seconds
    result = redis.hsetex(hash_name, field1, value1, ex=2)

    assert result == 1
    assert redis.hget(hash_name, field1) == value1
    
    # Wait for expiration
    time.sleep(3)
    assert redis.hget(hash_name, field1) is None


def test_hsetex_single_field_with_px(redis: Redis):
    hash_name = "myhash"
    field1 = "field1"
    value1 = "value1"

    # Set field with expiration in milliseconds
    result = redis.hsetex(hash_name, field1, value1, px=2000)

    assert result == 1
    assert redis.hget(hash_name, field1) == value1
    
    # Wait for expiration
    time.sleep(3)
    assert redis.hget(hash_name, field1) is None


def test_hsetex_single_field_with_exat(redis: Redis):
    hash_name = "myhash"
    field1 = "field1"
    value1 = "value1"

    # Set field with expiration at specific timestamp
    future_timestamp = int(time.time()) + 2
    result = redis.hsetex(hash_name, field1, value1, exat=future_timestamp)

    assert result == 1
    assert redis.hget(hash_name, field1) == value1
    
    # Wait for expiration
    time.sleep(3)
    assert redis.hget(hash_name, field1) is None


def test_hsetex_single_field_with_pxat(redis: Redis):
    hash_name = "myhash"
    field1 = "field1"
    value1 = "value1"

    # Set field with expiration at specific timestamp in milliseconds
    future_timestamp_ms = int(time.time() * 1000) + 2000
    result = redis.hsetex(hash_name, field1, value1, pxat=future_timestamp_ms)

    assert result == 1
    assert redis.hget(hash_name, field1) == value1
    
    # Wait for expiration
    time.sleep(3)
    assert redis.hget(hash_name, field1) is None


def test_hsetex_multiple_fields_with_ex(redis: Redis):
    hash_name = "myhash"

    # Set multiple fields with expiration
    result = redis.hsetex(hash_name, values={
        "field1": "value1",
        "field2": "value2",
        "field3": "value3"
    }, ex=2)

    assert result >= 1  # Returns success indicator
    assert redis.hget(hash_name, "field1") == "value1"
    assert redis.hget(hash_name, "field2") == "value2"
    assert redis.hget(hash_name, "field3") == "value3"
    
    # Wait for expiration
    time.sleep(3)
    assert redis.hget(hash_name, "field1") is None
    assert redis.hget(hash_name, "field2") is None
    assert redis.hget(hash_name, "field3") is None


def test_hsetex_with_fnx(redis: Redis):
    hash_name = "myhash"
    field1 = "field1"

    # Set field with FNX (only if field doesn't exist)
    result1 = redis.hsetex(hash_name, field1, "value1", fnx=True, ex=60)
    assert result1 == 1
    assert redis.hget(hash_name, field1) == "value1"

    # Try to set again with FNX (should not update)
    result2 = redis.hsetex(hash_name, field1, "value2", fnx=True, ex=60)
    assert result2 == 0
    assert redis.hget(hash_name, field1) == "value1"


def test_hsetex_with_fxx(redis: Redis):
    hash_name = "myhash"
    field1 = "field1"

    # Try to set with FXX when field doesn't exist (should fail)
    result1 = redis.hsetex(hash_name, field1, "value1", fxx=True, ex=60)
    assert result1 == 0
    assert redis.hget(hash_name, field1) is None

    # Set field first
    redis.hset(hash_name, field1, "initial")

    # Now set with FXX (should work)
    result2 = redis.hsetex(hash_name, field1, "value2", fxx=True, ex=60)
    assert result2 >= 0  # Returns success indicator
    assert redis.hget(hash_name, field1) == "value2"


def test_hsetex_with_keepttl(redis: Redis):
    hash_name = "myhash"
    field1 = "field1"

    # Set field with expiration
    redis.hset(hash_name, field1, "value1")
    redis.hexpire(hash_name, field1, 100)

    # Update field value but keep TTL
    result = redis.hsetex(hash_name, field1, "value2", keepttl=True)

    assert result >= 0  # Returns success indicator
    assert redis.hget(hash_name, field1) == "value2"
    
    # Verify TTL is still set (should be close to 100)
    ttl = redis.httl(hash_name, [field1])
    assert ttl[0] > 90  # Allow some time for execution


def test_hsetex_requires_field_value_pairs(redis: Redis):
    hash_name = "myhash"

    with pytest.raises(Exception, match="no key value pairs"):
        redis.hsetex(hash_name, ex=60)


def test_hsetex_mixed_field_and_values(redis: Redis):
    hash_name = "myhash"

    # Set both single field and multiple values
    result = redis.hsetex(
        hash_name,
        field="field1",
        value="value1",
        values={"field2": "value2", "field3": "value3"},
        ex=60
    )

    assert result >= 1  # Returns success indicator
    assert redis.hget(hash_name, "field1") == "value1"
    assert redis.hget(hash_name, "field2") == "value2"
    assert redis.hget(hash_name, "field3") == "value3"


def test_hsetex_updates_existing_field(redis: Redis):
    hash_name = "myhash"
    field1 = "field1"

    # Set initial value
    redis.hset(hash_name, field1, "initial")

    # Update with HSETEX
    result = redis.hsetex(hash_name, field1, "updated", ex=60)

    assert result >= 0  # Returns success indicator
    assert redis.hget(hash_name, field1) == "updated"

