import time

import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)
    yield
    redis.delete(hash_name)


def test_hgetex_single_field(redis: Redis):
    hash_name = "myhash"
    field1 = "field1"
    value1 = "value1"

    redis.hset(hash_name, field1, value1)

    # Get single field value without setting expiration
    result = redis.hgetex(hash_name, field1)

    assert result == [value1]


def test_hgetex_multiple_fields(redis: Redis):
    hash_name = "myhash"

    redis.hset(
        hash_name, values={"field1": "value1", "field2": "value2", "field3": "value3"}
    )

    # Get multiple field values
    result = redis.hgetex(hash_name, "field1", "field2")

    assert result == ["value1", "value2"]


def test_hgetex_with_ex(redis: Redis):
    hash_name = "myhash"

    redis.hset(hash_name, values={"field1": "value1", "field2": "value2"})

    # Get values and set expiration in seconds
    result = redis.hgetex(hash_name, "field1", "field2", ex=2)

    assert result == ["value1", "value2"]
    # Verify fields still exist
    assert redis.hget(hash_name, "field1") == "value1"
    assert redis.hget(hash_name, "field2") == "value2"

    # Wait for expiration
    time.sleep(3)
    assert redis.hget(hash_name, "field1") is None
    assert redis.hget(hash_name, "field2") is None


def test_hgetex_with_px(redis: Redis):
    hash_name = "myhash"

    redis.hset(hash_name, values={"field1": "value1", "field2": "value2"})

    # Get values and set expiration in milliseconds
    result = redis.hgetex(hash_name, "field1", "field2", px=2000)

    assert result == ["value1", "value2"]
    # Verify fields still exist
    assert redis.hget(hash_name, "field1") == "value1"

    # Wait for expiration
    time.sleep(3)
    assert redis.hget(hash_name, "field1") is None


def test_hgetex_with_exat(redis: Redis):
    hash_name = "myhash"
    field1 = "field1"
    value1 = "value1"

    redis.hset(hash_name, field1, value1)

    # Get value and set expiration at specific timestamp
    future_timestamp = int(time.time()) + 2
    result = redis.hgetex(hash_name, field1, exat=future_timestamp)

    assert result == [value1]
    # Verify field still exists
    assert redis.hget(hash_name, field1) == value1

    # Wait for expiration
    time.sleep(3)
    assert redis.hget(hash_name, field1) is None


def test_hgetex_with_pxat(redis: Redis):
    hash_name = "myhash"
    field1 = "field1"
    value1 = "value1"

    redis.hset(hash_name, field1, value1)

    # Get value and set expiration at specific timestamp in milliseconds
    future_timestamp_ms = int(time.time() * 1000) + 2000
    result = redis.hgetex(hash_name, field1, pxat=future_timestamp_ms)

    assert result == [value1]
    # Verify field still exists
    assert redis.hget(hash_name, field1) == value1

    # Wait for expiration
    time.sleep(3)
    assert redis.hget(hash_name, field1) is None


def test_hgetex_with_persist(redis: Redis):
    hash_name = "myhash"
    field1 = "field1"
    value1 = "value1"

    # Set field with expiration
    redis.hset(hash_name, field1, value1)
    redis.hexpire(hash_name, field1, 10)

    # Get value and remove expiration
    result = redis.hgetex(hash_name, field1, persist=True)

    assert result == [value1]
    # Verify field has no expiration
    ttl = redis.httl(hash_name, [field1])
    assert ttl == [-1]


def test_hgetex_non_existing_field(redis: Redis):
    hash_name = "myhash"

    redis.hset(hash_name, "field1", "value1")

    # Try to get existing and non-existing fields
    result = redis.hgetex(hash_name, "field1", "non_existing", ex=60)

    assert result == ["value1", None]


def test_hgetex_non_existing_hash(redis: Redis):
    hash_name = "non_existing_hash"

    # Try to get from non-existing hash
    result = redis.hgetex(hash_name, "field1", "field2", ex=60)

    assert result == [None, None]


def test_hgetex_requires_at_least_one_field(redis: Redis):
    hash_name = "myhash"

    with pytest.raises(Exception, match="requires at least one field"):
        redis.hgetex(hash_name)
