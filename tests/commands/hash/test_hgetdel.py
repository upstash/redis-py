import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)
    yield
    redis.delete(hash_name)


def test_hgetdel_single_field(redis: Redis):
    hash_name = "myhash"
    field1 = "field1"
    value1 = "value1"

    redis.hset(hash_name, field1, value1)

    # Get and delete the field
    result = redis.hgetdel(hash_name, field1)

    assert result == [value1]
    # Verify field is deleted
    assert redis.hget(hash_name, field1) is None


def test_hgetdel_multiple_fields(redis: Redis):
    hash_name = "myhash"

    redis.hset(hash_name, values={
        "field1": "value1",
        "field2": "value2",
        "field3": "value3"
    })

    # Get and delete multiple fields
    result = redis.hgetdel(hash_name, "field1", "field2")

    assert result == ["value1", "value2"]
    # Verify fields are deleted
    assert redis.hget(hash_name, "field1") is None
    assert redis.hget(hash_name, "field2") is None
    # Verify field3 still exists
    assert redis.hget(hash_name, "field3") == "value3"


def test_hgetdel_non_existing_field(redis: Redis):
    hash_name = "myhash"

    redis.hset(hash_name, "field1", "value1")

    # Get and delete existing and non-existing fields
    result = redis.hgetdel(hash_name, "field1", "non_existing")

    assert result == ["value1", None]
    assert redis.hget(hash_name, "field1") is None


def test_hgetdel_deletes_key_when_last_field_removed(redis: Redis):
    hash_name = "myhash"

    redis.hset(hash_name, "field1", "value1")

    # Get and delete the only field
    result = redis.hgetdel(hash_name, "field1")

    assert result == ["value1"]
    # Verify the entire key is deleted
    assert redis.exists(hash_name) == 0


def test_hgetdel_non_existing_hash(redis: Redis):
    hash_name = "non_existing_hash"

    # Try to get and delete from non-existing hash
    result = redis.hgetdel(hash_name, "field1")

    assert result == [None]


def test_hgetdel_requires_at_least_one_field(redis: Redis):
    hash_name = "myhash"

    with pytest.raises(Exception, match="requires at least one field"):
        redis.hgetdel(hash_name)

