import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)
    yield
    redis.delete(hash_name)


def test_hset(redis: Redis):
    hash_name = "myhash"

    # Use HSET command to set field-value pairs in the hash
    result = redis.hset(hash_name, "field1", "value1")
    assert result == 1  # 1 if field is a new field in the hash and value was set
    assert redis.hget(hash_name, "field1") == "value1"

    # Use HSET command to update an existing field
    result = redis.hset(hash_name, "field1", "updated_value")
    assert result == 0  # 0 if field already existed in the hash and value was updated
    assert redis.hget(hash_name, "field1") == "updated_value"

    # Use HSET command to set multiple field-value pairs at once
    field_value_pairs = {"field2": "value2", "field3": "value3"}
    result = redis.hset(hash_name, field_value_pairs=field_value_pairs)
    assert result == 2  # Number of fields added to the hash
    assert redis.hget(hash_name, "field2") == "value2"
    assert redis.hget(hash_name, "field3") == "value3"

    with pytest.raises(Exception):
        redis.hset("test_name", "asd")
