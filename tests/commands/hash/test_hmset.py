import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)


def test_hmset(redis: Redis):
    hash_name = "myhash"

    # Define the field-value pairs to set in the hash
    fields = {"field1": "value1", "field2": "value2", "field3": "value3"}

    # Set the field-value pairs in the hash using HMSET command
    result = redis.hmset(hash_name, fields)

    assert result is True

    assert redis.hmget(hash_name, *fields) == ["value1", "value2", "value3"]


def test_hmset_without_formatting(redis: Redis):
    redis._format_return = False

    hash_name = "myhash"

    # Define the field-value pairs to set in the hash
    fields = {"field1": "value1", "field2": "value2", "field3": "value3"}

    # Set the field-value pairs in the hash using HMSET command
    result = redis.hmset(hash_name, fields)

    assert result == "OK"
