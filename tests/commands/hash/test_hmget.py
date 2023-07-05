import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)
    yield
    redis.delete(hash_name)


def test_hmget(redis: Redis):
    hash_name = "myhash"

    # Set some fields in the hash
    redis.hset(hash_name, "field1", "value1")
    redis.hset(hash_name, "field2", "value2")
    redis.hset(hash_name, "field3", "value3")

    # Get multiple field values from the hash
    fields = ["field1", "field3", "non_existing_field"]
    result = redis.hmget(hash_name, *fields)

    expected_result = ["value1", "value3", None]
    assert result == expected_result

    # Additional assertions can be added here based on the expected behavior of HMGET command
