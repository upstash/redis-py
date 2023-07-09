import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)


def test_hrandfield_single(redis: Redis) -> None:
    hash_name = "myhash"

    # Add fields to the hash
    redis.hset(hash_name, "field1", "value1")
    redis.hset(hash_name, "field2", "value2")
    redis.hset(hash_name, "field3", "value3")

    # Get a single random field from the hash
    result = redis.hrandfield(hash_name)
    assert result in ["field1", "field2", "field3"]


def test_hrandfield_multiple(redis: Redis) -> None:
    hash_name = "myhash"

    # Add fields to the hash
    redis.hset(hash_name, "field1", "value1")
    redis.hset(hash_name, "field2", "value2")
    redis.hset(hash_name, "field3", "value3")

    # Get multiple random fields from the hash
    count = 1  # Number of random fields to retrieve
    result = redis.hrandfield(hash_name, count=count)

    assert result is not None
    assert len(result) == count  # Number of random fields returned
    assert all(field in ["field1", "field2", "field3"] for field in result)

    result = redis.hrandfield(hash_name, count=2, withvalues=True)
    assert isinstance(result, dict)

    for key, val in result.items():
        assert redis.hget(hash_name, key) == val
