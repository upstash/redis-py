import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)


def test_hincrby(redis: Redis):
    hash_name = "myhash"
    field = "counter"

    # Set an initial value for the field
    redis.hset(hash_name, field, str(5))

    # Increment the field value by a specific amount
    result = redis.hincrby(hash_name, field, 3)

    assert result == 8

    # Additional assertions can be added here based on the expected behavior of HINCRBY command
