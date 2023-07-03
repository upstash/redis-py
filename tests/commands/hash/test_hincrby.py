import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_hash():
    hash_name = "myhash"
    redis.delete(hash_name)


def test_hincrby():
    hash_name = "myhash"
    field = "counter"

    # Set an initial value for the field
    redis.hset(hash_name, field, str(5))

    # Increment the field value by a specific amount
    result = redis.hincrby(hash_name, field, 3)

    assert result == 8

    # Additional assertions can be added here based on the expected behavior of HINCRBY command
