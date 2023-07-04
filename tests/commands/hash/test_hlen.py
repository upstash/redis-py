import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_hash():
    hash_name = "myhash"
    redis.delete(hash_name)
    yield
    redis.delete(hash_name)


def test_hlen():
    hash_name = "myhash"

    # Set some fields in the hash
    redis.hset(hash_name, "field1", "value1")
    redis.hset(hash_name, "field2", "value2")
    redis.hset(hash_name, "field3", "value3")

    # Get the length of the hash
    result = redis.hlen(hash_name)

    assert result == 3
