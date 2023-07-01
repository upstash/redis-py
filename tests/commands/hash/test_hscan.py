import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_hash():
    hash_name = "myhash"
    redis.delete(hash_name)
    yield
    redis.delete(hash_name)

def test_hscan_with_match():
    hash_name = "myhash"

    # Set some field-value pairs in the hash
    redis.hset(hash_name, "field1", "value1")
    redis.hset(hash_name, "field2", "value2")
    redis.hset(hash_name, "field3", "value3")
    redis.hset(hash_name, "other_field", "other_value")

    # Use HSCAN command with match parameter to filter field-value pairs
    cursor = 0
    count = 2  # Maximum number of elements to retrieve per iteration
    match_pattern = "field*"  # Pattern to match field names
    result = {}

    while True:
        cursor, data = redis.hscan(hash_name, cursor, count=count, match_pattern=match_pattern)
        result.update(data)

        if cursor == 0:
            break

    # Assertions to check the retrieved field-value pairs
    assert result == {"field1": "value1", "field2": "value2", "field3": "value3"}