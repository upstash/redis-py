import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_hash():
    hash_name = "myhash"
    redis.delete(hash_name)
    yield
    redis.delete(hash_name)

def test_hkeys():
    hash_name = "myhash"

    redis.hset(hash_name, "field1", "value1")
    redis.hset(hash_name, "field2", "value2")
    redis.hset(hash_name, "field3", "value3")

    result = redis.hkeys(hash_name)

    assert sorted(result) == sorted(["field1", "field2", "field3"])
