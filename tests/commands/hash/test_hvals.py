import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_hash():
    hash_name = "myhash"
    redis.delete(hash_name)
    yield
    redis.delete(hash_name)

def test_hvals():
    hash_name = "myhash"

    redis.hset(hash_name, "field1", "value1")
    redis.hset(hash_name, "field2", "value2")
    redis.hset(hash_name, "field3", "value3")
    
    result = redis.hvals(hash_name)

    assert sorted(result) == sorted(["value1", "value2", "value3"])