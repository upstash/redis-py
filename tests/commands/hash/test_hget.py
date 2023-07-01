import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_hash():
    hash_name = "myhash"
    redis.delete(hash_name)
    yield
    redis.delete(hash_name)

def test_hget():
    hash_name = "myhash"
    field1 = "field1"
    field2 = "field2"

    redis.hset(hash_name, field1, "value1")
    redis.hset(hash_name, field2, "value2")

    value1 = redis.hget(hash_name, field1)
    value2 = redis.hget(hash_name, field2)
    value3 = redis.hget(hash_name, "non_existing_field")

    assert value1 == "value1"
    assert value2 == "value2"
    assert value3 is None
