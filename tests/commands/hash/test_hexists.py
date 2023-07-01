import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_hash():
    hash_name = "myhash"
    redis.delete(hash_name)

def test_hexists():
    hash_name = "myhash"
    field1 = "field1"
    field2 = "field2"

    redis.hset(hash_name, field1, "value1")

    exists_field1 = redis.hexists(hash_name, field1)
    exists_field2 = redis.hexists(hash_name, field2)

    assert exists_field1 is True
    assert exists_field2 is False

def test_hexists_without_formatting():
    redis.format_return = False
    hash_name = "myhash"
    field1 = "field1"
    field2 = "field2"

    redis.hset(hash_name, field1, "value1")

    exists_field1 = redis.hexists(hash_name, field1)
    exists_field2 = redis.hexists(hash_name, field2)

    assert exists_field1 is 1
    assert exists_field2 is 0

    redis.format_return = True