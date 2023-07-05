import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_hash():
    hash_name = "myhash"
    redis.delete(hash_name)
    yield
    redis.delete(hash_name)


def test_hdel():
    hash_name = "myhash"
    field1 = "field1"
    field2 = "field2"
    field3 = "field3"

    redis.hset(hash_name, field1, "value1")
    redis.hset(hash_name, field2, "value2")
    redis.hset(hash_name, field3, "value3")

    result = redis.hdel(hash_name, field2, field3)

    assert result == 2  # Number of fields deleted

    remaining_fields = redis.hkeys(hash_name)
    assert remaining_fields == [field1]  # Only field1 should remain


def test_hdel_with_pairs():
    hash_name = "myhash"

    pairs = {
        "field1": "value1",
        "field2": "value2",
        "field3": "value3",
    }
    field1 = "field1"
    field2 = "field2"
    field3 = "field3"

    redis.hset(hash_name, field_value_pairs=pairs)

    result = redis.hdel(hash_name, field3)

    assert result == 1

    remaining_fields = redis.hkeys(hash_name)
    assert sorted(remaining_fields) == sorted([field1, field2])
