import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_hash():
    hash_name = "myhash"
    redis.delete(hash_name)


def test_hsetnx():
    hash_name = "myhash"

    result = redis.hsetnx(hash_name, "field1", "value1")
    assert result == True
    assert redis.hget(hash_name, "field1") == "value1"

    result = redis.hsetnx(hash_name, "field1", "new_value")
    assert result == False
    assert redis.hget(hash_name, "field1") == "value1"
