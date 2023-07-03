from ast import Dict
from typing import List
import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_hash():
    hash_name = "myhash"
    redis.delete(hash_name)
    yield
    redis.delete(hash_name)


def test_hgetall():
    hash_name = "myhash"
    fields_values = {"field1": "value1", "field2": "value2"}

    redis.hset(hash_name, field_value_pairs=fields_values)

    result = redis.hgetall(hash_name)

    assert isinstance(result, dict)

    assert result == fields_values


def test_hgetall_without_formatting():
    redis.format_return = False

    hash_name = "myhash"
    fields_values = {"field1": "value1", "field2": "value2"}

    redis.hset(hash_name, field_value_pairs=fields_values)

    result = redis.hgetall(hash_name)

    assert isinstance(result, list)

    redis.format_return = True
