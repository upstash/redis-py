import pytest
from upstash_redis import Redis
from upstash_redis.typing import JSONValueT


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_numincrby"
    value: JSONValueT = {"int": 1, "str": "test", "object": {"int": 2}}
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)


def test_numincrby(redis: Redis):
    key = "json_numincrby"
    path = "$.int"
    increment = 1

    assert redis.json.numincrby(key, path, increment) == [2]
    assert redis.json.get(key, path) == [2]


def test_numincrby_nonintkey(redis: Redis):
    key = "json_numincrby"
    path = "$.str"
    increment = 1

    assert redis.json.numincrby(key, path, increment) == [None]


def test_numincrby_nonexisting_key(redis: Redis):
    key = "json_numincrby"
    path = "$.nonexisting_key"
    increment = 1

    assert redis.json.numincrby(key, path, increment) == []
    assert redis.json.get(key, path) == []


def test_numincrby_wildcard(redis: Redis):
    key = "json_numincrby"
    path = "$..int"
    increment = 1

    assert redis.json.numincrby(key, path, increment) == [3, 2]
    assert redis.json.get(key, path) == [3, 2]
