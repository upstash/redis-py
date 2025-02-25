import pytest
from upstash_redis import Redis
from upstash_redis.typing import JSONValueT


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_get"
    value: JSONValueT = {"int": 1, "array": [1, 2, 3, 4], "object": {"array": [1, 2, 3]}}
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)


def test_get(redis: Redis):
    key = "json_get"

    assert redis.json.get(key) == [{"int": 1, "array": [1, 2, 3, 4], "object": {"array": [1, 2, 3]}}]


def test_get_path(redis: Redis):
    key = "json_get"
    path = "$.array"
    assert redis.json.get(key, path) == [[1, 2, 3, 4]]


def test_get_multiple_path(redis: Redis):
    key = "json_get"
    paths = ["$.array", "$.object", "$.int"]
    assert redis.json.get(key, *paths) == {"$.array": [[1, 2, 3, 4]], "$.object": [{"array": [1, 2, 3]}], "$.int": [1]}


def test_get_nonexisting_key(redis: Redis):
    key = "json_get"
    path = "$.nonexisting_key"

    assert redis.json.get(key, path) == []


def test_get_wildcard(redis: Redis):
    key = "json_get"
    path = "$..array"

    assert redis.json.get(key, path) == [[1, 2, 3], [1, 2, 3, 4]]
