import pytest
from upstash_redis import Redis
from upstash_redis.typing import JSONValueT


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_objlen"
    value: JSONValueT = {
        "int": 2,
        "str": "test",
        "object": {"int": 3},
        "second_object": {"object": {"str": "test", "int": 2}},
    }
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)


def test_objlen(redis: Redis):
    key = "json_objlen"
    path = "$"

    assert redis.json.objlen(key, path) == [4]


def test_objlen_nonobjkey(redis: Redis):
    key = "json_objlen"
    path = "$.int"

    assert redis.json.objlen(key, path) == [None]


def test_objlen_nonexisting_key(redis: Redis):
    key = "json_objlen"
    path = "$.nonexisting_key"

    assert redis.json.objlen(key, path) == []


def test_objlen_wildcard(redis: Redis):
    key = "json_objlen"
    path = "$..object"

    assert redis.json.objlen(key, path) == [2, 1]
