import pytest

import upstash_redis.errors
from upstash_redis import Redis


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_set"
    redis.delete(json_key)
    yield
    redis.delete(json_key)


def test_set(redis: Redis):
    key = "json_set"

    assert redis.json.get(key) == None
    assert redis.json.set(key, '$', {"test": 1}) == True
    assert redis.json.set(key, "$.str", "test") == True
    assert redis.json.set(key, "$.int", 1) == True
    assert redis.json.set(key, "$.bool", True) == True
    assert redis.json.set(key, "$.array", [1, 2, "test"]) == True
    assert redis.json.set(key, "$.object", {"int": 1}) == True
    assert redis.json.set(key, "$.null", None) == True
    assert redis.json.get(key) == [{"int": 1, "test": 1, "str": "test", "bool": True, "array": [1, 2, "test"], "object": {"int": 1}, "null": None}]


def test_set_nonexisting_path(redis: Redis):
    key = "json_set"
    path = "$.nonexisting_key.str"

    assert redis.json.set(key, "$", {"int": 1}) == True
    assert redis.json.get(key) == [{"int": 1}]
    with pytest.raises(upstash_redis.errors.UpstashError) as ex:
        redis.json.set(key, path, "test")



def test_set_wildcard(redis: Redis):
    key = "json_set"
    path = "$..int"

    assert redis.json.set(key, "$", {"int": 1, "obj":{"int": 3}}) == True
    assert redis.json.get(key) == [{"int": 1, "obj":{"int": 3}}]
    assert redis.json.set(key, path, 2) == True
    assert redis.json.get(key) == [{"int": 2, "obj": {"int": 2}}]


def test_set_nx(redis: Redis):
    key = "json_set"
    path = "$.int"

    assert redis.json.set(key, "$", {"int": 1}) == True
    assert redis.json.get(key) == [{"int": 1}]
    assert redis.json.set(key, "$", {"str": "test"}, nx=True) == False
    assert redis.json.get(key) == [{"int": 1}]
    assert redis.json.set(key, path, 2, nx=True) == False
    assert redis.json.get(key) == [{"int": 1}]


def test_set_xx(redis: Redis):
    key = "json_set"

    assert redis.json.set(key, "$", {"int": 1}, xx=True) == False
    assert redis.json.get(key) is None
    assert redis.json.set(key, "$", {"int": 1}) == True
    assert redis.json.get(key) == [{"int": 1}]
    assert redis.json.set(key, "$", {"str": "test"}, xx=True) == True
    assert redis.json.get(key) == [{"str": "test"}]


