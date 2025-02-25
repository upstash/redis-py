import pytest
from upstash_redis import Redis
from upstash_redis.typing import JSONValueT


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_type"
    value: JSONValueT = {
        "number": 1,
        "array": [1, 2, 3, 4],
        "object": {"number": 3.1415},
        "str": "test",
        "bool": True,
        "null": None,
    }
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)


def test_type_int(redis: Redis):
    key = "json_type"
    path = "$.number"

    assert redis.json.type(key, path) == ["integer"]


def test_type_array(redis: Redis):
    key = "json_type"
    path = "$.array"

    assert redis.json.type(key, path) == ["array"]


def test_type_object(redis: Redis):
    key = "json_type"
    path = "$.object"

    assert redis.json.type(key, path) == ["object"]


def test_type_float(redis: Redis):
    key = "json_type"
    path = "$.object.number"

    assert redis.json.type(key, path) == ["number"]


def test_type_str(redis: Redis):
    key = "json_type"
    path = "$.str"

    assert redis.json.type(key, path) == ["string"]


def test_type_bool(redis: Redis):
    key = "json_type"
    path = "$.bool"

    assert redis.json.type(key, path) == ["boolean"]


def test_type_null(redis: Redis):
    key = "json_type"
    path = "$.null"

    assert redis.json.type(key, path) == ["null"]


def test_type_nonexisting_key(redis: Redis):
    key = "json_type"
    path = "$.nonexisting_key"

    assert redis.json.type(key, path) == []


def test_type_wildcard(redis: Redis):
    key = "json_type"
    path = "$..number"

    assert redis.json.type(key, path) == ["number", "integer"]
