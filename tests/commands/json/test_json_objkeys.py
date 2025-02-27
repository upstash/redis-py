import pytest

from upstash_redis import Redis
from upstash_redis.typing import JSONValueT


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_objkeys"
    value: JSONValueT = {
        "int": 2,
        "str": "test",
        "object": {"int": 3},
        "second_object": {"object": {"str": "test", "int": 2}},
    }
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)


def test_objkeys(redis: Redis):
    key = "json_objkeys"
    path = "$"

    response = redis.json.objkeys(key, path)

    assert response[0] is not None
    response[0].sort()

    assert response == [["int", "object", "second_object", "str"]]


def test_objkeys_nonobjkey(redis: Redis):
    key = "json_objkeys"
    path = "$.int"

    assert redis.json.objkeys(key, path) == [None]


def test_objkeys_nonexisting_key(redis: Redis):
    key = "json_objkeys"
    path = "$.nonexisting_key"

    assert redis.json.objkeys(key, path) == []


def test_objkeys_wildcard(redis: Redis):
    key = "json_objkeys"
    path = "$..object"

    response = redis.json.objkeys(key, path)
    for i in response:
        assert i is not None
        i.sort()

    response.sort()

    assert response == [["int"], ["int", "str"]]
