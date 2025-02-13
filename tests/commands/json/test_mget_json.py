import pytest
from upstash_redis import Redis


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_keys = ["json_mget_1", "json_mget_2"]
    value1 = {"int": 1, "array": [1, 2, 3, 4], "object": {"array": [1, 2, 3]}}
    value2 = {"int": 2, "array": [2, 2, 3, 4], "object": {"array": [2, 2, 3]}}
    redis.json.set(json_keys[0], "$", value1)
    redis.json.set(json_keys[1], "$", value2)
    yield
    for json_key in json_keys:
        redis.delete(json_key)


def test_mget(redis: Redis):
    keys = ["json_mget_1", "json_mget_2"]
    path = "$.int"

    assert redis.json.mget(keys, path) == [[1], [2]]


def test_mget_nonexisting_key(redis: Redis):
    keys = ["json_mget_1", "json_mget_2"]
    path = "$.nonexisting_key"

    assert redis.json.mget(keys, path) == [[], []]


def test_mget_wildcard(redis: Redis):
    keys = ["json_mget_1", "json_mget_2"]
    path = "$..array"

    assert redis.json.mget(keys, path) == [[[1, 2, 3], [1, 2, 3, 4]], [[2, 2, 3], [2, 2, 3, 4]]]
