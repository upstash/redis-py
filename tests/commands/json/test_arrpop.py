import pytest
from upstash_redis import Redis
from upstash_redis.typing import JSONValueT


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_arrpop"
    value: JSONValueT = {"int": 1, "array": [1, 2, 3, 4], "object": {"array": [5, 6, 7]}}
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)


def test_arrpop_single_element(redis: Redis):
    key = "json_arrpop"
    path = "$.array"
    index = 0

    removed_element = redis.json.arrpop(key, path, index)
    assert removed_element == [1]
    assert redis.json.get(key, path) == [[2, 3, 4]]


def test_arrpop_negative_index(redis: Redis):
    key = "json_arrpop"
    path = "$.array"
    index = -2

    removed_element = redis.json.arrpop(key, path, index)
    assert removed_element == [3]
    assert redis.json.get(key, path) == [[1, 2, 4]]


def test_arrpop_out_of_range_index(redis: Redis):
    key = "json_arrpop"
    path = "$.array"
    index = 4

    removed_element = redis.json.arrpop(key, path, index)
    assert removed_element == [4]
    assert redis.json.get(key, path) == [[1, 2, 3]]


def test_arrpop_nonarray_path(redis: Redis):
    key = "json_arrpop"
    path = "$.int"
    index = 0

    removed_element = redis.json.arrpop(key, path, index)
    assert removed_element == [None]
    assert redis.json.get(key, path) == [1]


def test_arrpop_wildcard(redis: Redis):
    key = "json_arrpop"
    path = "$..array"
    index = 1

    removed_element = redis.json.arrpop(key, path, index)
    assert removed_element == [6, 2]
    assert redis.json.get(key, path) == [[5, 7], [1, 3, 4]]
