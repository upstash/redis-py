import pytest
from upstash_redis import Redis
from upstash_redis.typing import JSONValueT


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_arrinsert"
    value: JSONValueT = {"int": 1, "array": [], "object": {"array": [0]}}
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)


def test_arrinsert_single_element(redis: Redis):
    key = "json_arrinsert"
    path = "$.array"

    assert redis.json.arrinsert(key, path,  0,1) == [1]
    assert redis.json.arrinsert(key, path,  0, 'new val') == [2]
    assert redis.json.arrinsert(key, path,  0, 1.5) == [3]
    assert redis.json.arrinsert(key, path,  0, True) == [4]
    assert redis.json.arrinsert(key, path,  0, [1]) == [5]
    assert redis.json.arrinsert(key, path,  0, {"key": "value"}) == [6]
    assert redis.json.get(key, path) == [[ {"key": "value"}, [1],True , 1.5, 'new val', 1 ]]


def test_arrinsert_multiple_elements(redis: Redis):
    key = "json_arrinsert"
    path = "$.array"
    new_values = [5, 6, 7]

    assert redis.json.arrinsert(key, path, 0, *new_values) == [3]
    assert redis.json.get(key, path) == [[5, 6, 7]]


def test_arrinsert_nonarray_path(redis: Redis):
    key = "json_arrinsert"
    path = "$.int"
    new_value = 9
    index = 0

    assert redis.json.arrinsert(key, path, index, new_value) == [None]
    assert redis.json.get(key, path) == [1]


def test_arrinsert_wildcard(redis: Redis):
    key = "json_arrinsert"
    path = "$..array"
    new_value = 9
    index = 0

    assert redis.json.arrinsert(key, path, index, new_value) == [2, 1]
    assert redis.json.get(key, path) == [[9, 0], [9]]
