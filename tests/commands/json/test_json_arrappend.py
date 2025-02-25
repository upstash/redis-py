import pytest
from upstash_redis import Redis
from upstash_redis.typing import JSONValueT
from typing import List


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_arrappend"
    value: JSONValueT = {"int": 1, "array": [], "object": {"array": [1]}}
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)


def test_arrappend_single_element(redis: Redis):
    key = "json_arrappend"
    path = "$.array"

    assert redis.json.arrappend(key, path, 1) == [1]
    assert redis.json.arrappend(key, path, "new val") == [2]
    assert redis.json.arrappend(key, path, 1.5) == [3]
    assert redis.json.arrappend(key, path, True) == [4]
    assert redis.json.arrappend(key, path, [1]) == [5]
    assert redis.json.arrappend(key, path, {"key": "value"}) == [6]
    assert redis.json.get(key, path) == [
        [1, "new val", 1.5, True, [1], {"key": "value"}]
    ]


def test_arrappend_multiple_elements(redis: Redis):
    key = "json_arrappend"
    path = "$.array"
    new_values: List[JSONValueT] = [1, "new val", 1.5, True, [1], {"key": "value"}]

    assert redis.json.arrappend(key, path, *new_values) == [6]
    assert redis.json.get(key, path) == [
        [1, "new val", 1.5, True, [1], {"key": "value"}]
    ]


def test_arrappend_nonarray_path(redis: Redis):
    key = "json_arrappend"
    path = "$.int"
    new_value = 9

    assert redis.json.arrappend(key, path, new_value) == [None]
    assert redis.json.get(key, path) == [1]


def test_arrappend_wildcard(redis: Redis):
    key = "json_arrappend"
    path = "$..array"
    new_value = 9

    assert redis.json.arrappend(key, path, new_value) == [2, 1]
    assert redis.json.get(key, path) == [[1, 9], [9]]
