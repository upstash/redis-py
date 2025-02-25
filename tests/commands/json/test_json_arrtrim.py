import pytest
from upstash_redis import Redis
from upstash_redis.typing import JSONValueT


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_arrtrim"
    value: JSONValueT = {
        "int": 1,
        "array": [1, 2, 3, 4],
        "object": {"array": [5, 6, 7]},
    }
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)


def test_arrtrim_default(redis: Redis):
    key = "json_arrtrim"
    path = "$.array"
    start = 1
    stop = 2

    newlen = redis.json.arrtrim(key, path, start, stop)
    assert newlen == [2]
    assert redis.json.get(key, path) == [[2, 3]]


def test_arrtrim_negative_index(redis: Redis):
    key = "json_arrtrim"
    path = "$.array"
    start = -2
    stop = -1

    newlen = redis.json.arrtrim(key, path, start, stop)
    assert newlen == [2]
    assert redis.json.get(key, path) == [[3, 4]]


def test_arrtrim_out_of_range_index(redis: Redis):
    key = "json_arrtrim"
    path = "$.array"
    start = 4
    stop = 5

    newlen = redis.json.arrtrim(key, path, start, stop)
    assert newlen == [0]
    assert redis.json.get(key, path) == [[]]


def test_arrtrim_start_greater_than_stop(redis: Redis):
    key = "json_arrtrim"
    path = "$.array"
    start = 1
    stop = 0

    newlen = redis.json.arrtrim(key, path, start, stop)
    assert newlen == [0]
    assert redis.json.get(key, path) == [[]]


def test_arrtrim_nonarray_path(redis: Redis):
    key = "json_arrtrim"
    path = "$.int"
    start = 0
    stop = 0

    newlen = redis.json.arrtrim(key, path, start, stop)
    assert newlen == [None]
    assert redis.json.get(key, path) == [1]


def test_arrtrim_wildcard(redis: Redis):
    key = "json_arrtrim"
    path = "$..array"
    start = 0
    stop = 1

    newlen = redis.json.arrtrim(key, path, start, stop)
    assert newlen == [2, 2]
    assert redis.json.get(key, path) == [[5, 6], [1, 2]]
