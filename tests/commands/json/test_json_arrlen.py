import pytest
from upstash_redis import Redis
from upstash_redis.typing import JSONValueT


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_arrlen"
    value: JSONValueT = {
        "array": [1, 2, 3, 4],
        "int": 1,
        "object": {"array": [1, 2, 3]},
    }
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)


def test_arrlen_existing_element(redis: Redis):
    key = "json_arrlen"
    path = "$.array"

    length = redis.json.arrlen(key, path)
    assert length == [4]


def test_arrlen_nonarray_path(redis: Redis):
    key = "json_arrlen"
    path = "$.int"

    length = redis.json.arrlen(key, path)
    assert length == [None]


def test_arrlen_wildcard(redis: Redis):
    key = "json_arrlen"
    path = "$..array"

    length = redis.json.arrlen(key, path)
    assert length == [3, 4]
