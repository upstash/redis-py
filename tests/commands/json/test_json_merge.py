import pytest

from upstash_redis import Redis
from upstash_redis.typing import JSONValueT


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_merge"
    value: JSONValueT = {
        "int": 1,
        "array": [1, 2, 3, 4],
        "object": {"array": [1, 2, 3]},
    }
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)


def test_merge(redis: Redis):
    key = "json_merge"

    assert redis.json.merge(key, "$", {"str": "test", "int": 2}) is True
    assert redis.json.get(key) == [
        {"int": 2, "array": [1, 2, 3, 4], "object": {"array": [1, 2, 3]}, "str": "test"}
    ]


def test_merge_nonexisting_key(redis: Redis):
    key = "json_merge_nonexisting"

    assert redis.json.merge(key, "$", {"str": "test", "int": 2}) is True
    assert redis.json.get(key) == [{"int": 2, "str": "test"}]


def test_merge_wildcard(redis: Redis):
    key = "json_merge"
    path = "$..array"

    assert redis.json.merge(key, path, [2, 2, 3, 4]) is True
    assert redis.json.get(key) == [
        {"int": 1, "array": [2, 2, 3, 4], "object": {"array": [2, 2, 3, 4]}}
    ]
