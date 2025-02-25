import pytest
from upstash_redis import Redis
from upstash_redis.typing import JSONValueT


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_forget"
    value: JSONValueT = {
        "int": 1,
        "array": [1, 2, 3, 4],
        "object": {"array": [1, 2, 3]},
    }
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)


def test_forget_single_element(redis: Redis):
    key = "json_forget"
    path = "$.array"

    removed_element = redis.json.forget(key, path)
    assert removed_element == 1
    assert redis.json.get(key) == [{"int": 1, "object": {"array": [1, 2, 3]}}]


def test_forget_wildcard(redis: Redis):
    key = "json_forget"
    path = "$..array"

    removed_element = redis.json.forget(key, path)
    assert removed_element == 2
    assert redis.json.get(key) == [{"int": 1, "object": {}}]
