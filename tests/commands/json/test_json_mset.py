import pytest

from upstash_redis import Redis
from upstash_redis.typing import JSONValueT


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    redis.json.delete("json_mset_1")
    redis.json.delete("json_mset_2")
    yield
    redis.json.delete("json_mset_1")
    redis.json.delete("json_mset_2")


def test_mset(redis: Redis):
    json_key_1 = "json_mset_1"
    json_key_2 = "json_mset_2"
    value1: JSONValueT = {
        "int": 1,
        "array": [1, 2, 3, 4],
        "object": {"array": [1, 2, 3]},
    }
    value2: JSONValueT = {
        "int": 2,
        "array": [2, 2, 3, 4],
        "object": {"array": [2, 2, 3]},
    }

    assert (
        redis.json.mset([(json_key_1, "$", value1), (json_key_2, "$", value2)]) is True
    )
    assert redis.json.mget([json_key_1, json_key_2], "$.int") == [[1], [2]]
    assert redis.json.mset([(json_key_1, "$.int", 2), (json_key_2, "$.int", 3)]) is True
    assert redis.json.mget([json_key_1, json_key_2], "$.int") == [[2], [3]]
    assert (
        redis.json.mset(
            [(json_key_1, "$..array[0]", 2), (json_key_2, "$..array[0]", 3)]
        )
        is True
    )
    assert redis.json.mget([json_key_1, json_key_2], "$..array[0]") == [[2, 2], [3, 3]]
