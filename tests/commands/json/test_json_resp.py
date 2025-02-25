import pytest
from upstash_redis import Redis
from upstash_redis.typing import JSONValueT


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_resp"
    value: JSONValueT = {"object": {"array": [1, 2, 3]}}
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)


def test_resp(redis: Redis):
    key = "json_resp"

    assert redis.json.resp(key, "$") == [
        ["{", "object", ["{", "array", ["[", 1, 2, 3]]]
    ]
