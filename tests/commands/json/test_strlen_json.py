import pytest
from upstash_redis import Redis


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_strlen"
    redis.json.set(json_key, "$", {"str": "test", "obj": {"str": "test_1"}})
    yield
    redis.delete(json_key)


def test_strlen(redis: Redis):
    key = "json_strlen"
    path = "$.str"

    assert redis.json.strlen(key, path) == [4]


def test_strlen_nonstr_path(redis: Redis):
    key = "json_strlen"
    path = "$.obj"

    assert redis.json.strlen(key, path) == [None]


def test_strlen_wildcard(redis: Redis):
    key = "json_strlen"
    path = "$..str"

    assert redis.json.strlen(key, path) == [6, 4]

