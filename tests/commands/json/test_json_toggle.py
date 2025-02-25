import pytest
from upstash_redis import Redis


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_toggle"
    redis.json.set(json_key, "$", {"bool": True, "obj": {"bool": False}})
    yield
    redis.delete(json_key)


def test_toggle(redis: Redis):
    key = "json_toggle"
    path = "$.bool"

    assert redis.json.toggle(key, path) == [0]
    assert redis.json.get(key, path) == [False]
    assert redis.json.toggle(key, path) == [1]
    assert redis.json.get(key, path) == [True]


def test_toggle_nonbool_path(redis: Redis):
    key = "json_toggle"
    path = "$.obj"

    assert redis.json.toggle(key, path) == [None]


def test_toggle_wildcard(redis: Redis):
    key = "json_toggle"
    path = "$..bool"

    assert redis.json.toggle(key, path) == [1, 0]
    assert redis.json.get(key, path) == [True, False]
    assert redis.json.toggle(key, path) == [0, 1]
    assert redis.json.get(key, path) == [False, True]
