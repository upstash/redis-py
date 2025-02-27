import pytest
from upstash_redis import Redis


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_strappend"
    redis.json.set(json_key, "$", {"str": "test", "obj": {"str": "test_1"}})
    yield
    redis.delete(json_key)


def test_strappend(redis: Redis):
    key = "json_strappend"
    path = "$.str"

    assert redis.json.strappend(key, path, "_append") == [11]
    assert redis.json.get(key, path) == ["test_append"]


def test_strappend_nonstr_path(redis: Redis):
    key = "json_strappend"
    path = "$.obj"

    assert redis.json.strappend(key, path, "_append") == [None]


def test_strappend_wildcard(redis: Redis):
    key = "json_strappend"
    path = "$..str"

    assert redis.json.strappend(key, path, "_append") == [13, 11]
    assert redis.json.get(key, path) == ["test_1_append", "test_append"]
