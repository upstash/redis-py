import pytest
from upstash_redis import Redis


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_arrindex"
    value = {"array": [1, 'test', ['a'], 1.5, {'test': 1}], "int": 1, "object": {"array": [ 0, 1 ]}}
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)

def test_arrindex_existing_element(redis: Redis):
    key = "json_arrindex"
    path = "$.array"

    assert redis.json.arrindex(key, path, 1) == [0]
    assert redis.json.arrindex(key, path, 'test') == [1]
    assert redis.json.arrindex(key, path, ['a']) == [2]
    assert redis.json.arrindex(key, path, 1.5) == [3]
    assert redis.json.arrindex(key, path, {'test': 1}) == [4]

def test_arrindex_nonexisting_element(redis: Redis):
    key = "json_arrindex"
    path = "$.array"
    value = 4

    assert redis.json.arrindex(key, path, value) == [-1]

def test_arrindex_nonarray_path(redis: Redis):
    key = "json_arrindex"
    path = "$.int"
    value = 4

    assert redis.json.arrindex(key, path, value) == [None]

def test_arrindex_wildcard(redis: Redis):
    key = "json_arrindex"
    path = "$..array"
    value = 1

    assert redis.json.arrindex(key, path, value) == [1, 0]
