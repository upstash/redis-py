import pytest
from upstash_redis import Redis


@pytest.fixture(autouse=True)
def setup_json(redis: Redis):
    json_key = "json_nummultby"
    value = {"int": 2, "str": "test", "object": {"int": 3}}
    redis.json.set(json_key, "$", value)
    yield
    redis.delete(json_key)


def test_nummultby(redis: Redis):
    key = "json_nummultby"
    path = "$.int"
    mult = 3

    assert redis.json.nummultby(key, path, mult) == [6]
    assert redis.json.get(key, path) == [6]


def test_nummultby_nonintkey(redis: Redis):
    key = "json_nummultby"
    path = "$.str"
    mult = 1

    assert redis.json.nummultby(key, path, mult) == [None]


def test_nummultby_nonexisting_key(redis: Redis):
    key = "json_nummultby"
    path = "$.nonexisting_key"
    mult = 3

    assert redis.json.nummultby(key, path, mult) == []
    assert redis.json.get(key, path) == []


def test_nummultby_wildcard(redis: Redis):
    key = "json_nummultby"
    path = "$..int"
    mult = 3

    assert redis.json.nummultby(key, path, mult) == [9, 6]
    assert redis.json.get(key, path) == [9, 6]
