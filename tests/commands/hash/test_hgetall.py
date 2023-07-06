import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)
    yield
    redis.delete(hash_name)


def test_hgetall(redis: Redis):
    hash_name = "myhash"
    fields_values = {"field1": "value1", "field2": "value2"}

    redis.hset(hash_name, values=fields_values)

    result = redis.hgetall(hash_name)

    assert isinstance(result, dict)

    assert result == fields_values
