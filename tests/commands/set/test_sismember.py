import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_set(redis: Redis):
    set_name = "myset"
    redis.delete(set_name)
    yield
    redis.delete(set_name)


def test_sismember(redis: Redis):
    set_name = "myset"

    # Add elements to the set
    redis.sadd(set_name, "element1", "element2", "element3")

    # Check if elements are members of the set
    assert redis.sismember(set_name, "element1") is True
    assert redis.sismember(set_name, "element2") == 1
    assert redis.sismember(set_name, "element3") == 1
    assert redis.sismember(set_name, "element4") == 0
