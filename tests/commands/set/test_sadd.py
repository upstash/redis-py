import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_set(redis: Redis):
    set_name = "myset"
    redis.delete(set_name)


def test_sadd(redis: Redis):
    set_name = "myset"

    # Add elements to the set
    result = redis.sadd(set_name, "element1", "element2", "element3")

    assert result == 3  # Number of elements added to the set

    # Verify that the set contains the added elements
    assert redis.smembers(set_name) == {"element1", "element2", "element3"}
