import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_set(redis: Redis):
    set_name = "myset"
    redis.delete(set_name)
    yield
    redis.delete(set_name)


def test_scard(redis: Redis):
    set_name = "myset"

    # Add elements to the set
    redis.sadd(set_name, "element1", "element3")

    # Get the cardinality of the set
    result = redis.scard(set_name)

    assert result == 2
