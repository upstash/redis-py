import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sorted_set(redis: Redis):
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zcard(redis: Redis):
    sorted_set = "sorted_set"

    # Add members to the sorted set
    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    # Get the cardinality of the sorted set
    result = redis.zcard(sorted_set)

    # Assert that the result is the correct cardinality
    assert result == 3
