import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zcard():
    sorted_set = "sorted_set"

    # Add members to the sorted set
    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    # Get the cardinality of the sorted set
    result = redis.zcard(sorted_set)

    # Assert that the result is the correct cardinality
    assert result == 3
