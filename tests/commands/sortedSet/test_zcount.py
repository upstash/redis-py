import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"
    redis.delete(sorted_set)
    yield
    redis.delete(sorted_set)


def test_zcount():
    sorted_set = "sorted_set"

    # Add members to the sorted set
    redis.zadd(
        sorted_set,
        {"member1": 10, "member2": 20, "member3": 30, "member4": 40, "member5": 50},
    )

    # Perform a range query using ZCOUNT
    result = redis.zcount(sorted_set, min_score=20, max_score=40)

    # Assert that the result is the correct count
    assert result == 3

    result = redis.zcount(sorted_set, min_score="-inf", max_score="+inf")
    assert result == 5
