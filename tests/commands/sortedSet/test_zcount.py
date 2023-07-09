import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sorted_set(redis: Redis):
    sorted_set = "sorted_set"
    redis.delete(sorted_set)
    yield
    redis.delete(sorted_set)


def test_zcount(redis: Redis):
    sorted_set = "sorted_set"

    # Add members to the sorted set
    redis.zadd(
        sorted_set,
        {"member1": 10, "member2": 20, "member3": 30, "member4": 40, "member5": 50},
    )

    # Perform a range query using ZCOUNT
    result = redis.zcount(sorted_set, min=20, max=40)

    # Assert that the result is the correct count
    assert result == 3

    result = redis.zcount(sorted_set, min="-inf", max="+inf")
    assert result == 5
