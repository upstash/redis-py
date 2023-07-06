import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sets(redis: Redis):
    set1 = "set1"
    set2 = "set2"
    set3 = "set3"

    redis.delete(set1, set2, set3)


def test_sinter(redis: Redis):
    set1 = "set1"
    set2 = "set2"
    set3 = "set3"

    # Add elements to the sets
    redis.sadd(set1, "element1", "element2", "element3")
    redis.sadd(set2, "element2", "element3", "element4")
    redis.sadd(set3, "element3", "element4", "element5")

    # Compute the intersection of sets
    result = redis.sinter(set1, set2, set3)

    expected_result = {"element3"}  # Expected elements in the intersection
    assert result == expected_result
