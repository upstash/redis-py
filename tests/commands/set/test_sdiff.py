import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sets(redis: Redis):
    set1 = "set1"
    set2 = "set2"
    set3 = "set3"

    redis.delete(set1, set2, set3)


def test_sdiff(redis: Redis):
    set1 = "set1"
    set2 = "set2"
    set3 = "set3"

    # Add elements to the sets
    redis.sadd(set1, "element1", "element2", "element3", "element5", "element6")
    redis.sadd(set2, "element2", "element3", "element4")
    redis.sadd(set3, "element3", "element4", "element5")

    result = redis.sdiff(set1, set2, set3)

    result.sort()
    assert result == ["element1", "element6"]
