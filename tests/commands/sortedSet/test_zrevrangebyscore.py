import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zrevrangebyscore():
    sorted_set = "sorted_set"

    redis.zadd(
        sorted_set, {"apple": 1, "banana": 2, "cherry": 3, "mango": 4, "orange": 5}
    )

    result = redis.zrevrangebyscore(sorted_set, "+inf", "-inf")
    assert result == ["orange", "mango", "cherry", "banana", "apple"]

    result = redis.zrevrangebyscore(sorted_set, 4, 2)
    assert result == ["mango", "cherry", "banana"]

    result = redis.zrevrangebyscore(
        sorted_set, 4, 2, withscores=True, limit_offset=1, limit_count=2
    )
    assert result == [("cherry", 3.0), ("banana", 2.0)]

    result = redis.zrevrangebyscore("nonexistent_set", "+inf", "-inf")
    assert result == []
