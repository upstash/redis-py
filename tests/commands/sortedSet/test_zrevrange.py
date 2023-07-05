import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zrevrange():
    sorted_set = "sorted_set"

    redis.zadd(
        sorted_set, {"apple": 10, "banana": 20, "cherry": 30, "mango": 40, "orange": 50}
    )

    result = redis.zrevrange(sorted_set, 1, 3)
    assert result == ["mango", "cherry", "banana"]

    result = redis.zrevrange(sorted_set, 1, 3, withscores=True)
    assert result == [("mango", 40.0), ("cherry", 30.0), ("banana", 20.0)]

    result = redis.zrevrange(sorted_set, 0, -1)
    assert result == ["orange", "mango", "cherry", "banana", "apple"]

    result = redis.zrevrange("nonexistent_set", 0, -1)
    assert result == []
