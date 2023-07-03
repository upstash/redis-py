import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zremrangebyscore():
    sorted_set = "sorted_set"

    redis.zadd(
        sorted_set, {"apple": 10, "banana": 20, "cherry": 30, "mango": 40, "orange": 50}
    )

    result = redis.zremrangebyscore(sorted_set, 20, 40)
    assert result == 3

    assert redis.zscore(sorted_set, "banana") is None
    assert redis.zscore(sorted_set, "cherry") is None
    assert redis.zscore(sorted_set, "mango") is None

    result = redis.zremrangebyscore("nonexistent_set", 0, 100)
    assert result == 0
