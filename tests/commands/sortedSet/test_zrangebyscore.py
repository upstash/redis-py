import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"

    redis.delete(sorted_set)

def test_zrangebyscore():
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"apple": 10, "banana": 20, "cherry": 30, "mango": 40, "orange": 50})

    result = redis.zrangebyscore(sorted_set, min_score=20, max_score=40)
    assert result == ["banana", "cherry", "mango"]

    result = redis.zrangebyscore(sorted_set, min_score=20, max_score=40, limit_offset=1, limit_count=2)
    assert result == ["cherry", "mango"]

    result = redis.zrangebyscore(sorted_set, min_score=20, max_score=40, limit_offset=1, limit_count=2, withscores=True)
    assert result == [("cherry", 30.0), ("mango", 40.0)]
