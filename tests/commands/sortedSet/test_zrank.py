import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"

    redis.delete(sorted_set)

def test_zrank():
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"apple": 10, "banana": 20, "cherry": 30, "mango": 40, "orange": 50})

    result = redis.zrank(sorted_set, "cherry")
    assert result == 2

    result = redis.zrank(sorted_set, "watermelon")
    assert result is None
