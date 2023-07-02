import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_sorted_sets():
    sorted_set1 = "sorted_set1"
    sorted_set2 = "sorted_set2"
    destination = "destination"

    redis.delete(sorted_set1)
    redis.delete(sorted_set2)
    redis.delete(destination)

def test_zinterstore():
    sorted_set1 = "sorted_set1"
    sorted_set2 = "sorted_set2"
    destination = "destination"

    redis.zadd(sorted_set1, {"member1": 10, "member2": 20, "member3": 30, "memberx": 3})
    redis.zadd(sorted_set2, {"member1": 5, "member3": 15, "member4": 25, "memberx": 5})

    weights = [2, 1]
    result = redis.zinterstore(destination, keys=[sorted_set1, sorted_set2], weights=weights, aggregate="SUM")
    assert result == 3

    assert redis.zrange(destination, 0, -1, withscores=True) == [
        ("memberx", 11.0),
        ("member1", 25.0),
        ("member3", 75.0),
    ]
