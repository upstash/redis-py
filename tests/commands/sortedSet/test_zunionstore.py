import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_sorted_sets():
    sorted_set1 = "sorted_set1"
    sorted_set2 = "sorted_set2"
    destination = "union_result"

    redis.delete(sorted_set1)
    redis.delete(sorted_set2)
    redis.delete(destination)

def test_zunionstore():
    sorted_set1 = "sorted_set1"
    sorted_set2 = "sorted_set2"
    destination = "union_result"

    redis.zadd(sorted_set1, {"member1": 10, "member2": 20, "member3": 30})

    redis.zadd(sorted_set2, {"member2": 5, "member3": 15, "member4": 25})

    num_elements = redis.zunionstore(destination, keys=[sorted_set1, sorted_set2], aggregate="SUM")
    assert num_elements == 4

    assert redis.zrange(destination, 0, -1, withscores=True) == [
        ("member1", 10.0),
        ("member2", 25.0),
        ("member4", 25.0),
        ("member3", 45.0),
    ]

    num_elements = redis.zunionstore(destination, keys=[sorted_set1, sorted_set2], aggregate="SUM", weights=[1,2])
    assert num_elements == 4

    assert redis.zrange(destination, 0, -1, withscores=True) == [
        ("member1", 10.0),
        ("member2", 30.0),
        ("member4", 50.0),
        ("member3", 60.0),
    ]
