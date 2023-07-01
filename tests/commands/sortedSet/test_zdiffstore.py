import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_sorted_sets():
    sorted_set1 = "sorted_set1"
    sorted_set2 = "sorted_set2"
    diff_result = "diff_result"

    redis.delete(sorted_set1)
    redis.delete(sorted_set2)
    redis.delete(diff_result)

def test_zdiffstore():
    sorted_set1 = "sorted_set1"
    sorted_set2 = "sorted_set2"
    diff_result = "diff_result"

    redis.zadd(sorted_set1, {"member1": 10, "member2": 20, "member3": 30})
    redis.zadd(sorted_set2, {"member2": 20, "member4": 40, "member5": 50})

    result = redis.zdiffstore(destination=diff_result, keys=[sorted_set1, sorted_set2])
    assert result == 2

    assert redis.zrange(diff_result, 0, -1, withscores=True) == [("member1", 10.0), ("member3", 30.0)]
