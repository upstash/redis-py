import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sorted_sets(redis: Redis):
    sorted_set1 = "sorted_set1"
    sorted_set2 = "sorted_set2"
    redis.delete(sorted_set1)
    redis.delete(sorted_set2)
    yield
    redis.delete(sorted_set1)
    redis.delete(sorted_set2)


def test_zdiff(redis: Redis):
    sorted_set1 = "sorted_set1"
    sorted_set2 = "sorted_set2"

    redis.zadd(sorted_set1, {"member1": 10, "member2": 20, "member3": 30})
    redis.zadd(sorted_set2, {"member2": 20, "member4": 40, "member5": 50})

    diff_result = redis.zdiff(keys=[sorted_set1, sorted_set2])
    assert diff_result == ["member1", "member3"]


def test_zdiff_with_scores(redis: Redis):
    sorted_set1 = "sorted_set1"
    sorted_set2 = "sorted_set2"

    redis.zadd(sorted_set1, {"member1": 10, "member2": 20, "member3": 30})
    redis.zadd(sorted_set2, {"member2": 20, "member4": 40, "member5": 50})

    diff_result = redis.zdiff(keys=[sorted_set1, sorted_set2], withscores=True)
    assert diff_result == [("member1", 10.0), ("member3", 30.0)]
