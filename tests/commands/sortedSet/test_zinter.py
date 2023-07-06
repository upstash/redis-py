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


def test_zinter(redis: Redis):
    sorted_set1 = "sorted_set1"
    sorted_set2 = "sorted_set2"

    redis.zadd(sorted_set1, {"member1": 10, "member2": 20, "member3": 30})
    redis.zadd(sorted_set2, {"member1": 5, "member3": 15, "member4": 25})

    result = redis.zinter(keys=[sorted_set1, sorted_set2])

    assert result == ["member1", "member3"]


def test_zinter_with_scores(redis: Redis):
    sorted_set1 = "sorted_set1"
    sorted_set2 = "sorted_set2"

    redis.zadd(sorted_set1, {"member1": 10, "member2": 20, "member3": 30})
    redis.zadd(sorted_set2, {"member1": 5, "member3": 15, "member4": 25})

    result = redis.zinter(keys=[sorted_set1, sorted_set2], withscores=True)

    assert result == [("member1", 15.0), ("member3", 45.0)]


def test_zinter_with_aggregate_and_weights(redis: Redis):
    sorted_set1 = "sorted_set1"
    sorted_set2 = "sorted_set2"

    redis.zadd(sorted_set1, {"member1": 10, "member2": 20, "member3": 30})
    redis.zadd(sorted_set2, {"member1": 5, "member3": 15, "member4": 25})

    weights = [2, 1]
    result = redis.zinter(
        keys=[sorted_set1, sorted_set2],
        weights=weights,
        aggregate="SUM",
        withscores=True,
    )

    assert result == [("member1", 25.0), ("member3", 75.0)]
