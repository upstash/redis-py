import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sorted_set(redis: Redis):
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zpopmin(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})
    assert redis.zscore(sorted_set, "member1") == 10

    result = redis.zpopmin(sorted_set)

    assert result == [("member1", 10.0)]

    assert redis.zscore(sorted_set, "member1") is None


def test_zpopmin_with_count(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    result = redis.zpopmin(sorted_set, count=2)

    assert result == [("member1", 10.0), ("member2", 20.0)]
