import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sorted_set(redis: Redis):
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zpopmax(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})
    assert redis.zscore(sorted_set, "member3") == 30

    result = redis.zpopmax(sorted_set)
    assert result == [("member3", 30.0)]

    assert redis.zscore(sorted_set, "member3") is None


def test_zpopmax_with_count(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    result = redis.zpopmax(sorted_set, count=2)
    assert result == [("member3", 30.0), ("member2", 20.0)]


def test_zpopmax_without_formatting(redis: Redis):
    redis._format_return = False
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    result = redis.zpopmax(sorted_set)
    assert result == ["member3", "30"]

    assert redis.zscore(sorted_set, "member3") is None
