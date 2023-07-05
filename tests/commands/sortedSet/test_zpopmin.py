import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zpopmin():
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})
    assert redis.zscore(sorted_set, "member1") == 10

    result = redis.zpopmin(sorted_set)

    assert result == [("member1", 10.0)]

    assert redis.zscore(sorted_set, "member1") is None


def test_zpopmin_with_count():
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    result = redis.zpopmin(sorted_set, count=2)

    assert result == [("member1", 10.0), ("member2", 20.0)]
