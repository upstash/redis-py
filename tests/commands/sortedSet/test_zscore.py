import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sorted_set(redis: Redis):
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zscore(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    score = redis.zscore(sorted_set, "member2")
    assert score == 20.0

    non_existent_score = redis.zscore(sorted_set, "nonexistent_member")
    assert non_existent_score is None
