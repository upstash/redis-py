import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zscore():
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    score = redis.zscore(sorted_set, "member2")
    assert score == 20.0

    non_existent_score = redis.zscore(sorted_set, "nonexistent_member")
    assert non_existent_score is None


def test_zscore_without_formatting():
    redis._format_return = False
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    score = redis.zscore(sorted_set, "member2")
    assert score == "20"

    non_existent_score = redis.zscore(sorted_set, "nonexistent_member")
    assert non_existent_score is None

    redis._format_return = True
