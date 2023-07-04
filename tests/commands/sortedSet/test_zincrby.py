import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"
    redis.delete(sorted_set)
    yield
    redis.delete(sorted_set)


def test_zincrby():
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    result = redis.zincrby(sorted_set, member="member2", increment=5)
    assert result == 25.0

    assert redis.zscore(sorted_set, "member2") == 25.0

    result = redis.zincrby(sorted_set, member="member4", increment=5)
    assert result == 5.0

    assert redis.zrange(sorted_set, 0, -1, withscores=True) == [
        ("member4", 5.0),
        ("member1", 10.0),
        ("member2", 25.0),
        ("member3", 30.0),
    ]
