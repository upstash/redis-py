import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sorted_set(redis: Redis):
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zrandmember(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    result = redis.zrandmember(sorted_set)

    assert result in ["member1", "member2", "member3"]


def test_zrandmember_with_count(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    result = redis.zrandmember(sorted_set, count=2)

    assert isinstance(result, list)
    assert len(result) == 2

    assert all(member in ["member1", "member2", "member3"] for member in result)


def test_zrandmember_with_withscores(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    result = redis.zrandmember(sorted_set, count=2, withscores=True)
    assert isinstance(result, list)

    assert all(isinstance(member, tuple) and len(member) == 2 for member in result)
    assert all(member[0] in ["member1", "member2", "member3"] for member in result)
    assert all(isinstance(member[1], float) for member in result)
