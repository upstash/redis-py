import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sorted_set(redis: Redis):
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zrange(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30, "member4": 40})

    result = redis.zrange(sorted_set, start=1, stop=2, withscores=True, rev=True)

    assert isinstance(result, list)
    assert all(isinstance(member, tuple) and len(member) == 2 for member in result)

    assert all(member[0] in ["member3", "member2"] for member in result)

    assert all(isinstance(member[1], float) for member in result)


def test_zrange_with_sortby(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30, "member4": 40})

    with pytest.raises(Exception):
        redis.zrange(sorted_set, start=0, stop=2, withscores=True, sortby="BYLEX")

    result = redis.zrange(
        sorted_set, start=0, stop=2, withscores=True, sortby="BYSCORE"
    )

    assert isinstance(result, list)
    assert all(isinstance(member, tuple) and len(member) == 2 for member in result)

    assert all(member[0] in ["member1", "member2", "member3"] for member in result)

    assert all(isinstance(member[1], float) for member in result)


def test_zrange_with_bylex(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"apple": 1, "banana": 2, "cherry": 3, "date": 4})

    result = redis.zrange(sorted_set, start="[a", stop="[d", sortby="BYLEX")
    assert isinstance(result, list)

    assert set(result) == {"apple", "banana", "cherry"}
