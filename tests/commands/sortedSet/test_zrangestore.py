import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"
    destination = "destination"

    redis.delete(sorted_set)
    redis.delete(destination)


def test_zrangestore():
    sorted_set = "sorted_set"
    destination = "destination"

    redis.zadd(sorted_set, {"member1": 1, "member2": 2, "member3": 3, "member4": 4})

    result = redis.zrangestore(destination, sorted_set, start=0, stop=2)
    assert result == 3

    assert redis.zrange(destination, 0, -1, withscores=True) == [
        ("member1", 1.0),
        ("member2", 2.0),
        ("member3", 3.0),
    ]


def test_zrangestore_rev():
    sorted_set = "sorted_set"
    destination = "destination"

    redis.zadd(sorted_set, {"member1": 1, "member2": 2, "member3": 3, "member4": 4})

    result = redis.zrangestore(destination, sorted_set, start=0, stop=2, rev=True)
    assert result == 3

    assert redis.zrange(destination, 0, -1, withscores=True) == [
        ("member2", 2.0),
        ("member3", 3.0),
        ("member4", 4.0),
    ]


def test_zrangestore_limit():
    sorted_set = "sorted_set"
    destination = "destination"

    redis.zadd(sorted_set, {"member1": 1, "member2": 2, "member3": 3, "member4": 4})

    result = redis.zrangestore(
        destination, sorted_set, start=0, stop=2, limit_offset=1, limit_count=1
    )
    assert result == 1

    assert redis.zrange(destination, 0, -1, withscores=True) == [("member2", 2.0)]
