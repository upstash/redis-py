import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sorted_set(redis: Redis):
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zmscore(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    members = ["member1", "member3", "non_existing_member"]
    result = redis.zmscore(sorted_set, members=members)

    assert result == [10.0, 30.0, None]


def test_zmscore_without_formatting(redis: Redis):
    redis._format_return = False

    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    members = ["member1", "member3", "non_existing_member"]
    result = redis.zmscore(sorted_set, members=members)

    assert result == ["10", "30", None]
