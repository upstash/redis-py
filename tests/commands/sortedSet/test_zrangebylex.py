import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sorted_set(redis: Redis):
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zrangebylex(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(
        sorted_set, {"apple": 1, "banana": 2, "cherry": 3, "mango": 4, "orange": 5}
    )

    result = redis.zrangebylex(sorted_set, min_score="-", max_score="(c")
    assert result == ["apple", "banana"]

    result = redis.zrangebylex(
        sorted_set, min_score="(b", max_score="+", limit_offset=1, limit_count=2
    )
    assert result == ["cherry", "mango"]
