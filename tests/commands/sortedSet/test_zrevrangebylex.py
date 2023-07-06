import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sorted_set(redis: Redis):
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zrevrangebylex(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(
        sorted_set, {"apple": 1, "banana": 2, "cherry": 3, "mango": 4, "orange": 5}
    )

    result = redis.zrevrangebylex(sorted_set, "[orange", "[cherry")
    assert result == ["orange", "mango", "cherry"]

    result = redis.zrevrangebylex(sorted_set, "[orange", "[cherry", offset=1, count=2)
    assert result == ["mango", "cherry"]

    result = redis.zrevrangebylex(sorted_set, "+", "-")
    assert result == ["orange", "mango", "cherry", "banana", "apple"]

    result = redis.zrevrangebylex("nonexistent_set", "+", "-")
    assert result == []
