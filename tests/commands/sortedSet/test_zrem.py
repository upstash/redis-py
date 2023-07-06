import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sorted_set(redis: Redis):
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zrem(redis: Redis):
    sorted_set = "sorted_set"

    redis.zadd(
        sorted_set, {"apple": 10, "banana": 20, "cherry": 30, "mango": 40, "orange": 50}
    )

    result = redis.zrem(sorted_set, "banana", "cherry")
    assert result == 2

    assert redis.zscore(sorted_set, "banana") is None

    result = redis.zrem(sorted_set, "watermelon")
    assert result == 0
