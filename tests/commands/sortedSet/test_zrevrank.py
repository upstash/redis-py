import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zrevrank():
    sorted_set = "sorted_set"

    redis.zadd(
        sorted_set, {"apple": 1, "banana": 2, "cherry": 3, "mango": 4, "orange": 5}
    )

    result = redis.zrevrank(sorted_set, "banana")
    assert result == 3

    result = redis.zrevrank(sorted_set, "watermelon")
    assert result is None

    result = redis.zrevrank("nonexistent_set", "apple")
    assert result is None
