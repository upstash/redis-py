import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zremrangebylex():
    sorted_set = "sorted_set"

    redis.zadd(
        sorted_set, {"apple": 10, "banana": 20, "cherry": 30, "mango": 40, "orange": 50}
    )

    result = redis.zremrangebylex(sorted_set, "[banana", "(mango")
    assert result == 2

    assert redis.zscore(sorted_set, "banana") is None
    assert redis.zscore(sorted_set, "cherry") is None

    result = redis.zremrangebylex("nonexistent_set", "[", "+")
    assert result == 0
