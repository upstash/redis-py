import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"

    redis.delete(sorted_set)


def test_zlexcount():
    sorted_set = "sorted_set"

    redis.zadd(
        sorted_set, {"apple": 1, "banana": 2, "cherry": 3, "grape": 4, "orange": 2}
    )

    result = redis.zlexcount(sorted_set, "[banana", "[orange")
    assert result == 4
