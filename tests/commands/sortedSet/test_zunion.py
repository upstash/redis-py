import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sorted_sets():
    sorted_set1 = "sorted_set1"
    sorted_set2 = "sorted_set2"

    redis.delete(sorted_set1)
    redis.delete(sorted_set2)


def test_zunion():
    sorted_set1 = "sorted_set1"
    sorted_set2 = "sorted_set2"
    destination = "union_result"

    redis.zadd(sorted_set1, {"member1": 10, "member2": 20, "member3": 30})

    redis.zadd(sorted_set2, {"member2": 5, "member3": 15, "member4": 25})

    num_elements = redis.zunion(keys=[sorted_set1, sorted_set2], aggregate="MAX")
    assert num_elements == ["member1", "member2", "member4", "member3"]

    res = redis.zunion(
        keys=[sorted_set1, sorted_set2],
        aggregate="SUM",
        withscores=True,
        weights=[2, 3],
    )

    assert isinstance(res, list)
    assert isinstance(res[0], tuple)

    assert res == [
        ("member1", 20.0),
        ("member2", 55.0),
        ("member4", 75.0),
        ("member3", 105.0),
    ]
