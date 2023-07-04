import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"
    redis.delete(sorted_set)
    yield
    redis.delete(sorted_set)


def test_zadd():
    sorted_set = "sorted_set"

    # Add members with their respective scores to the sorted set
    result = redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    # Assert that the result is the number of members added
    assert result == 3

    # Assert that the members are added with their scores
    assert redis.zrange(sorted_set, 0, -1, withscores=True) == [
        ("member1", 10.0),
        ("member2", 20.0),
        ("member3", 30.0),
    ]


def test_zadd_existing_member():
    sorted_set = "sorted_set"

    # Add an initial member to the sorted set
    redis.zadd(sorted_set, {"member1": 10})

    # Add an existing member with a different score to the sorted set
    result = redis.zadd(sorted_set, {"member1": 20})

    # Assert that the result is 0 since the member already exists
    assert result == 0

    # Assert that the score of the existing member is updated
    assert redis.zscore(sorted_set, "member1") == 20.0


def test_zadd_exception_with_incr():
    sorted_set = "sorted_set"

    # Add multiple members to the sorted set, incr with multiple should throw exception
    with pytest.raises(Exception):
        redis.zadd(sorted_set, {"member1": 10, "member2": 20}, incr=True)


def test_zadd_multiple_members():
    sorted_set = "sorted_set"

    # Add multiple members to the sorted set with their scores
    result = redis.zadd(sorted_set, {"member1": 10, "member2": 20, "member3": 30})

    # Assert that the result is the number of members added
    assert result == 3

    result = redis.zadd(sorted_set, {"member1": 5}, incr=True)
    assert result == 15

    result = redis.zadd(sorted_set, {"member2": 10}, incr=True)
    assert result == 30

    # Assert that the members' scores are incremented correctly
    assert redis.zrange(sorted_set, 0, -1, withscores=True) == [
        ("member1", 15.0),
        ("member2", 30.0),
        ("member3", 30.0),
    ]
