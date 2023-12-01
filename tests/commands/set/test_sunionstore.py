import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sets(redis: Redis):
    set1 = "set1"
    set2 = "set2"
    union_set = "union_set"

    redis.delete(set1)
    redis.delete(set2)
    redis.delete(union_set)
    yield
    redis.delete(set1)
    redis.delete(set2)
    redis.delete(union_set)


def test_sunionstore(redis: Redis):
    set1 = "set1"
    set2 = "set2"
    union_set = "union_set"

    # Add members to set1
    redis.sadd(set1, "apple", "banana", "cherry")

    # Add members to set2
    redis.sadd(set2, "banana", "cherry", "date")

    # Perform SUNIONSTORE operation on set1 and set2, storing the result in union_set
    result = redis.sunionstore(union_set, set1, set2)

    # Assert that the result is the number of members in the union set
    assert result == 4

    # Assert that the members in the union set are as expected
    members = redis.smembers(union_set)
    members.sort()
    assert members == ["apple", "banana", "cherry", "date"]


def test_sunionstore_empty_sets(redis: Redis):
    set1 = "set1"
    set2 = "set2"
    union_set = "union_set"

    # Perform SUNIONSTORE operation on empty set1 and set2, storing the result in union_set
    result = redis.sunionstore(union_set, set1, set2)

    # Assert that the result is 0 since both sets are empty
    assert result == 0

    # Assert that the union set is also empty
    assert redis.smembers(union_set) == []


def test_sunionstore_single_set(redis: Redis):
    set1 = "set1"
    union_set = "union_set"

    # Add members to set1
    redis.sadd(set1, "apple", "banana", "cherry")

    # Perform SUNIONSTORE operation on set1 only, storing the result in union_set
    result = redis.sunionstore(union_set, set1)

    # Assert that the result is the number of members in the set1 itself
    assert result == 3

    # Assert that the members in the union set are the same as set1
    members = redis.smembers(union_set)
    members.sort()
    assert members == ["apple", "banana", "cherry"]
