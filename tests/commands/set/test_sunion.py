import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sets():
    set1 = "set1"
    set2 = "set2"

    redis.delete(set1)
    redis.delete(set2)


def test_sunion():
    set1 = "set1"
    set2 = "set2"

    # Add members to set1
    redis.sadd(set1, "apple", "banana", "cherry")

    # Add members to set2
    redis.sadd(set2, "banana", "cherry", "date")

    # Perform SUNION operation on set1 and set2
    result = redis.sunion(set1, set2)

    # Assert that the union of both sets is returned
    assert result == {"apple", "banana", "cherry", "date"}


def test_sunion_empty_sets():
    set1 = "set1"
    set2 = "set2"

    # Perform SUNION operation on empty set1 and set2
    result = redis.sunion(set1, set2)

    # Assert that an empty set is returned
    assert result == set()


def test_sunion_single_set():
    set1 = "set1"

    # Add members to set1
    redis.sadd(set1, "apple", "banana", "cherry")

    # Perform SUNION operation on set1 only
    result = redis.sunion(set1)

    # Assert that set1 itself is returned
    assert result == {"apple", "banana", "cherry"}

    with pytest.raises(Exception):
        redis.sunion()
