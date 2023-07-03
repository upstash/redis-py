import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sets():
    set1 = "set1"
    set2 = "set2"
    set3 = "set3"
    result_set = "result_set"

    redis.delete(set1, set2, set3, result_set)


def test_sinterstore():
    set1 = "set1"
    set2 = "set2"
    set3 = "set3"
    result_set = "result_set"

    # Add elements to the sets
    redis.sadd(set1, "element1", "element2", "element3")
    redis.sadd(set2, "element2", "element3", "element4")
    redis.sadd(set3, "element3", "element4", "element5")

    # Compute the intersection of sets and store the result in a new set
    result = redis.sinterstore(result_set, set1, set2, set3)

    expected_result = 1  # Number of elements in the resulting set
    assert result == expected_result

    # Check the elements in the resulting set
    result_set_elements = redis.smembers(result_set)
    expected_elements = {"element3"}  # Expected elements in the resulting set
    assert result_set_elements == expected_elements
