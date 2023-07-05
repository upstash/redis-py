import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_sets():
    set1 = "set1"
    set2 = "set2"
    set3 = "set3"
    destination_set = "diff_set"

    redis.delete(set1, set2, set3, destination_set)


def test_sdiffstore():
    set1 = "set1"
    set2 = "set2"
    set3 = "set3"
    destination_set = "diff_set"

    # Add elements to the sets
    redis.sadd(set1, "element1", "element2", "element3")
    redis.sadd(set2, "element2", "element3", "element4")
    redis.sadd(set3, "element3", "element4", "element5")

    # Compute and store the difference of sets in a destination set
    result = redis.sdiffstore(destination_set, set1, set2, set3)

    expected_result = 1  # Expected number of elements in the destination set
    assert result == expected_result

    # Get the elements from the destination set
    destination_set_elements = redis.smembers(destination_set)

    expected_set_elements = {"element1"}  # Expected elements in the destination set
    assert destination_set_elements == expected_set_elements
