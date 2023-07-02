import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_sets():
    set1 = "set1"
    set2 = "set2"
    set3 = "set3"

    redis.delete(set1, set2, set3)

def test_sdiff():
    set1 = "set1"
    set2 = "set2"
    set3 = "set3"

    # Add elements to the sets
    redis.sadd(set1, "element1", "element2", "element3", "element5", "element6")
    redis.sadd(set2, "element2", "element3", "element4")
    redis.sadd(set3, "element3", "element4", "element5")

    result = redis.sdiff(set1, set2, set3)

    expected_result = {"element6", "element1"}
    assert result == expected_result
