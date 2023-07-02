import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_sorted_set():
    sorted_set = "sorted_set"

    redis.delete(sorted_set)

def test_zscan():
    sorted_set = "sorted_set"

    redis.zadd(sorted_set, {"apple": 1, "banana": 2, "cherry": 3, "mango": 4, "orange": 5})

    cursor = 0
    members = []

    while True:
        cursor, scan_members = redis.zscan(sorted_set, cursor=cursor)

        members.extend(scan_members)

        if cursor == 0:
            break

    expected_members = [("apple", 1.0), ("banana", 2.0), ("cherry", 3.0), ("mango", 4.0), ("orange", 5.0)]
    assert sorted(members) == sorted(expected_members)

    cursor = 0
    pattern = "m*"
    matching_members = []

    while True:
        cursor, scan_members = redis.zscan(sorted_set, cursor=cursor, match_pattern=pattern)

        matching_members.extend(scan_members)

        if cursor == 0:
            break

    expected_matching_members = [("mango", 4.0)]
    assert matching_members == expected_matching_members
