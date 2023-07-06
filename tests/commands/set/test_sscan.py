import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_set(redis: Redis):
    set_name = "myset"

    redis.delete(set_name)


def test_sscan_with_match_and_count(redis: Redis):
    set_name = "myset"

    # Add members to the set
    members = ["apple", "banana", "cherry", "avocado", "apricot"]
    redis.sadd(set_name, *members)

    # Use SSCAN to retrieve members matching a pattern with a count of 2
    cursor, matching_members = redis.sscan(set_name, match="a*", count=2)

    # Assert that the matching members are returned with the specified count
    assert all(member in members for member in matching_members)


def test_sscan_without_match_and_count(redis: Redis):
    set_name = "myset"

    # Add members to the set
    members = ["apple", "banana", "cherry", "avocado", "apricot"]
    redis.sadd(set_name, *members)

    # Use SSCAN to retrieve members with a count of 3
    cursor, scanned_members = redis.sscan(set_name, count=5)

    # Assert that the specified count of members is returned
    assert len(scanned_members) == 5
