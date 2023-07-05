import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_set():
    set_name = "myset"
    redis.delete(set_name)
    yield
    redis.delete(set_name)


def test_srem_existing_members():
    set_name = "myset"
    members = ["member1", "member2", "member3"]

    # Add members to the set
    redis.sadd(set_name, *members)

    # Perform the srem operation
    result = redis.srem(set_name, "member1", "member3")

    # Assert that the result is the number of members removed
    assert result == 2

    # Assert that the removed members no longer exist in the set
    assert redis.sismember(set_name, "member1") == 0
    assert redis.sismember(set_name, "member3") is False


def test_srem_nonexistent_members():
    set_name = "myset"
    members = ["member1", "member2", "member3"]

    # Add members to the set
    redis.sadd(set_name, *members)

    # Perform the srem operation with non-existent members
    result = redis.srem(set_name, "member4", "member5")

    # Assert that the result is 0, as no members were removed
    assert result == 0

    # Assert that the original members still exist in the set
    assert redis.sismember(set_name, "member1") == 1
    assert redis.sismember(set_name, "member2") is True
    assert redis.sismember(set_name, "member3") is True
