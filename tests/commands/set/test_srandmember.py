import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_set():
    set_name = "myset"

    redis.delete(set_name)

def test_srandmember_existing_set():
    set_name = "myset"
    members = ["member1", "member2", "member3"]

    # Add members to the set
    redis.sadd(set_name, *members)

    # Perform the srandmember operation
    result = redis.srandmember(set_name)

    # Assert that the result is a member of the set
    assert result in members

def test_srandmember_nonexistent_set():
    set_name = "nonexistent_set"

    # Perform the srandmember operation on a nonexistent set
    result = redis.srandmember(set_name)

    # Assert that the result is None
    assert result is None
