import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_sets(redis: Redis):
    set_name1 = "set1"
    set_name2 = "set2"
    redis.delete(set_name1)
    redis.delete(set_name2)
    yield
    redis.delete(set_name1)
    redis.delete(set_name2)


def test_smove_existing_member(redis: Redis):
    set_name1 = "set1"
    set_name2 = "set2"

    # Add a member to set1
    redis.sadd(set_name1, "member1")

    # Move the member from set1 to set2
    result = redis.smove(set_name1, set_name2, "member1")

    # Assert that the move operation was successful
    assert result == 1

    # Assert that the member has been moved to set2
    assert redis.sismember(set_name2, "member1")
    assert not redis.sismember(set_name1, "member1")


def test_smove_nonexistent_member(redis: Redis):
    set_name1 = "set1"
    set_name2 = "set2"

    # Attempt to move a non-existent member from set1 to set2
    result = redis.smove(set_name1, set_name2, "nonexistent")

    # Assert that the move operation returns 0, indicating the member doesn't exist
    assert result == 0

    # Assert that both sets remain empty
    assert redis.scard(set_name1) == 0
    assert redis.scard(set_name2) == 0
