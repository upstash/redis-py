import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_set(redis: Redis):
    set_name = "myset"
    redis.delete(set_name)
    yield
    redis.delete(set_name)


def test_spop_existing_member(redis: Redis):
    set_name = "myset"

    # Add members to the set
    redis.sadd(set_name, "member1", "member2", "member3")

    # Perform the spop operation
    result = redis.spop(set_name)

    # Assert that the result is not None
    assert result is not None
    assert isinstance(result, str)

    # Assert that the popped member is not present in the set anymore
    assert not redis.sismember(set_name, result)


def test_spop_empty_set(redis: Redis):
    set_name = "myset"

    # Perform the spop operation on an empty set
    result = redis.spop(set_name)

    # Assert that the result is None
    assert result is None


def test_spop_nonexistent_set(redis: Redis):
    set_name = "nonexistent_set"

    # Perform the spop operation on a nonexistent set
    result = redis.spop(set_name)

    # Assert that the result is None
    assert result is None
