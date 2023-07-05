import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_set(redis: Redis):
    set_name = "myset"
    redis.delete(set_name)
    yield
    redis.delete(set_name)


def test_smismember(redis: Redis):
    set_name = "myset"

    redis.sadd(set_name, "element1", "element2", "element3")

    members = redis.smismember(set_name, "element1", "element3", "non_existing_element")

    assert isinstance(members, list)

    assert members == [True, 1, False]


def test_smismember_nonexisting_set(redis: Redis):
    members = redis.smismember(
        "non_existing_set", "element1", "element3", "non_existing_element"
    )

    assert members == [False, False, False]
