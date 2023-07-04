import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_set():
    set_name = "myset"
    redis.delete(set_name)
    yield
    redis.delete(set_name)


def test_sismember():
    set_name = "myset"

    # Add elements to the set
    redis.sadd(set_name, "element1", "element2", "element3")

    # Check if elements are members of the set
    assert redis.sismember(set_name, "element1") == True
    assert redis.sismember(set_name, "element2") == 1
    assert redis.sismember(set_name, "element3") == 1
    assert redis.sismember(set_name, "element4") == 0
