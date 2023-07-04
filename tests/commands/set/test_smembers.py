import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_set():
    set_name = "myset"

    redis.delete(set_name)


def test_smembers():
    set_name = "myset"

    # Add elements to the set
    redis.sadd(set_name, "element1", "element2", "element3")

    # Get all members of the set
    members = redis.smembers(set_name)

    # Assert that the members are correct
    assert members == {"element1", "element2", "element3"}
