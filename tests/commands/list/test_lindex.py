from pytest import fixture, mark
from tests.sync_client import redis

@fixture(scope="module")
def setup_teardown():
    # Add test data
    redis.rpush("mylist", "value1", "value2", "value3")

    yield

    # Clean up the list
    redis.delete("mylist")

@mark.usefixtures("setup_teardown")
def test_lindex_existing_index():
    # Test lindex command with an existing index
    result = redis.lindex("mylist", 0)
    assert result == "value1"

@mark.usefixtures("setup_teardown")
def test_lindex_invalid_index():
    # Test lindex command with an invalid index
    result = redis.lindex("mylist", 10)
    assert result is None

@mark.usefixtures("setup_teardown")
def test_lindex_empty_list():
    # Clear the list
    redis.delete("mylist")

    # Test lindex command on an empty list
    result = redis.lindex("mylist", 0)
    assert result is None
