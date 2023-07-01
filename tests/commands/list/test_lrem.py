import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_mylist():
    mylist = "mylist"

    redis.delete(mylist)

def test_lrem_existing_value():
    mylist = "mylist"
    values = ["value1", "value2", "value3", "value2"]

    redis.rpush(mylist, *values)

    result = redis.lrem(mylist, count=0, element="value2")
    assert result == 2  # Number of removed elements

    expected_list = ["value1", "value3"]
    assert redis.lrange(mylist, 0, -1) == expected_list

def test_lrem_nonexistent_value():
    mylist = "mylist"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)

    result = redis.lrem(mylist, count=0, element="value4")
    assert result == 0  # No elements removed

    assert redis.lrange(mylist, 0, -1) == values

def test_lrem_with_count():
    mylist = "mylist"
    values = ["value1", "value2", "value3", "value2"]

    redis.rpush(mylist, *values)

    result = redis.lrem(mylist, count=1, element="value2")
    assert result == 1  # Number of removed elements

    expected_list = ["value1", "value3", "value2"]
    assert redis.lrange(mylist, 0, -1) == expected_list
