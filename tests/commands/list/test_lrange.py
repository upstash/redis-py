import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_mylist():
    mylist = "mylist"

    redis.delete(mylist)

def test_lrange_full_range():
    mylist = "mylist"
    values = ["value1", "value2", "value3", "value4"]

    redis.rpush(mylist, *values)

    result = redis.lrange(mylist, 0, -1)
    assert result == values

def test_lrange_partial_range():
    mylist = "mylist"
    values = ["value1", "value2", "value3", "value4"]

    redis.rpush(mylist, *values)

    result = redis.lrange(mylist, 1, 2)
    assert result == values[1:3]

def test_lrange_out_of_range_start():
    mylist = "mylist"
    values = ["value1", "value2", "value3", "value4"]

    redis.rpush(mylist, *values)

    result = redis.lrange(mylist, 10, 15)
    assert result == []

def test_lrange_out_of_range_end():
    mylist = "mylist"
    values = ["value1", "value2", "value3", "value4"]

    redis.rpush(mylist, *values)

    result = redis.lrange(mylist, 1, 10)
    assert result == values[1:]

def test_lrange_empty_list():
    mylist = "mylist"

    result = redis.lrange(mylist, 0, -1)
    assert result == []
