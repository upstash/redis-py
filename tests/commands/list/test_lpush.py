import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_mylists():
    mylists = ["mylist1", "mylist2", "mylist3", "mylist4"]

    for mylist in mylists:
        redis.delete(mylist)

def test_lpush_single_value():
    mylist = "mylist1"
    value = "value1"

    result = redis.lpush(mylist, value)
    assert result == 1
    assert redis.lrange(mylist, 0, -1) == [value]

def test_lpush_multiple_values():
    mylist = "mylist2"
    values = ["value1", "value2", "value3"]
    reverse_values = ["value3", "value2", "value1"]

    result = redis.lpush(mylist, *values)
    assert result == len(values)
    assert redis.lrange(mylist, 0, -1) == reverse_values

def test_lpush_existing_list():
    mylist = "mylist3"
    values = ["value1", "value2"]

    redis.lpush(mylist, *values)

    new_values = ["value3", "value4"]
    result = redis.lpush(mylist, *new_values)
    assert result == len(new_values) + len(values)
    assert redis.lrange(mylist, 0, -1) == ["value4", "value3", "value2", "value1"]

def test_lpush_empty_list():
    mylist = "mylist4"
    values = []

    with pytest.raises(Exception):
        redis.lpush(mylist, *values)

    
