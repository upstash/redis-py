import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_mylists():
    mylists = ["mylist1", "mylist2", "mylist3", "mylist4"]

    for mylist in mylists:
        redis.delete(mylist)

def test_llen_empty_list():
    mylist = "mylist1"

    result = redis.llen(mylist)
    assert result == 0

def test_llen_non_empty_list():
    mylist = "mylist2"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)

    result = redis.llen(mylist)
    assert result == 3

def test_llen_nonexistent_list():
    mylist = "mylist3"

    result = redis.llen(mylist)
    assert result == 0

def test_llen_after_deletion():
    mylist = "mylist4"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)
    redis.delete(mylist)

    result = redis.llen(mylist)
    assert result == 0
