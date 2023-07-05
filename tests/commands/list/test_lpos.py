import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_mylists():
    mylists = ["mylist1", "mylist2", "mylist3", "mylist4", "mylist5"]

    for mylist in mylists:
        redis.delete(mylist)


def test_lpos_existing_value():
    mylist = "mylist1"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)

    result = redis.lpos(mylist, "value2")
    assert result == 1


def test_lpos_nonexistent_value():
    mylist = "mylist2"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)

    result = redis.lpos(mylist, "value4")
    assert result is None


def test_lpos_with_count():
    mylist = "mylist3"
    values = ["value1", "value2", "value2", "value3", "value2"]

    redis.rpush(mylist, *values)

    result = redis.lpos(mylist, "value2", count=2)
    assert result == [1, 2]


def test_lpos_with_rank():
    mylist = "mylist4"
    values = ["value1", "value2", "value3", "value2"]

    redis.rpush(mylist, *values)

    result = redis.lpos(mylist, "value2", rank=2)
    assert result == 3


def test_lpos_with_maxlen():
    mylist = "mylist5"
    values = ["value1", "value2", "value3", "value4"]

    redis.rpush(mylist, *values)

    result = redis.lpos(mylist, "value2", maxlen=3)
    assert result == 1
