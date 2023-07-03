import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_mylists():
    mylists = ["mylist1", "mylist2", "mylist3", "mylist4", "mylist5"]

    for mylist in mylists:
        redis.delete(mylist)


def test_lpop_existing_key():
    mylist = "mylist1"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)

    result = redis.lpop(mylist)
    assert result == "value1"
    assert redis.lrange(mylist, 0, -1) == ["value2", "value3"]

    redis.delete(mylist)


def test_lpop_empty_list():
    mylist = "mylist2"

    result = redis.lpop(mylist)
    assert result is None

    redis.delete(mylist)


def test_lpop_nonexistent_key():
    mylist = "mylist3"

    result = redis.lpop(mylist)
    assert result is None

    redis.delete(mylist)


def test_lpop_with_count():
    mylist = "mylist4"
    values = ["value1", "value2", "value3", "value4"]

    redis.rpush(mylist, *values)

    # Pop two elements using count parameter
    result = redis.lpop(mylist, count=2)
    assert result == ["value1", "value2"]
    assert redis.lrange(mylist, 0, -1) == ["value3", "value4"]

    # Pop all remaining elements
    result = redis.lpop(mylist, count=2)
    assert result == ["value3", "value4"]
    assert redis.lrange(mylist, 0, -1) == []

    redis.delete(mylist)


def test_lpop_with_count_zero():
    mylist = "mylist5"
    values = ["value1", "value2", "value3", "value4"]

    redis.rpush(mylist, *values)

    # Pop zero elements
    result = redis.lpop(mylist, count=0)
    assert result == []
    assert redis.lrange(mylist, 0, -1) == ["value1", "value2", "value3", "value4"]

    redis.delete(mylist)
