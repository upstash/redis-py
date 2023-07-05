import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_mylists(redis: Redis):
    mylists = ["mylist1", "mylist2", "mylist3", "mylist4", "mylist5"]

    for mylist in mylists:
        redis.delete(mylist)


def test_lpos_existing_value(redis: Redis):
    mylist = "mylist1"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)

    result = redis.lpos(mylist, "value2")
    assert result == 1


def test_lpos_nonexistent_value(redis: Redis):
    mylist = "mylist2"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)

    result = redis.lpos(mylist, "value4")
    assert result is None


def test_lpos_with_count(redis: Redis):
    mylist = "mylist3"
    values = ["value1", "value2", "value2", "value3", "value2"]

    redis.rpush(mylist, *values)

    result = redis.lpos(mylist, "value2", count=2)
    assert result == [1, 2]


def test_lpos_with_rank(redis: Redis):
    mylist = "mylist4"
    values = ["value1", "value2", "value3", "value2"]

    redis.rpush(mylist, *values)

    result = redis.lpos(mylist, "value2", rank=2)
    assert result == 3


def test_lpos_with_maxlen(redis: Redis):
    mylist = "mylist5"
    values = ["value1", "value2", "value3", "value4"]

    redis.rpush(mylist, *values)

    result = redis.lpos(mylist, "value2", maxlen=3)
    assert result == 1
