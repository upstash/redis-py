import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_mylists(redis: Redis):
    mylists = ["mylist1", "mylist2", "mylist3", "mylist4"]

    for mylist in mylists:
        redis.delete(mylist)


def test_llen_empty_list(redis: Redis):
    mylist = "mylist1"

    result = redis.llen(mylist)
    assert result == 0


def test_llen_non_empty_list(redis: Redis):
    mylist = "mylist2"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)

    result = redis.llen(mylist)
    assert result == 3


def test_llen_nonexistent_list(redis: Redis):
    mylist = "mylist3"

    result = redis.llen(mylist)
    assert result == 0


def test_llen_after_deletion(redis: Redis):
    mylist = "mylist4"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)
    redis.delete(mylist)

    result = redis.llen(mylist)
    assert result == 0
