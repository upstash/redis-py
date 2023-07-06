import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_lists(redis: Redis):
    lists = ["list1", "list2", "list3", "nonexistent_list"]

    for list_name in lists:
        redis.delete(list_name)

    yield

    for list_name in lists:
        redis.delete(list_name)


def test_rpush_existing_list(redis: Redis):
    mylist = "list1"
    values = ["value1", "value2", "value3"]

    result = redis.rpush(mylist, *values)
    assert result == 3

    expected_list = ["value1", "value2", "value3"]
    assert redis.lrange(mylist, 0, -1) == expected_list


def test_rpush_empty_list(redis: Redis):
    mylist = "list2"

    with pytest.raises(Exception):
        redis.rpush(mylist)


def test_rpush_nonexistent_list(redis: Redis):
    mylist = "nonexistent_list"

    result = redis.rpush(mylist, "value1", "value2")
    assert result == 2

    expected_list = ["value1", "value2"]
    assert redis.lrange(mylist, 0, -1) == expected_list
