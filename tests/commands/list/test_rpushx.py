import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_lists():
    lists = ["list1", "list2", "list3"]

    for list_name in lists:
        redis.delete(list_name)

    yield

    for list_name in lists:
        redis.delete(list_name)


def test_rpushx_existing_list():
    mylist = "list1"
    values = ["value1", "value2", "value3"]

    result = redis.rpush(mylist, *values)
    assert result == 3

    new_values = ["value4", "value5"]

    result = redis.rpushx(mylist, *new_values)
    assert result == 5

    expected_list = ["value1", "value2", "value3", "value4", "value5"]
    assert redis.lrange(mylist, 0, -1) == expected_list


def test_rpushx_empty_list():
    mylist = "list2"
    values = ["value1", "value2", "value3"]

    result = redis.rpushx(mylist, *values)
    assert result == 0

    assert redis.llen(mylist) == 0


def test_rpushx_nonexistent_list():
    mylist = "nonexistent_list"
    values = ["value1", "value2"]

    result = redis.rpushx(mylist, *values)
    assert result == 0

    assert redis.llen(mylist) == 0
