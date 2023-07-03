import pytest
from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_mylist():
    mylist = "mylist"

    redis.delete(mylist)


def test_lset_existing_index():
    mylist = "mylist"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)

    redis.lset(mylist, 1, "new_value")

    expected_list = ["value1", "new_value", "value3"]
    assert redis.lrange(mylist, 0, -1) == expected_list


def test_lset_nonexistent_index():
    mylist = "mylist"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)

    with pytest.raises(Exception):
        redis.lset(mylist, 3, "new_value")

    assert redis.lrange(mylist, 0, -1) == values
