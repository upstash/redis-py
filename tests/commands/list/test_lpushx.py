import pytest

from tests.sync_client import redis


@pytest.fixture(autouse=True)
def flush_mylists():
    mylists = ["mylist1", "mylist2"]

    for mylist in mylists:
        redis.delete(mylist)


def test_lpushx_existing_list():
    mylist = "mylist1"
    values = ["value1", "value2"]
    reversed_values = ["value2", "value1"]

    # Create an initial list
    redis.lpush(mylist, *values)

    # Perform LPUSHX on the existing list
    new_value = "new_value"
    result = redis.lpushx(mylist, new_value)
    assert result == 3  # The length of the list after LPUSHX

    # Verify that the new value is added to the list
    expected_list = [new_value] + reversed_values
    assert redis.lrange(mylist, 0, -1) == expected_list


def test_lpushx_nonexistent_list():
    mylist = "mylist2"
    value = "value1"

    # Perform LPUSHX on a nonexistent list
    result = redis.lpushx(mylist, value)
    assert result == 0  # LPUSHX should have no effect

    # Verify that the list remains empty
    assert redis.lrange(mylist, 0, -1) == []
