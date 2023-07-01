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

def test_rpoplpush_existing_elements():
    source_list = "list1"
    destination_list = "list2"
    values = ["value1", "value2", "value3"]

    redis.rpush(source_list, *values)

    result = redis.rpoplpush(source_list, destination_list)
    assert result == "value3"

    expected_source_list = ["value1", "value2"]
    assert redis.lrange(source_list, 0, -1) == expected_source_list

    expected_destination_list = ["value3"]
    assert redis.lrange(destination_list, 0, -1) == expected_destination_list

def test_rpoplpush_empty_source_list():
    source_list = "list2"
    destination_list = "list3"

    result = redis.rpoplpush(source_list, destination_list)
    assert result is None

    assert redis.llen(source_list) == 0
    assert redis.llen(destination_list) == 0

def test_rpoplpush_nonexistent_lists():
    source_list = "nonexistent_list1"
    destination_list = "nonexistent_list2"

    assert redis.rpoplpush(source_list, destination_list) is None
