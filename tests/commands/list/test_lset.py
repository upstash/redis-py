import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_mylist(redis: Redis):
    mylist = "mylist"

    redis.delete(mylist)


def test_lset_existing_index(redis: Redis):
    mylist = "mylist"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)

    redis.lset(mylist, 1, "new_value")

    expected_list = ["value1", "new_value", "value3"]
    assert redis.lrange(mylist, 0, -1) == expected_list


def test_lset_nonexistent_index(redis: Redis):
    mylist = "mylist"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)

    with pytest.raises(Exception):
        redis.lset(mylist, 3, "new_value")

    assert redis.lrange(mylist, 0, -1) == values
