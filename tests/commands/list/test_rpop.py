import pytest

from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_mylist(request, redis: Redis):
    mylist = f"mylist_{request.node.name}"
    redis.delete(mylist)
    yield
    redis.delete(mylist)


def test_rpop_existing_element(redis: Redis):
    mylist = "mylist_existing"
    values = ["value1", "value2", "value3"]

    redis.rpush(mylist, *values)

    result = redis.rpop(mylist)
    assert result == "value3"

    expected_list = ["value1", "value2"]
    assert redis.lrange(mylist, 0, -1) == expected_list


def test_rpop_empty_list(redis: Redis):
    mylist = "mylist_empty"

    result = redis.rpop(mylist)
    assert result is None


def test_rpop_nonexistent_list(redis: Redis):
    mylist = "nonexistent_list"

    assert redis.rpop(mylist) is None
