from pytest import fixture

from upstash_redis import Redis


@fixture(autouse=True)
def setup_teardown(redis: Redis):
    redis.rpush("mylist", "value1", "value2", "value3")
    yield
    redis.delete("mylist")


def test_lindex_existing_index(redis: Redis):
    result = redis.lindex("mylist", 0)
    assert result == "value1"


def test_lindex_invalid_index(redis: Redis):
    result = redis.lindex("mylist", 10)
    assert result is None


def test_lindex_empty_list(redis: Redis):
    redis.delete("mylist")

    result = redis.lindex("mylist", 0)
    assert result is None
