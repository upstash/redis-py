from pytest import fixture, mark
from tests.sync_client import redis


@fixture(scope="module")
def setup_teardown():
    redis.rpush("mylist", "value1", "value2", "value3")
    yield
    redis.delete("mylist")


@mark.usefixtures("setup_teardown")
def test_lindex_existing_index():
    result = redis.lindex("mylist", 0)
    assert result == "value1"


@mark.usefixtures("setup_teardown")
def test_lindex_invalid_index():
    result = redis.lindex("mylist", 10)
    assert result is None


@mark.usefixtures("setup_teardown")
def test_lindex_empty_list():
    redis.delete("mylist")

    result = redis.lindex("mylist", 0)
    assert result is None
