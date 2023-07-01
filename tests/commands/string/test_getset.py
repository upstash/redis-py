import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)

def test_getset():
    key = "mykey"
    initial_value = "Hello"
    new_value = "Goodbye"

    redis.set(key, initial_value)

    result = redis.getset(key, new_value)
    assert result == initial_value

    updated_value = redis.get(key)
    assert updated_value == new_value
