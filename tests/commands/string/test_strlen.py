import pytest
from tests.sync_client import redis

@pytest.fixture(autouse=True)
def flush_key():
    key = "mykey"
    redis.delete(key)
    yield
    redis.delete(key)

def test_strlen():
    key = "mykey"
    value = "Hello, World!"

    redis.set(key, value)

    result = redis.strlen(key)

    assert result == 13  # Length of the string

