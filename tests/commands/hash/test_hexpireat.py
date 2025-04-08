import pytest
import time
from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)


def test_hexpireat_sets_expiry(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    future_timestamp = int(time.time()) + 2
    assert redis.hexpireat(hash_name, field, future_timestamp)[0] == 1

    time.sleep(3)
    assert redis.hexists(hash_name, field) == 0
