import pytest
import time
from upstash_redis import Redis


@pytest.fixture(autouse=True)
def flush_hash(redis: Redis):
    hash_name = "myhash"
    redis.delete(hash_name)


def test_hpexpireat_sets_expiry_in_milliseconds(redis: Redis):
    hash_name = "myhash"
    field = "field1"
    value = "value1"

    redis.hset(hash_name, field, value)
    future_timestamp_ms = int(time.time() * 1000) + 500
    assert redis.hpexpireat(hash_name, field, future_timestamp_ms) == [1]

    time.sleep(1)
    assert redis.hget(hash_name, field) is None
