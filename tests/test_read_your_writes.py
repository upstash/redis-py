import pytest

from upstash_redis import Redis

def test_set(redis: Redis):
    # key = "mykey"
    # value = "myvalue"
    # ex_seconds = 10
    #
    # result = redis.set(key, value, ex=ex_seconds)
    # print('heyy!!!!')
    # assert result is True
    # assert redis.get(key) == value
    # assert redis.ttl(key) == ex_seconds
    print("LETS GO")
    redis = Redis.from_env(read_your_writes=True)

    initial_token = redis._upstash_sync_token

    redis.set("key", "value")

    updated_token = redis._upstash_sync_token

    redis.get("key")

    final_token = redis._upstash_sync_token
    print(f"'{initial_token}' - '{updated_token}' - '{final_token}'")
    assert initial_token != updated_token