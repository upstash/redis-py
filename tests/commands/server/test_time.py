from upstash_redis import Redis


def test_time(redis: Redis):
    result = redis.time()
    assert isinstance(result, tuple)
    assert len(result) == 2
    assert isinstance(result[0], int)
    assert isinstance(result[1], int)
