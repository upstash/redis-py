from typing import Dict, List

from upstash_redis import Redis


def test_time(redis: Redis):
    result = redis.time()
    assert isinstance(result, Dict)
    assert len(result) == 2
    assert isinstance(result["microseconds"], int)
    assert isinstance(result["seconds"], int)


def test_time_without_formatting(redis: Redis):
    redis._format_return = False

    result = redis.time()
    assert isinstance(result, List)
    assert len(result) == 2

    assert result[0].isdigit()
    assert result[1].isdigit()
