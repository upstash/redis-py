from typing import Dict, List

from tests.sync_client import redis


def test_time():
    result = redis.time()
    assert isinstance(result, Dict)
    assert len(result) == 2
    assert isinstance(result["microseconds"], int)
    assert isinstance(result["seconds"], int)


def test_time_without_formatting():
    redis._format_return = False

    result = redis.time()
    assert isinstance(result, List)
    assert len(result) == 2

    assert result[0].isdigit()
    assert result[1].isdigit()

    redis._format_return = True
