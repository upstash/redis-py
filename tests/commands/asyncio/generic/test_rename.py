from pytest import mark

from tests.execute_on_http import execute_on_http
from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    assert await async_redis.rename("string_for_rename", newkey="rename") is True

    assert await execute_on_http("GET", "rename") == "test"
