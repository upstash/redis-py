from pytest import mark

from tests.execute_on_http import execute_on_http
from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    await execute_on_http("EXPIRE", "string_for_ttl", "5")

    # > 1000 would rather indicate milliseconds.
    assert await async_redis.ttl("string_for_ttl") < 1000
