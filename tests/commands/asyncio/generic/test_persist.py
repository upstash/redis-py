from pytest import mark

from tests.execute_on_http import execute_on_http
from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    await execute_on_http("EXPIRE", "string_for_persist", "5")

    assert await async_redis.persist("string_for_persist") is True

    # Check if the expiry was correctly removed.
    assert await execute_on_http("TTL", "string_for_persist") == -1
