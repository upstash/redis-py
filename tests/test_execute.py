from pytest import mark

from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis):
    assert await async_redis.execute(["PING"]) == "PONG"
