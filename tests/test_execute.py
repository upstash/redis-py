from pytest import mark

from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis):
    assert await async_redis.execute(["PING"]) == "PONG"
