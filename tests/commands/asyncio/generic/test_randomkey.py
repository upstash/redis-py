from pytest import mark

from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    assert isinstance(await async_redis.randomkey(), str)
