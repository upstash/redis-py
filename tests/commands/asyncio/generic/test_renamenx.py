from pytest import mark

from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    assert await async_redis.renamenx("string", newkey="string") is False
