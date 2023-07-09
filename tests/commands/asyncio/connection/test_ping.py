from pytest import mark

from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    assert await async_redis.ping() == "PONG"


@mark.asyncio
async def test_with_message(async_redis: Redis) -> None:
    assert await async_redis.ping(message="Upstash is nice!") == "Upstash is nice!"
