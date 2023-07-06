from pytest import mark

from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    assert await async_redis.echo(message="Upstash is nice!") == "Upstash is nice!"
