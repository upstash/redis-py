from pytest import mark, raises

from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    assert await async_redis.bitcount("string") == 17


@mark.asyncio
async def test_with_interval(async_redis: AsyncRedis) -> None:
    assert await async_redis.bitcount("string", start=1, end=2) == 9


@mark.asyncio
async def test_with_invalid_interval(async_redis: AsyncRedis) -> None:
    with raises(Exception) as exception:
        await async_redis.bitcount("string", end=2)

    assert str(exception.value) == 'Both "start" and "end" must be specified.'
