from pytest import mark, raises

from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    assert await async_redis.bitcount("string") == 17


@mark.asyncio
async def test_with_interval(async_redis: Redis) -> None:
    assert await async_redis.bitcount("string", start=1, end=2) == 9


@mark.asyncio
async def test_with_invalid_interval(async_redis: Redis) -> None:
    with raises(Exception) as exception:
        await async_redis.bitcount("string", end=2)

    assert str(exception.value) == 'Both "start" and "end" must be specified.'
