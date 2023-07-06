from pytest import mark, raises

from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    assert await async_redis.bitpos("string", bit=1) == 1


@mark.asyncio
async def test_with_interval(async_redis: AsyncRedis) -> None:
    assert await async_redis.bitpos("string", bit=0, start=1, end=0) == -1

    assert await async_redis.bitpos("string", bit=0, start=1) == 8


@mark.asyncio
async def test_with_start_and_not_end(async_redis: AsyncRedis) -> None:
    with raises(Exception) as exception:
        await async_redis.bitpos("string", bit=0, end=2)

    assert str(exception.value) == '"end" is specified, but "start" is missing.'
