from pytest import mark, raises

from tests.async_client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.bitcount("string") == 17


@mark.asyncio
async def test_with_interval() -> None:
    async with redis:
        assert await redis.bitcount("string", start=1, end=2) == 9


@mark.asyncio
async def test_with_invalid_interval() -> None:
    async with redis:
        with raises(Exception) as exception:
            await redis.bitcount("string", end=2)

        assert str(exception.value) == 'Both "start" and "end" must be specified.'
