from pytest import mark, raises
from tests.client import redis


@mark.asyncio
async def test_bitpos() -> None:
    async with redis:
        assert await redis.bitpos("string", bit=1) == 1


@mark.asyncio
async def test_bitpos_with_interval() -> None:
    async with redis:
        assert await redis.bitpos("string", bit=0, start=1, end=0) == -1

        assert await redis.bitpos("string", bit=0, start=1) == 8

        with raises(Exception) as exception:
            await redis.bitpos("string", bit=0, end=2)

        assert str(exception.value) == "\"end\" is specified, but \"start\" is missing."
