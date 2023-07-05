from pytest import mark

from tests.async_client import redis


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.renamenx("string", newkey="string") is False


@mark.asyncio
async def test_without_formatting() -> None:
    redis._format_return = False

    async with redis:
        assert await redis.renamenx("string", newkey="string") == 0

    redis._format_return = True
