from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_rename() -> None:
    async with redis:
        assert await redis.renamenx("string", new_name="string") is False


@mark.asyncio
async def test_rename_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.renamenx("string", new_name="string") == 0

    redis.format_return = True
