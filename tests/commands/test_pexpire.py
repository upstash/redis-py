from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_pexpire() -> None:
    async with redis:
        assert await redis.pexpire("non_existing_key", milliseconds=1000) is False


@mark.asyncio
async def test_pexpire_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.pexpire("non_existing_key",  milliseconds=1000) == 0

    redis.format_return = True
