from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_pexpireat() -> None:
    async with redis:
        assert await redis.pexpireat("non_existing_key", unix_time_milliseconds=1704067200) is False


@mark.asyncio
async def test_pexpire_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.pexpireat("non_existing_key", unix_time_milliseconds=1704067200) == 0

    redis.format_return = True
