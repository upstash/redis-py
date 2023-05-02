from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_expireat() -> None:
    async with redis:
        assert await redis.expireat("non_existing_key", unix_time_seconds=1704067200) is False


@mark.asyncio
async def test_expireat_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.expireat("non_existing_key", unix_time_seconds=1704067200) == 0

    redis.format_return = True
