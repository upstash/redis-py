from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_expire() -> None:
    async with redis:
        assert await redis.expire("string_for_expire", 1704067200) is True


@mark.asyncio
async def test_expire_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.expire("non_existing_key", 1704067200) == 0

    redis.format_return = True
