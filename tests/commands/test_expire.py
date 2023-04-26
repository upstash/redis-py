from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_expire() -> None:
    async with redis:
        assert await redis.expire("string_with_no_expiry", 1) is True


@mark.asyncio
async def test_expire_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.expire("non_existing_key", 1) == 0

    redis.format_return = True
