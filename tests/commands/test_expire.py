from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_expire() -> None:
    async with redis:
        assert await redis.expire("non_existing_key", seconds=1) is False


@mark.asyncio
async def test_expire_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.expire("non_existing_key", seconds=1) == 0

    redis.format_return = True
