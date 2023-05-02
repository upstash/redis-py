from pytest import mark
from tests.client import redis


@mark.asyncio
async def test_persist() -> None:
    async with redis:
        # "string" doesn't have an expiry time set.
        assert await redis.persist("string") is False


@mark.asyncio
async def test_persist_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.persist("string") == 0

    redis.format_return = True
