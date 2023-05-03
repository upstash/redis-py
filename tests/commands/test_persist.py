from pytest import mark
from tests.client import redis
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test_persist() -> None:
    async with redis:
        await execute_on_http("EXPIRE", "string_for_persist", "5")

        assert await redis.persist("string_for_persist") is True

        # Check if the expiry was correctly removed.
        assert await execute_on_http("TTL", "string_for_persist") == -1


@mark.asyncio
async def test_persist_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        # "string" doesn't have an expiry time set.
        assert await redis.persist("string") == 0

    redis.format_return = True
