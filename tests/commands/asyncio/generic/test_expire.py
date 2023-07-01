from pytest import mark
from tests.async_client import redis
from time import sleep
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.expire("string_for_expire", seconds=1) is True

        # Check if the expiry was correctly set.
        sleep(1)
        assert await execute_on_http("EXISTS", "string_for_expire") == 0


@mark.asyncio
async def test_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.expire("non_existing_key", seconds=1) == 0

    redis.format_return = True
