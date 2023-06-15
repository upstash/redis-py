from pytest import mark
from tests.client import redis
from time import sleep
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test() -> None:
    async with redis:
        assert await redis.pexpire("string_for_pexpire", milliseconds=1000) is True

        # Check if the expiry was correctly set.
        sleep(1)
        assert await execute_on_http("EXISTS", "string_for_pexpire") == 0


@mark.asyncio
async def test_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.pexpire("non_existing_key", milliseconds=1000) == 0

    redis.format_return = True
