from time import sleep, time

from pytest import mark

from tests.async_client import redis
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test() -> None:
    async with redis:
        # Set the expiry one second from the current time.
        assert (
            await redis.pexpireat(
                "string_for_pexpireat", unix_time_milliseconds=int(time() * 1000) + 1000
            )
            is True
        )

        sleep(2)
        assert await execute_on_http("EXISTS", "string_for_pexpireat") == 0


@mark.asyncio
async def test_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert (
            await redis.pexpireat("non_existing_key", unix_time_milliseconds=1704067200)
            == 0
        )

    redis.format_return = True
