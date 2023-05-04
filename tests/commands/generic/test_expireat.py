from pytest import mark
from tests.client import redis
from time import time, sleep
from tests.execute_on_http import execute_on_http


@mark.asyncio
async def test() -> None:
    async with redis:
        # Set the expiry one second from the current time.
        assert await redis.expireat("string_for_expireat", unix_time_seconds=int(time()) + 1) is True

        sleep(2)
        assert await execute_on_http("EXISTS", "string_for_expireat") == 0


@mark.asyncio
async def test_without_formatting() -> None:
    redis.format_return = False

    async with redis:
        assert await redis.expireat("non_existing_key", unix_time_seconds=1704067200) == 0

    redis.format_return = True
