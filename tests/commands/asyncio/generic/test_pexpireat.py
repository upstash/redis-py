from asyncio import sleep
from time import time

from pytest import mark

from tests.execute_on_http import execute_on_http
from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    # Set the expiry one second from the current time.
    assert (
        await async_redis.pexpireat(
            "string_for_pexpireat", unix_time_milliseconds=int(time() * 1000) + 1000
        )
        is True
    )

    await sleep(2)
    assert await execute_on_http("EXISTS", "string_for_pexpireat") == 0
