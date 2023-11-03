from asyncio import sleep
import datetime
from time import time

from pytest import mark

from tests.execute_on_http import execute_on_http
from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    # Set the expiry one second from the current time.
    assert (
        await async_redis.expireat(
            "string_for_expireat", unix_time_seconds=int(time()) + 1
        )
        is True
    )

    await sleep(2)
    assert await execute_on_http("EXISTS", "string_for_expireat") == 0

@mark.asyncio
async def test_with_datetime(async_redis: Redis) -> None:
    assert await async_redis.expireat("string_for_expireat_dt", datetime.datetime.now() + datetime.timedelta(seconds=1)) is True

    # Check if the expiry was correctly set.
    await sleep(1)
    assert await execute_on_http("EXISTS", "string_for_expireat_dt") == 0