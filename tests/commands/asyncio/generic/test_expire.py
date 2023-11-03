from asyncio import sleep

from pytest import mark

from tests.execute_on_http import execute_on_http
from upstash_redis.asyncio import Redis

import datetime


@mark.asyncio
async def test(async_redis: Redis) -> None:
    assert await async_redis.expire("string_for_expire", seconds=1) is True

    # Check if the expiry was correctly set.
    await sleep(1)
    assert await execute_on_http("EXISTS", "string_for_expire") == 0

@mark.asyncio
async def test_with_datetime(async_redis: Redis) -> None:
    assert await async_redis.expire("string_for_expire_dt", datetime.timedelta(seconds=1)) is True

    # Check if the expiry was correctly set.
    await sleep(1)
    assert await execute_on_http("EXISTS", "string_for_expire_dt") == 0