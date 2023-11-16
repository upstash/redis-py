from asyncio import sleep
import datetime

from pytest import mark

from tests.execute_on_http import execute_on_http
from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    assert await async_redis.pexpire("string_for_pexpire", milliseconds=1000) is True

    # Check if the expiry was correctly set.
    await sleep(1)
    assert await execute_on_http("EXISTS", "string_for_pexpire") == 0

@mark.asyncio
async def test_with_datetime(async_redis: Redis) -> None:
    assert await async_redis.pexpire("string_for_pexpire_dt", datetime.timedelta(milliseconds=200)) is True

    # Check if the expiry was correctly set.
    await sleep(0.2)
    assert await execute_on_http("EXISTS", "string_for_pexpire_dt") == 0