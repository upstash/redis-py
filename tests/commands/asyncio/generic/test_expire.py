from asyncio import sleep

from pytest import mark

from tests.execute_on_http import execute_on_http
from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    assert await async_redis.expire("string_for_expire", seconds=1) is True

    # Check if the expiry was correctly set.
    await sleep(1)
    assert await execute_on_http("EXISTS", "string_for_expire") == 0


@mark.asyncio
async def test_without_formatting(async_redis: AsyncRedis) -> None:
    async_redis._format_return = False

    assert await async_redis.expire("non_existing_key", seconds=1) == 0
