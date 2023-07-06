from pytest import mark

from tests.execute_on_http import execute_on_http
from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    assert (
        await async_redis.copy(source="string", destination="copy_destination") is True
    )

    assert await execute_on_http("GET", "copy_destination") == "test"


@mark.asyncio
async def test_with_replace(async_redis: AsyncRedis) -> None:
    assert (
        await async_redis.copy(
            source="string", destination="string_as_copy_destination", replace=True
        )
        is True
    )

    assert await execute_on_http("GET", "string_as_copy_destination") == "test"


@mark.asyncio
async def test_with_formatting(async_redis: AsyncRedis) -> None:
    await async_redis.copy(source="string", destination="copy_destination_2")
    assert (
        await async_redis.copy(source="string", destination="copy_destination_2")
        is False
    )
