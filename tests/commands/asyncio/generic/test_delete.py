from pytest import mark, raises

from tests.execute_on_http import execute_on_http
from upstash_redis.asyncio import Redis


@mark.asyncio
async def test(async_redis: Redis) -> None:
    assert await async_redis.delete("string_for_delete_1", "string_for_delete_2") == 2

    assert (
        await execute_on_http("EXISTS", "string_for_delete_1", "string_for_delete_2")
        == 0
    )


@mark.asyncio
async def test_without_keys(async_redis: Redis) -> None:
    with raises(Exception) as exception:
        await async_redis.delete()

    assert str(exception.value) == "At least one key must be deleted."
