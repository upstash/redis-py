from pytest import mark, raises

from upstash_redis import AsyncRedis


@mark.asyncio
async def test(async_redis: AsyncRedis) -> None:
    assert await async_redis.unlink("string_for_unlink_1", "string_for_unlink_2") == 2


@mark.asyncio
async def test_without_keys(async_redis: AsyncRedis) -> None:
    with raises(Exception) as exception:
        await async_redis.unlink()

    assert str(exception.value) == "At least one key must be specified."
